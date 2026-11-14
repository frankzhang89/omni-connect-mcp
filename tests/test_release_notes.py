"""Offline tests for release note selection and GitHub CLI publication behavior."""

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import call, patch


ROOT = Path(__file__).resolve().parents[1]


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'scripts' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


extractor = load_script('extract_release_notes')
publisher = load_script('publish_release')


class ExtractNotesTest(unittest.TestCase):
    def test_only_selected_version_with_markdown_preserved(self):
        document = ('# Release Notes\n\n## Unreleased\n- Future change\n\n'
                    '## v0.2.0\n\n### Added\n- **Kafka** and [RabbitMQ](https://example.com).\n\n'
                    '## v0.1.1\n- Nacos\n\n## v0.1.0\n- Initial release\n')
        self.assertEqual(extractor.extract_notes(document, 'v0.2.0'),
                         '### Added\n- **Kafka** and [RabbitMQ](https://example.com).\n')
        self.assertEqual(extractor.extract_notes(document, 'v0.1.0'), '- Initial release\n')

    def test_fenced_examples_do_not_start_sections(self):
        for fence in ('```', '~~~~'):
            with self.subTest(fence=fence):
                body = f'- Example:\n\n{fence}markdown\n## v0.2.0\n## v9.9.9\n{fence}\n'
                self.assertEqual(extractor.extract_notes('## v0.2.0\n' + body + '\n## v0.1.1\n- Older\n', 'v0.2.0'), body)

    def test_crlf_and_closed_heading(self):
        self.assertEqual(extractor.extract_notes('## v0.2.0 ##\r\n\r\n- Change\r\n', 'v0.2.0'), '- Change\n')

    def test_missing_duplicate_and_empty_versions_fail(self):
        documents = ['## v0.1.1\n- Old\n',
                     '## v0.2.0\n- First\n## v0.2.0\n- Second\n',
                     '## v0.2.0\n \n## v0.1.1\n- Old\n',
                     '## v0.2.0\n<!-- Pending -->\n']
        for document in documents:
            with self.subTest(document=document), self.assertRaises(ValueError):
                extractor.extract_notes(document, 'v0.2.0')

    def test_cli_checks_version_before_writing_output(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pom, notes, output = root / 'pom.xml', root / 'notes.md', root / 'generated/body.md'
            pom.write_text('<project xmlns="http://maven.apache.org/POM/4.0.0"><version>0.2.0</version></project>', encoding='utf-8')
            notes.write_text('\ufeff## v0.2.0\n- Change\n', encoding='utf-8')
            command = [sys.executable, str(ROOT / 'scripts/extract_release_notes.py'),
                       'v0.2.0', '--notes-file', str(notes), '--pom-file', str(pom), '--output', str(output)]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(output.read_text(encoding='utf-8'), '- Change\n')
            output.unlink()
            command[2] = 'v0.1.1'
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('must match project version', result.stderr)
            self.assertFalse(output.exists())


class PublishReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='release notes with spaces ')
        self.addCleanup(self.temp.cleanup)
        self.dist = Path(self.temp.name)
        self.notes = self.dist / 'body.md'
        self.notes.write_text('- RabbitMQ and Kafka\n', encoding='utf-8')
        self.archives = [self.dist / f'omni-connect-mcp-v0.2.0-{suffix}'
                         for suffix in ('windows.zip', 'linux.tar.gz')]
        for archive in self.archives:
            archive.write_bytes(b'fixture')

    @patch.object(publisher.subprocess, 'run')
    def test_new_release_attaches_both_archives_and_prepared_notes(self, run):
        run.side_effect = [subprocess.CompletedProcess([], 1), subprocess.CompletedProcess([], 0)]
        publisher.publish_release('v0.2.0', self.notes, self.dist)
        self.assertEqual(run.call_args_list, [
            call(['gh', 'release', 'view', 'v0.2.0'], capture_output=True, text=True),
            call(['gh', 'release', 'create', 'v0.2.0', *map(str, self.archives),
                  '--verify-tag', '--title', 'v0.2.0', '--notes-file', str(self.notes)], check=True)])

    @patch.object(publisher.subprocess, 'run')
    def test_existing_release_refreshes_assets_and_body_for_only_that_tag(self, run):
        run.return_value = subprocess.CompletedProcess([], 0)
        publisher.publish_release('v0.2.0', self.notes, self.dist)
        self.assertEqual(run.call_args_list, [
            call(['gh', 'release', 'view', 'v0.2.0'], capture_output=True, text=True),
            call(['gh', 'release', 'upload', 'v0.2.0', *map(str, self.archives), '--clobber'], check=True),
            call(['gh', 'release', 'edit', 'v0.2.0', '--notes-file', str(self.notes)], check=True)])

    @patch.object(publisher.subprocess, 'run')
    def test_empty_notes_or_missing_assets_never_call_github(self, run):
        self.notes.write_text(' \n', encoding='utf-8')
        with self.assertRaises(ValueError):
            publisher.publish_release('v0.2.0', self.notes, self.dist)
        self.notes.write_text('- Change\n', encoding='utf-8')
        self.archives[1].unlink()
        with self.assertRaises(FileNotFoundError):
            publisher.publish_release('v0.2.0', self.notes, self.dist)
        run.assert_not_called()

    @patch.object(publisher.subprocess, 'run')
    def test_upload_failure_stops_before_updating_notes(self, run):
        run.side_effect = [subprocess.CompletedProcess([], 0), subprocess.CalledProcessError(1, 'gh')]
        with self.assertRaises(subprocess.CalledProcessError):
            publisher.publish_release('v0.2.0', self.notes, self.dist)
        self.assertEqual(run.call_count, 2)


if __name__ == '__main__':
    unittest.main()

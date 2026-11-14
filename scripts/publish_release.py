"""Create or update one GitHub Release using prepared notes and platform archives."""

import argparse
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def publish_release(version, notes, dist):
    if not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)*', version):
        raise ValueError('Expected a version tag such as v0.2.0.')
    if not notes.read_text(encoding='utf-8').strip():
        raise ValueError('Prepared release notes must not be empty.')
    archives = [dist / f'omni-connect-mcp-{version}-{suffix}'
                for suffix in ('windows.zip', 'linux.tar.gz')]
    for archive in archives:
        if not archive.is_file():
            raise FileNotFoundError(archive)

    existing = subprocess.run(['gh', 'release', 'view', version],
                              capture_output=True, text=True)
    if existing.returncode == 0:
        subprocess.run(['gh', 'release', 'upload', version,
                        *map(str, archives), '--clobber'], check=True)
        subprocess.run(['gh', 'release', 'edit', version,
                        '--notes-file', str(notes)], check=True)
    else:
        subprocess.run(['gh', 'release', 'create', version, *map(str, archives),
                        '--verify-tag', '--title', version, '--notes-file', str(notes)], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('version', help='Release tag, for example v0.2.0')
    parser.add_argument('--notes-file', type=Path, required=True)
    parser.add_argument('--dist-dir', type=Path, default=ROOT / 'target/dist')
    args = parser.parse_args()
    try:
        publish_release(args.version, args.notes_file, args.dist_dir)
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        parser.exit(1, f'Release publication failed: {error}\n')


if __name__ == '__main__':
    main()

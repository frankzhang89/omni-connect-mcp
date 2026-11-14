"""Validate a release tag against pom.xml and extract its Markdown release notes."""

import argparse
from pathlib import Path
import re
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]


def validate_version(tag, pom):
    project = ET.parse(pom).getroot()
    namespace = {'m': 'http://maven.apache.org/POM/4.0.0'}
    version = project.findtext('m:version', namespaces=namespace)
    if not version or tag != 'v' + version.strip():
        raise ValueError(f'Release tag {tag!r} must match project version v{(version or "<missing>").strip()}.')


def extract_notes(markdown, tag):
    lines = markdown.splitlines(keepends=True)
    headings = []
    fence = None
    for index, line in enumerate(lines):
        marker = re.match(r'^ {0,3}(`{3,}|~{3,})(.*)$', line.rstrip('\r\n'))
        if fence:
            if (marker and marker[1][0] == fence[0] and len(marker[1]) >= fence[1]
                    and not marker[2].strip()):
                fence = None
            continue
        if marker:
            fence = (marker[1][0], len(marker[1]))
            continue
        heading = re.match(r'^ {0,3}##[ \t]+(.+?)\s*$', line)
        if heading:
            title = re.sub(r'[ \t]+#+[ \t]*$', '', heading[1]).strip()
            headings.append((index, title))

    matches = [i for i, (_, title) in enumerate(headings) if title == tag]
    if len(matches) != 1:
        raise ValueError(f'Expected exactly one "## {tag}" section; found {len(matches)}.')
    section = matches[0]
    start = headings[section][0] + 1
    end = headings[section + 1][0] if section + 1 < len(headings) else len(lines)
    body = ''.join(lines[start:end]).strip()
    if not re.sub(r'<!--.*?-->', '', body, flags=re.DOTALL).strip():
        raise ValueError(f'Release notes for {tag} are empty.')
    return body + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('version', help='Release tag, for example v0.2.0')
    parser.add_argument('--notes-file', type=Path, default=ROOT / 'RELEASE_NOTES.md')
    parser.add_argument('--pom-file', type=Path, default=ROOT / 'pom.xml')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        validate_version(args.version, args.pom_file)
        notes = extract_notes(args.notes_file.read_text(encoding='utf-8-sig'), args.version)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(notes, encoding='utf-8')
    except (ValueError, OSError, ET.ParseError) as error:
        parser.error(str(error))
    print(f'Prepared release notes for {args.version}: {args.output}')


if __name__ == '__main__':
    main()

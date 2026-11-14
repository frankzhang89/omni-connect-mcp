"""Create Windows and Linux distribution archives from the exported runnable JAR."""

import argparse
from pathlib import Path
import re
import tarfile
import zipfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('version', help='Version tag, for example v0.2.0')
    args = parser.parse_args()
    if not re.fullmatch(r'v[A-Za-z0-9][A-Za-z0-9._-]*', args.version):
        parser.error('Version must start with v and contain only letters, digits, dots, underscores, or hyphens.')

    root = Path(__file__).resolve().parents[1]
    jar = root / 'target/release/omni-connect-mcp/omni-connect-mcp.jar'
    common = [(jar, jar.name), (root / 'application.yml', 'application.yml')]
    windows = common + [(root / name, name) for name in (
        'omni-connect.cmd', 'omni-connect.ps1',
        'omni-connect-stdio.cmd', 'omni-connect-stdio.ps1')]
    linux = common + [(root / name, name) for name in (
        'omni-connect.sh', 'omni-connect-stdio')]
    for source, _ in windows + linux:
        if not source.is_file():
            raise FileNotFoundError(source)

    output = root / 'target/dist'
    output.mkdir(parents=True, exist_ok=True)
    prefix = 'omni-connect-mcp/'
    windows_archive = output / f'omni-connect-mcp-{args.version}-windows.zip'
    linux_archive = output / f'omni-connect-mcp-{args.version}-linux.tar.gz'
    with zipfile.ZipFile(windows_archive, 'w', zipfile.ZIP_DEFLATED) as archive:
        for source, name in windows:
            archive.write(source, prefix + name)
    with tarfile.open(linux_archive, 'w:gz') as archive:
        for source, name in linux:
            info = archive.gettarinfo(str(source), prefix + name)
            info.mode = 0o755 if name in ('omni-connect.sh', 'omni-connect-stdio') else 0o644
            info.uid = info.gid = 0
            info.uname = info.gname = ''
            with source.open('rb') as stream:
                archive.addfile(info, stream)

    # Verify exact layouts and contents before making the archives available for upload.
    with zipfile.ZipFile(windows_archive) as archive:
        assert set(archive.namelist()) == {prefix + name for _, name in windows}
        for source, name in windows:
            assert archive.read(prefix + name) == source.read_bytes()
    with tarfile.open(linux_archive) as archive:
        assert set(archive.getnames()) == {prefix + name for _, name in linux}
        for source, name in linux:
            assert archive.extractfile(prefix + name).read() == source.read_bytes()
        for name in ('omni-connect.sh', 'omni-connect-stdio'):
            assert archive.getmember(prefix + name).mode == 0o755
    print(windows_archive)
    print(linux_archive)


if __name__ == '__main__':
    main()

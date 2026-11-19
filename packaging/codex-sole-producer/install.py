"""Offline image assembly from closed, hash-pinned public release inputs."""
from contextlib import ExitStack, contextmanager
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import sys
import tarfile
import tempfile

CHUNK = 65536
TAR_LIMIT = 512*1024*1024
OUTPUT_LIMIT = 768*1024*1024


def relative(name):
    path = PurePosixPath(name)
    if (not name or path.is_absolute() or str(path) != name or '..' in path.parts
            or '\\' in name or len(name.encode()) > 256): raise ValueError('path refused')
    return path


def checked_file(directory, row, limit):
    relative(row['name'])
    if '/' in row['name']: raise ValueError('input name refused')
    fd = os.open(row['name'], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    stream = os.fdopen(fd, 'rb')
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or not 0 < info.st_size <= limit
                or 'size' in row and info.st_size != row['size']): raise ValueError('input refused')
        if hashlib.file_digest(stream, 'sha256').hexdigest() != row['sha256']: raise ValueError('digest refused')
        stream.seek(0)
        return stream
    except BaseException:
        stream.close(); raise


class MetadataReader:
    def __init__(self, stream): self.stream = stream
    def read(self, size):
        if not 0 <= size <= CHUNK: raise ValueError('tar metadata refused')
        return self.stream.read(size)
    def __getattr__(self, name): return getattr(self.stream, name)


def unpack(stream, row, output, total):
    for name in row['directories']: relative(name)
    for name, target in row['files'].items():
        relative(name)
        if target is not None: relative(target)
    expected = set(row['files']) | set(row['directories'])
    if len(expected) != len(row['files'])+len(row['directories']) or len(expected) > 32:
        raise ValueError('inventory refused')
    # Cap decompression and extended tar metadata before tarfile parses it.
    with tempfile.TemporaryFile() as plain, gzip.GzipFile(fileobj=stream) as compressed:
        length = 0
        while data := compressed.read(CHUNK):
            length += len(data)
            if length > TAR_LIMIT: raise ValueError('tar bound refused')
            plain.write(data)
        plain.seek(0)
        with tarfile.open(fileobj=MetadataReader(plain), mode='r:') as archive:
            members = {}
            for member in archive:
                name = member.name.rstrip('/') if member.isdir() else member.name
                if (name not in expected or name in members or member.pax_headers or member.issparse()
                        or member.mode & 0o7000 or member.type not in (tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.DIRTYPE)
                        or member.isdir() != (name in row['directories'])):
                    raise ValueError('member refused')
                if not 0 <= member.size <= TAR_LIMIT or member.isdir() and member.size:
                    raise ValueError('member bound refused')
                if not 0 <= member.offset_data <= length-member.size:
                    raise ValueError('payload range refused')
                total += member.size
                if total > OUTPUT_LIMIT: raise ValueError('output bound refused')
                members[name] = member
            if set(members) != expected: raise ValueError('inventory mismatch')
            for name, target in row['files'].items():
                if target is None: continue
                destination = output/target
                destination.parent.mkdir(parents=True, exist_ok=True)
                executable = not target.startswith('licenses/')
                # Metadata is fully validated. Payload buffering must not pass
                # through MetadataReader (ExFileObject sizes vary by Python).
                member = members[name]
                plain.seek(member.offset_data)
                remaining = member.size
                with destination.open('xb') as sink:
                    if executable:
                        if remaining < 20: raise ValueError('ELF size refused')
                        header = plain.read(20)
                        if (header[:6] != b'\x7fELF\x02\x01' or header[18:20] != b'\x3e\x00'):
                            raise ValueError('ELF target refused')
                        sink.write(header); remaining -= len(header)
                    while remaining:
                        data = plain.read(min(CHUNK, remaining))
                        if not data: raise ValueError('payload truncated')
                        sink.write(data); remaining -= len(data)
                destination.chmod(0o555 if executable else 0o444)
    return total


@contextmanager
def verified_inputs(inputs, specification):
    if type(specification.get('version')) is not int or specification['version'] != 1:
        raise ValueError('input specification refused')
    if inputs.resolve() != inputs or not inputs.is_dir(): raise ValueError('directory refused')
    rows = specification['archives']+specification['licenses']
    if len({row['name'] for row in rows}) != len(rows): raise ValueError('duplicate input')
    with ExitStack() as stack:
        directory = os.open(inputs, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        stack.callback(os.close, directory)
        if set(os.listdir(directory)) != {row['name'] for row in rows}: raise ValueError('input inventory refused')
        streams = [stack.enter_context(checked_file(directory, row,
            128*1024*1024 if row in specification['archives'] else CHUNK)) for row in rows]
        yield streams


def install(inputs, destination, specification):
    if (destination.exists() or destination.is_symlink()
            or destination.parent.resolve() != destination.parent): raise ValueError('directory refused')
    with verified_inputs(inputs, specification) as streams:
        with tempfile.TemporaryDirectory(prefix='codex-assembly-', dir=destination.parent) as temporary:
            tree = Path(temporary)/'tree'; tree.mkdir(mode=0o755)
            total = 0
            for row, stream in zip(specification['archives'], streams): total = unpack(stream, row, tree, total)
            for row, stream in zip(specification['licenses'], streams[len(specification['archives']):]):
                relative(row['target'])
                target = tree/row['target']; target.parent.mkdir(parents=True, exist_ok=True)
                with target.open('xb') as sink: sink.write(stream.read(CHUNK))
                target.chmod(0o444)
            metadata = {'layoutVersion': 1, 'version': '0.169.0', 'target': 'x86_64-unknown-linux-musl',
                'variant': 'codex', 'entrypoint': 'bin/codex', 'execServer': 'bin/codex-exec-server',
                'resourcesDir': 'codex-resources', 'pathDir': 'codex-path'}
            (tree/'codex-package.json').write_text(json.dumps(metadata, sort_keys=True)+'\n')
            files = {}
            for path in tree.rglob('*'):
                if path.is_file():
                    with path.open('rb') as source:
                        files[str(path.relative_to(tree))] = hashlib.file_digest(source, 'sha256').hexdigest()
            (tree/'native-manifest.json').write_text(json.dumps({'version': 1, 'sha256': files,
                'source': specification['source'], 'base_manifest': specification['base_manifest']}, sort_keys=True)+'\n')
            for name in ('codex-package.json', 'native-manifest.json'): (tree/name).chmod(0o444)
            if destination.exists() or destination.is_symlink(): raise ValueError('output exists')
            tree.rename(destination)


if __name__ == '__main__':
    try:
        if len(sys.argv) != 3: raise ValueError('arguments refused')
        specification = json.loads(Path(__file__).with_name('inputs.json').read_bytes())
        if sys.argv[1] == '--check-only':
            with verified_inputs(Path(sys.argv[2]), specification): pass
        else:
            install(Path(sys.argv[1]), Path(sys.argv[2]), specification)
    except Exception:
        raise SystemExit('codex overlay inputs refused') from None

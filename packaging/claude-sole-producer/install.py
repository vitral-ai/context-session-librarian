"""Fetch and assemble the pinned Claude Code linux-x64 npm package (closed inventory)."""
import base64
import hashlib
import json
import os
from pathlib import Path
import sys
import tarfile
import tempfile
import urllib.request

CHUNK = 65536


def fetch(url, size, spool):
    with urllib.request.urlopen(url, timeout=120) as response:
        total = 0
        while data := response.read(CHUNK):
            total += len(data)
            if total > size: raise ValueError('input too large')
            spool.write(data)
    if total != size: raise ValueError('input size refused')


def check(spool, row):
    spool.seek(0)
    sha256, sha512 = hashlib.sha256(), hashlib.sha512()
    while data := spool.read(CHUNK):
        sha256.update(data); sha512.update(data)
    integrity = 'sha512-' + base64.b64encode(sha512.digest()).decode()
    if sha256.hexdigest() != row['sha256'] or integrity != row['integrity']: raise ValueError('digest refused')
    spool.seek(0)


def unpack(spool, row, output):
    seen = {}
    with tarfile.open(fileobj=spool, mode='r:gz') as archive:
        for member in archive:
            if member.name in seen or member.name not in row['members'] or not member.isreg():
                raise ValueError('inventory refused')
            if member.size != row['members'][member.name] or member.sparse: raise ValueError('member refused')
            seen[member.name] = member.size
            target = row['install'].get(member.name)
            if target is None: continue
            path = output/target
            path.parent.mkdir(parents=True, exist_ok=True)
            source = archive.extractfile(member)
            with open(path, 'xb') as sink:
                remaining = member.size
                while remaining:
                    data = source.read(min(CHUNK, remaining))
                    if not data: raise ValueError('member truncated')
                    sink.write(data); remaining -= len(data)
    if seen.keys() != row['members'].keys(): raise ValueError('inventory incomplete')
    binary = output/'bin/claude'
    with open(binary, 'rb') as stream:
        header = stream.read(20)
    # ELF64, little endian, x86-64.
    if header[:5] != b'\x7fELF\x02' or header[5] != 1 or header[18:20] != b'\x3e\x00':
        raise ValueError('binary refused')
    os.chmod(binary, 0o555)
    os.chmod(output/'licenses/LICENSE.md', 0o444)
    files = {}
    for target in sorted(row['install'].values()):
        with open(output/target, 'rb') as stream:
            files[target] = hashlib.file_digest(stream, 'sha256').hexdigest()
    manifest = {'input': {k: row[k] for k in ('url', 'size', 'sha256', 'integrity')}, 'files': files}
    (output/'native-manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True)+'\n')
    os.chmod(output/'native-manifest.json', 0o444)


def main(inputs, output):
    row = json.loads(Path(inputs).read_text())
    output = Path(output)
    output.mkdir(mode=0o755)
    with tempfile.TemporaryFile() as spool:
        fetch(row['url'], row['size'], spool)
        check(spool, row)
        unpack(spool, row, output)


if __name__ == '__main__': main(*sys.argv[1:])

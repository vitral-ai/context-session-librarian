"""Operator build entry: exact local base descriptor, public inputs, no push."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def blob(layout, digest):
    if not re.fullmatch('sha256:[0-9a-f]{64}', digest): raise ValueError('descriptor refused')
    fd = os.open(layout, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for name in ('blobs', 'sha256'):
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd); fd = child
        source = os.open(digest[7:], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        with os.fdopen(source, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= 1048576:
                raise ValueError('OCI JSON refused')
            value = stream.read(1048577)
    finally: os.close(fd)
    if hashlib.sha256(value).hexdigest() != digest[7:]: raise ValueError('base digest refused')
    return json.loads(value)


def command(layout, inputs, output, source):
    specification = json.loads((HERE/'inputs.json').read_bytes())
    for directory in (layout, inputs):
        if (not directory.is_absolute() or directory.resolve() != directory or not directory.is_dir()
                or not re.fullmatch(r'/[A-Za-z0-9/._-]+', str(directory))): raise ValueError('build path refused')
    if not re.fullmatch('[0-9a-f]{40}', source): raise ValueError('source pin refused')
    if (not output.is_absolute() or not re.fullmatch(r'/[A-Za-z0-9/._-]+', str(output))
            or output.parent.resolve() != output.parent or output.suffix != '.tar'
            or output.exists() or output.is_symlink()): raise ValueError('output refused')
    if any(path.is_relative_to(ROOT) for path in (layout, inputs, output)):
        raise ValueError('build data must stay outside checkout')
    metadata = output.with_suffix('.metadata.json')
    if metadata.exists() or metadata.is_symlink(): raise ValueError('metadata output exists')
    info = output.parent.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077: raise ValueError('private evidence refused')
    descriptor = specification['base_manifest']
    manifest = blob(layout, descriptor)
    if manifest.get('schemaVersion') != 2 or manifest.get('mediaType') not in (
            'application/vnd.oci.image.manifest.v1+json', 'application/vnd.docker.distribution.manifest.v2+json'):
        raise ValueError('base manifest refused')
    config = blob(layout, manifest['config']['digest'])
    if (config.get('os'), config.get('architecture')) != ('linux', 'amd64'):
        raise ValueError('base platform refused')
    spec = importlib.util.spec_from_file_location('native_installer', HERE/'install.py')
    installer = importlib.util.module_from_spec(spec); spec.loader.exec_module(installer)
    with installer.verified_inputs(inputs, specification): pass
    return ['docker', 'buildx', 'build', '--platform', 'linux/amd64', '--progress', 'plain',
        '--build-context', 'reviewed_collector=oci-layout://'+str(layout)+'@'+descriptor,
        '--build-context', 'native_inputs='+str(inputs), '--file', str(HERE/'Containerfile'),
        '--label', 'org.opencontainers.image.revision='+source,
        '--metadata-file', str(metadata), '--output', 'type=oci,dest='+str(output), str(ROOT)]


def main():
    try:
        if len(sys.argv) != 4: raise ValueError('arguments refused')
        source = subprocess.run(['git', 'rev-parse', '--verify', 'HEAD'], cwd=ROOT,
            check=True, capture_output=True, text=True, timeout=5).stdout.strip()
        status = subprocess.run(['git', 'status', '--porcelain', '--untracked-files=all'], cwd=ROOT,
            check=True, capture_output=True, timeout=5)
        if status.stdout: raise ValueError('source is not clean')
        argv = command(*(Path(value) for value in sys.argv[1:]), source)
        return subprocess.run(argv, cwd=ROOT, timeout=1200).returncode
    except Exception:
        print('codex overlay build refused', file=sys.stderr)
        return 2


if __name__ == '__main__': raise SystemExit(main())

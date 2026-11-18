"""Build an explicit, data-free runtime tree; never copy a checkout recursively."""
import hashlib
import json
from pathlib import Path
import shutil
import sys


def assemble(source, destination):
    specification = source/'packaging/collector-image/runtime.json'
    value = json.loads(specification.read_bytes())
    paths = value['modules'] + value['data']
    if (value['version'] != 1 or len(paths) != len(set(paths))
            or any(Path(name).is_absolute() or '..' in Path(name).parts for name in paths)):
        raise ValueError('invalid image inventory')
    destination.mkdir(mode=0o755)
    files = {}
    for name in paths:
        origin = source/name
        if origin.is_symlink() or not origin.is_file():
            raise ValueError('invalid image source')
        target = destination/name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(origin, target)
        target.chmod(0o444)
        files[name] = hashlib.sha256(target.read_bytes()).hexdigest()
    (destination/'runtime-manifest.json').write_text(json.dumps(
        {'version': 1, 'sha256': files}, sort_keys=True, separators=(',', ':'))+'\n')
    (destination/'runtime-manifest.json').chmod(0o444)


if __name__ == '__main__':
    assemble(Path(sys.argv[1]), Path(sys.argv[2]))

"""Verify the exact OCI image from an operator-provided, reviewed digest pin."""

import hashlib
import json
import re
import sys
import tarfile


def verify(path, digest):
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
        raise ValueError('invalid digest pin')
    with tarfile.open(path, 'r:') as archive:
        def open_member(name):
            matches = [member for member in archive.getmembers() if member.name == name]
            if len(matches) != 1 or not matches[0].isfile():
                raise ValueError('missing, duplicate or non-file OCI member')
            return archive.extractfile(matches[0])

        def read_json(stream):
            with stream:
                data = stream.read(1024 * 1024 + 1)
            if len(data) > 1024 * 1024:
                raise ValueError('OCI JSON exceeds fixture limit')
            return json.loads(data)

        def verify_blob(descriptor):
            algorithm, separator, expected = descriptor['digest'].partition(':')
            if (algorithm != 'sha256' or not separator
                    or not re.fullmatch(r'[0-9a-f]{64}', expected)):
                raise ValueError('invalid OCI blob digest')
            name = 'blobs/sha256/' + expected
            with open_member(name) as stream:
                if stream.seek(0, 2) != descriptor['size']:
                    raise ValueError('OCI blob size mismatch')
                stream.seek(0)
                if hashlib.file_digest(stream, 'sha256').hexdigest() != expected:
                    raise ValueError('OCI blob digest mismatch')
            return name

        index = read_json(open_member('index.json'))
        matches = [descriptor for descriptor in index['manifests']
                   if descriptor['digest'] == digest]
        if len(matches) != 1:
            raise ValueError('reviewed image missing or ambiguous')
        manifest = read_json(open_member(verify_blob(matches[0])))
        verify_blob(manifest['config'])
        for layer in manifest['layers']:
            verify_blob(layer)


if __name__ == '__main__':
    try:
        verify(*sys.argv[1:])
    except (OSError, ValueError, TypeError, KeyError, tarfile.TarError):
        raise SystemExit('OCI fixture archive failed verification')
    print('OCI fixture manifest, config and layers match the reviewed digest')

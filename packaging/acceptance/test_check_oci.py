import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

from check_oci import verify


class OciArchive(unittest.TestCase):
    def test_reviewed_manifest_and_layers(self):
        files = {}

        def add_blob(data):
            digest = hashlib.sha256(data).hexdigest()
            files['blobs/sha256/' + digest] = data
            return {'digest': 'sha256:' + digest, 'size': len(data)}

        layer = add_blob(b'synthetic layer')
        config = add_blob(b'{}')
        manifest = add_blob(json.dumps({'config': config, 'layers': [layer]}).encode())
        files['index.json'] = json.dumps({'manifests': [manifest]}).encode()

        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'fixture.tar'

            def write_archive():
                with tarfile.open(path, 'w') as archive:
                    for name, value in files.items():
                        member = tarfile.TarInfo(name)
                        member.size = len(value)
                        archive.addfile(member, io.BytesIO(value))

            write_archive()
            verify(path, manifest['digest'])
            with self.assertRaises(ValueError):
                verify(path, config['digest'])
            files['blobs/sha256/' + layer['digest'].split(':')[1]] = b'x' * layer['size']
            write_archive()
            with self.assertRaises(ValueError):
                verify(path, manifest['digest'])

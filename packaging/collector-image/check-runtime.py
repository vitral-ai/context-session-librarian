"""No-network import/helper/profile smoke for the exact assembled image."""
import base64
import hashlib
import importlib
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import tempfile
import time

from csl_workload_identity import WorkloadKey, proof_message
from csl_workload_config import PROVIDER_PROFILES
from csl_workload_runner import boot_clock


def check():
    root = Path(__file__).resolve().parent
    manifest = json.loads((root/'runtime-manifest.json').read_bytes())
    for name, digest in manifest['sha256'].items():
        assert hashlib.sha256((root/name).read_bytes()).hexdigest() == digest
        if name.endswith('.py'): importlib.import_module(Path(name).stem)
    assert len(PROVIDER_PROFILES) == 3
    assert hasattr(time, 'CLOCK_BOOTTIME') and boot_clock() > 0
    assert os.getuid() == 10001
    assert shutil.which('openssl') == '/usr/bin/openssl'
    version = subprocess.check_output(['/usr/bin/openssl', 'version'], timeout=5, text=True)
    assert version.split()[:2] == ['OpenSSL', '3.5.9']
    assert ssl.create_default_context().get_ca_certs()
    with tempfile.TemporaryDirectory(prefix='collector-image-') as temporary:
        key = WorkloadKey(Path(temporary)/'identity')
        _, thumb = key.ensure()
        issuer, credential, nonce, digest = 'https://identity.example.test/native-auth/v1', 'synthetic', b'n'*32, b'm'*32
        signature = key.proof(issuer, credential, nonce, digest, expected_thumbprint=thumb)
        message = Path(temporary)/'message'
        message.write_bytes(proof_message(issuer, credential, nonce, thumb, digest))
        signed = Path(temporary)/'signature'
        signed.write_bytes(base64.urlsafe_b64decode(signature+'='*(-len(signature) % 4)))
        public = Path(temporary)/'public.pem'
        public.write_bytes(key._run('pkey', '-in', str(key.path), '-pubout'))
        subprocess.run(['/usr/bin/openssl', 'pkeyutl', '-verify', '-rawin', '-pubin',
            '-inkey', str(public), '-in', str(message), '-sigfile', str(signed)],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
    print(json.dumps({'imports': True, 'profiles': 3, 'helper_sign_verify': True, 'network': False,
                      'helper': version.split()[1], 'tls': ssl.OPENSSL_VERSION}))


if __name__ == '__main__': check()

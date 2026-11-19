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
import sys
import tempfile
import time

from csl_workload_identity import WorkloadKey, proof_message
from csl_workload_config import PROVIDER_PROFILES
from csl_workload_runner import boot_clock
from csl_text_workload import WorkloadTextProvider
from csl_workload_stream import Bounds, Redactor, encoded


def check_codec():
    """Exercise the packaged codec, not an installed checkout or real transcript."""
    sid = '11111111-2222-4333-8444-555555555555'
    hidden, canary = 'OMITTED_IMAGE_METADATA', 'sk-proj-'+'Q'*32
    usage = dict.fromkeys(('input_tokens', 'cached_input_tokens', 'output_tokens',
                          'reasoning_output_tokens', 'total_tokens'), 0)
    settings = {'model': hidden, 'model_provider_id': hidden, 'cwd': '/'+hidden,
        'approval_policy': 'on-request', 'approvals_reviewer': 'user',
        'requested_approvals_reviewer': 'user', 'disabled_plugin_ids': [],
        'runtime_workspace_roots': [], 'permission_profile': {'type': 'managed',
            'network': 'restricted', 'file_system': {'type': 'restricted', 'entries': []}},
        'collaboration_mode': {'mode': 'default', 'settings': {
            'model': hidden, 'reasoning_effort': None, 'developer_instructions': None}}}
    records = [
        {'type': 'session_meta', 'payload': {'id': sid, 'cli_version': '0.169.0',
                                            'history_mode': 'paginated'}},
        {'type': 'response_item', 'payload': {'type': 'message', 'role': 'user',
            'content': [{'type': 'input_text', 'text': 'IMAGE_CODEC_MARKER '+canary}]}},
        {'type': 'response_item', 'payload': {'type': 'additional_tools', 'role': 'developer',
            'tools': [{'name': hidden, 'description': canary}]}},
        {'type': 'world_state', 'payload': {'full': True, 'state': {'instructions': hidden+canary}}},
        {'type': 'token_usage_record', 'payload': {'thread_id': sid, 'session_id': sid,
            'turn_id': hidden, 'root_turn_id': hidden, 'response_id': hidden,
            'usage': usage, 'turn_token_usage': usage, 'thread_token_usage': usage}},
        {'type': 'event_msg', 'payload': {'type': 'thread_settings_applied',
                                        'thread_id': sid, 'thread_settings': settings}},
    ]
    for ordinal, record in enumerate(records): record['ordinal'] = ordinal
    provider = WorkloadTextProvider.__new__(WorkloadTextProvider)
    provider.provider, provider.profile = 'codex', PROVIDER_PROFILES['codex']
    provider.bounds = Bounds()
    provider.redactor = Redactor(provider.bounds)
    metadata, content, view = provider.project(records)
    expected = ['Native tool declarations omitted; workload text is view-only.',
                'Native world state omitted; workload text is view-only.',
                'Native token usage omitted; workload text is view-only.',
                'Native thread settings omitted; workload text is view-only.']
    assert [row['text'] for row in view if row['kind'] == 'notice'] == expected
    assert any(row['kind'] == 'text' and 'IMAGE_CODEC_MARKER' in row['text'] for row in view)
    assert metadata['record_count'] == len(records)
    for artifact in (content, encoded(metadata), encoded(view)):
        assert hidden.encode() not in artifact and canary.encode() not in artifact
    for row in view:
        record = json.loads(content[row['raw_offset']['offset']:].splitlines()[0])
        assert record['workload_profile_v1'] == PROVIDER_PROFILES['codex']
    return True


def check():
    root = Path(__file__).resolve().parent
    manifest = json.loads((root/'runtime-manifest.json').read_bytes())
    for name, digest in manifest['sha256'].items():
        assert hashlib.sha256((root/name).read_bytes()).hexdigest() == digest
        if name.endswith('.py'): importlib.import_module(Path(name).stem)
    assert len(PROVIDER_PROFILES) == 3
    assert check_codec()
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
                      'helper': version.split()[1], 'tls': ssl.OPENSSL_VERSION,
                      'codex_metadata_omission': True, 'python': sys.version.split()[0],
                      'machine': os.uname().machine,
                      'runtime_manifest_sha256': hashlib.sha256((root/'runtime-manifest.json').read_bytes()).hexdigest()}))


if __name__ == '__main__': check()

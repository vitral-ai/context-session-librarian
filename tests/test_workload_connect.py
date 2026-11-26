import json
from pathlib import Path
import shutil
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import UUID

from csl_sync_state import SyncError
from csl_workload_connect import ConnectHTTP, NAMESPACE, credential_from_file, enroll_runtime, proofs_for, signed_message
from csl_workload_identity import WorkloadKey, b64, unb64

ORGANIZATION = 'a' * 24
ENVIRONMENT = str(UUID(int=1))
ENROLLMENT = str(UUID(int=2))
CHALLENGE = str(UUID(int=3))
NONCE = b64(b'c' * 32)
HOST_ID = 'host-0123456789abcdef'
CREDENTIAL = 'synthetic-oidc-token'
LOCATION = {'label': 'testing', 'provider': 'kubernetes', 'cluster': 'dev',
            'namespace': 'test', 'workload': 'fixture', 'replica': 'pod-1'}


class ConnectHTTPTests(unittest.TestCase):
    def test_only_the_two_fixed_connect_paths_use_the_https_origin(self):
        transport = ConnectHTTP('https://identity.example.test')
        with patch('csl_workload_connect.HTTP.request', return_value={}) as request:
            transport.post('challenges', {'request_id': ENROLLMENT})
            request.assert_called_once_with(
                transport, 'https://identity.example.test/vitral-connect/v1/challenges', 'POST',
                data=json.dumps({'request_id': ENROLLMENT}, separators=(',', ':')).encode(),
                headers={'Content-Type': 'application/json'},
            )
            with self.assertRaisesRegex(SyncError, 'workload_connect_endpoint_refused'):
                transport.post('https://other.example.test', {})
            with self.assertRaisesRegex(SyncError, 'workload_connect_endpoint_refused'):
                ConnectHTTP('https://identity.example.test:8443').post('exchanges', {})
            self.assertEqual(request.call_count, 1)


@unittest.skipUnless(shutil.which('ssh-keygen') and shutil.which('openssl'), 'native signing helpers required')
class RuntimeEnrollmentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.host_state = root / 'host'
        self.host_state.mkdir(mode=0o700)
        self.collector_state = root / 'collector'
        self.collector_state.mkdir(mode=0o700)
        self.credential = root / 'credential'
        self.credential.write_text(CREDENTIAL + '\n')
        self.credential.chmod(0o440)
        for key in ('tunnel_key', 'ssh_host_ed25519_key'):
            subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(self.host_state / key)],
                           check=True, timeout=10)
        self.host = {
            'username': 'agent', 'hostname': 'pod.test',
            'tunnel_public_key': (self.host_state / 'tunnel_key.pub').read_text().strip(),
            'host_public_key': (self.host_state / 'ssh_host_ed25519_key.pub').read_text().strip(),
        }
        self.key = WorkloadKey(self.collector_state / 'identity')
        public, _ = self.key.ensure()
        self.collector = {'public_jwk': public, 'workload': LOCATION}
        self.config = SimpleNamespace(state_root=self.collector_state, origin='https://identity.example.test',
                                      location=LOCATION, ca_file=None)

    def test_both_private_keys_sign_one_challenge(self):
        request = {'organization_id': ORGANIZATION, 'method': 'oidc', 'request_id': ENROLLMENT,
                   'challenge_id': CHALLENGE, 'credential': CREDENTIAL, 'host': self.host,
                   'collector': self.collector}
        proof = proofs_for(request, NONCE, self.host_state, self.key)
        message, _ = signed_message(request, NONCE)
        modified, _ = signed_message({**request, 'credential': 'different'}, NONCE)
        signature = self.host_state / 'proof.sig'
        signature.write_text(proof['host'])
        allowed = self.host_state / 'allowed'
        allowed.write_text('fixture ' + self.host['tunnel_public_key'] + '\n')
        argv = ['ssh-keygen', '-Y', 'verify', '-f', str(allowed), '-I', 'fixture',
                '-n', NAMESPACE, '-s', str(signature)]
        self.assertEqual(subprocess.run(argv, input=message, capture_output=True, timeout=10).returncode, 0)
        self.assertNotEqual(subprocess.run(argv, input=modified, capture_output=True, timeout=10).returncode, 0)

        raw, sig = self.host_state / 'request', self.host_state / 'collector.sig'
        raw.write_bytes(message)
        sig.write_bytes(unb64(proof['collector'], 64))
        key_file = self.host_state / 'collector.der'
        key_file.write_bytes(bytes.fromhex('302a300506032b6570032100') + unb64(self.collector['public_jwk']['x'], 32))
        subprocess.run(['openssl', 'pkeyutl', '-verify', '-rawin', '-pubin', '-keyform', 'DER',
                        '-inkey', str(key_file), '-sigfile', str(sig), '-in', str(raw)], check=True,
                       capture_output=True, timeout=10)

    def test_retries_same_request_and_recovers_after_host_redemption(self):
        events, requests = [], []
        fail_collector = True

        def post(resource, payload):
            requests.append((resource, payload))
            if resource == 'challenges':
                self.assertNotIn('credential', payload)
                return {'challenge_id': CHALLENGE, 'nonce': NONCE}
            self.assertEqual(payload['credential'], CREDENTIAL)
            return {'enrollment_id': ENROLLMENT, 'environment_id': ENVIRONMENT, 'generation': 2,
                    'host': {'assignment_assertion': 'signed.host.fixture'},
                    'collector': {'assignment_assertion': 'signed.collector.fixture'}}

        def host(argv, env):
            self.assertEqual(env['VITRAL_HOST_TERMINAL_MODE'], 'embedded')
            if argv[1] == 'assignment-identity':
                return json.dumps(self.host).encode()
            events.append('host')
            assertion = Path(argv[-1])
            self.assertEqual(assertion.stat().st_mode & 0o777, 0o600)
            self.assertEqual(assertion.read_text(), 'signed.host.fixture')
            (self.host_state / 'api.HOST_ID').write_text(HOST_ID)
            (self.host_state / 'api.ENVIRONMENT_ID').write_text(ENVIRONMENT)
            return b''

        def collector(assertion):
            nonlocal fail_collector
            events.append('collector')
            self.assertEqual(assertion, 'signed.collector.fixture')
            if fail_collector:
                fail_collector = False
                raise SyncError('fixture_lost_response')
            identity['state'] = 'ready'

        identity = {'public_jwk': self.collector['public_jwk'], 'state': 'signed_out'}
        tokens = SimpleNamespace(key=self.key, connect_identity=lambda: dict(identity), enroll_connect=collector)
        args = dict(organization=ORGANIZATION, method='oidc', credential_path=self.credential, host_state=self.host_state,
                    host_binary='vitral-host', host_kind='container', http=SimpleNamespace(post=post))
        with patch('csl_workload_connect.run_host', host):
            with self.assertRaisesRegex(SyncError, 'fixture_lost_response'):
                enroll_runtime(self.config, tokens, **args)
            result = enroll_runtime(self.config, tokens, **args)
            self.credential.unlink()
            self.assertEqual(enroll_runtime(self.config, tokens, **args), result)
        self.assertEqual(result, {'host_id': HOST_ID, 'environment_id': ENVIRONMENT, 'enrollment_id': ENROLLMENT})
        self.assertEqual(events, ['host', 'collector', 'host', 'collector'])
        self.assertEqual(len(requests), 4)
        self.assertEqual(len({payload['request_id'] for _, payload in requests}), 1)
        state = (self.host_state / 'connect.json').read_text()
        self.assertNotIn(CREDENTIAL, state)
        self.assertNotIn('signed.', state)
        self.assertEqual((self.collector_state / 'host-id').read_text(), HOST_ID + '\n')

    def test_credential_projection_and_public_mode_refusal(self):
        projection = self.credential.with_name('projected')
        projection.symlink_to(self.credential)
        self.assertEqual(credential_from_file(projection), CREDENTIAL)
        self.credential.chmod(0o644)
        with self.assertRaisesRegex(SyncError, 'workload_connect_credential_refused'):
            credential_from_file(projection)

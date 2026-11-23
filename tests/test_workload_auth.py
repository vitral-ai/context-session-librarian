"""Person approval and loss/restart state machines with synthetic credentials."""
import copy
import hashlib
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch
import urllib.parse
import urllib.error

from csl_sync_state import SyncError, exclusive
from csl_workload_auth import DEVICE_GRANT, MANAGED_AUDIENCE, WorkloadTokens
from csl_sync_http import HTTP
from csl_workload_http import Origin, WorkloadHTTP
from csl_workload_identity import AUDIENCE, CLIENT_ID, SCOPES, WorkloadKey, b64, canonical, metadata, public_key
from test_workload_http import discovery

LOCATION = {'provider': 'kubernetes', 'label': 'fixture collector', 'cluster': 'local-kind',
            'namespace': 'test', 'workload': 'fixture-job', 'replica': 'fixture-1'}
ACCOUNT = '1234567890abcdef12345678'
DEVICE = 'ses_fixture-device'
ORIGIN = 'https://identity.example.test'


class Clock:
    def __init__(self): self.now, self.wall, self.boot = 1000, 1800000000, 'fixture-boot'
    def advance(self, seconds): self.now += seconds; self.wall += seconds


class Key:
    def __init__(self): self.raw = b'k'*32; self.proofs = []
    def public(self): return public_key(self.raw)
    ensure = public
    def proof(self, *args, **kwargs):
        self.proofs.append((args, kwargs))
        return b64(b's'*64)


class Issuer(WorkloadHTTP):
    def __init__(self, clock, key, origin=ORIGIN):
        self.origin, self.clock, self.key = Origin(origin), clock, key
        self.calls, self.effects = [], []
        self.pending, self.slow, self.lost = False, False, False
        self.mutate, self.hook, self.refusal, self.challenge_refusal = None, None, None, None
        self.mutate_response = None
        self.serial = 1

    def credentials(self):
        thumb = b64(self.key.public()[1])
        claims = {'iss': self.origin.issuer, 'aud': [AUDIENCE], 'client_id': CLIENT_ID,
            'sub': ACCOUNT, 'sid': DEVICE, 'device_id': DEVICE, 'cnf': {'jkt': thumb},
            'scope': SCOPES, 'token_use': 'native_access', 'exp': self.clock.wall+300}
        if self.mutate: self.mutate(claims)
        token = '.'.join((b64(canonical({'alg': 'RS256'})), b64(canonical(claims)), b64(b'test-signature')))
        response = {'token_type': 'Bearer', 'access_token': token, 'refresh_token': b64(bytes([self.serial])*32),
            'expires_in': 300, 'scope': SCOPES, 'session_id': DEVICE, 'device_id': DEVICE,
            'account': {'id': ACCOUNT, 'masked_email': 'not-retained@example.test'}}
        if self.mutate_response: self.mutate_response(response)
        return response

    def request(self, url, method='GET', *, data=None, headers=None, retry=False):
        self.origin.endpoint(url)
        if retry: raise AssertionError('credential effects must never have generic retries')
        form = dict(urllib.parse.parse_qsl(data.decode())) if data else {}
        path = urllib.parse.urlsplit(url).path
        self.calls.append((path, form))
        if path == '/native-auth/v1/.well-known/oauth-authorization-server': return discovery(self.origin.value)
        if self.hook: self.hook(path, form)
        if path.endswith('/device_authorization'):
            self.effects.append(('authorization', copy.deepcopy(form)))
            self.assert_location(form)
            return {'device_code': b64(b'd'*32), 'user_code': 'ABCD-EFGH', 'interval': 5, 'expires_in': 600,
                'verification_uri': self.origin.issuer+'/device', 'exchange_nonce': b64(b'n'*32),
                'workload_metadata': LOCATION, 'workload_metadata_sha256': metadata(LOCATION)[1]}
        if path.endswith('/host_refresh_challenge'):
            if self.challenge_refusal: raise SyncError(self.challenge_refusal, status=400)
            return {'refresh_nonce': b64(b'r'*32), 'expires_in': 60}
        if path.endswith('/managed_workload_token'):
            self.effects.append(('managed', copy.deepcopy(form)))
            return self.credentials()
        if not path.endswith('/token'): raise AssertionError('unexpected fixture route')
        if self.refusal: raise SyncError(self.refusal, status=400)
        if form['grant_type'] == DEVICE_GRANT:
            if self.slow: self.slow = False; raise SyncError('slow_down', status=400)
            if self.pending: self.pending = False; raise SyncError('authorization_pending', status=400)
            self.effects.append(('device', copy.deepcopy(form)))
        else:
            self.serial = 2
            self.effects.append(('refresh', copy.deepcopy(form)))
        response = self.credentials()
        if self.lost:
            self.lost = False
            raise SyncError('registry_unavailable', retryable=True)
        return response

    def assert_location(self, form):
        if (form.get('client_id') != CLIENT_ID or form.get('scope') != SCOPES
                or json.loads(form['workload']) != LOCATION or form.get('key_thumbprint') != b64(self.key.public()[1])):
            raise AssertionError('wrong approval binding')


class WorkloadAuthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='workload-auth-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.clock, self.key = Clock(), Key()
        self.issuer = Issuer(self.clock, self.key)

    def client(self, **options):
        return WorkloadTokens(self.root/'auth', ORIGIN, LOCATION, http=self.issuer, key=self.key,
            clock=lambda: self.clock.now, wall_clock=lambda: self.clock.wall,
            boot=lambda: self.clock.boot, **options)

    def journal(self): return json.loads((self.root/'auth/workload-auth.json').read_text())

    def enroll(self):
        client = self.client(); client.begin(); self.clock.advance(5)
        self.assertEqual(client.poll(), {'state': 'ready'})
        return client

    def assignment(self, **overrides):
        claims = {
            'iss': 'vitral-pool-controller', 'aud': MANAGED_AUDIENCE, 'sub': ACCOUNT,
            'owner_issuer': ORIGIN + '/native-auth/v1', 'workload_jkt': b64(self.key.public()[1]),
            'environment_id': '87654321-1234-4321-8432-123456789abc', 'generation': 4,
        } | overrides
        return '.'.join((b64(canonical({'alg': 'RS256'})), b64(canonical(claims)), b64(b'test-signature')))

    def test_managed_enrollment_uses_bound_proof_and_existing_refresh(self):
        client = self.client()
        self.assertEqual(client.managed_identity(), {'workload_jkt': b64(self.key.public()[1]), 'state': 'signed_out'})
        assertion = self.assignment()
        client.enroll_managed(assertion)
        self.client().enroll_managed(assertion)
        self.assertEqual([kind for kind, _ in self.issuer.effects], ['managed'])
        form = self.issuer.effects[0][1]
        self.assertEqual(form['assignment_assertion'], assertion)
        self.assertEqual(json.loads(form['workload']), LOCATION)
        nonce = hashlib.sha256(b'vitral-managed-collector-v1\0' + assertion.encode()).digest()
        self.assertEqual(self.key.proofs[0][0][2], nonce)
        self.assertNotIn(assertion, json.dumps(self.journal()))
        self.client().access_token(force_refresh=True)
        self.assertEqual(self.journal()['managed_assignment']['generation'], 4)
        self.assertEqual([kind for kind, _ in self.issuer.effects], ['managed', 'refresh'])

    def test_managed_enrollment_rejects_wrong_key_owner_and_other_generation(self):
        client = self.client()
        with self.assertRaisesRegex(SyncError, 'workload_assignment_refused'):
            client.enroll_managed(self.assignment(workload_jkt=b64(b'x' * 32)))
        self.assertEqual(self.issuer.calls, [])
        with self.assertRaisesRegex(SyncError, 'workload_identity_binding_changed'):
            client.enroll_managed(self.assignment(sub='0' * 24))
        self.assertEqual(client.status()['state'], 'signed_out')
        client.enroll_managed(self.assignment())
        with self.assertRaisesRegex(SyncError, 'workload_identity_binding_changed'):
            self.client().enroll_managed(self.assignment(generation=5))
        self.assertEqual(len(self.issuer.calls), 2)

    def test_discovery_uses_existing_oauth_route_without_oidc_fallback(self):
        self.client().begin()
        self.assertEqual([path for path, _ in self.issuer.calls], [
            '/native-auth/v1/.well-known/oauth-authorization-server',
            '/native-auth/v1/device_authorization'])

    def test_discovery_failure_does_not_retry_another_path_or_initialize_key(self):
        def unavailable(url, *args, **kwargs):
            self.assertEqual(url, ORIGIN+'/native-auth/v1/.well-known/oauth-authorization-server')
            raise SyncError('registry_unavailable', status=404)
        with patch.object(self.issuer, 'request', side_effect=unavailable) as request, \
                patch.object(self.key, 'ensure') as initialize:
            with self.assertRaises(SyncError): self.client().begin()
        request.assert_called_once()
        initialize.assert_not_called()
        self.assertFalse((self.root/'auth/workload-auth.json').exists())

    def test_approval_waits_poll_interval_and_resumes_own_persisted_binding(self):
        client = self.client()
        approval = client.begin()
        self.assertEqual(set(approval), {'user_code', 'verification_uri', 'expires_in', 'interval'})
        self.assertEqual(client.poll(), {'state': 'pending'})
        self.assertEqual(len(self.issuer.calls), 2)
        self.clock.advance(5); self.issuer.pending = True
        self.assertEqual(self.client().poll(), {'state': 'pending'})
        self.clock.advance(5); self.issuer.slow = True
        self.assertEqual(client.poll(), {'state': 'pending'})
        self.assertEqual(self.journal()['interval'], 10)
        self.clock.advance(9); before = len(self.issuer.calls); client.poll()
        self.assertEqual(before, len(self.issuer.calls))
        self.clock.advance(1); self.assertEqual(self.client().poll(), {'state': 'ready'})
        self.assertEqual(client.status(), {'state': 'ready', 'account_id': ACCOUNT, 'device_id': DEVICE})
        self.assertEqual(self.journal()['phase'], 'ready')
        self.assertNotIn('masked_email', json.dumps(self.journal()))
        before = len(self.issuer.calls)
        self.assertTrue(self.client().access_token())
        self.assertEqual(before, len(self.issuer.calls))
        self.assertEqual((self.root/'auth').stat().st_mode & 0o777, 0o700)
        self.assertEqual((self.root/'auth/workload-auth.json').stat().st_mode & 0o777, 0o600)

    def test_every_credential_effect_has_a_durable_pre_send_record(self):
        phases = []
        def observe(path, form):
            phases.append((path, self.journal()['phase']))
        self.issuer.hook = observe
        client = self.enroll(); client.access_token(force_refresh=True)
        self.assertEqual([phase for path, phase in phases if path.endswith('/token')],
                         ['device_inflight', 'refresh_inflight'])
        self.assertEqual(phases[0][1], 'authorization_inflight')
        self.assertTrue(self.key.proofs[-1][1]['refresh'])

    def test_lost_device_ack_is_never_redeemed_or_reenrolled_again(self):
        client = self.client(); client.begin(); self.clock.advance(5); self.issuer.lost = True
        with self.assertRaisesRegex(SyncError, '^workload_enrollment_outcome_unknown$'): client.poll()
        before = len(self.issuer.calls)
        for action in (self.client().poll, self.client().begin, self.client().access_token):
            with self.assertRaises(SyncError): action()
        self.assertEqual(len(self.issuer.calls), before)
        self.assertEqual(self.journal()['phase'], 'device_inflight')

    def test_lost_refresh_ack_replays_exact_proof_once_after_journal_reopen(self):
        client = self.enroll(); self.issuer.lost = True
        with self.assertRaisesRegex(SyncError, '^refresh_outcome_unknown$'):
            client.access_token(force_refresh=True)
        fields = copy.deepcopy(self.journal()['fields'])
        self.clock.advance(2)
        self.assertTrue(self.client().access_token())
        refreshes = [form for kind, form in self.issuer.effects if kind == 'refresh']
        self.assertEqual(refreshes, [fields, fields])
        self.assertEqual(self.journal()['refresh_token'], b64(b'\x02'*32))
        self.assertEqual(self.journal()['phase'], 'ready')
        self.assertNotIn('fields', self.journal())

    def test_failed_replay_is_not_retried_and_expired_or_rebooted_grace_refuses(self):
        for mode in ('twice', 'deadline', 'reboot', 'backwards'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(dir=self.root) as root:
                self.root = Path(root); self.clock = Clock(); self.issuer.clock = self.clock
                client = self.enroll(); self.issuer.lost = True
                with self.assertRaises(SyncError): client.access_token(force_refresh=True)
                if mode == 'deadline': self.clock.advance(30)
                elif mode == 'reboot': self.clock.boot = 'another-boot'
                elif mode == 'backwards': self.clock.now -= 1
                else:
                    self.issuer.lost = True
                    with self.assertRaises(SyncError): self.client().access_token()
                before = len(self.issuer.calls)
                with self.assertRaisesRegex(SyncError, '^refresh_outcome_unknown$'): self.client().access_token()
                self.assertEqual(before, len(self.issuer.calls))
                self.assertEqual(self.journal()['phase'], 'refresh_inflight')
            self.root = Path(self.tmp.name)

    def test_identity_key_scope_audience_and_metadata_changes_never_rebind(self):
        client = self.enroll()
        for field, changed in (('sub', 'f'*24), ('device_id', 'ses_other'), ('sid', 'ses_other'),
                               ('aud', ['other']), ('scope', 'terminal_host offline_access'),
                               ('cnf', {'jkt': b64(b'x'*32)}), ('iss', 'https://elsewhere.test/native-auth/v1'),
                               ('client_id', 'vitral-host-agent')):
            with self.subTest(field=field):
                saved = (self.root/'auth/workload-auth.json').read_bytes()
                self.issuer.mutate = lambda claims: claims.update({field: changed})
                with self.assertRaisesRegex(SyncError, '^refresh_outcome_unknown$'):
                    client.access_token(force_refresh=True)
                self.assertEqual(self.journal()['account_id'], ACCOUNT)
                self.assertEqual(self.journal()['device_id'], DEVICE)
                (self.root/'auth/workload-auth.json').write_bytes(saved)
        self.issuer.mutate = None
        self.key.raw = b'x'*32
        before = len(self.issuer.calls)
        with self.assertRaisesRegex(SyncError, '^workload_key_binding_changed$'): client.access_token()
        self.assertEqual(len(self.issuer.calls), before)
        changed = {**LOCATION, 'replica': 'different-replica'}
        other = WorkloadTokens(self.root/'auth', ORIGIN, changed, http=self.issuer, key=self.key)
        with self.assertRaisesRegex(SyncError, '^workload_auth_journal_refused$'): other.status()

    def test_suspend_deadline_stop_and_boot_changes_are_checked_at_effect_boundary(self):
        client = self.client(); client.begin(timeout=10); self.clock.advance(11)
        before = len(self.issuer.calls)
        with self.assertRaisesRegex(SyncError, '^workload_approval_expired$'): client.poll()
        self.assertEqual(before, len(self.issuer.calls))
        self.assertEqual(self.journal()['phase'], 'expired')

    def test_stop_or_expiry_during_signing_prevents_token_request(self):
        for stopped in (False, True):
            with self.subTest(stopped=stopped), tempfile.TemporaryDirectory(dir=self.root) as root:
                self.root = Path(root); self.clock = Clock(); self.issuer.clock = self.clock
                stop = [False]; client = self.client(stopped=lambda: stop[0]); client.begin(timeout=10)
                self.clock.advance(5)
                def proof(*args, **kwargs):
                    if stopped: stop[0] = True
                    else: self.clock.advance(6)
                    return b64(b's'*64)
                before = len(self.issuer.calls)
                with patch.object(self.key, 'proof', side_effect=proof), self.assertRaises(SyncError): client.poll()
                self.assertEqual(before, len(self.issuer.calls))
            self.root = Path(self.tmp.name)

    def test_acceptance_rechecks_deadline_and_never_extends_expiry_during_key_validation(self):
        client = self.enroll()
        original = self.key.public
        def public():
            self.clock.advance(1)
            return original()
        with patch.object(self.key, 'public', side_effect=public):
            client.access_token(force_refresh=True)
        self.assertLess(self.journal()['expires_boot'], self.clock.now+300)
        self.clock.advance(301)
        before = len(self.issuer.calls)
        client.access_token()
        self.assertGreater(len(self.issuer.calls), before)
        with tempfile.TemporaryDirectory(dir=self.root) as root:
            self.root = Path(root); client = self.client(); client.begin(timeout=10); self.clock.advance(5)
            def delay_after_response(path, form):
                if path.endswith('/token'):
                    calls = [0]
                    def late_public():
                        calls[0] += 1
                        if calls[0] == 2: self.clock.advance(6)
                        return original()
                    self.key.public = late_public
            self.issuer.hook = delay_after_response
            with self.assertRaisesRegex(SyncError, '^workload_auth_deadline$'): client.poll()
            self.assertEqual(self.journal()['phase'], 'device_inflight')

    def test_suspended_or_rebooted_cached_token_forces_fresh_proof(self):
        client = self.enroll(); self.clock.now += 400
        before = len(self.issuer.calls); client.access_token()
        self.assertGreater(len(self.issuer.calls), before)
        self.clock.boot = 'new-boot'
        before = len(self.issuer.calls); self.client().access_token()
        self.assertGreater(len(self.issuer.calls), before)

    def test_response_identity_and_token_contract_errors_keep_uncertainty(self):
        client = self.enroll()
        for field, value in (('account', {'id': 'f'*24}), ('device_id', 'ses_other'),
                             ('session_id', 'ses_other'), ('scope', 'session_sync'),
                             ('expires_in', True), ('expires_in', float('inf')),
                             ('refresh_token', 'not-a-token'), ('access_token', 'not-a-jwt')):
            saved = (self.root/'auth/workload-auth.json').read_bytes()
            with self.subTest(field=field, value_type=type(value).__name__):
                self.issuer.mutate_response = lambda response: response.update({field: value})
                with self.assertRaisesRegex(SyncError, '^refresh_outcome_unknown$'):
                    client.access_token(force_refresh=True)
                self.assertEqual(self.journal()['phase'], 'refresh_inflight')
                self.assertEqual(self.journal()['account_id'], ACCOUNT)
                (self.root/'auth/workload-auth.json').write_bytes(saved)

    def test_cancellation_and_persistence_failure_cannot_hide_remote_uncertainty(self):
        client = self.enroll()
        def cancel(path, form):
            if path.endswith('/token'): raise KeyboardInterrupt()
        self.issuer.hook = cancel
        with self.assertRaises(KeyboardInterrupt): client.access_token(force_refresh=True)
        self.assertEqual(self.journal()['phase'], 'refresh_inflight')
        self.issuer.hook = None
        before = len(self.issuer.calls)
        with patch.object(client, '_write', side_effect=OSError('fixture disk unavailable')):
            with self.assertRaises(OSError): client.access_token()
        self.assertEqual(before, len(self.issuer.calls))
        self.assertEqual(self.journal()['phase'], 'refresh_inflight')

    def test_successor_persistence_failure_reopens_same_exact_refresh_plan(self):
        client = self.enroll()
        write = client._write
        def fail_successor(value):
            if value['phase'] == 'ready': raise OSError('fixture write failed')
            write(value)
        with patch.object(client, '_write', side_effect=fail_successor), self.assertRaises(OSError):
            client.access_token(force_refresh=True)
        fields = self.journal()['fields']
        self.assertTrue(self.client().access_token())
        self.assertEqual([form for kind, form in self.issuer.effects if kind == 'refresh'], [fields, fields])

    def test_concurrent_begin_has_one_approval_request_and_no_retry(self):
        client = self.client()
        def begin(_):
            try: client.begin()
            except SyncError as error:
                self.assertIn(error.code, ('collector_busy', 'workload_auth_already_started'))
                return False
            return True
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(sum(pool.map(begin, range(8))), 1)
        self.assertEqual([kind for kind, form in self.issuer.effects], ['authorization'])

    def test_storage_errors_are_scrubbed_and_no_authorization_is_sent(self):
        client = self.client()
        with patch('csl_workload_auth.private_json', side_effect=OSError('synthetic-secret-canary')):
            with self.assertRaisesRegex(SyncError, '^workload_auth_storage_unavailable$') as failure:
                client.begin()
        self.assertIsNone(failure.exception.__context__)
        self.assertEqual(self.issuer.effects, [])

    def test_mount_root_is_untouched_and_existing_permissive_auth_child_refuses(self):
        self.root.chmod(0o770)
        self.client().begin()
        self.assertEqual(self.root.stat().st_mode & 0o777, 0o770)
        (self.root/'auth').chmod(0o750)
        with self.assertRaisesRegex(SyncError, '^workload_auth_storage_refused$'): self.client()
        self.assertEqual((self.root/'auth').stat().st_mode & 0o777, 0o750)

    def test_revocation_at_challenge_and_token_is_terminal_not_expiry(self):
        for challenge in (False, True):
            with self.subTest(challenge=challenge), tempfile.TemporaryDirectory(dir=self.root) as root:
                self.root = Path(root); self.clock = Clock(); self.issuer.clock = self.clock
                client = self.enroll()
                if challenge: self.issuer.challenge_refusal = 'invalid_grant'
                else: self.issuer.refusal = 'session_revoked'
                with self.assertRaisesRegex(SyncError, '^credential_revoked$'): client.access_token(force_refresh=True)
                self.assertEqual(self.journal()['phase'], 'revoked')
                self.assertNotIn('refresh_token', self.journal())
                before = len(self.issuer.calls)
                with self.assertRaisesRegex(SyncError, '^credential_revoked$'): self.client().access_token()
                self.assertEqual(before, len(self.issuer.calls))
                self.issuer.refusal = self.issuer.challenge_refusal = None
            self.root = Path(self.tmp.name)

    def test_fifo_hardlink_corrupt_state_and_concurrent_writer_refuse(self):
        client = self.enroll(); path = self.root/'auth/workload-auth.json'; original = path.read_bytes()
        path.unlink(); os.mkfifo(path, 0o600)
        with self.assertRaisesRegex(SyncError, '^workload_auth_storage_refused$'): client.status()
        path.unlink(); path.write_bytes(original); path.chmod(0o600)
        linked = self.root/'copy'; os.link(path, linked)
        with self.assertRaisesRegex(SyncError, '^workload_auth_storage_refused$'): client.status()
        linked.unlink()
        with exclusive(client.root, 'workload-auth.lock'), self.assertRaisesRegex(SyncError, '^collector_busy$'):
            client.access_token()
        path.write_text('{"secret":"synthetic-canary", invalid')
        with self.assertRaises(SyncError) as failure: client.status()
        self.assertNotIn('synthetic-canary', str(failure.exception))
        self.assertIsNone(failure.exception.__context__)

    def test_metadata_echo_or_off_origin_verification_refuses_without_polling(self):
        request = self.issuer.request
        for mutation in ({'workload_metadata_sha256': '0'*64}, {'verification_uri': 'https://elsewhere.test/device'}):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory(dir=self.root) as root:
                self.root = Path(root)
                def response(*args, **kwargs):
                    value = request(*args, **kwargs)
                    if args[0].endswith('/device_authorization'): value.update(mutation)
                    return value
                with patch.object(self.issuer, 'request', side_effect=response), self.assertRaises(SyncError):
                    self.client().begin()
                self.assertEqual(self.journal()['phase'], 'authorization_inflight')
            self.root = Path(self.tmp.name)

    def test_invalid_origin_or_metadata_refuses_before_state_creation(self):
        for origin, location in (('http://identity.example.test', LOCATION),
                                 (ORIGIN, {**LOCATION, 'owner': ACCOUNT}),
                                 (ORIGIN, {**LOCATION, 'label': ' private '})):
            with self.subTest(origin=origin, keys=set(location)), self.assertRaises(SyncError):
                WorkloadTokens(self.root/'not-created', origin, location, http=self.issuer, key=self.key)
            self.assertFalse((self.root/'not-created').exists())

    def test_device_denial_and_known_server_expiry_are_distinct_terminal_states(self):
        for error in ('access_denied', 'expired_token'):
            with self.subTest(error=error), tempfile.TemporaryDirectory(dir=self.root) as root:
                self.root = Path(root); client = self.client(); client.begin(); self.clock.advance(5)
                self.issuer.refusal = error
                with self.assertRaisesRegex(SyncError, '^workload_approval_expired$' if error == 'expired_token'
                                            else '^workload_enrollment_refused$'): client.poll()
                self.assertEqual(self.journal()['phase'], 'expired' if error == 'expired_token' else 'revoked')
                self.assertNotIn('device_code', self.journal())
                self.issuer.refusal = None
            self.root = Path(self.tmp.name)

    def test_workload_oauth_error_allowlist_does_not_change_desktop_errors(self):
        url = ORIGIN+'/native-auth/v1/token'
        for error in ('authorization_pending', 'slow_down', ['invalid']):
            for client in (HTTP(attempts=1), WorkloadHTTP(ORIGIN, attempts=1)):
                failure = urllib.error.HTTPError(url, 400, 'fixture', {},
                    io.BytesIO(json.dumps({'error': error}).encode()))
                with self.subTest(error=error, client=type(client).__name__), \
                     patch.object(client.opener, 'open', side_effect=failure), self.assertRaises(SyncError) as raised:
                    client.request(url, 'POST', data=b'fixture')
                expected = error if isinstance(client, WorkloadHTTP) and isinstance(error, str) else 'http_400'
                self.assertEqual(raised.exception.code, expected)


@unittest.skipUnless(shutil.which('openssl'), 'OpenSSL required for owned local TLS fixture')
class AuthTLSTests(unittest.TestCase):
    def test_real_tls_pending_oauth_code_and_actual_helper_enrollment_refresh(self):
        with tempfile.TemporaryDirectory(prefix='workload-auth-tls-') as directory:
            root = Path(directory); key_path, cert = root/'tls.pem', root/'tls.crt'
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost', '-keyout', str(key_path),
                '-out', str(cert)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
            clock, key = Clock(), WorkloadKey(root/'identity')
            key.ensure()
            issuer = None
            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *args): pass
                def do_GET(self): self.reply()
                def do_POST(self): self.reply()
                def reply(self):
                    data = self.rfile.read(int(self.headers.get('Content-Length', '0'))) or None
                    try: result = issuer.request(issuer.origin.value+self.path, self.command, data=data)
                    except SyncError as error:
                        status, result = 400, {'error': error.code}
                    else: status = 200
                    body = json.dumps(result).encode()
                    self.send_response(status); self.send_header('Content-Length', str(len(body))); self.end_headers()
                    self.wfile.write(body)
            with ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
                tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); tls.load_cert_chain(cert, key_path)
                server.socket = tls.wrap_socket(server.socket, server_side=True)
                thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=.01), daemon=True); thread.start()
                try:
                    origin = 'https://localhost:'+str(server.server_port)
                    issuer = Issuer(clock, key, origin); issuer.pending = True
                    client = WorkloadTokens(root/'auth', origin, LOCATION, key=key,
                        http=WorkloadHTTP(origin, ca_file=str(cert), attempts=1, timeout=2),
                        clock=lambda: clock.now, wall_clock=lambda: clock.wall, boot=lambda: clock.boot)
                    client.begin(); clock.advance(5); self.assertEqual(client.poll(), {'state': 'pending'})
                    clock.advance(5); self.assertEqual(client.poll(), {'state': 'ready'})
                    self.assertTrue(client.access_token(force_refresh=True))
                    self.assertEqual([kind for kind, form in issuer.effects], ['authorization', 'device', 'refresh'])
                finally:
                    server.shutdown(); thread.join(3)


if __name__ == '__main__': unittest.main()

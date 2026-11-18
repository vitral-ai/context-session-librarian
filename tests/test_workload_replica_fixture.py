"""Fixture adapter boundaries. TLS peer models W7; it is not real W7 authority."""
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import select
import signal
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.parse

from csl_sync_state import SyncError
from csl_workload_config import PROVIDER_PROFILES
from csl_workload_identity import (AUDIENCE, CLIENT_ID, SCOPES, b64, canonical, metadata,
                                   proof_message, SPKI_PREFIX, unb64)
from test_workload_auth import Clock, Issuer, Key, LOCATION
from test_workload_http import discovery
from test_workload_runner import config_at, environment

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = Path(os.environ.get('VITRAL_FIXTURE_CSL_RUNTIME', str(ROOT)))
ADAPTER = ROOT/'scripts/fixtures/workload-replica/enroll.py'
spec = importlib.util.spec_from_file_location('replica_fixture', ADAPTER)
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)
CANARY = 'FIXTURE-MUST-NOT-LEAVE-PRIVATE-STATE-5731'


class AdapterUnitTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='replica-fixture-unit-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        ca = self.root/'ca.crt'; ca.touch()
        self.config = replace(config_at(self.root), location=LOCATION, ca_file=str(ca))
        self.clock, self.key = Clock(), Key()
        self.clock.wall = time.time()
        self.issuer = Issuer(self.clock, self.key)
        self.events = []

    def enroll(self, emit=None, config=None):
        config = config or self.config
        with patch('csl_workload_runner.DeadlineHTTP', return_value=self.issuer), \
                patch('csl_workload_identity.WorkloadKey', return_value=self.key):
            adapter.enroll(environment(config), emit or (lambda record, end: self.events.append(record)),
                           deadline=self.clock.now+30, clock=lambda: self.clock.now, sleep=self.clock.advance)

    def test_only_public_consent_projection_and_ready_state_leave_replica(self):
        self.enroll()
        consent, result = self.events
        self.assertEqual(set(consent), {'v', 'kind', 'user_code', 'verification_uri', 'expires_in',
                         'interval', 'metadata_sha256', 'provider', 'profile'})
        self.assertEqual(consent['metadata_sha256'], metadata(LOCATION)[1])
        self.assertEqual(result, {'v': 1, 'kind': 'result', 'state': 'ready'})
        self.assertNotIn('access_token', json.dumps(self.events))
        self.assertNotIn('device_code', json.dumps(self.events))
        before = list(self.issuer.calls)
        with self.assertRaisesRegex(SyncError, 'already_started'): self.enroll()
        self.assertEqual(self.issuer.calls, before)

    def test_cancel_or_suspend_before_poll_retains_pending_without_second_grant(self):
        def stop(record, end):
            self.events.append(record)
            self.clock.advance(100)
        with self.assertRaises(adapter.FixtureRefused): self.enroll(stop)
        row = json.loads((self.config.state_root/'auth/workload-auth.json').read_bytes())
        self.assertEqual(row['phase'], 'pending')
        self.assertEqual(len(self.issuer.effects), 1)

    def test_clock_jump_during_final_callback_refuses_success_without_reset(self):
        def late(record, deadline):
            self.events.append(record)
            if record.get('state') == 'ready': self.clock.advance(100)
        with self.assertRaises(adapter.FixtureRefused): self.enroll(late)
        journal = self.config.state_root/'auth/workload-auth.json'
        before = journal.read_bytes()
        self.assertEqual(json.loads(before)['phase'], 'ready')
        calls = list(self.issuer.calls)
        with self.assertRaises(SyncError): self.enroll()
        self.assertEqual(journal.read_bytes(), before)
        self.assertEqual(self.issuer.calls, calls)

    def test_real_pipe_write_rechecks_acceptance_for_consent_and_results(self):
        events = ({'v': 1, 'kind': 'consent'}, {'v': 1, 'kind': 'result', 'state': 'ready'},
                  {'v': 1, 'kind': 'result', 'state': 'pending'})
        original = os.write
        for event in events:
            with self.subTest(kind=event['kind'], state=event.get('state')):
                read, write = os.pipe()
                writer = adapter.pipe_writer(write)
                clock = [0]
                def late(fd, data):
                    count = original(fd, data)
                    clock[0] = 2
                    return count
                try:
                    with patch.object(adapter.os, 'write', side_effect=late), self.assertRaises(adapter.FixtureRefused):
                        adapter.write_public(writer, event, 1, lambda: clock[0],
                                             best_effort=event.get('state') == 'pending')
                    # Bytes cannot be undone; refusal, not a late exit0, is the proof.
                    self.assertEqual(json.loads(os.read(read, 2048)), event)
                finally:
                    os.close(writer); os.close(write); os.close(read)

    def test_main_rechecks_deadline_after_enrollment_and_cleanup(self):
        original_close = os.close
        for stage in ('enroll', 'cleanup'):
            with self.subTest(stage=stage):
                read, write = os.pipe()
                clock = [0]
                def enrolled(*args, **kwargs):
                    if stage == 'enroll': clock[0] = 100
                def closed(fd):
                    original_close(fd)
                    if stage == 'cleanup': clock[0] = 100
                try:
                    with patch.object(adapter, 'boottime', side_effect=lambda: clock[0]), \
                            patch.object(adapter, 'enroll', side_effect=enrolled), \
                            patch.object(adapter.os, 'close', side_effect=closed):
                        result = adapter.main(['--runtime', str(ROOT), '--consent-fd', str(write),
                                               '--budget-seconds', '5'])
                    self.assertEqual(result, 2)
                finally:
                    original_close(write); original_close(read)

    def test_two_invocations_share_existing_gate_and_create_one_authorization(self):
        admitted, release = threading.Event(), threading.Event()
        def pause(path, form):
            if path.endswith('/device_authorization'):
                admitted.set()
                if not release.wait(3): raise AssertionError('fixture release not received')
        self.issuer.hook = pause
        def attempt(_):
            try:
                adapter.enroll(environment(self.config), lambda *args: None,
                    deadline=self.clock.now+30, clock=lambda: self.clock.now, sleep=self.clock.advance)
                return True
            except SyncError: return False
        with patch('csl_workload_runner.DeadlineHTTP', return_value=self.issuer), \
                patch('csl_workload_identity.WorkloadKey', return_value=self.key), \
                ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(attempt, 0)
            try:
                self.assertTrue(admitted.wait(2))
                second = pool.submit(attempt, 1)
                self.assertFalse(second.result(timeout=2))
            finally: release.set()
            outcomes = [first.result(timeout=2), second.result(timeout=2)]
        self.assertEqual(sum(outcomes), 1)
        self.assertEqual(sum(kind == 'authorization' for kind, _ in self.issuer.effects), 1)

    def test_unsafe_origin_missing_ca_or_wrong_profile_refuses_before_effect(self):
        for config in (replace(self.config, origin='https://www.vitralai.com'),
                       replace(self.config, ca_file=None), replace(self.config, profile='auto')):
            with self.subTest(config=config), self.assertRaises((adapter.FixtureRefused, SyncError)):
                self.enroll(config=config)
        self.assertEqual(self.issuer.calls, [])
        self.assertEqual(list(self.config.state_root.iterdir()), [])

    def test_public_schema_refuses_unexpected_or_secret_looking_fields(self):
        public = dict(user_code='ABCD-EFGH', verification_uri=self.config.origin+'/native-auth/v1/device',
                      expires_in=60, interval=5)
        for wrong in ({**public, 'access_token': CANARY}, {**public, 'user_code': CANARY},
                      {**public, 'verification_uri': 'https://elsewhere.test/native-auth/v1/device'},
                      {**public, 'interval': float('nan')}):
            with self.subTest(fields=list(wrong)), self.assertRaises(adapter.FixtureRefused):
                adapter.consent_record(self.config, wrong)
        self.assertEqual(adapter.failure_record(RuntimeError(CANARY)),
                         {'v': 1, 'kind': 'result', 'state': 'pending'})

    def test_pipe_only_bounded_output_and_broken_rendezvous(self):
        path = self.root/'not-a-pipe'
        with path.open('wb') as output:
            with self.assertRaises(adapter.FixtureRefused): adapter.pipe_writer(output.fileno())
            self.assertEqual(adapter.main(['--runtime', str(ROOT), '--consent-fd', str(output.fileno())]), 2)
        self.assertEqual(path.read_bytes(), b'')
        read, write = os.pipe()
        try:
            with self.assertRaises(adapter.FixtureRefused): adapter.pipe_writer(read)
            writer = adapter.pipe_writer(write)
            try:
                adapter.write_public(writer, {'v': 1, 'kind': 'result', 'state': 'ready'}, adapter.boottime()+1)
                self.assertEqual(json.loads(os.read(read, 2048)), {'v': 1, 'kind': 'result', 'state': 'ready'})
                while True:
                    try: os.write(writer, b'x'*4096)
                    except BlockingIOError: break
                start = adapter.boottime()
                self.assertFalse(adapter.write_public(writer, {'v': 1}, start+10, best_effort=True))
                self.assertLess(adapter.boottime()-start, .5)
                start = adapter.boottime()
                with self.assertRaises(adapter.FixtureRefused):
                    adapter.write_public(writer, {'v': 1}, start+0.05)
                self.assertLess(adapter.boottime()-start, .5)
            finally: os.close(writer)
        finally:
            os.close(read); os.close(write)


class Peer:
    """Owned TLS model; approval is explicit, and proof signatures are verified."""
    def __init__(self, root):
        self.root, self.records, self.approved = root, {}, set()
        self.calls, self.credentials, self.verified = [], [], 0
        self.authorization_seen = threading.Event()
        self.discovery_seen = threading.Event()
        self.discovery_error = False
        self.deny = self.lose = False
        self.cert, key = root/'ca.crt', root/'tls-key.pem'
        subprocess.run(['/usr/bin/openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
            '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost',
            '-keyout', str(key), '-out', str(self.cert)], check=True, timeout=10,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        peer = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def reply(self, code, value):
                body = canonical(value)
                self.send_response(code); self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body))); self.end_headers()
                try: self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError): pass
            def do_GET(self):
                peer.calls.append(('GET', self.path))
                peer.discovery_seen.set()
                if peer.discovery_error:
                    self.reply(503, {'error': 'temporarily_unavailable'}); return
                if self.path != '/native-auth/v1/.well-known/oauth-authorization-server':
                    self.reply(404, {}); return
                self.reply(200, discovery(peer.origin))
            def do_POST(self):
                try: self.post()
                except Exception: self.reply(500, {'error': 'temporarily_unavailable'})
            def post(self):
                form = dict(urllib.parse.parse_qsl(self.rfile.read(int(self.headers['Content-Length'])).decode()))
                peer.calls.append(('POST', self.path))
                if self.path.endswith('/device_authorization'):
                    index = len(peer.records)+1
                    code = b64(bytes([index])*32)
                    location = json.loads(form['workload'])
                    row = dict(device_code=code, user_code='ABCD-EFG'+'23456789ABCDEFGHJKLMNPQRSTUVWXYZ'[index],
                               location=location, thumb=form['key_thumbprint'])
                    peer.records[code] = row
                    peer.authorization_seen.set()
                    self.reply(200, {'device_code': code, 'user_code': row['user_code'], 'expires_in': 60,
                        'interval': .05, 'verification_uri': peer.origin+'/native-auth/v1/device',
                        'exchange_nonce': b64(b'n'*32), 'workload_metadata': location,
                        'workload_metadata_sha256': metadata(location)[1]})
                    return
                if self.path != '/native-auth/v1/token': self.reply(404, {}); return
                row = peer.records[form['device_code']]
                if peer.deny: self.reply(400, {'error': 'access_denied'}); return
                if row['user_code'] not in peer.approved:
                    self.reply(400, {'error': 'authorization_pending'}); return
                jwk = json.loads(form['host_public_key'])
                thumb = hashlib.sha256(canonical(jwk)).digest()
                if b64(thumb) != row['thumb']: self.reply(400, {'error': 'invalid_grant'}); return
                with tempfile.TemporaryDirectory(dir=root) as private:
                    private = Path(private)
                    (private/'public.der').write_bytes(SPKI_PREFIX+unb64(jwk['x'], 32))
                    (private/'message').write_bytes(proof_message(peer.origin+'/native-auth/v1', row['device_code'],
                        b'n'*32, thumb, bytes.fromhex(metadata(row['location'])[1])))
                    (private/'signature').write_bytes(unb64(form['host_key_proof'], 64))
                    verification = subprocess.run(['/usr/bin/openssl', 'pkeyutl', '-verify', '-pubin', '-keyform', 'DER',
                        '-inkey', str(private/'public.der'), '-rawin', '-in', str(private/'message'),
                        '-sigfile', str(private/'signature')], timeout=5, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if verification.returncode: self.reply(400, {'error': 'invalid_grant'}); return
                peer.verified += 1
                device = 'ses_fixture-'+row['user_code']
                claims = dict(iss=peer.origin+'/native-auth/v1', aud=[AUDIENCE], client_id=CLIENT_ID,
                    sub='1234567890abcdef12345678', sid=device, device_id=device, cnf={'jkt': row['thumb']},
                    scope=SCOPES, token_use='native_access', exp=time.time()+300, canary=CANARY)
                access = '.'.join((b64(canonical({'alg': 'RS256'})), b64(canonical(claims)), b64(b'synthetic')))
                response = dict(token_type='Bearer', access_token=access, refresh_token=b64(b'r'*32), expires_in=300,
                                scope=SCOPES, device_id=device, session_id=device, account={'id': claims['sub']})
                peer.credentials.extend((access, response['refresh_token'], row['device_code'], form['host_key_proof']))
                if peer.lose:
                    self.close_connection = True
                    self.connection.shutdown(2); self.connection.close(); return
                self.reply(200, response)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.origin = 'https://localhost:'+str(self.server.server_port)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(self.cert, key)
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=.01), daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=2)


@unittest.skipUnless(Path('/usr/bin/openssl').is_file(), 'OpenSSL for owned TLS and key fixture')
class AdapterProcessTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='replica-fixture-tls-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.peer = Peer(self.root)
        self.addCleanup(self.peer.close)
        self.config = replace(config_at(self.root), origin=self.peer.origin, ca_file=str(self.peer.cert))
        self.config.state_root.chmod(0o770)

    def start(self, config=None, budget=5, stdout=subprocess.PIPE):
        config = config or self.config
        env = {'PATH': os.defpath, 'HOME': str(self.root), 'LANG': 'C', 'PYTHONDONTWRITEBYTECODE': '1',
               'UNUSED_CANARY': CANARY, **environment(config)}
        process = subprocess.Popen([sys.executable, '-E', '-s', '-S', str(ADAPTER), '--runtime', str(RUNTIME),
            '--consent-fd', '1', '--budget-seconds', str(budget)], env=env,
            stdout=stdout, stderr=subprocess.PIPE, bufsize=0)
        def cleanup():
            if process.poll() is None: process.kill()
            process.communicate(timeout=3)
        self.addCleanup(cleanup)
        return process

    def consent(self, process):
        self.assertTrue(select.select([process.stdout], [], [], 4)[0], 'bounded public rendezvous')
        line = process.stdout.readline()
        record = json.loads(line)
        self.assertEqual(record['kind'], 'consent')
        self.assertNotIn(CANARY, line.decode())
        return record, line

    def finish(self, process, prefix=b''):
        output, errors = process.communicate(timeout=6)
        output = prefix+output
        self.assertEqual(errors, b'')
        self.assertNotIn(CANARY.encode(), output)
        for credential in self.peer.credentials: self.assertNotIn(credential.encode(), output)
        return [json.loads(line) for line in output.splitlines()]

    def test_real_helper_tls_pipe_and_private_volume_consumption(self):
        process = self.start()
        consent, line = self.consent(process)
        self.assertEqual(consent['metadata_sha256'], metadata(self.config.location)[1])
        self.peer.approved.add(consent['user_code'])
        records = self.finish(process, line)
        self.assertEqual(process.returncode, 0)
        self.assertEqual(records[-1]['state'], 'ready')
        self.assertEqual(self.peer.verified, 1)
        self.assertEqual(self.config.state_root.stat().st_mode & 0o777, 0o770)
        for child in ('auth', 'identity'):
            path = self.config.state_root/child
            self.assertEqual(path.stat().st_mode & 0o777, 0o700)
            self.assertTrue(all(item.stat().st_mode & 0o777 == 0o600 for item in path.iterdir()))
        # Consume the same volume's approved journal in a separate replica-local
        # process. Only readiness leaves it; no bearer/private state is copied.
        code = ('import sys; from pathlib import Path; sys.path.insert(0,sys.argv[1]); '
                'from csl_workload_auth import WorkloadTokens; from csl_workload_identity import WorkloadKey; '
                'from csl_workload_config import Configuration; import os; c=Configuration.from_environment(os.environ); '
                't=WorkloadTokens(c.state_root/"auth",c.origin,c.location,key=WorkloadKey(c.state_root/"identity")); '
                'assert t.access_token(); print(t.status()["state"])')
        consumed = subprocess.run([sys.executable, '-E', '-s', '-S', '-c', code, str(RUNTIME)],
            env={'PATH': os.defpath, **environment(self.config)}, capture_output=True, timeout=5)
        self.assertEqual(consumed.returncode, 0, consumed.stderr.decode())
        self.assertEqual(consumed.stdout, b'ready\n')
        calls = list(self.peer.calls)
        again = self.start()
        self.assertEqual(self.finish(again)[-1]['state'], 'existing_state')
        self.assertEqual(again.returncode, 2)
        self.assertEqual(self.peer.calls, calls)

    def test_lost_token_ack_retains_state_and_never_reenrolls(self):
        self.peer.lose = True
        process = self.start()
        consent, line = self.consent(process)
        self.peer.approved.add(consent['user_code'])
        self.assertEqual(self.finish(process, line)[-1]['state'], 'pending')
        journal = self.config.state_root/'auth/workload-auth.json'
        before = journal.read_bytes()
        self.assertEqual(json.loads(before)['phase'], 'device_inflight')
        calls = list(self.peer.calls)
        again = self.start()
        self.assertEqual(self.finish(again)[-1]['state'], 'existing_state')
        self.assertEqual(journal.read_bytes(), before)
        self.assertEqual(self.peer.calls, calls)

    def test_denial_and_budget_or_sigterm_are_not_reclassified(self):
        self.peer.deny = True
        process = self.start()
        consent, line = self.consent(process)
        self.assertEqual(self.finish(process, line)[-1]['state'], 'denied')
        self.assertEqual(json.loads((self.config.state_root/'auth/workload-auth.json').read_bytes())['phase'], 'revoked')
        self.peer.deny = False
        for name, stop in (('deadline', False), ('term', True)):
            state = self.root/name; state.mkdir()
            config = replace(self.config, state_root=state, location={**self.config.location, 'replica': name})
            process = self.start(config, budget=5 if stop else 1)
            _, line = self.consent(process)
            if stop: process.send_signal(signal.SIGTERM)
            records = self.finish(process, line)
            self.assertEqual(process.returncode, 2)
            self.assertFalse(any(row.get('state') in ('ready', 'denied', 'expired') for row in records))

    def test_independent_replica_owns_different_key_and_journal(self):
        keys, devices = [], []
        for name in ('first', 'second'):
            state = self.root/name; state.mkdir()
            config = replace(self.config, state_root=state, location={**self.config.location, 'replica': name})
            process = self.start(config)
            consent, line = self.consent(process)
            self.peer.approved.add(consent['user_code'])
            self.assertEqual(self.finish(process, line)[-1]['state'], 'ready')
            keys.append(hashlib.sha256((state/'identity/workload-ed25519.pem').read_bytes()).hexdigest())
            devices.append(json.loads((state/'auth/workload-auth.json').read_bytes())['device_id'])
        self.assertNotEqual(keys[0], keys[1])
        self.assertNotEqual(devices[0], devices[1])

    def test_all_packaged_profiles_use_the_same_public_consent_contract(self):
        for provider, profile in PROVIDER_PROFILES.items():
            with self.subTest(provider=provider):
                state = self.root/provider; state.mkdir()
                config = replace(self.config, state_root=state, provider=provider, profile=profile,
                                 location={**self.config.location, 'replica': provider})
                process = self.start(config)
                consent, line = self.consent(process)
                self.assertEqual((consent['provider'], consent['profile']), (provider, profile))
                self.assertEqual(consent['metadata_sha256'], metadata(config.location)[1])
                self.peer.approved.add(consent['user_code'])
                self.assertEqual(self.finish(process, line)[-1]['state'], 'ready')
                self.assertEqual(process.returncode, 0)

    def test_wrong_public_ca_refuses_before_http_or_volume_state(self):
        other_root = self.root/'other-ca'; other_root.mkdir()
        other = Peer(other_root)
        self.addCleanup(other.close)
        process = self.start(replace(self.config, ca_file=str(other.cert)))
        self.assertEqual(self.finish(process)[-1]['state'], 'pending')
        self.assertEqual(process.returncode, 2)
        self.assertEqual(self.peer.calls, [])
        # Creating private empty children is not a credential import or grant.
        self.assertFalse((self.config.state_root/'auth/workload-auth.json').exists())
        self.assertFalse((self.config.state_root/'identity/workload-ed25519.pem').exists())

    def filled_rendezvous(self):
        read, write = os.pipe()
        self.addCleanup(os.close, read)
        self.addCleanup(os.close, write)
        os.set_blocking(write, False)
        size = 0
        while True:
            try: size += os.write(write, b'x'*4096)
            except BlockingIOError: break
        return read, write, size

    def full_pipe(self, config, budget):
        read, write, size = self.filled_rendezvous()
        self.peer.authorization_seen.clear()
        process = self.start(config, budget=budget, stdout=write)
        self.assertTrue(self.peer.authorization_seen.wait(3))
        journal = config.state_root/'auth/workload-auth.json'
        end = time.monotonic()+3
        while time.monotonic() < end:
            if journal.exists() and json.loads(journal.read_bytes())['phase'] == 'pending':
                break
            self.assertIsNone(process.poll())
            time.sleep(.01)
        else: self.fail('device authorization did not reach durable pending')
        return process, journal, read, size

    def test_term_and_int_with_full_pipe_abort_without_a_second_wait(self):
        for number, name in ((signal.SIGTERM, 'term-full'), (signal.SIGINT, 'int-full')):
            with self.subTest(signal=name):
                state = self.root/name; state.mkdir()
                config = replace(self.config, state_root=state, location={**self.config.location, 'replica': name})
                before_count = len(self.peer.records)
                process, journal, read, size = self.full_pipe(config, budget=8)
                before = journal.read_bytes()
                key = (state/'identity/workload-ed25519.pem').read_bytes()
                started = time.monotonic()
                process.send_signal(number)
                _, errors = process.communicate(timeout=1)
                self.assertLess(time.monotonic()-started, 1)
                self.assertEqual(process.returncode, 2)
                self.assertEqual(errors, b'')
                self.assertEqual(journal.read_bytes(), before)
                self.assertEqual((state/'identity/workload-ed25519.pem').read_bytes(), key)
                self.assertEqual(len(self.peer.records), before_count+1)
                self.assertEqual(os.read(read, size+1), b'x'*size)
                self.assertFalse(any(path.endswith('/token') for _, path in self.peer.calls))

    def test_full_pipe_without_cancel_obeys_original_backpressure_deadline(self):
        started = time.monotonic()
        process, journal, read, size = self.full_pipe(self.config, budget=1)
        _, errors = process.communicate(timeout=2)
        self.assertLess(time.monotonic()-started, 3)
        self.assertEqual(process.returncode, 2)
        self.assertEqual(errors, b'')
        self.assertEqual(json.loads(journal.read_bytes())['phase'], 'pending')
        self.assertEqual(len(self.peer.records), 1)
        self.assertEqual(os.read(read, size+1), b'x'*size)

    def test_failure_reporting_never_waits_for_space_on_a_full_pipe(self):
        self.peer.discovery_error = True
        read, write, size = self.filled_rendezvous()
        process = self.start(budget=8, stdout=write)
        self.assertTrue(self.peer.discovery_seen.wait(3))
        _, errors = process.communicate(timeout=1)
        self.assertEqual(process.returncode, 2)
        self.assertEqual(errors, b'')
        self.assertEqual(len(self.peer.records), 0)
        self.assertEqual(os.read(read, size+1), b'x'*size)
        self.assertFalse((self.config.state_root/'auth/workload-auth.json').exists())


if __name__ == '__main__': unittest.main()

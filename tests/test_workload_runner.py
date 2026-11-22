"""Local-only process, deadline and actual sanitized-uploader fixtures."""
import copy
from dataclasses import replace
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import shutil
import signal
import select
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch
import uuid

from csl_sync_state import State, SyncError, private_json
from csl_workload_auth import WorkloadTokens, boot_clock, boot_identity
from csl_workload_config import Configuration, PREFIX, PROVIDER_PROFILES
from csl_workload_identity import AUDIENCE, CLIENT_ID, SCOPES, WorkloadKey, b64, canonical
from csl_workload_runner import (DeadlineHTTP, PassProcess, Runner, bounds, child, environment,
    clean_receipts, parent_death, read_record, run_pass, scope, sources)
from csl_workload_producer import guardian, producer_exec
from sync_support import Peer
from test_workload_text import CANARY, SID, native


def config_at(root):
    for name in ('transcripts', 'state'): (root/name).mkdir(exist_ok=True)
    return Configuration('https://identity.example.test', 'codex', PROVIDER_PROFILES['codex'],
        root/'transcripts', root/'state', {'label': 'synthetic collector', 'provider': 'kubernetes',
        'cluster': 'kind', 'namespace': 'test', 'workload': 'fixture', 'replica': 'replica-1'},
        2, 128*1024*1024, None)


def stopped_record_after_guardian_exit(path, guardian_fd, deadline):
    # Writer/child exit does not mean the guardian finished atomic publication.
    poll = select.poll()
    poll.register(guardian_fd, select.POLLIN)
    remaining = deadline-boot_clock()
    if remaining <= 0 or not poll.poll(int(remaining*1000)) or boot_clock() >= deadline:
        raise AssertionError('original guardian did not exit within fixture cap')
    value = read_record(path)
    if boot_clock() >= deadline:
        raise AssertionError('guardian record read exceeded fixture cap')
    if value is None or value.get('phase') != 'stopped':
        raise AssertionError('original guardian did not persist stopped state')
    return value


class GuardianReadOrderingTests(unittest.TestCase):
    def test_strict_read_follows_guardian_exit(self):
        events = []
        poll = Mock()
        poll.poll.side_effect = lambda timeout: events.append(('wait', timeout)) or [(7, select.POLLIN)]
        value = {'phase': 'stopped'}
        with patch('test_workload_runner.select.poll', return_value=poll), \
             patch('test_workload_runner.boot_clock', return_value=10), \
             patch('test_workload_runner.read_record', side_effect=lambda _: events.append(('read',)) or value):
            self.assertEqual(stopped_record_after_guardian_exit(Path('owned.json'), 7, 13), value)
        poll.register.assert_called_once_with(7, select.POLLIN)
        self.assertEqual(events, [('wait', 3000), ('read',)])

    def test_missing_or_late_exit_never_reads_a_live_record(self):
        for ready, times in (([], (10,)), ([(7, select.POLLIN)], (10, 13)), ([], (13,))):
            with self.subTest(ready=ready, times=times), \
                 patch('test_workload_runner.select.poll') as polling, \
                 patch('test_workload_runner.boot_clock', side_effect=times), \
                 patch('test_workload_runner.read_record') as read:
                polling.return_value.poll.return_value = ready
                with self.assertRaises(AssertionError):
                    stopped_record_after_guardian_exit(Path('owned.json'), 7, 13)
                read.assert_not_called()

    def test_exit_does_not_waive_unsafe_or_missing_stop_state(self):
        for outcome in (SyncError('workload_runtime_storage_refused'), None, {'phase': 'running'}):
            with self.subTest(outcome=type(outcome).__name__), \
                 patch('test_workload_runner.select.poll') as polling, \
                 patch('test_workload_runner.boot_clock', return_value=10), \
                 patch('test_workload_runner.read_record') as read:
                polling.return_value.poll.return_value = [(7, select.POLLIN)]
                if isinstance(outcome, Exception): read.side_effect = outcome
                else: read.return_value = outcome
                with self.assertRaises(SyncError if isinstance(outcome, Exception) else AssertionError):
                    stopped_record_after_guardian_exit(Path('owned.json'), 7, 13)
                read.assert_called_once()

    def test_late_record_read_cannot_extend_original_wait_cap(self):
        with patch('test_workload_runner.select.poll') as polling, \
             patch('test_workload_runner.boot_clock', side_effect=(10, 10, 13)), \
             patch('test_workload_runner.read_record', return_value={'phase': 'stopped'}):
            polling.return_value.poll.return_value = [(7, select.POLLIN)]
            with self.assertRaisesRegex(AssertionError, 'exceeded fixture cap'):
                stopped_record_after_guardian_exit(Path('owned.json'), 7, 13)

    def test_atomic_replacement_keeps_strict_unlinked_inode_refusal(self):
        with tempfile.TemporaryDirectory(prefix='guardian-rename-') as directory:
            path = Path(directory)/'producer.json'
            private_json(path, {'phase': 'running'})
            original, observed = os.fstat, []
            def replace_before_stat(fd):
                if not observed:
                    observed.append(original(fd).st_nlink)
                    private_json(path, {'phase': 'stopped'})
                    observed.append(original(fd).st_nlink)
                return original(fd)
            with patch('csl_workload_state.os.fstat', side_effect=replace_before_stat), \
                 self.assertRaises(SyncError) as refusal:
                read_record(path)
            self.assertEqual(refusal.exception.code, 'workload_runtime_storage_refused')
            self.assertEqual(observed, [1, 0])
            self.assertEqual(read_record(path), {'phase': 'stopped'})


class Clock:
    def __init__(self): self.now = 1000
    def __call__(self): return self.now
    def sleep(self, value): self.now += value


class FakePass:
    def __init__(self, receipt=None, *, hung=False):
        self.value = receipt or {'ok': True, 'synced': 1, 'pending': 0, 'manifest': [{'path': 'main.jsonl'}]}
        self.hung, self.killed, self.removed = hung, False, False
        self.reap_budgets = []
    def poll(self): return None if self.hung else 0
    def receipt(self): return self.value
    def kill(self): self.killed = True
    def reap(self, seconds): self.reap_budgets.append(seconds); return True
    def remove_receipt(self): self.removed = True


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='workload-runner-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.config = config_at(self.root)

    def test_hung_final_pass_is_killed_and_pending_within_budget(self):
        clock, worker = Clock(), FakePass(hung=True)
        runner = Runner(self.config, clock=clock, sleep=clock.sleep, pass_factory=lambda *args: worker)
        runner.stop()
        start = clock(); result = runner.run()
        self.assertFalse(result['complete']); self.assertTrue(result['pending'])
        self.assertTrue(worker.killed); self.assertTrue(worker.removed)
        self.assertLessEqual(clock()-start, self.config.flush_seconds)

    def test_suspend_jump_never_dispatches_more_work_or_claims_success(self):
        clock, workers = Clock(), []
        def launch(*args):
            worker = FakePass(); workers.append(worker); clock.now += 100
            return worker
        runner = Runner(self.config, clock=clock, sleep=clock.sleep, pass_factory=launch); runner.stop()
        result = runner.run()
        self.assertEqual(len(workers), 1)
        self.assertFalse(result['complete']); self.assertTrue(result['pending'])

    def test_watch_ack_without_quiescence_is_not_a_completed_close(self):
        clock = Clock()
        runner = Runner(self.config, clock=clock, sleep=clock.sleep, pass_factory=lambda *args: FakePass())
        runner.stop(); result = runner.run()
        self.assertFalse(result['complete']); self.assertTrue(result['pending'])
        self.assertFalse(result['producer_quiesced']); self.assertEqual(result['synced'], 1)

    def test_suspend_does_not_extend_reaping_past_the_original_budget(self):
        clock, worker = Clock(), FakePass(hung=True)
        def launch(*args): clock.now += 100; return worker
        runner = Runner(self.config, clock=clock, sleep=clock.sleep, pass_factory=launch); runner.stop()
        self.assertTrue(runner.run()['pending'])
        self.assertTrue(worker.killed)
        self.assertEqual(worker.reap_budgets, [0])

    def test_literal_command_launched_once_and_only_final_ack_can_complete(self):
        clock = Clock(); writer = Mock(returncode=7); writer.poll.return_value = 7; writer.quiesced.return_value = True
        worker = FakePass(); calls = []
        def launch(*args): calls.append(args); return worker
        runner = Runner(self.config, clock=clock, sleep=clock.sleep, pass_factory=launch)
        command = ['/literal/program', 'space value', ';not-a-shell', '$(not-executed)']
        with patch('csl_workload_runner.Producer', return_value=writer) as launched:
            result = runner.run(command)
        self.assertTrue(result['complete']); self.assertFalse(result['pending'])
        self.assertEqual(result['writer_exit'], 7); self.assertEqual(len(calls), 1)
        launched.assert_called_once()
        self.assertEqual(launched.call_args.args[-1], command)

    def test_receipt_without_view_ack_or_without_reaped_worker_cannot_complete(self):
        clock = Clock(); worker = FakePass({'ok': False, 'synced': 1, 'pending': 1, 'manifest': []})
        runner = Runner(self.config, clock=clock, sleep=clock.sleep, pass_factory=lambda *args: worker); runner.stop()
        self.assertTrue(runner.run()['pending'])
        self.assertLessEqual(clock(), 1000+self.config.flush_seconds)

    def test_failure_cleanup_still_stops_group_when_writer_root_already_exited(self):
        clock = Clock(); writer = Mock(pid=123456, returncode=0)
        writer.poll.side_effect = [None, 0]
        def failed(*args): raise SyncError('fixture_failure')
        runner = Runner(self.config, clock=clock, sleep=clock.sleep, pass_factory=failed)
        with patch('csl_workload_runner.Producer', return_value=writer):
            self.assertTrue(runner.run(['/literal/command'])['pending'])
        writer.stop.assert_called_once()
        writer.wait.assert_called_once()

    def test_worker_environment_is_allowlisted_and_no_configuration_in_argv(self):
        runtime = child(self.config.state_root, 'runtime')
        private_json(runtime/'configuration.json', environment(self.config))
        process = Mock(); process.poll.return_value = None
        with patch.dict(os.environ, {'AWS_SECRET_ACCESS_KEY': 'SYNTHETIC-CANARY', 'CODEX_HOME': 'canary'}), \
             patch('csl_workload_runner.subprocess.Popen', return_value=process) as popen:
            PassProcess(self.config, str(uuid.uuid4()), boot_clock()+10)
        options = popen.call_args.kwargs
        self.assertEqual(set(options['env']), {'PATH', 'LANG', 'HOME', 'PYTHONDONTWRITEBYTECODE'})
        self.assertNotIn('SYNTHETIC-CANARY', repr(popen.call_args))
        self.assertNotIn(self.config.origin, repr(popen.call_args.args))
        self.assertEqual(options['stdin'], subprocess.DEVNULL)

    def test_receipt_cleanup_failure_cannot_change_pending_into_complete(self):
        clock, worker = Clock(), FakePass()
        writer = Mock(returncode=0); writer.poll.return_value = 0; writer.quiesced.return_value = True
        runner = Runner(self.config, clock=clock, sleep=clock.sleep, pass_factory=lambda *args: worker)
        with patch.object(worker, 'remove_receipt', side_effect=[OSError('owned cleanup unit'), None]), \
             patch('csl_workload_runner.Producer', return_value=writer):
            result = runner.run(['/literal/fixture'])
        self.assertFalse(result['complete']); self.assertTrue(result['pending'])

    def test_expired_write_boundary_refuses_before_opener_or_worker_spawn(self):
        with patch('csl_workload_runner.boot_clock', return_value=1000):
            http = DeadlineHTTP(self.config.origin, 1001, attempts=1)
            with patch('urllib.request.OpenerDirector.open', side_effect=AssertionError('must not open')):
                with patch('csl_workload_runner.boot_clock', return_value=1002), \
                     self.assertRaisesRegex(SyncError, '^workload_flush_deadline$'):
                    http.request(self.config.origin+'/native-auth/v1/token', 'POST', data=b'fixture')
            with patch('csl_workload_runner.subprocess.Popen') as popen:
                with self.assertRaisesRegex(SyncError, '^workload_flush_deadline$'):
                    PassProcess(self.config, str(uuid.uuid4()), 999)
                popen.assert_not_called()

    def test_lazy_request_bodies_cannot_read_past_the_write_admission_check(self):
        client = DeadlineHTTP(self.config.origin, boot_clock()+5, attempts=1)
        with patch('http.client.HTTPSConnection.connect', side_effect=AssertionError('no connect')):
            for data in (io.BytesIO(b'private fixture'), iter([b'fixture']), 'not bytes'):
                with self.assertRaisesRegex(SyncError, '^workload_request_body_refused$'):
                    client.request(self.config.origin+'/native-auth/v1/token', 'POST', data=data)

    def test_worker_refuses_wrong_parent_lifetime(self):
        # Do not change this test process's PDEATHSIG; exercise a synthetic libc.
        libc = Mock(); libc.prctl.return_value = 0
        with patch('csl_workload_runner.ctypes.CDLL', return_value=libc), \
             patch('csl_workload_runner.os.getppid', return_value=123):
            with self.assertRaisesRegex(SyncError, '^workload_parent_unavailable$'): parent_death(124)

    def test_expected_session_admits_only_the_pinned_project_file(self):
        from csl_workload_runner import bound_sources
        sid = '0b9f6b2a-1c3d-4e5f-8a7b-9c0d1e2f3a4b'
        config = replace(self.config, provider='claude', profile='claude-2.1.308-text-v1', expect_session=sid)
        source = config.transcript_root
        (source/'-work').mkdir(); (source/'-work'/(sid+'.jsonl')).write_bytes(b'')
        (source/'-work'/sid).mkdir(); (source/'-work'/sid/'tool-results.txt').write_bytes(b'not jsonl')
        self.assertEqual(bound_sources(config), ('-work/'+sid+'.jsonl',))
        for extra in ('-work/11111111-2222-4333-8444-555555555555.jsonl', sid+'.jsonl',
                      '-work/'+sid+'/subagents/agent-1.jsonl'):
            path = source/extra; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(b'')
            with self.subTest(extra=extra), self.assertRaisesRegex(SyncError, '^workload_session_binding_refused$'):
                bound_sources(config)
            path.unlink()
        self.assertEqual(bound_sources(replace(config, expect_session=None)), ('-work/'+sid+'.jsonl',))

    def test_only_bounded_declared_jsonl_files_and_no_symlinks_are_discovered(self):
        source = self.config.transcript_root
        (source/'ignored.db').write_bytes(b'not read')
        (source/'nested').mkdir(); (source/'nested/main.jsonl').write_bytes(b'')
        self.assertEqual(sources(source), ('nested/main.jsonl',))
        (source/'escape').symlink_to(self.root)
        with self.assertRaisesRegex(SyncError, '^workload_source_refused$'): sources(source)
        (source/'escape').unlink()
        for i in range(33): (source/f'{i}.jsonl').write_bytes(b'')
        with self.assertRaisesRegex(SyncError, '^workload_source_limit$'): sources(source)

    def test_private_children_leave_volume_root_unchanged_and_refuse_unsafe_state(self):
        self.config.state_root.chmod(0o770)
        leaf = child(self.config.state_root, 'runtime')
        self.assertEqual(self.config.state_root.stat().st_mode & 0o777, 0o770)
        self.assertEqual(leaf.stat().st_mode & 0o777, 0o700)
        leaf.chmod(0o750)
        with self.assertRaisesRegex(SyncError, '^workload_runtime_storage_refused$'): child(self.config.state_root, 'runtime')

    def test_small_spool_fails_before_any_worker_or_directory_creation(self):
        with self.assertRaisesRegex(SyncError, '^workload_spool_budget_too_small$'):
            Runner(replace(self.config, spool_bytes=1024))
        self.assertEqual(list(self.config.state_root.iterdir()), [])

    def test_direct_runtime_api_cannot_bypass_profile_or_metadata_codec(self):
        for config in (replace(self.config, profile='auto'), replace(self.config, provider='antigravity'),
                       replace(self.config, location={**self.config.location, 'owner': 'not-allowed'})):
            with self.subTest(provider=config.provider), self.assertRaises(SyncError): Runner(config)
        self.assertEqual(list(self.config.state_root.iterdir()), [])

    def test_receipt_paths_cannot_escape_and_restart_removes_only_own_orphans(self):
        with self.assertRaises(SyncError): PassProcess(self.config, '../foreign', boot_clock()+2)
        runtime = child(self.config.state_root, 'runtime'); receipts = child(runtime, 'receipts')
        for name in (str(uuid.uuid4())+'.json', '.write-fixture'):
            path = receipts/name; path.write_bytes(b'partial own receipt'); path.chmod(0o600)
        clean_receipts(runtime); self.assertEqual(list(receipts.iterdir()), [])
        path = receipts/'unrelated.json'; path.write_bytes(b'unrelated'); path.chmod(0o600)
        with self.assertRaises(SyncError): clean_receipts(runtime)
        self.assertEqual(path.read_bytes(), b'unrelated')
        with self.assertRaises(SyncError): child(self.config.state_root, '../outside')

    def test_runner_does_not_adopt_or_clean_an_unclaimed_runtime(self):
        runtime = child(self.config.state_root, 'runtime'); receipts = child(runtime, 'receipts')
        path = receipts/(str(uuid.uuid4())+'.json'); path.write_text('unrelated private data'); path.chmod(0o600)
        runner = Runner(self.config); runner.stop()
        with self.assertRaisesRegex(SyncError, '^workload_runtime_storage_refused$'): runner.run()
        self.assertEqual(path.read_text(), 'unrelated private data')

    def test_v1_runtime_cannot_silently_assume_an_old_producer_was_stopped(self):
        runtime = child(self.config.state_root, 'runtime')
        private_json(runtime/'runner.json', {'version': 1, 'scope': scope(self.config)})
        with patch('csl_workload_runner.Producer') as producer:
            with self.assertRaisesRegex(SyncError, '^workload_runtime_binding_changed$'):
                Runner(self.config).run(['/not-launched'])
            producer.assert_not_called()
        self.assertEqual(read_record(runtime/'runner.json')['version'], 1)

    def test_uncertain_guardian_launch_is_durable_and_not_retried(self):
        with patch('csl_workload_producer.subprocess.Popen', side_effect=OSError('synthetic failure')) as launched:
            self.assertTrue(Runner(self.config).run(['/not-launched'])['pending'])
            with self.assertRaisesRegex(SyncError, '^workload_producer_outcome_unknown$'):
                Runner(self.config).run(['/still-not-launched'])
            self.assertEqual(launched.call_count, 1)
        self.assertEqual(read_record(self.config.state_root/'runtime/producer.json')['phase'], 'launching')

    def test_exec_gate_refuses_eof_and_parent_death_before_literal_exec(self):
        for allow in (False, True):
            read_fd, write_fd = os.pipe()
            if allow: os.write(write_fd, b'G')
            os.close(write_fd)
            libc = Mock(); libc.prctl.return_value = 0
            with patch('csl_workload_producer.ctypes.CDLL', return_value=libc), \
                 patch('csl_workload_producer.os.getppid', side_effect=[123, 124]), \
                 patch('csl_workload_producer.os.execvp') as execute:
                with self.assertRaisesRegex(SyncError, '^workload_parent_unavailable$'):
                    producer_exec(123, read_fd, ['/not-launched'])
                execute.assert_not_called()

    def test_guardian_does_not_release_exec_gate_before_identity_is_durable(self):
        runtime = child(self.config.state_root, 'runtime')
        run_id = str(uuid.uuid4())
        claim = {'version': 1, 'scope': scope(self.config), 'run_id': run_id,
                 'boot': boot_identity(), 'phase': 'launching'}
        control, parent_control = os.pipe()
        writer = Mock(pid=123456, returncode=None)
        libc = Mock(); libc.prctl.return_value = 0
        try:
            with patch('csl_workload_producer.ctypes.CDLL', return_value=libc), \
                 patch('csl_workload_producer.read_record', return_value=claim), \
                 patch('csl_workload_producer.os.getppid', return_value=123), \
                 patch('csl_workload_producer.subprocess.Popen', return_value=writer), \
                 patch('csl_workload_producer.private_json', side_effect=OSError('fixture disk failure')), \
                 patch('csl_workload_producer.os.write') as release, \
                 patch('csl_workload_producer.group_signal'):
                with self.assertRaises(OSError): guardian(runtime, run_id, 123, control, ['/not-executed'])
                release.assert_not_called()
                writer.wait.assert_called_once()
        finally: os.close(parent_control)


@unittest.skipUnless(shutil.which('openssl'), 'OpenSSL for owned TLS and replica key fixture')
class UploaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='workload-upload-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.config = config_at(self.root)
        cert, tls_key = self.root/'ca.crt', self.root/'tls.pem'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
            '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost', '-keyout', str(tls_key), '-out', str(cert)],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
        self.peer = Peer(); self.addCleanup(self.peer.close)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); tls.load_cert_chain(cert, tls_key)
        self.peer.server.socket = tls.wrap_socket(self.peer.server.socket, server_side=True)
        origin = 'https://localhost:'+str(self.peer.server.server_port)
        self.config = replace(self.config, origin=origin, ca_file=str(cert))
        key = WorkloadKey(self.config.state_root/'identity'); key.ensure()
        http = DeadlineHTTP(origin, boot_clock()+300, ca_file=str(cert), attempts=1)
        tokens = WorkloadTokens(self.config.state_root/'auth', origin, self.config.location, http=http, key=key)
        thumb = b64(key.public()[1]); device = 'ses_fixture'; account = '1234567890abcdef12345678'
        claims = {'iss': http.origin.issuer, 'aud': [AUDIENCE], 'sub': account, 'sid': device, 'device_id': device,
            'cnf': {'jkt': thumb}, 'client_id': CLIENT_ID, 'scope': SCOPES, 'token_use': 'native_access', 'exp': int(time.time())+300}
        access = '.'.join((b64(canonical({'alg': 'RS256'})), b64(canonical(claims)), b64(b'synthetic-signature')))
        private_json(tokens.path, {'version': 1, 'binding': tokens.binding, 'thumbprint': thumb, 'phase': 'ready',
            'account_id': account, 'device_id': device, 'access_token': access, 'refresh_token': b64(b'r'*32),
            'boot': boot_identity(), 'expires_boot': boot_clock()+300, 'expires_wall': time.time()+300})
        handler = self.peer.server.RequestHandlerClass
        original = handler.handle_request
        answer = handler.answer
        self.proof_gate, self.proof_seen, self.proof_mutation = None, threading.Event(), None
        self.proof_transport = None
        self.access = access
        def proved_response(request, status, value):
            if request.command == 'PUT' and request.path.startswith('/sessions/') and len(request.path.strip('/').split('/')) == 3:
                key = request.path.strip('/')
                plan = self.peer.sessions.get(key)
                if status == 200 and plan:
                    complete = all(len(self.peer.files.get((key, f['file_key'], f['revision']), b'')) == f['length']
                        and hashlib.sha256(self.peer.files.get((key, f['file_key'], f['revision']), b'')).hexdigest() == f['sha256']
                        for f in plan['files'])
                    complete = complete and len(self.peer.views.get((key, plan['view']['generation']), [])) == plan['view']['last_seq']
                    value = {**value, 'snapshot_id': plan['snapshot_id'], 'view': copy.deepcopy(plan['view']), 'sync': {'complete': complete}}
                    if complete:
                        self.proof_seen.set()
                        if self.proof_gate is not None: self.proof_gate.wait(10)
                    if self.proof_mutation: self.proof_mutation(value)
                    if self.proof_transport and not self.proof_transport(request, value): return None
            try: return answer(request, status, value)
            except (BrokenPipeError, ConnectionResetError, ssl.SSLError): return None
        def mounted(request):
            if not request.path.startswith('/session-registry/v1/') or request.headers.get('Authorization') != 'Bearer '+access:
                return request.fail(401, 'unauthenticated')
            request.path = request.path[len('/session-registry/v1'):]
            request.headers.replace_header('Authorization', 'Bearer fixture-access')
            return original(request)
        handler.handle_request = mounted
        handler.answer = proved_response
        self.write(native('codex'))

    def write(self, rows):
        path = self.config.transcript_root/'main.jsonl'
        path.write_bytes(b''.join(json.dumps(row).encode()+b'\n' for row in rows))
        return path

    def one_pass(self): return run_pass(self.config, boot_clock()+10)

    def test_actual_worker_process_uploads_only_sanitized_bytes_and_acknowledges_view(self):
        self.config.state_root.chmod(0o770)
        runtime = child(self.config.state_root, 'runtime')
        private_json(runtime/'configuration.json', environment(self.config))
        worker = PassProcess(self.config, str(uuid.uuid4()), boot_clock()+10)
        self.assertTrue(worker.reap(10)); self.addCleanup(worker.remove_receipt)
        receipt = worker.receipt()
        self.assertTrue(receipt['ok']); self.assertEqual(receipt['pending'], 0)
        self.assertEqual(len(receipt['manifest']), 1)
        self.assertTrue(self.peer.views)
        for data in self.peer.files.values(): self.assertNotIn(CANARY.encode(), data)
        self.assertNotIn(CANARY, json.dumps(self.peer.sessions)+json.dumps(list(self.peer.views.values())))
        self.assertNotIn('access_token', json.dumps(receipt))
        self.assertFalse(next(iter(self.peer.sessions.values()))['package']['available'])
        self.assertEqual(self.config.state_root.stat().st_mode & 0o777, 0o770)
        self.assertEqual((self.config.state_root/'identity').stat().st_mode & 0o777, 0o700)

    def test_claude_profile_uses_same_actual_runtime_boundary(self):
        self.config = replace(self.config, provider='claude', profile=PROVIDER_PROFILES['claude'])
        self.write(native('claude'))
        self.assertTrue(self.one_pass()['ok'])
        self.assertTrue(all('/claude/' in key for key in self.peer.sessions))
        for data in self.peer.files.values(): self.assertNotIn(CANARY.encode(), data)
        self.assertTrue(self.peer.views)

    def test_antigravity_profile_uploads_redacted_sqlite_view(self):
        from test_antigravity import make_session
        from test_workload_antigravity import add_user_step
        self.config = replace(self.config, provider='antigravity', profile=PROVIDER_PROFILES['antigravity'])
        db = make_session(self.config.transcript_root, str(uuid.uuid4()), self.root)
        add_user_step(db, 2, 'secret ' + CANARY)
        result = self.one_pass()
        self.assertTrue(result['ok']); self.assertEqual(len(result['manifest']), 1)
        self.assertTrue(all('/antigravity/' in key for key in self.peer.sessions))
        for data in self.peer.files.values(): self.assertNotIn(CANARY.encode(), data)
        self.assertTrue(self.peer.views)
        self.assertFalse(next(iter(self.peer.sessions.values()))['package']['available'])

    def test_append_rewrite_lost_view_ack_and_restart_have_no_duplicate_or_raw_offset(self):
        self.peer.drop_view_once = True
        result = self.one_pass(); self.assertFalse(result['ok'])
        result = self.one_pass(); self.assertTrue(result['ok'])
        rows = native('codex'); rows.append({'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': 'second'}})
        self.write(rows); self.assertTrue(self.one_pass()['ok'])
        views = list(self.peer.views.values())[-1]
        self.assertEqual(sum(row['text'] == 'second' for row in views), 1)
        rows[-1]['payload']['content'] = 'rewrite'; self.write(rows)
        self.assertTrue(self.one_pass()['ok'])
        self.assertGreater(len(self.peer.views), 1)
        for (key, generation), view in self.peer.views.items():
            self.assertEqual([row['seq'] for row in view], list(range(1, len(view)+1)))

    def test_partial_tail_append_during_ack_and_disappeared_sources_never_claim_complete(self):
        path = self.config.transcript_root/'main.jsonl'
        original = path.read_bytes(); path.write_bytes(original+b'{"incomplete":')
        with self.assertRaisesRegex(SyncError, '^workload_source_incomplete$'): self.one_pass()
        path.write_bytes(original); self.assertTrue(self.one_pass()['ok'])
        path.unlink()
        with self.assertRaisesRegex(SyncError, '^workload_source_disappeared$'): self.one_pass()

    def test_source_append_after_capture_but_before_ack_refuses_final_receipt(self):
        from concurrent.futures import ThreadPoolExecutor
        self.peer.first_chunk, self.peer.release = threading.Event(), threading.Event()
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(self.one_pass)
                self.assertTrue(self.peer.first_chunk.wait(5))
                extra = {'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': 'late complete record'}}
                with (self.config.transcript_root/'main.jsonl').open('ab') as stream:
                    stream.write(json.dumps(extra).encode()+b'\n')
                self.peer.release.set()
                with self.assertRaisesRegex(SyncError, '^source_changed$'): future.result(timeout=5)
        finally: self.peer.release.set()
        self.peer.first_chunk = None
        self.assertTrue(self.one_pass()['ok'])

    def test_other_approved_identity_cannot_reuse_old_ack_journal(self):
        self.assertTrue(self.one_pass()['ok'])
        path = self.config.state_root/'auth/workload-auth.json'
        auth = read_record(path); auth['account_id'] = 'f'*24
        private_json(path, auth)
        before = len(self.peer.calls)
        with self.assertRaisesRegex(SyncError, '^workload_runtime_binding_changed$'): self.one_pass()
        self.assertEqual(before, len(self.peer.calls))

    def test_populated_journal_without_binding_is_not_adopted(self):
        sync = child(self.config.state_root, 'sync')
        state = State(sync)
        state.save_session('codex', SID, {'status': 'synced', 'files': {}}); state.close()
        with self.assertRaisesRegex(SyncError, '^workload_runtime_binding_changed$'): self.one_pass()
        self.assertEqual(self.peer.calls, [])

    def test_durable_rotation_progresses_beyond_a_repeated_first_item_failure(self):
        for i in range(31): (self.config.transcript_root/f'{i:02}.jsonl').write_bytes(b'fixture')
        observed = []
        def cut_after_first(collector, **kwargs):
            observed.append(collector.providers.sources[0][1])
            raise SyncError('registry_unavailable')
        with patch('csl_workload_runner.Collector.run', autospec=True, side_effect=cut_after_first):
            for _ in range(33):
                with self.assertRaisesRegex(SyncError, '^registry_unavailable$'): self.one_pass()
        self.assertEqual(set(observed), set(sources(self.config.transcript_root)))
        self.assertEqual(observed[0], observed[32])
        self.assertEqual(self.peer.calls, [])

    def test_output_and_view_loss_resume_instead_of_reporting_final_success(self):
        self.peer.drop_once = True
        self.assertFalse(self.one_pass()['ok']); self.assertTrue(self.one_pass()['ok'])
        for data in self.peer.files.values():
            self.assertNotIn(CANARY.encode(), data)
        requested = self.peer.upload_requests
        self.assertTrue(requested)
        self.assertEqual(len(self.peer.files), 1)

    def test_auth_absent_or_source_profile_invalid_has_no_upload_fallback(self):
        path = self.config.state_root/'auth/workload-auth.json'
        path.unlink()
        with self.assertRaisesRegex(SyncError, '^workload_approval_required$'): self.one_pass()
        self.assertEqual(self.peer.calls, [])
        self.assertEqual(self.peer.sessions, {})

    def test_real_literal_writer_then_final_content_and_view_ack(self):
        argv_file = self.root/'arguments.json'
        command = [sys.executable, '-S', '-c',
            'import json,sys;from pathlib import Path;Path(sys.argv[1]).write_text(json.dumps(sys.argv[2:]));',
            str(argv_file), 'space value', ';no-shell', '$(no-exec)']
        result = Runner(replace(self.config, flush_seconds=5)).run(command)
        self.assertTrue(result['complete'], result); self.assertFalse(result['pending'])
        self.assertTrue(result['producer_quiesced']); self.assertEqual(result['writer_exit'], 0)
        self.assertEqual(json.loads(argv_file.read_text()), command[5:])
        self.assertTrue(self.peer.views)

    def test_stalled_upload_final_deadline_is_pending_and_next_run_reconciles(self):
        gate = threading.Event()
        final_started = []
        def launch(config, sequence, deadline):
            claim = read_record(config.state_root/'runtime/producer.json')
            if claim and claim.get('phase') == 'stopped':
                final_started.append(sequence)
                self.proof_seen.clear()
                self.proof_gate = gate
            return PassProcess(config, sequence, deadline)
        command = [sys.executable, '-S', '-c', 'pass']
        started = boot_clock()
        try:
            result = Runner(replace(self.config, flush_seconds=3), pass_factory=launch).run(command)
            self.assertFalse(result['complete']); self.assertTrue(result['pending'])
            self.assertTrue(final_started); self.assertTrue(self.proof_seen.is_set())
            self.assertLess(boot_clock()-started, 5)
        finally: gate.set()
        self.proof_gate = None
        self.assertTrue(self.one_pass()['ok'])

    def test_interrupted_live_content_can_reconcile_and_complete_in_final_budget(self):
        self.peer.first_chunk, self.peer.release = threading.Event(), threading.Event()
        exit_gate = self.root/'writer-exit'
        observed = []
        def finish_writer():
            observed.append(self.peer.first_chunk.wait(5)); exit_gate.touch()
        helper = threading.Thread(target=finish_writer); helper.start()
        command = [sys.executable, '-S', '-c',
            'import sys,time;from pathlib import Path;end=time.monotonic()+6;'
            '\nwhile not Path(sys.argv[1]).exists() and time.monotonic()<end: time.sleep(.01)', str(exit_gate)]
        try:
            result = Runner(replace(self.config, flush_seconds=5)).run(command)
            self.assertTrue(result['complete'], result)
            self.assertEqual(observed, [True])
        finally: self.peer.release.set(); exit_gate.touch(); helper.join(6)
        self.peer.first_chunk = None

    def test_post_tls_suspend_refuses_headers_and_body_before_peer_contact(self):
        clock = Clock()
        connect = http.client.HTTPSConnection.connect
        def jumped(connection):
            connect(connection); clock.now += 100
        with patch('csl_workload_runner.boot_clock', clock):
            client = DeadlineHTTP(self.config.origin, clock()+2, ca_file=self.config.ca_file, attempts=1)
            with patch.object(http.client.HTTPSConnection, 'connect', jumped):
                with self.assertRaisesRegex(SyncError, '^workload_flush_deadline$'):
                    client.request(self.config.origin+'/session-registry/v1/devices/ses_fixture', 'PUT',
                        data=b'{}', headers={'Authorization': 'Bearer '+self.access})
        self.assertEqual(self.peer.calls, [])

    def test_suspend_during_response_read_cannot_accept_success(self):
        clock = Clock()
        read = http.client.HTTPResponse.read
        def jumped(response, *args, **kwargs):
            result = read(response, *args, **kwargs); clock.now += 100
            return result
        with patch('csl_workload_runner.boot_clock', clock):
            client = DeadlineHTTP(self.config.origin, clock()+2, ca_file=self.config.ca_file, attempts=1)
            with patch.object(http.client.HTTPResponse, 'read', jumped):
                with self.assertRaisesRegex(SyncError, '^workload_flush_deadline$'):
                    client.request(self.config.origin+'/session-registry/v1/limits',
                                   headers={'Authorization': 'Bearer '+self.access})
        self.assertEqual(len(self.peer.calls), 1)

    def test_unchanged_empty_missing_mismatched_and_negative_final_proofs(self):
        self.assertTrue(self.one_pass()['ok'])
        before = len(self.peer.calls)
        self.assertTrue(self.one_pass()['ok'])
        self.assertEqual(sum(method == 'PUT' and path == '/sessions/codex/'+SID
                             for method, path in self.peer.calls[before:]), 2)
        for mutate in (lambda value: value.pop('sync'), lambda value: value.update(sync={'complete': False}),
                       lambda value: value.update(snapshot_id='not-the-plan'),
                       lambda value: value['view'].update(generation='another-generation'),
                       lambda value: value['view'].update(last_seq=1000)):
            self.proof_mutation = mutate
            with self.assertRaisesRegex(SyncError, '^workload_final_proof_refused$'): self.one_pass()
        self.proof_mutation = None
        self.write([native('codex')[0]])
        self.assertTrue(self.one_pass()['ok'])
        plan = next(iter(self.peer.sessions.values()))
        self.assertEqual(plan['view']['last_seq'], 0)
        self.assertTrue(self.one_pass()['ok'])

    def test_lost_fresh_final_proof_is_pending_and_restart_uses_same_plan(self):
        self.assertTrue(self.one_pass()['ok'])
        plan = copy.deepcopy(next(iter(self.peer.sessions.values())))
        calls = [0]
        def lost(request, value):
            calls[0] += 1
            if calls[0] == 2:
                request.connection.shutdown(2); request.connection.close()
                return False
            return True
        self.proof_transport = lost
        # Use the subprocess boundary: failures are private pending receipts.
        runtime = child(self.config.state_root, 'runtime')
        private_json(runtime/'configuration.json', environment(self.config))
        worker = PassProcess(self.config, str(uuid.uuid4()), boot_clock()+5)
        try:
            self.assertTrue(worker.reap(6)); self.assertFalse(worker.receipt()['ok'])
        finally: worker.kill(); worker.reap(1); worker.remove_receipt()
        self.assertEqual(calls[0], 2)
        self.proof_transport = None
        self.assertTrue(self.one_pass()['ok'])
        self.assertEqual(next(iter(self.peer.sessions.values()))['snapshot_id'], plan['snapshot_id'])
        self.assertEqual(next(iter(self.peer.sessions.values()))['view'], plan['view'])

    def test_projection_only_unbound_cache_is_refused_and_preserved(self):
        projection = child(self.config.state_root, 'projection')
        (projection/'streams').mkdir()
        path = projection/'streams/original'; path.write_bytes(b'original private projection'); path.chmod(0o600)
        with self.assertRaisesRegex(SyncError, '^workload_runtime_binding_changed$'): self.one_pass()
        self.assertEqual(self.peer.calls, [])
        self.assertEqual(path.read_bytes(), b'original private projection')

    @unittest.skipUnless(hasattr(os, 'pidfd_open'), 'Linux pidfd for exact owned worker lifetime')
    def test_worker_dies_with_parent_and_restart_reconciles_uncertain_upload(self):
        runtime = child(self.config.state_root, 'runtime')
        private_json(runtime/'configuration.json', environment(self.config))
        pid_file, release = self.root/'worker.pid', self.root/'parent-release'
        source = Path(__file__).parents[1]
        script = ('import json,os,sys,time;from pathlib import Path;'
            'sys.path.insert(0,sys.argv[1]);'
            'from csl_workload_config import Configuration;'
            'from csl_workload_runner import PassProcess,read_record;'
            'from csl_workload_auth import boot_clock;'
            'import uuid;'
            'config=Configuration.from_environment(read_record(Path(sys.argv[2])));'
            'worker=PassProcess(config,str(uuid.uuid4()),boot_clock()+20);'
            'Path(sys.argv[3]).write_text(str(worker.process.pid));'
            '\nwhile not Path(sys.argv[4]).exists(): time.sleep(.01)\n'
            'os._exit(0)')
        self.peer.first_chunk, self.peer.release = threading.Event(), threading.Event()
        parent = subprocess.Popen([sys.executable, '-S', '-c', script, str(source),
            str(runtime/'configuration.json'), str(pid_file), str(release)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={'PATH': os.defpath, 'HOME': str(self.root)})
        pidfd = None
        try:
            self.assertTrue(self.peer.first_chunk.wait(5))
            pidfd = os.pidfd_open(int(pid_file.read_text()))
            release.touch(); parent.wait(timeout=3)
            poll = select.poll(); poll.register(pidfd, select.POLLIN)
            self.assertTrue(poll.poll(3000), 'owned worker survived parent death')
        finally:
            release.touch(); self.peer.release.set()
            if pidfd is not None: os.close(pidfd)
            if parent.poll() is None: parent.kill(); parent.wait(timeout=2)
        self.peer.first_chunk = None
        self.assertTrue(self.one_pass()['ok'])

    def test_real_watch_updates_complete_records_then_sigterm_flushes_before_return(self):
        timing = self.root/'writer-times.json'
        template = self.root/'writer-input.json'
        template.write_text(json.dumps([{'type': 'response_item', 'payload': {
            'type': 'message', 'role': 'user', 'content': 'marker-'+str(i)}} for i in range(3)]))
        script = ('import json,sys,time;from pathlib import Path;'
            'rows=json.loads(Path(sys.argv[1]).read_text());times=[];'
            '\nfor row in rows:\n'
            ' with Path(sys.argv[2]).open("ab") as out: out.write(json.dumps(row).encode()+b"\\n");out.flush()\n'
            ' times.append(time.time());Path(sys.argv[3]).write_text(json.dumps(times));time.sleep(.5)\n'
            'time.sleep(6)')
        config = replace(self.config, flush_seconds=4)
        command = [sys.executable, '-S', str(Path(__file__).parents[1]/'csl_workload_runner.py'), '--',
            sys.executable, '-S', '-c', script, str(template), str(config.transcript_root/'main.jsonl'), str(timing)]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env={'PATH': os.defpath, 'HOME': str(self.root), **environment(config)})
        observed = {}
        try:
            until = boot_clock()+6
            while boot_clock() < until and process.poll() is None:
                for view in list(self.peer.views.values()):
                    for row in list(view):
                        if row['text'].startswith('marker-'): observed.setdefault(row['text'], time.time())
                if len(observed) == 3: break
                time.sleep(.02)
            self.assertEqual(len(observed), 3)
            process.send_signal(signal.SIGTERM)
            output = process.communicate(timeout=8)[0]
            result = json.loads(output)
            self.assertTrue(result['complete'], result)
            self.assertEqual(result['writer_exit'], -signal.SIGTERM)
            self.assertEqual(process.returncode, 143)
            self.assertNotIn(CANARY.encode(), output)
            for i, written in enumerate(json.loads(timing.read_text())):
                self.assertLess(observed['marker-'+str(i)]-written, 3)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try: process.communicate(timeout=12)
                except subprocess.TimeoutExpired: process.kill(); process.communicate(timeout=2)

    @unittest.skipUnless(hasattr(os, 'pidfd_open'), 'Linux pidfd for owned producer crash proof')
    def test_supervisor_crash_stops_original_group_before_restart_close(self):
        root_pid, child_pid = self.root/'root.pid', self.root/'child.pid'
        release, counter = self.root/'release', self.root/'launches'
        source_path = self.config.transcript_root/'main.jsonl'
        child_script = ('import json,os,sys,time;from pathlib import Path;'
            'Path(sys.argv[1]).write_text(str(os.getpid()));end=time.monotonic()+15;'
            '\nwhile not Path(sys.argv[2]).exists() and time.monotonic()<end: time.sleep(.01)\n'
            'with Path(sys.argv[3]).open("ab") as out: out.write(json.dumps({"type":"response_item",'
            '"payload":{"type":"message","role":"user","content":"late child write"}}).encode()+b"\\n")')
        root_script = ('import os,sys,time,subprocess;from pathlib import Path;'
            'Path(sys.argv[1]).write_text(str(os.getpid()));'
            'with_counter=Path(sys.argv[2]);with_counter.write_text(with_counter.read_text()+"launch\\n" if with_counter.exists() else "launch\\n");'
            'subprocess.Popen([sys.executable,"-S","-c",sys.argv[3],*sys.argv[4:]]);'
            'time.sleep(15)')
        script = Path(__file__).parents[1]/'csl_workload_runner.py'
        command = [sys.executable, '-S', str(script), '--', sys.executable, '-S', '-c', root_script,
            str(root_pid), str(counter), child_script, str(child_pid), str(release), str(source_path)]
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env={'PATH': os.defpath, 'HOME': str(self.root), **environment(self.config)})
        descriptors = []
        guardian_fd = None
        try:
            until = boot_clock()+6
            while not child_pid.exists() and boot_clock() < until and process.poll() is None: time.sleep(.01)
            self.assertTrue(child_pid.exists())
            descriptors = [os.pidfd_open(int(path.read_text())) for path in (root_pid, child_pid)]
            # The native child could run only after its guardian's running record
            # was durable. Retain that original guardian identity before killing Runner.
            guardian_fd = os.pidfd_open(read_record(self.config.state_root/'runtime/producer.json')['guardian_pid'])
            process.kill(); process.wait(timeout=2)
            # An immediate restart either refuses the live claim or observes its
            # completed guardian stop. It never relaunches the original argv.
            try: result = Runner(replace(self.config, flush_seconds=5)).run([sys.executable, '-S', '-c', 'pass'])
            except SyncError as error:
                self.assertEqual(error.code, 'workload_producer_outcome_unknown'); result = {'complete': False}
            if result['complete']:
                for fd in descriptors:
                    poll = select.poll(); poll.register(fd, select.POLLIN); self.assertTrue(poll.poll(0))
            for fd in descriptors:
                poll = select.poll(); poll.register(fd, select.POLLIN); self.assertTrue(poll.poll(6000))
            stopped_record_after_guardian_exit(self.config.state_root/'runtime/producer.json', guardian_fd, boot_clock()+3)
            before = source_path.read_bytes(); release.touch()
            result = Runner(replace(self.config, flush_seconds=5)).run([sys.executable, '-S', '-c', 'pass'])
            self.assertTrue(result['complete'], result)
            self.assertEqual(source_path.read_bytes(), before)
            self.assertEqual(counter.read_text().splitlines(), ['launch'])
        finally:
            release.touch()
            if process.poll() is None: process.kill(); process.wait(timeout=2)
            for fd in descriptors:
                try: signal.pidfd_send_signal(fd, signal.SIGKILL)
                except ProcessLookupError: pass
                os.close(fd)
            if guardian_fd is not None:
                try: signal.pidfd_send_signal(guardian_fd, signal.SIGKILL)
                except ProcessLookupError: pass
                os.close(guardian_fd)


if __name__ == '__main__': unittest.main()


class KeepAliveTransportTests(unittest.TestCase):
    def test_one_tls_connection_is_reused_within_a_pass_and_errors_drop_it(self):
        import http.server
        tmp = tempfile.TemporaryDirectory(prefix='workload-keepalive-'); self.addCleanup(tmp.cleanup)
        root = Path(tmp.name); cert, tls_key = root/'ca.crt', root/'tls.pem'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
            '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost', '-keyout', str(tls_key), '-out', str(cert)],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
        connections = []

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def setup(self):
                super().setup(); connections.append(self.client_address)
            def log_message(self, *args): pass
            def do_GET(self):
                self.rfile.read(int(self.headers.get('Content-Length') or 0))
                status, body = (404, b'{"error":{"code":"not_found"}}') if self.path.endswith('/missing') else (200, b'{"ok":true}')
                self.send_response(status); self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)
            do_PUT = do_GET

        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); tls.load_cert_chain(cert, tls_key)
        server.socket = tls.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        origin = 'https://localhost:' + str(server.server_port)
        client = DeadlineHTTP(origin, boot_clock()+30, ca_file=str(cert), attempts=1)
        base = origin + '/session-registry/v1'
        for path in ('/a', '/b', '/c'):
            self.assertEqual(client.request(base + path, 'PUT', data=b'{}'), {'ok': True})
        self.assertEqual(len(connections), 1)
        with self.assertRaises(SyncError) as refused:
            client.request(base + '/missing')
        self.assertEqual(refused.exception.code, 'not_found')
        self.assertEqual(client.request(base + '/d'), {'ok': True})
        self.assertEqual(len(connections), 2)
        with self.assertRaises(SyncError):
            client.request('https://127.0.0.1:' + str(server.server_port) + '/session-registry/v1/other-host')


class EnrollCommandTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='workload-enroll-'); self.addCleanup(self.tmp.cleanup)
        self.config = config_at(Path(self.tmp.name))

    def run_enroll(self, tokens):
        import io as _io
        import csl_workload_runner as module
        out = _io.StringIO()
        with patch.object(module, 'WorkloadTokens', lambda *a, **k: tokens):
            code = module.enroll(self.config, out=out, sleep=lambda _: None)
        return code, [json.loads(line) for line in out.getvalue().splitlines()]

    def test_prints_only_public_consent_then_ready_and_never_tokens(self):
        polls = iter([{'state': 'pending'}, {'state': 'ready'}])
        tokens = Mock()
        tokens.status.side_effect = [{'state': 'signed_out', 'account_id': None, 'device_id': None},
                                     {'state': 'ready', 'account_id': 'a'*24, 'device_id': 'ses_x'}]
        tokens.begin.return_value = {'user_code': 'ABCD-EFGH', 'verification_uri': 'https://x/device', 'expires_in': 600, 'interval': 5}
        tokens.poll.side_effect = lambda: next(polls)
        code, lines = self.run_enroll(tokens)
        self.assertEqual(code, 0)
        self.assertEqual(lines[0], {'consent': tokens.begin.return_value})
        self.assertEqual(lines[-1], {'state': 'ready', 'account_id': 'a'*24, 'device_id': 'ses_x'})
        self.assertNotIn('token', json.dumps(lines).replace('user_code', ''))

    def test_refusals_report_the_code_and_ready_state_does_not_begin_again(self):
        tokens = Mock(); tokens.status.return_value = {'state': 'signed_out', 'account_id': None, 'device_id': None}
        tokens.begin.side_effect = SyncError('http_502')
        self.assertEqual(self.run_enroll(tokens), (2, [{'state': 'refused', 'code': 'http_502'}]))
        tokens = Mock(); tokens.status.return_value = {'state': 'signed_out', 'account_id': None, 'device_id': None}
        tokens.begin.return_value = {'user_code': 'ABCD-EFGH', 'verification_uri': 'u', 'expires_in': 600, 'interval': 5}
        tokens.poll.side_effect = SyncError('workload_approval_expired')
        self.assertEqual(self.run_enroll(tokens)[1][-1], {'state': 'refused', 'code': 'workload_approval_expired'})
        tokens = Mock(); tokens.status.return_value = {'state': 'ready', 'account_id': 'a', 'device_id': 'd'}
        self.assertEqual(self.run_enroll(tokens), (0, [{'state': 'ready'}]))
        tokens.begin.assert_not_called()

"""Local-only process, deadline and actual sanitized-uploader fixtures."""
import copy
from dataclasses import replace
import hashlib
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
from sync_support import Peer
from test_workload_text import CANARY, SID, native
from test_workload_gemini import records as gemini_records, CANARY as GEMINI_CANARY


def config_at(root):
    for name in ('transcripts', 'state'): (root/name).mkdir(exist_ok=True)
    return Configuration('https://identity.example.test', 'codex', PROVIDER_PROFILES['codex'],
        root/'transcripts', root/'state', {'label': 'synthetic collector', 'provider': 'kubernetes',
        'cluster': 'kind', 'namespace': 'test', 'workload': 'fixture', 'replica': 'replica-1'},
        2, 128*1024*1024, None)


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
        clock = Clock(); writer = Mock(pid=123456, returncode=7); writer.poll.return_value = 7
        worker = FakePass(); calls = []
        def launch(*args): calls.append(args); return worker
        runner = Runner(self.config, clock=clock, sleep=clock.sleep, pass_factory=launch)
        command = ['/literal/program', 'space value', ';not-a-shell', '$(not-executed)']
        with patch('csl_workload_runner.subprocess.Popen', return_value=writer) as popen, \
             patch('csl_workload_runner.group_exists', return_value=False), \
             patch('csl_workload_runner.signal_group'):
            result = runner.run(command)
        self.assertTrue(result['complete']); self.assertFalse(result['pending'])
        self.assertEqual(result['writer_exit'], 7); self.assertEqual(len(calls), 1)
        popen.assert_called_once()
        self.assertEqual(popen.call_args.args[0], command)
        self.assertNotIn('shell', popen.call_args.kwargs)

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
        with patch('csl_workload_runner.subprocess.Popen', return_value=writer), \
             patch('csl_workload_runner.signal_group') as signaled:
            self.assertTrue(runner.run(['/literal/command'])['pending'])
        signaled.assert_called_once_with(writer.pid, signal.SIGKILL)
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

    def test_worker_refuses_wrong_parent_lifetime(self):
        # Do not change this test process's PDEATHSIG; exercise a synthetic libc.
        libc = Mock(); libc.prctl.return_value = 0
        with patch('csl_workload_runner.ctypes.CDLL', return_value=libc), \
             patch('csl_workload_runner.os.getppid', return_value=123):
            with self.assertRaisesRegex(SyncError, '^workload_parent_unavailable$'): parent_death(124)

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
        def mounted(request):
            if not request.path.startswith('/session-registry/v1/') or request.headers.get('Authorization') != 'Bearer '+access:
                return request.fail(401, 'unauthenticated')
            request.path = request.path[len('/session-registry/v1'):]
            request.headers.replace_header('Authorization', 'Bearer fixture-access')
            return original(request)
        handler.handle_request = mounted
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

    def test_gemini_profile_is_distinct_and_uses_sanitized_runtime_boundary(self):
        self.config = replace(self.config, provider='gemini', profile=PROVIDER_PROFILES['gemini'])
        self.write(gemini_records())
        self.assertTrue(self.one_pass()['ok'])
        self.assertTrue(all('/gemini/' in key for key in self.peer.sessions))
        for data in self.peer.files.values(): self.assertNotIn(GEMINI_CANARY.encode(), data)
        self.assertTrue(self.peer.views)

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
        self.peer.first_chunk, self.peer.release = threading.Event(), threading.Event()
        command = [sys.executable, '-S', '-c', 'pass']
        started = boot_clock()
        try:
            result = Runner(replace(self.config, flush_seconds=1)).run(command)
            self.assertFalse(result['complete']); self.assertTrue(result['pending'])
            self.assertLess(boot_clock()-started, 3)
        finally: self.peer.release.set()
        self.peer.first_chunk = None
        self.assertTrue(self.one_pass()['ok'])

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


if __name__ == '__main__': unittest.main()

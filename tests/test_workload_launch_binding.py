"""Owned native-shaped records/guardian/TLS workers, NOT a native or A8 run."""
import copy
from contextlib import nullcontext
from dataclasses import replace
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from csl_workload_auth import boot_clock
from csl_workload_launch_binding import LaunchAcceptanceDriver, PreparedLaunch
from csl_workload_observation_driver import runtime_fingerprint
from csl_workload_producer import Producer
from csl_workload_runner import Runner, PassProcess, scope
import test_workload_runner as fixture


class LaunchBindingTests(unittest.TestCase):
    setUp = fixture.UploaderTests.setUp
    write = fixture.UploaderTests.write

    def joined(self, *, extra_source=False, absent_source=False, wrong_kernel=False,
               replace_root=False, replace_inode=False, mutate_final=None, ready=True, stalled=False,
               corrupt=False, resource=None, cancelled=False):
        self.config = replace(self.config, observe_session=None, flush_seconds=5)
        root = self.config.transcript_root
        (root/'main.jsonl').unlink()
        lease = PreparedLaunch()
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd); views = ((info.st_dev, info.st_ino),)*2
            lease.capture(fd, views)
        finally: os.close(fd)
        rows = fixture.native('codex')
        if not absent_source: rows[0]['payload']['source'] = 'exec'
        if corrupt: rows.append({'type': 'unrecognized-native-control', 'payload': {}})
        payload = self.root/'owned-fixture-input.json'
        payload.write_text(json.dumps(rows))
        exit_gate = self.root/'owned-producer-exit'
        program = ('import json,sys,time;from pathlib import Path;'
            'p=Path(sys.argv[1]); rows=json.loads(Path(sys.argv[2]).read_text());'
            'p.write_bytes(b"".join(json.dumps(r).encode()+b"\\n" for r in rows));'
            'end=time.monotonic()+8;'
            '\nwhile not Path(sys.argv[3]).exists() and time.monotonic()<end: time.sleep(.01)')
        command = [sys.executable, '-S', '-c', program, str(root/'not-the-uuid.jsonl'), str(payload), str(exit_gate)]
        lease.handoff(views, command)
        if replace_root:
            root.rename(self.root/'retained-old-root')
            root.mkdir()
        if extra_source:
            other = copy.deepcopy(rows); other[0]['payload']['id'] = str(uuid.uuid4())
            (root/'second.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in other))
        driver = LaunchAcceptanceDriver(source=runtime_fingerprint(), scope=scope(self.config),
            account_id='1234567890abcdef12345678', device_id='ses_fixture', prepared=lease)
        driver.prepare(self.config, scope(self.config))
        until = boot_clock()+3
        while driver.valid and not driver.launch_prepared and boot_clock() < until:
            driver.poll(); time.sleep(.001)  # Explicit fixture preparation, not Runner.
        self.assertTrue(driver.launch_prepared)
        if cancelled: driver.invalidate()
        receipts = []
        entered, release = threading.Event(), threading.Event()
        fstat = os.fstat
        def stalled_stat(fd):
            if stalled and threading.current_thread() is driver.binder:
                entered.set(); release.wait(40)
            return fstat(fd)
        real_popen = subprocess.Popen
        def launch(config, sequence, deadline, *, composition=None):
            # Synthetic sole-producer fixture is ordered before its first pass.
            until = min(deadline, boot_clock()+2)
            while not (root/'not-the-uuid.jsonl').exists() and boot_clock() < until: time.sleep(.001)
            def process(argv, **kwargs):
                if len(argv) >= 4 and argv[3] == '--worker':
                    argv = [argv[0], '-B', '-S', str(Path(__file__).with_name('launch_worker_fixture.py')),
                            *argv[4:], 'ready-fixture' if ready else 'no-wait']
                return real_popen(argv, **kwargs)
            with patch('csl_workload_runner.subprocess.Popen', side_effect=process):
                worker = PassProcess(config, sequence, deadline, composition=composition)
            original = worker.receipt
            def receipt():
                value = original()
                final = exit_gate.exists()
                if value and value.get('observation_pending'):
                    until = min(deadline, boot_clock()+1)
                    while driver.valid and boot_clock() < until:
                        driver.poll()
                        if value['observation_pending'] in driver.finals: break
                        time.sleep(.001)  # Fixture-only finished-seal availability.
                if final and mutate_final is not None: value = mutate_final(value)
                receipts.append(copy.deepcopy(value))
                if not final:
                    if replace_inode:
                        path = root/'not-the-uuid.jsonl'
                        data = path.read_bytes()
                        path.rename(self.root/'retired-owned-source.jsonl')
                        path.write_bytes(data)
                    exit_gate.touch()
                return value
            worker.receipt = receipt
            return worker
        original_kernel = Producer.poll_launch_identity
        def invalid_kernel(writer):
            value = original_kernel(writer)
            return {**value, 'run_id': str(uuid.uuid4())} if value is not None else None
        started = boot_clock()
        real_start = threading.Thread.start
        pipe = os.pipe2
        pipe_calls = []
        def failed_carrier(flags):
            pipe_calls.append(flags)
            if len(pipe_calls) == 1: raise OSError('owned carrier allocation failure')
            return pipe(flags)
        def start_thread(thread):
            if thread.name == 'csl-launch-binding': raise RuntimeError('owned binder startup failure')
            return real_start(thread)
        resource_control = (patch('csl_workload_launch_binding.socket.socketpair', side_effect=OSError('owned socket failure'))
                            if resource == 'socket' else patch.object(threading.Thread, 'start', start_thread)
                            if resource == 'thread' else patch('csl_workload_producer.os.pipe2', side_effect=failed_carrier)
                            if resource == 'pipe' else nullcontext())
        try:
            with patch.object(Producer, 'poll_launch_identity', invalid_kernel if wrong_kernel else original_kernel), \
                 patch('csl_workload_launch_binding.os.fstat', side_effect=stalled_stat), resource_control:
                result = Runner(self.config, observation_driver=driver, pass_factory=launch).run(command)
            elapsed = boot_clock()-started
        finally:
            release.set()
            exit_gate.touch(); driver.close()
            if driver.binder is not None and driver.binder.ident is not None: driver.binder.join(2)
            if driver.agent is not None:
                driver.agent.launcher.join(2)
                if driver.agent.instance is not None: driver.agent.process.wait(timeout=2)
        self.assertEqual(result['complete'], not corrupt, result)
        self.assertTrue(result['producer_quiesced']); self.assertTrue(result['worker_reaped'])
        self.assertEqual(result['writer_exit'], 0)
        self.assertNotIn('observation', result)
        self.assertIsNone(self.config.observe_session)
        if not corrupt: self.assertTrue(self.peer.views)
        if stalled:
            self.assertTrue(entered.is_set())
            self.assertLess(elapsed, 5)
            self.assertFalse(driver.binder.is_alive())
            print(json.dumps({'fixture': 'launch_binder_storage_stall', 'elapsed_seconds': round(elapsed, 3),
                              'evidence': driver.result, 'producer_quiesced': result['producer_quiesced']}))
        return driver, receipts

    def test_actual_parent_provider_and_first_put_bind_without_preselected_uuid(self):
        driver, receipts = self.joined()
        self.assertIsNotNone(driver.result)
        self.assertEqual(driver.birth['session'], fixture.SID)
        self.assertEqual(driver.birth['relative'], 'not-the-uuid.jsonl')
        self.assertEqual(driver.kernel['run_id'], driver.original_run)
        self.assertEqual(driver.kernel['guardian_pid'], driver.original_guardian)
        self.assertNotEqual(driver.kernel['guardian_pid'], driver.kernel['producer_pid'])
        self.assertTrue(all(value['native_birth'] == driver.birth for value in receipts))
        self.assertEqual(sum(m == 'PUT' and p.endswith('/content') for m, p in self.peer.calls), 1)
        self.assertEqual(sum(m == 'PUT' and p.endswith('/view') for m, p in self.peer.calls), 1)

    def test_multiple_qualified_sources_cannot_choose_first_uuid(self):
        driver, _ = self.joined(extra_source=True)
        self.assertIsNone(driver.result); self.assertIsNone(driver.birth)

    def test_profile_compatible_missing_exec_provenance_is_not_birth_authority(self):
        driver, _ = self.joined(absent_source=True)
        self.assertIsNone(driver.result); self.assertIsNone(driver.birth)

    def test_changed_root_is_not_the_held_preparation_root(self):
        driver, _ = self.joined(replace_root=True)
        self.assertIsNone(driver.result); self.assertIsNone(driver.birth)

    def test_same_uuid_in_replaced_native_inode_is_not_adopted(self):
        driver, _ = self.joined(replace_inode=True)
        self.assertIsNotNone(driver.birth)
        self.assertIsNone(driver.result)

    def test_wrong_original_producer_kernel_fact_only_loses_evidence(self):
        driver, _ = self.joined(wrong_kernel=True)
        self.assertIsNone(driver.result); self.assertIsNone(driver.birth)

    def test_final_worker_cannot_replace_parent_selected_birth(self):
        def changed(value):
            value = copy.deepcopy(value)
            if value and value.get('native_birth'):
                value['native_birth']['session'] = str(uuid.uuid4())
            return value
        driver, _ = self.joined(mutate_final=changed)
        self.assertIsNone(driver.result)

    def test_final_receipt_cannot_replace_original_writer_kernel_identity(self):
        def changed(value):
            value = copy.deepcopy(value)
            if value and value.get('native_kernel'):
                value['native_kernel']['producer_start'] += 1
            return value
        driver, _ = self.joined(mutate_final=changed)
        self.assertIsNone(driver.result)

    def test_cancelled_binding_cannot_be_reprepared_or_adopt_a_stale_helper(self):
        driver, _ = self.joined(cancelled=True)
        self.assertIsNone(driver.birth); self.assertIsNone(driver.result)

    def test_stalled_optional_binder_does_not_delay_product_completion(self):
        driver, _ = self.joined(stalled=True, ready=False)
        self.assertIsNone(driver.result)

    def test_unqualified_complete_batch_has_no_session_put_or_observed_uuid(self):
        driver, _ = self.joined(corrupt=True)
        self.assertIsNone(driver.birth); self.assertIsNone(driver.result)
        self.assertFalse(any(m == 'PUT' and p.startswith('/sessions/') for m, p in self.peer.calls))

    def test_socket_exhaustion_only_loses_evidence_not_delivery(self):
        driver, _ = self.joined(resource='socket')
        self.assertIsNone(driver.result)

    def test_binder_thread_start_failure_only_loses_evidence_not_delivery(self):
        driver, _ = self.joined(resource='thread')
        self.assertIsNone(driver.result)

    def test_missing_carrier_pipe_does_not_change_delivery_or_retry_producer(self):
        driver, _ = self.joined(resource='pipe', ready=False)
        self.assertIsNone(driver.kernel); self.assertIsNone(driver.result)


class LaunchCarrierTests(unittest.TestCase):
    def test_actual_guardian_pipe_is_original_and_parent_poll_has_no_file_reads(self):
        with tempfile.TemporaryDirectory(prefix='launch-carrier-') as directory:
            writer = Producer(Path(directory), 'owned-fixture', [sys.executable, '-S', '-c', 'pass'], capture_launch=True)
            try:
                writer.wait(timeout=5)
                with patch('csl_workload_producer.read_record', side_effect=AssertionError('optional disk read')):
                    value = writer.poll_launch_identity()
                self.assertEqual(value['run_id'], writer.run_id)
                self.assertEqual(value['boot'], writer.boot)
                self.assertEqual(value['guardian_pid'], writer.process.pid)
                self.assertGreater(value['producer_start'], 0)
            finally:
                writer.stop()
                writer.process.wait(timeout=6)

    def test_unarmed_or_reused_lease_cannot_become_a_launch(self):
        lease = PreparedLaunch()
        self.assertFalse(lease.handoff(((1, 2),)*2, ['literal']))
        lease.close()
        with self.assertRaises(ValueError): lease.capture(0, ((1, 2),)*2)

    def test_packaged_validator_holds_exact_empty_root_and_rejects_existing_source(self):
        from csl_workload_runner import environment
        location = Path(__file__).resolve().parents[1]/'packaging/codex-sole-producer/prepare.py'
        spec = importlib.util.spec_from_file_location('owned_launch_prepare', location)
        gate = importlib.util.module_from_spec(spec); spec.loader.exec_module(gate)
        with tempfile.TemporaryDirectory(prefix='prepared-root-') as directory:
            root = Path(directory); config = fixture.config_at(root)
            home = root/'private'; native = home/'.codex'; sessions = native/'sessions'
            home.mkdir(mode=0o700); native.mkdir(mode=0o700); sessions.mkdir()
            config = replace(config, transcript_root=sessions)
            env = {**environment(config), 'HOME': str(home), 'CODEX_HOME': str(native)}
            lease = PreparedLaunch()
            try:
                with patch.multiple(gate, HOME=home, NATIVE=native, TRANSCRIPTS=sessions, STATE=config.state_root), \
                     patch.object(gate.os.path, 'ismount', return_value=False), \
                     patch.object(gate, 'readonly', side_effect=[False, False, False, True, False]*2):
                    # Mount flags are explicitly modeled, not a real bind mount receipt.
                    views = gate.validate(env, prepared=lease)
                    held = os.fstat(lease.fd)
                    self.assertEqual(views, ((held.st_dev, held.st_ino),)*2)
                    self.assertFalse(lease.armed)
                    (sessions/'prior.jsonl').write_text('synthetic old source')
                    with self.assertRaisesRegex(gate.Refused, 'source not fresh'):
                        gate.validate(env, prepared=lease)
            finally: lease.close()

    def test_lost_optional_root_duplication_is_not_product_validation_failure(self):
        lease = PreparedLaunch()
        with patch('csl_workload_launch_binding.os.dup', side_effect=OSError('owned descriptor exhaustion')):
            lease.capture(7, ((1, 2),)*2)
        self.assertTrue(lease.failed)
        self.assertFalse(lease.handoff(((1, 2),)*2, ['literal']))
        lease.close()

    def test_grant_cannot_adopt_another_session_or_cross_original_deadline(self):
        import socket
        from csl_workload_identity import canonical
        import hashlib
        from csl_workload_launch_binding import LaunchObservation, send
        with tempfile.TemporaryDirectory(prefix='grant-boundary-') as directory:
            config = fixture.config_at(Path(directory))
            binding = {'scope': scope(config), 'account_id': '1234567890abcdef12345678', 'device_id': 'ses_fixture'}
            owner = hashlib.sha256(canonical({k: binding[k] for k in ('account_id', 'device_id')})).hexdigest()
            cases = ((None, 2), ('session', 0), ('run', 0), ('source', 0), ('worker', 0),
                     ('seal', 0), ('deadline', 0), (None, 0))
            for changed_field, now in cases:
                parent, worker = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
                helper, remote = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
                assignment = {'mode': 'fresh-codex', 'worker': str(uuid.uuid4()), 'seal': str(uuid.uuid4()),
                    'launch': str(uuid.uuid4()), 'run': str(uuid.uuid4()), 'source': '1'*64,
                    'deadline': 1, 'scope': binding['scope'], 'binding': owner,
                    'observer_fd': worker.fileno()}
                observed = LaunchObservation(config, assignment, binding, assignment['worker'])
                try:
                    observed.session = fixture.SID
                    observed.birth = {'session': fixture.SID}
                    context = {'run': assignment['run'], 'scope': binding['scope'], 'source': '1'*64,
                        'binding': owner, 'session': fixture.SID}
                    grant = {'context': context, 'worker': assignment['worker'], 'seal': assignment['seal'],
                             'deadline': 1, 'predecessor': '2'*64}
                    if changed_field in ('session', 'run'): context[changed_field] = str(uuid.uuid4())
                    elif changed_field == 'source': context['source'] = '2'*64
                    elif changed_field in ('worker', 'seal'): grant[changed_field] = str(uuid.uuid4())
                    elif changed_field == 'deadline': grant['deadline'] = 2
                    fact = {'run_id': str(uuid.uuid4()), 'boot': str(uuid.uuid4()), 'guardian_pid': 10,
                            'guardian_start': 1, 'producer_pid': 11, 'producer_start': 2}
                    send(parent, {'event': 'grant', 'launch': assignment['launch'], 'birth': observed.birth,
                                  'kernel': fact, 'assignment': grant}, (remote.fileno(),))
                    with patch('csl_workload_launch_binding.boot_clock', return_value=now):
                        if changed_field is None and now == 0: observed.grant()
                        else:
                            with self.assertRaises(ValueError): observed.grant()
                    self.assertEqual(observed.session, fixture.SID)
                finally:
                    observed.close(); parent.close(); worker.close(); helper.close(); remote.close()

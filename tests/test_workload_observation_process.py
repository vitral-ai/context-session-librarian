"""Owned TLS/process fixtures; no actual W7, personal histories or provider calls.

Positive evidence uses explicitly prepared helpers and modeled token-work latency.
Production never waits for that readiness; cold/lagging helpers mean None.
"""
from dataclasses import replace
import copy
import json
from pathlib import Path
import subprocess
import socket
import sys
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from csl_sync_state import private_json
from csl_workload_auth import boot_clock
from csl_workload_observation_driver import AcceptanceDriver, runtime_fingerprint
from csl_workload_observation_agent import AgentProcess, Channel, MAX_PACKET, MAX_EVENTS
from csl_workload_runner import Runner, PassProcess, scope, read_record
import test_workload_runner as fixture


class ProcessObservationTests(unittest.TestCase):
    setUp = fixture.UploaderTests.setUp
    write = fixture.UploaderTests.write
    one_pass = fixture.UploaderTests.one_pass

    def driver(self, **overrides):
        return AcceptanceDriver(**{'source': runtime_fingerprint(), 'scope': scope(self.config),
            'account_id': '1234567890abcdef12345678', 'device_id': 'ses_fixture',
            'session': fixture.SID, **overrides})

    def joined(self, *, mutate=None, driver=None, complete=True, before_launch=None, warm=True,
               padding=True, request_ready=True, padding_live=False, on_worker=None):
        self.config = replace(self.config, observe_session=fixture.SID, flush_seconds=5)
        driver = driver or self.driver()
        if warm:
            driver.prepare(self.config, scope(self.config))
            end = boot_clock()+3
            while driver.valid and not driver.started and boot_clock() < end:
                driver.poll(); time.sleep(.01)  # Fixture setup, NOT Runner/request behavior.
        gate = self.root/'fixture-producer-exit'
        command = [sys.executable, '-S', '-c',
            'import sys,time;from pathlib import Path;end=time.monotonic()+8;'
            '\nwhile not Path(sys.argv[1]).exists() and time.monotonic()<end: time.sleep(.01)', str(gate)]
        receipts, stops, requested = [], [], []
        real_popen = subprocess.Popen
        def launch(config, sequence, deadline, *, composition=None):
            final = read_record(config.state_root/'runtime/producer.json')['phase'] == 'stopped'
            if final: stops.append(boot_clock())
            if before_launch is not None: before_launch(final, driver)
            def fixture_worker(argv, **kwargs):
                if len(argv) >= 4 and argv[3] == '--worker':
                    argv = [argv[0], '-B', '-S', str(Path(__file__).with_name('observation_worker_fixture.py')),
                            *argv[4:], 'ready-fixture' if request_ready else 'latency-fixture']
                return real_popen(argv, **kwargs)
            with patch('csl_workload_runner.subprocess.Popen', side_effect=fixture_worker):
                worker = PassProcess(config, sequence, deadline, composition=composition)
            if on_worker is not None: on_worker(final, worker)
            original = worker.receipt
            def received():
                result = original()
                # Positive fixture controls readiness separately from the real
                # Runner. Stalled/liveness controls explicitly disable this.
                if (padding or padding_live and not final) and result and result.get('observation_pending'):
                    end = min(deadline, boot_clock()+1)
                    while driver.valid and boot_clock() < end:
                        driver.poll()
                        if result['observation_pending'] in driver.finals: break
                        time.sleep(.001)
                receipts.append((final, copy.deepcopy(result), worker.path))
                if mutate is not None: result = mutate(final, result, worker, driver)
                if not final: requested.append(boot_clock()); gate.touch()
                return result
            worker.receipt = received
            return worker
        runner = Runner(self.config, observation_driver=driver, pass_factory=launch)
        consumed, checks = driver.consume, []
        def consume(*args, **kwargs):
            checks.append(receipts[-1][2].exists())
            return consumed(*args, **kwargs)
        started = boot_clock()
        try:
            with patch.object(driver, 'consume', side_effect=consume): result = runner.run(command)
        finally:
            gate.touch()
            driver.close()
            if driver.agent is not None:
                driver.agent.launcher.join(2)
                if driver.agent.instance is not None: driver.agent.process.wait(timeout=2)
        self.assertEqual(result['complete'], complete, result)
        self.assertEqual(result['pending'], not complete, result)
        self.assertTrue(result['worker_reaped']); self.assertTrue(result['producer_quiesced'])
        self.assertEqual(result['writer_exit'], 0)
        self.assertEqual(checks, [True] if complete else [])
        self.assertTrue(any(not final for final, *_ in receipts))
        self.assertTrue(all(not path.exists() for _, _, path in receipts))
        self.assertNotIn('observation', result)
        self.timings = {'elapsed': boot_clock()-started, 'stop': stops[0]-started if stops else None}
        self.timings.update(stop_after_request=stops[0]-requested[0] if stops and requested else None,
                            elapsed_after_request=boot_clock()-requested[0] if requested else None)
        return driver, receipts

    def test_actual_parent_workers_private_reader_without_reupload(self):
        driver, receipts = self.joined()
        self.assertIsNotNone(driver.result)
        self.assertTrue(all(not v['canary_present'] for v in driver.result.values()))
        live = next(r for final, r, _ in receipts if not final)
        final = next(r for is_final, r, _ in receipts if is_final)
        self.assertNotEqual(live['sequence'], final['sequence'])
        self.assertEqual(live['observation_run'], final['observation_run'])
        self.assertNotEqual(live['observation_seal'], final['observation_seal'])
        self.assertEqual(driver.result['content']['matched_identity'], driver.expected_plan['revision'])
        self.assertEqual(sum(m == 'PUT' and p.endswith('/content') for m, p in self.peer.calls), 1)
        self.assertEqual(sum(m == 'PUT' and p.endswith('/view') for m, p in self.peer.calls), 1)
        copied = driver.result; copied['snapshot']['canary_present'] = True
        self.assertFalse(driver.result['snapshot']['canary_present'])

    def test_stale_live_receipt_cannot_choose_expected_final_nonce(self):
        old = []
        def replay(final, result, *_):
            if not final: old.append(copy.deepcopy(result))
            return old[0] if final else result
        driver, _ = self.joined(mutate=replay)
        self.assertIsNone(driver.result)

    def test_wrong_current_seal_is_only_evidence_loss(self):
        def wrong(final, result, *_):
            if final: result['observation_seal'] = str(uuid.uuid4())
            return result
        driver, _ = self.joined(mutate=wrong)
        self.assertIsNone(driver.result)

    def test_helper_plan_cannot_replace_retained_product_proof(self):
        def wrong(final, result, worker, driver):
            if final:
                driver.poll()
                candidate = driver.finals[result['observation_pending']]
                candidate['observation_plan']['revision'] = str(uuid.uuid4())
            return result
        driver, _ = self.joined(mutate=wrong)
        self.assertIsNone(driver.result)

    def test_current_nonce_same_user_mutation_is_not_crypto_resistance(self):
        def mutate(final, result, worker, driver):
            if final:
                driver.poll()
                driver.finals[result['observation_pending']]['observation']['snapshot']['canary_present'] = True
            return result
        driver, _ = self.joined(mutate=mutate)
        self.assertTrue(driver.result['snapshot']['canary_present'])

    def test_rollback_cannot_replace_parent_retained_predecessor(self):
        prior = self.root/'owned-fixture-earlier-evidence.json'
        def rollback(final, driver):
            path = self.config.state_root/'observation'/(driver.run+'.json')
            if not final: prior.write_bytes(path.read_bytes())
        root = str(Path(__file__).resolve().parents[1])
        script = ('import sys,json;from pathlib import Path;sys.path.insert(0,sys.argv[1]);'
            'from csl_workload_observation_durable import DurableObservation;'
            'from csl_sync_state import private_json;'
            'from csl_workload_observation_agent import serve;'
            'original=DurableObservation.__init__;prior=Path(sys.argv[4]);'
            '\ndef before_read(self,config,assignment,*a):\n'
            ' if json.loads((config.state_root/"runtime/producer.json").read_bytes())["phase"]=="stopped":\n'
            '  private_json(config.state_root/"observation"/(assignment["context"]["run"]+".json"),json.loads(prior.read_bytes()))\n'
            ' return original(self,config,assignment,*a)\n'
            'DurableObservation.__init__=before_read\nserve(int(sys.argv[2]),int(sys.argv[3]))')
        def command(agent, fd):
            return [sys.executable, '-B', '-S', '-c', script, root, str(fd), str(__import__('os').getpid()), str(prior)]
        with patch.object(AgentProcess, 'command', command): driver, _ = self.joined(before_launch=rollback)
        self.assertIsNone(driver.result)

    def test_lost_live_receipt_cannot_heal_evidence(self):
        driver, _ = self.joined(mutate=lambda final, result, *_: result if final else None)
        self.assertIsNone(driver.result)

    def test_unsafe_storage_after_live_pass_does_not_block_product(self):
        def changed(final, result, worker, driver):
            if not final:
                path = self.config.state_root/'observation'/(driver.run+'.json')
                path.unlink(); path.mkdir(mode=0o700)
            return result
        driver, _ = self.joined(mutate=changed)
        self.assertIsNone(driver.result)

    def test_original_deadline_and_incomplete_live_status_are_not_adopted(self):
        def late(final, result, worker, driver):
            if not final: driver.deadline = boot_clock()-1
            return result
        driver, _ = self.joined(mutate=late)
        self.assertIsNone(driver.result)

    def test_unobserved_previous_content_is_not_adopted(self):
        self.assertTrue(Runner(self.config).run([sys.executable, '-S', '-c', 'pass'])['complete'])
        driver, _ = self.joined()
        self.assertIsNone(driver.result)

    def test_source_or_account_mismatch_only_loses_evidence(self):
        self.config = replace(self.config, observe_session=fixture.SID)
        driver, _ = self.joined(driver=self.driver(source='0'*64))
        self.assertIsNone(driver.result)
        self.assertTrue(self.peer.views)

    def stall(self, phase):
        marker = self.root/('stall-'+phase)
        root = str(Path(__file__).resolve().parents[1])
        script = ('import sys,time,json;from pathlib import Path;sys.path.insert(0,sys.argv[1]);'
            'import csl_workload_observation_driver as d;'
            'from csl_workload_observation_journal import Evidence;'
            'from csl_workload_observation_agent import serve;'
            'phase=sys.argv[4];marker=Path(sys.argv[5]);state=Path(sys.argv[6]);'
            'owner=d.runtime_fingerprint if phase=="startup" else d.StoredDriver.retain if phase=="retain" else Evidence.begin if phase=="intent" else Evidence.finalized;'
            '\ndef stalled(*a,**k):\n'
            ' if phase=="seal" and json.loads((state/"runtime/producer.json").read_bytes())["phase"]!="stopped": return owner(*a,**k)\n'
            ' marker.touch();time.sleep(40);return owner(*a,**k)\n'
            'if phase=="startup": d.runtime_fingerprint=stalled\n'
            'elif phase=="retain": d.StoredDriver.retain=stalled\n'
            'elif phase=="intent": Evidence.begin=stalled\n'
            'else: Evidence.finalized=stalled\n'
            'serve(int(sys.argv[2]),int(sys.argv[3]))')
        def command(agent, fd):
            return [sys.executable, '-B', '-S', '-c', script, root, str(fd), str(__import__('os').getpid()),
                    phase, str(marker), str(self.config.state_root)]
        with patch.object(AgentProcess, 'command', command):
            driver, _ = self.joined(warm=phase != 'startup', padding=False,
                                   request_ready=phase in ('retain', 'seal'), padding_live=phase in ('retain', 'seal'))
        self.assertTrue(marker.exists(), phase)
        self.assertIsNone(driver.result)
        self.assertLess(self.timings['stop_after_request'], 1.5, self.timings)
        self.assertLess(self.timings['elapsed_after_request'], self.config.flush_seconds, self.timings)
        self.assertIsNotNone(driver.agent.process.poll())
        print(json.dumps({'fixture': 'observer_stall', 'phase': phase,
                          'stop_after_request_seconds': round(self.timings['stop_after_request'], 3),
                          'elapsed_after_request_seconds': round(self.timings['elapsed_after_request'], 3),
                          'evidence': None, 'helper_reaped': True}))

    def test_stalled_parent_retention_cannot_delay_producer_stop(self): self.stall('retain')
    def test_stalled_pre_dispatch_intent_does_not_delay_request_or_completion(self): self.stall('intent')
    def test_stalled_final_seal_does_not_suppress_product_receipt(self): self.stall('seal')
    def test_stalled_startup_provenance_never_blocks_parent(self): self.stall('startup')

    def test_killed_worker_after_request_intent_cannot_qualify_reconciliation(self):
        self.peer.first_chunk, self.peer.release = threading.Event(), threading.Event()
        threads, killed = [], []
        def interrupt(final, worker):
            if not final:
                def stop():
                    if self.peer.first_chunk.wait(3): worker.kill(); killed.append(True); self.peer.release.set()
                thread = threading.Thread(target=stop); threads.append(thread); thread.start()
        try:
            driver, _ = self.joined(on_worker=interrupt)
            self.assertTrue(killed); self.assertIsNone(driver.result)
        finally:
            self.peer.release.set()
            for thread in threads: thread.join(4)

    def test_missing_final_proof_preserves_normal_product_pending(self):
        drops = []
        def final(final, driver):
            if final:
                def lost(request, value):
                    drops.append(True); request.connection.shutdown(2); request.connection.close()
                    return False
                self.proof_transport = lost
        driver, _ = self.joined(before_launch=final, complete=False)
        self.assertTrue(drops); self.assertIsNone(driver.result)

    def test_unsupported_body_size_does_not_change_product_delivery(self):
        rows = fixture.native('codex')
        rows.append({'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': 'public-fixture '*6000}})
        self.write(rows)
        driver, _ = self.joined()
        self.assertIsNone(driver.result)
        self.assertGreater(sum(len(value) for value in self.peer.files.values()), 48*1024)

    def test_assignment_pipe_failure_does_not_change_product_delivery(self):
        with patch('csl_workload_runner.os.pipe2', side_effect=OSError('owned pipe failure')):
            driver, _ = self.joined()
        self.assertIsNone(driver.result)

    def test_helper_never_acquires_product_auth_lock(self):
        root = str(Path(__file__).resolve().parents[1])
        script = ('import sys,sqlite3;sys.path.insert(0,sys.argv[1]);'
            'from csl_workload_auth import WorkloadTokens;'
            'from csl_workload_observation_agent import serve;'
            '\ndef refused(*a,**k): raise AssertionError("optional helper product lock")\n'
            'WorkloadTokens._lock=refused;sqlite3.connect=refused\nserve(int(sys.argv[2]),int(sys.argv[3]))')
        def command(agent, fd):
            return [sys.executable, '-B', '-S', '-c', script, root, str(fd), str(__import__('os').getpid())]
        with patch.object(AgentProcess, 'command', command): driver, _ = self.joined()
        self.assertIsNotNone(driver.result)

    def test_missing_preselected_session_stays_explicitly_unqualified(self):
        self.config = replace(self.config, observe_session=None)
        driver = self.driver()
        result = Runner(self.config, observation_driver=driver).run([sys.executable, '-S', '-c', 'pass'])
        self.assertTrue(result['complete'])
        self.assertIsNone(driver.result)
        self.assertIsNone(driver.agent)


class ChannelBoundaryTests(unittest.TestCase):
    def test_full_unread_owned_socket_refuses_without_wait(self):
        left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.addCleanup(left.close); self.addCleanup(right.close)
        left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        channel = Channel(left)
        self.assertEqual(left.gettimeout(), 0)
        for _ in range(64):
            try: channel.send({'fixed': 'x'*1024})
            except BlockingIOError: break
        else: self.fail('owned buffer did not backpressure')
        with self.assertRaises(BlockingIOError): channel.send({'fixed': 'x'*1024})

    def test_packet_and_event_caps_do_not_wait_for_peer(self):
        left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.addCleanup(left.close); self.addCleanup(right.close)
        channel = Channel(left)
        with self.assertRaises(ValueError): channel.send({'fixed': 'x'*MAX_PACKET})
        channel.events = MAX_EVENTS
        with self.assertRaises(ValueError): channel.send({'fixed': True})


if __name__ == '__main__': unittest.main()

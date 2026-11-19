"""Owned synthetic TLS registry, private modeled identity and real pass workers.

No personal transcript, actual W7 consent, provider/model or image operation.
"""
from dataclasses import replace
import copy
import json
import sys
import threading
import unittest
from unittest.mock import patch
import uuid

from csl_sync_state import private_json
from csl_workload_auth import boot_clock
from csl_workload_observation_driver import AcceptanceDriver, runtime_fingerprint
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

    def joined(self, *, mutate=None, driver=None, complete=True, before_launch=None):
        self.config = replace(self.config, observe_session=fixture.SID, flush_seconds=5)
        driver = driver or self.driver()
        gate = self.root/'fixture-producer-exit'
        self.addCleanup(gate.touch)
        command = [sys.executable, '-S', '-c',
            'import sys,time;from pathlib import Path;end=time.monotonic()+8;'
            '\nwhile not Path(sys.argv[1]).exists() and time.monotonic()<end: time.sleep(.01)', str(gate)]
        receipts = []
        def launch(config, sequence, deadline, *, composition=None):
            final = read_record(config.state_root/'runtime/producer.json')['phase'] == 'stopped'
            if before_launch is not None: before_launch(final, driver)
            worker = PassProcess(config, sequence, deadline, composition=composition)
            original = worker.receipt
            def received():
                result = original()
                receipts.append((final, copy.deepcopy(result), worker.path))
                if mutate is not None: result = mutate(final, result, worker, driver)
                if not final: gate.touch()
                return result
            worker.receipt = received
            return worker
        runner = Runner(self.config, observation_driver=driver, pass_factory=launch)
        consumed = driver.consume
        checks = []
        def consume(*args, **kwargs):
            checks.append(receipts[-1][2].exists())
            return consumed(*args, **kwargs)
        with patch.object(driver, 'consume', side_effect=consume): result = runner.run(command)
        self.assertEqual(result['complete'], complete, result)
        self.assertEqual(result['pending'], not complete, result)
        self.assertTrue(result['worker_reaped'])
        self.assertTrue(result['producer_quiesced'])
        self.assertEqual(result['writer_exit'], 0)
        self.assertEqual(checks, [True] if complete else [])
        self.assertTrue(receipts and any(not final for final, *_ in receipts))
        self.assertTrue(all(not path.exists() for _, _, path in receipts))
        self.assertNotIn('observation', result)
        return driver, receipts

    def test_actual_parent_workers_config_retention_and_private_reader(self):
        driver, receipts = self.joined()
        self.assertIsNotNone(driver.result)
        self.assertTrue(all(not v['canary_present'] for v in driver.result.values()))
        live = next(r for final, r, _ in receipts if not final)
        final = next(r for is_final, r, _ in receipts if is_final)
        self.assertNotEqual(live['sequence'], final['sequence'])
        self.assertEqual(live['observation_run'], final['observation_run'])
        self.assertNotEqual(live['observation_seal'], final['observation_seal'])
        self.assertEqual(final['observation_plan'], driver.expected_plan)
        # Only initial observed content/view writes; final proof adds snapshots,
        # never replay/reupload just to manufacture observation coverage.
        self.assertEqual(sum(m == 'PUT' and p.endswith('/content') for m, p in self.peer.calls), 1)
        self.assertEqual(sum(m == 'PUT' and p.endswith('/view') for m, p in self.peer.calls), 1)
        result = driver.result; result['snapshot']['canary_present'] = True
        self.assertFalse(driver.result['snapshot']['canary_present'])
        for field, value in (('observation_seal', str(uuid.uuid4())), ('observation_run', str(uuid.uuid4())),
                             ('ok', False), ('pending', True), ('synced', True), ('manifest', [])):
            bad = copy.deepcopy(final); bad[field] = value
            driver.consume(bad, final['sequence'], complete=True, now=driver.deadline-1)
            self.assertIsNone(driver.result, field)
        driver.consume(final, final['sequence'], complete=False, now=driver.deadline-1)
        self.assertIsNone(driver.result)
        driver.consume(final, final['sequence'], complete=True, now=driver.deadline+1)
        self.assertIsNone(driver.result)

    def test_new_run_storage_failure_clears_previous_parent_result_before_io(self):
        driver, _ = self.joined()
        self.assertIsNotNone(driver.result)
        with patch('csl_workload_runner.child', side_effect=OSError('owned storage unit')):
            with self.assertRaises(OSError): Runner(self.config, observation_driver=driver).run()
        self.assertIsNone(driver.result)

    def test_late_live_worker_cannot_be_retained_as_previous_coverage(self):
        def expire(final, result, worker, driver):
            if not final: driver.deadline = boot_clock()-1
            return result
        driver, _ = self.joined(mutate=expire)
        self.assertIsNone(driver.result)

    def test_current_nonce_bytes_are_not_a_same_user_tamper_resistance_claim(self):
        driver, receipts = self.joined()
        final = next(r for is_final, r, _ in receipts if is_final)
        # An in-process owner has all trusted facts and can forge a current
        # result. The private reader is freshness binding, not cryptography.
        driver.valid = True
        final['observation']['snapshot']['canary_present'] = True
        driver.consume(final, final['sequence'], complete=True, now=driver.deadline-1)
        self.assertTrue(driver.result['snapshot']['canary_present'])

    def test_durable_write_failure_cannot_reuse_successful_live_seal(self):
        def fail(final, result, worker, driver):
            if not final:
                self.assertIsNotNone(result['observation'])
                driver.evidence.path.unlink()
                driver.evidence.path.mkdir(mode=0o700)
            return result
        driver, _ = self.joined(mutate=fail)
        self.assertIsNone(driver.result)

    def test_restored_old_worker_journal_and_receipt_cannot_choose_expected_nonce(self):
        old = []
        def replay(final, result, worker, driver):
            if not final: old.append(copy.deepcopy(result))
            else: return old[0]
            return result
        driver, _ = self.joined(mutate=replay)
        self.assertIsNone(driver.result)

    def test_current_worker_result_with_wrong_plan_does_not_qualify(self):
        def mutate(final, result, *_):
            if final:
                result['observation']['content']['matched_identity'] = str(uuid.uuid4())
            return result
        driver, _ = self.joined(mutate=mutate)
        self.assertIsNone(driver.result)

    def test_lost_live_result_remains_unqualified_without_changing_delivery(self):
        driver, _ = self.joined(mutate=lambda final, result, *_: result if final else None)
        self.assertIsNone(driver.result)

    def test_source_binding_mismatch_is_only_evidence_failure(self):
        self.config = replace(self.config, observe_session=fixture.SID)
        driver, _ = self.joined(driver=self.driver(source='0'*64))
        self.assertIsNone(driver.result)
        self.assertTrue(self.peer.views)

    def test_private_assignment_pipe_failure_only_loses_evidence(self):
        with patch('csl_workload_runner.os.pipe2', side_effect=OSError('owned pipe failure')):
            driver, _ = self.joined()
        self.assertIsNone(driver.result)
        self.assertEqual(sum(m == 'PUT' and p.endswith('/content') for m, p in self.peer.calls), 1)

    def test_wrong_expected_account_is_not_adopted_from_worker(self):
        self.config = replace(self.config, observe_session=fixture.SID)
        driver, _ = self.joined(driver=self.driver(account_id='f'*24))
        self.assertIsNone(driver.result)

    def test_unobserved_prior_upload_is_not_adopted(self):
        self.assertTrue(Runner(self.config).run([sys.executable, '-S', '-c', 'pass'])['complete'])
        before = len(self.peer.calls)
        driver, _ = self.joined()
        self.assertIsNone(driver.result)
        self.assertFalse(any(m == 'PUT' and p.endswith('/content') for m, p in self.peer.calls[before:]))

    def test_retained_predecessor_rejects_journal_rollback(self):
        old = []
        def rollback(final, driver):
            if not final: old.append(driver.evidence.path.read_bytes())
            else: private_json(driver.evidence.path, json.loads(old[0]))
        driver, _ = self.joined(before_launch=rollback)
        self.assertIsNone(driver.result)

    def test_final_worker_loss_at_original_deadline_leaves_no_successful_live_seal(self):
        gate = threading.Event(); self.addCleanup(gate.set)
        def stalled(final, driver):
            if final: self.proof_gate = gate
        try:
            driver, _ = self.joined(before_launch=stalled, complete=False)
            self.assertIsNone(driver.result)
        finally: gate.set()

    def test_lost_final_proof_response_preserves_delivery_failure(self):
        lost = []
        def mutate(final, result, worker, driver):
            if not final:
                def drop(request, value):
                    lost.append(True)
                    request.connection.shutdown(2); request.connection.close()
                    return False
                self.proof_transport = drop
            return result
        # Final proof never arrives: original pending is preserved, no evidence.
        driver, _ = self.joined(mutate=mutate, complete=False)
        self.assertIsNone(driver.result)
        self.assertTrue(lost)

    def test_worker_killed_after_durable_intent_never_qualifies_reconciliation(self):
        self.config = replace(self.config, observe_session=fixture.SID, flush_seconds=5)
        driver = self.driver()
        self.peer.first_chunk, self.peer.release = threading.Event(), threading.Event()
        workers, killed = [], []
        def launch(config, sequence, deadline, *, composition=None):
            worker = PassProcess(config, sequence, deadline, composition=composition); workers.append(worker)
            if len(workers) == 1:
                def interrupt():
                    if self.peer.first_chunk.wait(4):
                        worker.kill(); killed.append(True); self.peer.release.set()
                helper = threading.Thread(target=interrupt); helper.start()
                self.addCleanup(helper.join, 5)
            return worker
        try:
            result = Runner(self.config, observation_driver=driver, pass_factory=launch).run(
                [sys.executable, '-S', '-c', 'import time;time.sleep(.8)'])
            self.assertTrue(result['complete'], result)
            self.assertTrue(killed)
            self.assertIsNone(driver.result)
            self.assertTrue(all(w.poll() is not None for w in workers))
            self.assertTrue(driver.evidence.load()['pending'] is not None)
        finally: self.peer.release.set()


if __name__ == '__main__': unittest.main()

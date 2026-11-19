"""Bounded body/intent/ACK controls; process lifecycle is tested separately."""
import copy
import hashlib
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from csl_workload_identity import canonical
from csl_workload_observation_durable import DurableObservation
from csl_workload_observation_journal import Evidence
from csl_workload_runner import boot_clock
import test_workload_observation as fixture


class DurableBoundaryTests(unittest.TestCase):
    snapshot = fixture.ObservationTests.snapshot
    upload = fixture.ObservationTests.upload
    view = fixture.ObservationTests.view
    prove = fixture.ObservationTests.prove

    def setUp(self):
        fixture.ObservationTests.setUp(self)
        self.binding = {'scope': '1'*64, 'account_id': '2'*24, 'device_id': 'ses_unit'}
        self.context = {'run': str(uuid.uuid4()), 'session': fixture.SID, 'scope': self.binding['scope'],
            'binding': hashlib.sha256(canonical({k: self.binding[k] for k in ('account_id', 'device_id')})).hexdigest(),
            'source': '3'*64}
        self.config = SimpleNamespace(origin=fixture.ORIGIN, provider='codex', observe_session=fixture.SID, state_root=self.root)
        self.evidence = Evidence(self.root, self.context, create=True)
        self.deadline = boot_clock()+100
        self.worker()

    def worker(self):
        self.sequence, self.seal = str(uuid.uuid4()), str(uuid.uuid4())
        self.assignment = {'context': self.context, 'worker': self.sequence, 'seal': self.seal, 'deadline': self.deadline,
                           'predecessor': hashlib.sha256(canonical(self.evidence.load())).hexdigest()}
        self.observer = DurableObservation(self.config, self.assignment, self.binding, self.context['source'], self.deadline, self.sequence)
        self.http.observation = self.observer

    def test_actual_request_intents_cross_chunks_and_workers_then_fresh_proof(self):
        self.snapshot(); self.upload(chunk=23); self.view(); self.prove()
        self.assertTrue(self.observer.finalize()['observation']['content']['canary_present'])
        previous_calls = len(self.calls)
        self.worker(); self.snapshot(); self.prove()
        result = self.observer.finalize()
        self.assertTrue(result['observation']['content']['canary_present'])
        self.assertEqual(result['observation_seal'], self.seal)
        self.assertFalse(any('/content?' in url or url.endswith('/view') for _, url, _ in self.calls[previous_calls:]))

    def test_failure_to_persist_intent_does_not_prevent_existing_upload(self):
        self.snapshot()
        with patch.object(self.observer.evidence, 'begin', side_effect=OSError('owned disk unit')):
            self.upload(); self.view(); self.prove()
        self.assertEqual(self.remote, self.data)
        self.assertEqual(self.checkpoint['offset'], len(self.data))
        self.assertEqual(self.observer.finalize(), {})

    def test_durable_body_intent_exists_before_actual_opener(self):
        opened = self.http.opener.open
        seen = []
        def checked(request, **kwargs):
            if request.get_method() == 'PUT':
                pending = self.evidence.load()['pending']
                self.assertEqual(pending['worker'], self.sequence)
                self.assertEqual(pending['body_sha256'], hashlib.sha256(request.data).hexdigest())
                seen.append(pending['kind'])
            return opened(request, **kwargs)
        self.http.opener.open = checked
        self.snapshot(); self.upload(); self.view(); self.prove()
        self.assertEqual(set(seen), {'snapshot', 'content', 'view'})
        self.assertTrue(self.observer.finalize())

    def test_post_seal_storage_failure_never_returns_previous_success(self):
        self.snapshot(); self.upload(); self.view(); self.prove()
        self.assertTrue(self.observer.finalize())
        with patch.object(self.observer.evidence, 'finalized', side_effect=OSError('owned disk unit')):
            self.assertEqual(self.observer.finalize(), {})

    def test_deadline_during_seal_is_not_late_acceptance(self):
        self.snapshot(); self.upload(); self.view(); self.prove()
        before = self.deadline-1
        with patch('csl_workload_observation_durable.boot_clock', side_effect=[before, self.deadline+1]):
            self.assertEqual(self.observer.finalize(), {})

    def test_deadline_during_intent_does_not_change_transport_or_qualify(self):
        with patch('csl_workload_observation_durable.boot_clock', side_effect=[self.deadline-1, self.deadline+1]):
            self.snapshot()
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(self.observer.failed)
        self.assertEqual(self.observer.finalize(), {})

    def test_lost_ack_and_pending_intent_cannot_be_adopted_by_fresh_worker(self):
        self.snapshot(); self.fail_content = 1; self.upload()
        self.assertTrue(self.observer.failed)
        self.assertIsNotNone(self.evidence.load()['pending'])
        self.evidence.worker(str(uuid.uuid4()), str(uuid.uuid4()))
        self.assertTrue(self.evidence.load()['poisoned'])
        self.assertEqual(self.observer.finalize(), {})

    def test_parent_expected_assignment_and_source_binding_are_not_adopted(self):
        for field, bad in (('worker', str(uuid.uuid4())), ('seal', str(uuid.uuid4())), ('deadline', self.deadline+1)):
            assignment = {**self.assignment, field: bad}
            with self.subTest(field=field), self.assertRaises((ValueError, KeyError)):
                DurableObservation(self.config, assignment, self.binding, self.context['source'], self.deadline, self.sequence)
        for field in ('scope', 'binding', 'source', 'session'):
            assignment = copy.deepcopy(self.assignment)
            assignment['context'][field] = str(uuid.uuid4()) if field == 'session' else 'f'*64
            with self.subTest(field=field), self.assertRaises((ValueError, KeyError)):
                DurableObservation(self.config, assignment, self.binding, self.context['source'], self.deadline, self.sequence)


if __name__ == '__main__': unittest.main()

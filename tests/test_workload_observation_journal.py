"""Pure owned private-file prototype; no worker, network, provider or auth."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from csl_sync_state import SyncError, private_json
from csl_workload_observation import CANARY
from csl_workload_observation_journal import Evidence, EMPTY


def uid(): return str(uuid.uuid4())
def sha(value): return hashlib.sha256(value).hexdigest()


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='composition-source-unit-'); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.root.chmod(0o770)
        self.context = {'run': uid(), 'session': uid(), 'scope': '1'*64, 'binding': '2'*64, 'source': '3'*40}
        self.data = CANARY+b' public unit'
        self.body = b'public metadata unit'
        self.plan = {'snapshot': uid(), 'revision': uid(), 'generation': uid(), 'length': len(self.data),
                     'sha256': sha(self.data), 'last_seq': 1, 'body_sha256': sha(self.body)}
        self.e = Evidence(self.root, self.context, create=True)
        self.worker = uid(); self.e.worker(self.worker); self.e.plan(self.plan)

    def metadata(self):
        attempt = self.e.begin(self.worker, 'snapshot', self.body)
        self.e.acknowledged(attempt, self.plan['snapshot'])

    def content(self, start, end):
        attempt = self.e.begin(self.worker, 'content', self.data[start:end], start=start, end=end,
                               prefix=sha(self.data[:start]))
        self.e.acknowledged(attempt, self.plan['revision'], end=end, sha256=sha(self.data[:end]))

    def view(self):
        attempt = self.e.begin(self.worker, 'view', b'public view unit', first=1, last=1)
        self.e.acknowledged(attempt, self.plan['generation'], last=1)

    def finish(self):
        seal = self.e.finalized(self.worker, **{k: self.plan[k] for k in
            ('snapshot', 'revision', 'generation', 'length', 'sha256', 'last_seq')})
        parent = self.root/'runtime'/'receipts'; parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent.parent.chmod(0o700)
        receipt = parent/(self.worker+'.json')
        private_json(receipt, {'sequence': self.worker, 'scope': self.context['scope'],
            'observation_run': self.context['run'], 'observation_seal': seal,
            'ok': True, 'pending': 0, 'synced': 1, 'manifest': [{'path': 'synthetic.jsonl'}]})
        return seal, receipt

    def complete(self):
        self.metadata(); self.content(0, len(self.data)); self.view()
        return self.finish()

    def test_cross_worker_ack_coverage_and_split_canary_survive_reopen(self):
        self.metadata(); self.content(0, 20); self.view()
        self.e = Evidence(self.root, self.context)
        self.worker = uid(); self.e.worker(self.worker); self.e.plan(self.plan)
        self.content(20, len(self.data)); self.metadata()
        seal, receipt = self.finish()
        result = Evidence(self.root, self.context).consume(self.worker, seal, receipt)
        self.assertTrue(result['content']['canary_present'])
        self.assertEqual(result['content']['matched_identity'], self.plan['revision'])
        self.assertNotIn(CANARY.decode(), json.dumps(result))
        self.assertNotIn(CANARY, self.e.path.read_bytes())
        self.assertEqual(self.root.stat().st_mode & 0o777, 0o770)
        self.assertEqual(self.e.root.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.e.path.stat().st_mode & 0o777, 0o600)

    def test_final_worker_can_use_previously_observed_ack_ranges_without_reupload(self):
        self.metadata(); self.content(0, len(self.data)); self.view()
        self.worker = uid(); self.e = Evidence(self.root, self.context); self.e.worker(self.worker); self.e.plan(self.plan)
        self.metadata()  # Only normal final snapshot observation, no content/view replay.
        seal, receipt = self.finish()
        self.assertIsNotNone(self.e.consume(self.worker, seal, receipt))

    def test_crash_before_ack_is_permanent_unknown_not_remote_reconciliation(self):
        self.e.begin(self.worker, 'content', self.data, start=0, end=len(self.data), prefix=EMPTY)
        self.e = Evidence(self.root, self.context); self.worker = uid(); self.e.worker(self.worker)
        with self.assertRaises(SyncError): self.e.plan(self.plan)
        self.assertTrue(self.e.load()['poisoned'])
        self.assertIsNone(self.e.consume(self.worker, uid(), self.root/'missing.json'))

    def test_nonzero_unobserved_prefix_wrong_prefix_and_missing_surface_refuse(self):
        for start, prefix in ((1, EMPTY), (0, '4'*64)):
            with self.subTest(start=start), self.assertRaises(SyncError):
                self.e.begin(self.worker, 'content', self.data[start:], start=start, end=len(self.data), prefix=prefix)
        with self.assertRaises(SyncError): self.finish()

    def test_wrong_ack_cannot_advance_and_tampered_state_is_not_consumed(self):
        attempt = self.e.begin(self.worker, 'content', self.data, start=0, end=len(self.data), prefix=EMPTY)
        with self.assertRaises(SyncError):
            self.e.acknowledged(attempt, self.plan['revision'], end=len(self.data), sha256='0'*64)
        self.assertEqual(self.e.load()['offset'], 0)

    def test_binding_run_source_or_session_change_refuses_existing_evidence(self):
        self.complete()
        for field in ('scope', 'binding', 'source', 'session'):
            context = {**self.context, field: uid() if field == 'session' else '4'*len(self.context[field])}
            with self.subTest(field=field), self.assertRaises(SyncError): Evidence(self.root, context)
        with self.assertRaises(SyncError): Evidence(self.root, self.context, create=True)

    def test_expected_worker_and_private_receipt_fields_are_required(self):
        seal, receipt = self.complete()
        self.assertIsNone(self.e.consume(uid(), seal, receipt))
        self.assertIsNone(self.e.consume(self.worker, uid(), receipt))
        original = json.loads(receipt.read_bytes())
        for field, bad in (('scope', '0'*64), ('sequence', uid()), ('observation_run', uid()),
                           ('observation_seal', uid()), ('ok', False), ('pending', True),
                           ('synced', True), ('manifest', [])):
            private_json(receipt, {**original, field: bad})
            with self.subTest(field=field): self.assertIsNone(self.e.consume(self.worker, seal, receipt))

    def test_special_or_shared_files_fail_closed_without_replacing(self):
        original = self.e.path.read_bytes()
        self.e.path.unlink(); os.mkfifo(self.e.path, 0o600)
        with self.assertRaises(SyncError): Evidence(self.root, self.context)
        self.assertTrue(self.e.path.exists()); self.e.path.unlink(); self.e.path.write_bytes(original); self.e.path.chmod(0o600)
        alias = self.root/'alias'; os.link(self.e.path, alias)
        with self.assertRaises(SyncError): Evidence(self.root, self.context)
        self.assertEqual(alias.read_bytes(), original)

    def test_schema_bounds_and_pending_bytes_never_allow_clean_claim(self):
        with patch('csl_workload_observation_journal.MAX_BODY', 2), self.assertRaises(SyncError): self.metadata()
        self.assertTrue(self.e.load()['poisoned'])
        with self.assertRaises(SyncError): self.finish()

    def test_state_corruption_preserves_file_and_refuses(self):
        value = self.e.load(); value['private_raw_field'] = 'not allowed'
        private_json(self.e.path, value)
        original = self.e.path.read_bytes()
        with self.assertRaises(SyncError): Evidence(self.root, self.context)
        self.assertEqual(self.e.path.read_bytes(), original)

    def test_consumer_does_not_open_arbitrary_or_symlinked_receipts(self):
        seal, receipt = self.complete()
        other = self.root/(self.worker+'.json'); other.write_text('public do-not-open unit')
        with patch('csl_workload_observation_journal.read_record', side_effect=AssertionError('unexpected read')):
            self.assertIsNone(self.e.consume(self.worker, seal, other))
        receipt.unlink(); receipt.symlink_to(other)
        with patch('csl_workload_observation_journal.read_record', side_effect=AssertionError('unexpected read')):
            self.assertIsNone(self.e.consume(self.worker, seal, receipt))
        self.assertEqual(other.read_text(), 'public do-not-open unit')

    def test_inconsistent_but_typed_final_coverage_refuses_consumption(self):
        seal, receipt = self.complete()
        value = self.e.load(); value['offset'] -= 1
        private_json(self.e.path, value)
        self.assertIsNone(self.e.consume(self.worker, seal, receipt))

    def test_special_lock_refuses_before_wait_without_changing_it(self):
        lock = self.e.root/(self.context['run']+'.lock')
        lock.unlink(); os.mkfifo(lock, 0o600)
        with self.assertRaises(SyncError): Evidence(self.root, self.context)
        self.assertTrue(lock.exists())

    def test_new_snapshot_can_extend_same_revision_and_view_without_losing_detection(self):
        self.metadata(); self.content(0, 20); self.view()
        self.plan = {**self.plan, 'snapshot': uid(), 'length': len(self.data)+1, 'sha256': sha(self.data+b'x')}
        self.data += b'x'
        self.worker = uid(); self.e.worker(self.worker); self.e.plan(self.plan)
        self.content(20, len(self.data)); self.metadata()
        seal, receipt = self.finish()
        self.assertTrue(self.e.consume(self.worker, seal, receipt)['content']['canary_present'])


if __name__ == '__main__': unittest.main()

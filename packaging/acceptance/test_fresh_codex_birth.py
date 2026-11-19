import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from csl_text_workload import WorkloadTextProvider
from fresh_codex_birth import FreshCodexBirth


SID = '11111111-2222-4333-8444-555555555555'


class FreshBirthTests(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / 'transcripts'
        self.source.mkdir()
        self.path = self.source / 'different-name.jsonl'
        self.tracker = FreshCodexBirth(self.source)

    def write_header(self):
        self.path.write_text(json.dumps({'type': 'session_meta', 'payload': {
            'id': SID, 'cli_version': '0.169.0'}}) + '\n')

    def prepare(self):
        adapter = WorkloadTextProvider('codex', ((self.source, self.path.name),),
                                       self.root / 'projection')
        return adapter, adapter.list_sessions('codex')

    def test_accepted_header_identity_not_filename(self):
        self.write_header()
        birth = self.tracker.bind_accepted(*self.prepare())
        self.assertEqual(birth.native_id, SID)
        self.assertEqual(birth, self.tracker.bind_accepted(*self.prepare()))

    def test_preexisting_source_and_additional_source_refuse(self):
        self.write_header()
        with self.assertRaisesRegex(ValueError, 'not_empty'):
            FreshCodexBirth(self.source)
        (self.source / 'unexpected.jsonl').touch()
        with self.assertRaisesRegex(ValueError, 'ambiguous'):
            self.tracker.bind_accepted(*self.prepare())

    def test_replacement_refuses_even_with_same_id(self):
        self.write_header()
        self.tracker.bind_accepted(*self.prepare())
        self.path.rename(self.root / 'retired')
        self.write_header()
        with self.assertRaisesRegex(ValueError, 'changed'):
            self.tracker.bind_accepted(*self.prepare())

    def test_unaccepted_observer_id_refuses(self):
        self.write_header()
        adapter, _ = self.prepare()
        with self.assertRaisesRegex(ValueError, 'unqualified'):
            self.tracker.bind_accepted(adapter, [{'native_id': '11111111-2222-4333-8444-555555555556'}])


class GuardianIdentityTests(unittest.TestCase):
    def test_real_guardian_records_pre_exec_process_identities(self):
        import sys
        from csl_workload_producer import Producer

        with TemporaryDirectory() as directory:
            runtime = Path(directory)
            producer = Producer(runtime, 'fixture-scope', [sys.executable, '-c', 'pass'])
            try:
                self.assertEqual(producer.wait(timeout=5), 0)
                self.assertTrue(producer.quiesced())
                facts = producer.launch_identity()
                self.assertEqual(facts['run_id'], producer.run_id)
                self.assertEqual(facts['guardian_pid'], producer.process.pid)
                self.assertNotEqual(facts['guardian_pid'], facts['producer_pid'])
                self.assertGreater(facts['guardian_start'], 0)
                self.assertGreater(facts['producer_start'], 0)
            finally:
                producer.stop()
                if producer.process.poll() is None:
                    producer.process.wait(timeout=6)

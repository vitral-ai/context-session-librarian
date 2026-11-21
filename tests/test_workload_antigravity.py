"""Antigravity workload projection over synthetic SQLite; no agy, login or real history."""
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid

from csl_antigravity_workload import PROFILE, WorkloadAntigravityProvider
from csl_sync_state import SyncError
from test_antigravity import make_session, pb

CANARY = 'sk-proj-' + 'A' * 32


def add_user_step(path, idx, text):
    db = sqlite3.connect(path)
    db.execute('INSERT INTO steps VALUES (?,14,3,?,0,NULL)', (idx, pb((1, 14), (4, 3), (19, pb((2, text))))))
    db.commit(); db.close()


class AntigravityWorkloadTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix='workload-agy-'); self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.source = self.root / 'transcripts'; self.cwd = self.root / 'project'; self.cwd.mkdir()
        self.sid = str(uuid.uuid4())
        self.db = make_session(self.source, self.sid, self.cwd)
        add_user_step(self.db, 2, 'deploy with key ' + CANARY)

    def provider(self, **options):
        relative = 'conversations/' + self.sid + '.db'
        return WorkloadAntigravityProvider(((self.source, relative),), self.root / 'projection', **options)

    def test_projects_redacted_view_text_and_hides_native_paths(self):
        before = self.db.read_bytes()
        provider = self.provider()
        row, = provider.list_sessions('antigravity')
        self.assertEqual(row['native_id'], self.sid); self.assertEqual(row['title'], 'Remember SYNTHETIC-4827')
        self.assertIsNone(row['cwd'])
        output = Path(provider.raw_files('antigravity', self.sid)[0]['path']).read_bytes()
        self.assertNotIn(CANARY.encode(), output); self.assertNotIn(str(self.cwd).encode(), output)
        lines = [json.loads(line) for line in output.splitlines()]
        self.assertTrue(all(line['workload_profile_v1'] == PROFILE for line in lines))
        view = provider.view_records('antigravity', self.sid)['records']
        self.assertEqual([r['role'] for r in view], ['user', 'assistant', 'user'])
        self.assertIn('Use decision B', view[1]['text'])
        self.assertNotIn(CANARY, json.dumps(view))
        self.assertFalse(provider.package_descriptor('antigravity', self.sid)['available'])
        self.assertEqual(self.db.read_bytes(), before)
        self.assertFalse(Path(str(self.db) + '-shm').exists())

    def test_new_steps_extend_the_projection(self):
        first = self.provider(); first.list_sessions('antigravity')
        old = Path(first.raw_files('antigravity', self.sid)[0]['path']).read_bytes()
        add_user_step(self.db, 3, 'next step marker')
        second = self.provider(); second.list_sessions('antigravity')
        new = Path(second.raw_files('antigravity', self.sid)[0]['path']).read_bytes()
        self.assertTrue(new.startswith(old) and len(new) > len(old))
        self.assertEqual(second.view_records('antigravity', self.sid)['records'][-1]['text'], 'next step marker')

    def test_refuses_other_paths_and_unknown_schema(self):
        for relative in ('conversation_summaries.db', 'conversations/x.db', 'conversations/' + self.sid + '.jsonl'):
            with self.assertRaises(SyncError):
                WorkloadAntigravityProvider(((self.source, relative),), self.root / 'p')
        db = sqlite3.connect(self.db); db.execute('UPDATE steps SET step_format=1'); db.commit(); db.close()
        with self.assertRaises(SyncError) as caught:
            self.provider().list_sessions('antigravity')
        self.assertEqual(caught.exception.code, 'workload_antigravity_format_refused')


if __name__ == '__main__':
    unittest.main()

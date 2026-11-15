"""Collector integration with fabricated SQLite histories; no native model calls."""
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path

from csl_sync_engine import Collector, PROVIDERS
from csl_sync_http import HTTP, Registry
from csl_sync_state import State
import csl_sync_providers as providers
from sync_support import FakeTokens, Peer
from test_antigravity import make_session, pb


class SQLiteSyncTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.home = self.root / 'native'
        self.project = self.root / 'project'
        self.project.mkdir()
        self.sid = str(uuid.uuid4())
        self.path = make_session(self.home, self.sid, self.project)
        home = self.home

        class Bound:
            def list_sessions(self, p): return providers.list_sessions(p, home=home)
            def raw_files(self, p, n): return providers.raw_files(p, n, home=home)
            def view_records(self, p, n, **kw): return providers.view_records(p, n, home=home, **kw)
            def package_descriptor(self, p, n): return providers.package_descriptor(p, n, home=home)

        self.provider = Bound()
        self.state = State(self.root / 'state')
        self.addCleanup(self.state.close)
        self.peer = Peer()
        self.addCleanup(self.peer.close)
        self.collector = Collector(self.state, Registry(self.peer.url, FakeTokens(), http=HTTP(sleep=lambda _: None)), self.provider)

    def run_pass(self):
        return self.collector.run(days=None, providers=['antigravity'])

    def local(self):
        return self.state.session('antigravity', self.sid)

    def wal(self):
        db = sqlite3.connect(self.path)
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA wal_autocheckpoint=0')
        self.addCleanup(db.close)
        return db

    def test_database_uses_replace_upload_and_repeat_is_idempotent(self):
        self.assertIn('antigravity', PROVIDERS)
        original = self.path.read_bytes()
        self.assertEqual(self.run_pass()['synced'], 1)
        plan = next(iter(self.peer.sessions.values()))
        self.assertEqual(plan['files'][0]['mode'], 'replace')
        revision = self.local()['files']['main']['revision']
        writes = len(self.peer.writes)
        self.assertEqual(self.run_pass()['synced'], 1)
        self.assertEqual(len(self.peer.writes), writes)
        self.assertEqual(self.local()['files']['main']['revision'], revision)
        self.assertEqual(self.path.read_bytes(), original)

    def test_wal_appears_changes_checkpoints_and_disappears_between_passes(self):
        self.assertEqual(self.run_pass()['synced'], 1)
        db = self.wal()
        db.execute('INSERT INTO gen_metadata VALUES (1,?)', (b'synthetic',))
        db.commit()
        self.assertEqual(self.run_pass()['synced'], 1)
        first = self.local()
        self.assertEqual(set(first['files']), {'main', 'sqlite-wal'})
        main_before = self.path.read_bytes()
        body = pb((1,14),(4,3),(19,pb((2,'Revised synthetic decision'))))
        db.execute('UPDATE steps SET step_payload=? WHERE idx=0', (body,))
        db.commit()
        wal_before = Path(str(self.path) + '-wal').read_bytes()
        self.assertEqual(self.run_pass()['synced'], 1)
        changed = self.local()
        self.assertNotEqual(first['files']['sqlite-wal']['revision'], changed['files']['sqlite-wal']['revision'])
        self.assertNotEqual(first['view']['generation'], changed['view']['generation'])
        self.assertEqual(main_before, self.path.read_bytes())
        self.assertEqual(wal_before, Path(str(self.path) + '-wal').read_bytes())
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        self.assertEqual(self.run_pass()['synced'], 1)
        db.close()
        self.assertEqual(self.run_pass()['synced'], 1)
        self.assertEqual(set(self.local()['files']), {'main'})
        self.assertEqual({f['file_key'] for f in next(iter(self.peer.sessions.values()))['files']}, {'main'})

    def test_wal_view_hash_mismatch_is_rejected_even_when_main_matches(self):
        db = self.wal()
        db.execute('INSERT INTO gen_metadata VALUES (1,?)', (b'synthetic',))
        db.commit()
        render = self.provider.view_records
        def wrong(*args, **kwargs):
            page = render(*args, **kwargs)
            page['source_files'][1]['sha256'] = '0' * 64
            return page
        self.provider.view_records = wrong
        self.assertEqual(self.run_pass()['pending'], 1)
        self.assertEqual(self.local()['last_error'], 'source_changed')
        self.assertFalse(self.peer.sessions)
        self.assertFalse(self.peer.writes)

    def test_missing_wal_anchor_is_rejected(self):
        db = self.wal()
        db.execute('INSERT INTO gen_metadata VALUES (1,?)', (b'synthetic',))
        db.commit()
        render = self.provider.view_records
        def missing(*args, **kwargs):
            page = render(*args, **kwargs)
            page['source_files'] = page['source_files'][:1]
            return page
        self.provider.view_records = missing
        self.assertEqual(self.run_pass()['pending'], 1)
        self.assertFalse(self.peer.sessions)

    def test_wal_appearing_after_view_is_caught_by_inventory_fence(self):
        check = self.collector.check_sources
        def changing(files):
            check(files)
            db = self.wal()
            db.execute('INSERT INTO gen_metadata VALUES (1,?)', (b'synthetic',))
            db.commit()
        self.collector.check_sources = changing
        self.assertEqual(self.run_pass()['pending'], 1)
        self.assertEqual(self.local()['last_error'], 'source_changed')
        self.assertFalse(self.peer.sessions)

    def test_wal_disappearing_after_view_is_caught_by_inventory_fence(self):
        db = self.wal()
        db.execute('INSERT INTO gen_metadata VALUES (1,?)', (b'synthetic',))
        db.commit()
        check = self.collector.check_sources
        def changing(files):
            check(files)
            db.close()
        self.collector.check_sources = changing
        self.assertEqual(self.run_pass()['pending'], 1)
        self.assertFalse(self.peer.sessions)

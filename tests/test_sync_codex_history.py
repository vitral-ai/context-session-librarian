"""End-to-end collector snapshot fences for multi-file Codex lineages."""
from pathlib import Path
import tempfile
import unittest

from csl_sync_engine import Collector
from csl_sync_http import HTTP, Registry
from csl_sync_state import State
import csl_sync_providers as providers
from sync_support import FakeTokens, Peer
from test_codex_history import write_rollout


class CodexHistorySyncTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.home = self.root / 'native'
        self.base, _, position = write_rollout(self.home, archived=True)
        self.child, self.sid, _ = write_rollout(self.home, base=position, label='CHILD')
        home, selected = self.home, self.sid
        class Bound:
            def list_sessions(self, p):
                return [r for r in providers.list_sessions(p, home=home) if r['native_id'] == selected]
            def raw_files(self, p, n): return providers.raw_files(p, n, home=home)
            def view_records(self, p, n, **kw): return providers.view_records(p, n, home=home, **kw)
            def package_descriptor(self, p, n): return providers.package_descriptor(p, n, home=home)
        self.provider = Bound()
        self.state = State(self.root / 'state')
        self.addCleanup(self.state.close)
        self.peer = Peer()
        self.addCleanup(self.peer.close)
        self.collector = Collector(self.state, Registry(self.peer.url, FakeTokens(),
            http=HTTP(sleep=lambda _: None)), self.provider)

    def run_pass(self):
        return self.collector.run(days=None, providers=['codex'])

    def test_multiple_raw_files_upload_and_repeat_is_idempotent(self):
        self.assertEqual(self.run_pass()['synced'], 1)
        plan = next(iter(self.peer.sessions.values()))
        self.assertEqual(len(plan['files']), 2)
        self.assertEqual({f['role'] for f in plan['files']}, {'transcript', 'context'})
        self.assertIn('codex_history', plan['package']['manifest']['sessions'][0])
        writes = len(self.peer.writes)
        self.assertEqual(self.run_pass()['synced'], 1)
        self.assertEqual(len(self.peer.writes), writes)

    def test_missing_base_view_anchor_is_rejected(self):
        render = self.provider.view_records
        def missing(*args, **kwargs):
            page = render(*args, **kwargs)
            page['source_files'] = page['source_files'][:1]
            return page
        self.provider.view_records = missing
        self.assertEqual(self.run_pass()['pending'], 1)
        self.assertFalse(self.peer.sessions)
        self.assertFalse(self.peer.writes)

    def test_changed_base_after_view_cannot_publish_mixed_snapshot(self):
        check = self.collector.check_sources
        def changed(files):
            check(files)
            self.base.write_bytes(self.base.read_bytes().replace(b'BASE-ANSWER', b'EDIT-ANSWER'))
        self.collector.check_sources = changed
        self.assertEqual(self.run_pass()['pending'], 1)
        self.assertFalse(self.peer.sessions)

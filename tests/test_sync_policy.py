import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from csl_sync_policy import configure, selected
from csl_sync_state import State, SyncError
from csl_sync_engine import Collector
from csl_sync_providers import _user_title


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = State(Path(self.tmp.name) / 'sync')
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.state.close)

    def configure(self, kind, **kwargs):
        with patch('csl_sync_policy.now', return_value='2026-11-26T12:00:00Z'):
            return configure(self.state, kind, **kwargs)

    def test_new_uses_creation_not_recent_activity_and_excludes_win(self):
        policy = self.configure('new')
        old = dict(native_id='old', created_at='2020-01-01T00:00:00Z', updated_at='2026-11-26T13:00:00Z')
        new = dict(native_id='new', created_at='2026-11-26T13:00:00Z')
        self.assertFalse(selected(policy, 'claude', old))
        self.assertTrue(selected(policy, 'codex', new))
        collector = Collector(self.state, None, None)
        self.state.select('codex', 'new', False)
        self.assertFalse(collector.selected('codex', new, None, set()))
        self.state.select('claude', 'old', True)
        self.assertTrue(collector.selected('claude', old, 30, set()))

    def test_last_cross_provider_snapshot_then_follows_new(self):
        rows = [dict(provider=p, native_id=p, created_at='2020-01-01T00:00:00Z', updated_at=t)
                for p, t in [('codex', '2026-11-26T10:00:00Z'), ('claude', '2026-11-26T11:00:00Z')]]
        policy = self.configure('last', limit=1, rows=rows)
        self.assertFalse(selected(policy, 'codex', rows[0]))
        self.assertTrue(selected(policy, 'claude', rows[1]))
        self.assertTrue(selected(policy, 'codex', dict(native_id='next', created_at='2026-11-26T13:00:00Z')))

    def test_folder_boundary_and_unknown_creation(self):
        policy = self.configure('folders', folders=[self.tmp.name + '/project'])
        self.assertTrue(selected(policy, 'claude', dict(native_id='a', cwd=self.tmp.name + '/project/sub')))
        self.assertFalse(selected(policy, 'claude', dict(native_id='a', cwd=self.tmp.name + '/project-other')))
        self.assertFalse(selected(self.configure('new'), 'claude', dict(native_id='a')))

    def test_invalid_options_preserve_policy_and_optouts(self):
        before = self.configure('all')
        self.state.select('claude', 'excluded', False)
        for kind, options in [('last', {}), ('new', {'limit': 1}), ('all', {'limit': 0}), ('folders', {})]:
            with self.assertRaises(SyncError): self.configure(kind, **options)
        self.assertEqual(self.state.get('selection_policy'), before)
        self.assertFalse(self.state.selection('claude', 'excluded'))

    def test_titles_ignore_bootstrap_and_keep_real_prose(self):
        for text in ['# AGENTS.md instructions\n<INSTRUCTIONS>setup</INSTRUCTIONS>', '<environment_context>setup</environment_context>']:
            self.assertIsNone(_user_title({}, text))
        self.assertIsNone(_user_title({'isMeta': True}, 'bootstrap'))
        self.assertEqual(_user_title({}, [{'type': 'text', 'text': 'Fix AGENTS.md please'}]), 'Fix AGENTS.md please')
        self.assertEqual(_user_title({}, 'Explain this code'), 'Explain this code')


if __name__ == '__main__': unittest.main()

"""Native Windows checks for ACLs, live file handles, and wake notifications."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

from csl_sync_engine import Collector, Providers
from csl_sync import pull
from csl_sync_auth import Tokens
from csl_sync_http import Registry
from csl_codex_history import decoded
from sync_support import FakeTokens, Peer

from csl_sync_fs import WINDOWS, open_read
from csl_sync_state import State, exclusive, private_directory, private_json, read_private_json
from csl_sync_watch import history_notification, run_pass


@unittest.skipUnless(WINDOWS, 'Windows filesystem behavior')
class WindowsSyncTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = private_directory(Path(temporary.name) / 'sync')

    def test_private_state_and_replace_while_open(self):
        path = self.root / 'credentials.json'
        private_json(path, {'generation': 1})
        with os.fdopen(open_read(path), 'rb') as old:
            private_json(path, {'generation': 2})
            self.assertEqual(old.read(), b'{"generation":1}')
        self.assertEqual(read_private_json(path), {'generation': 2})

    def test_refresh_marker_is_written_through_before_token_exchange(self):
        from csl_sync_windows import move_file

        tokens = Tokens(self.root, {'token_endpoint': 'https://localhost/token', 'client_id': 'fixture'})
        private_json(tokens.path, {'access_token': 'expired', 'refresh_token': 'fixture',
                                  'refresh_state': 'ready', 'expires_at': 0})
        with patch('csl_sync_windows.move_file', wraps=move_file) as move:
            def exchange(_fields):
                self.assertEqual(read_private_json(tokens.path)['refresh_state'], 'inflight')
                self.assertEqual(move.call_args.args[2], 0x9)
                return {'token_type': 'bearer', 'access_token': 'renewed', 'refresh_token': 'successor',
                        'expires_in': 600}

            with patch.object(tokens, '_exchange', side_effect=exchange):
                self.assertEqual(tokens.access_token(), 'renewed')
            self.assertEqual(move.call_count, 2)

    def test_watch_ignores_collector_state_and_accepts_native_history(self):
        from csl_sync_windows import DirectoryChanges

        sessions = self.root / 'sessions'
        sessions.mkdir()
        with DirectoryChanges([self.root], lambda _root, name: history_notification('codex', name)) as changes:
            self.assertFalse(changes.wait(0.05))
            (self.root / 'state.sqlite3').write_bytes(b'unrelated')
            self.assertFalse(changes.wait(2))
            (sessions / 'latest.jsonl').write_bytes(b'{}\r\n')
            self.assertTrue(changes.wait(2))

    def test_lineage_reader_allows_rollout_rotation(self):
        path = self.root / 'rollout.jsonl'
        path.write_bytes(b'{}\r\n')
        with decoded(path) as source:
            path.rename(path.with_suffix('.old'))
            self.assertEqual(source.read(), b'{}\r\n')

    def test_pull_uses_a_regular_file_for_ntfs_reserved_names(self):
        class Registry:
            def download(self, path, sink, _limit):
                sink.write(b'PK\x03\x04')
                requests.append(path)
                return 4

        requests = []
        stored = Path(pull(Registry(), 'session:123', self.root)['path'])
        self.assertNotIn(':', stored.name)
        self.assertEqual(stored.read_bytes(), b'PK\x03\x04')
        cases = [Path(pull(Registry(), name, self.root)['path']) for name in ('sess-a', 'Sess-a')]
        self.assertNotEqual(cases[0].name.casefold(), cases[1].name.casefold())
        self.assertEqual(requests, ['/sessions/session%3A123/package', '/sessions/sess-a/package',
                                    '/sessions/Sess-a/package'])

    def test_replace_falls_back_when_posix_rename_is_unsupported(self):
        def unsupported(*_):
            ctypes.set_last_error(50)
            return False

        path = self.root / 'state.json'
        with patch('csl_sync_windows.set_file_info', side_effect=unsupported):
            private_json(path, {'generation': 1})
            private_json(path, {'generation': 2})
        self.assertEqual(read_private_json(path), {'generation': 2})

    def test_watch_worker_resolves_relative_state_from_caller_directory(self):
        args = argparse.Namespace(sync_home=Path('sync'), all=False, days=30, providers=['codex'])
        cwd = Path.cwd()
        os.chdir(self.root.parent)
        try:
            with exclusive(self.root) as fd:
                code, _, stderr, _ = run_pass(args, fd)
        finally:
            os.chdir(cwd)
        self.assertEqual(code, 2)
        self.assertIn('"sign_in_required"', stderr)

    def test_notification_and_rotate_while_reading(self):
        from csl_sync_windows import DirectoryChanges
        path = self.root / 'session.jsonl'
        path.write_bytes(b'{"first":true}\r\n')
        with DirectoryChanges([self.root]) as changes:
            self.assertFalse(changes.wait(0.05))
            with os.fdopen(open_read(path), 'rb') as source:
                moved = path.with_suffix('.rotated')
                path.rename(moved)
                path.write_bytes(b'{"second":true}\r\n')
                self.assertTrue(changes.wait(2))
                self.assertEqual(source.read(), b'{"first":true}\r\n')
            self.assertEqual(path.read_bytes(), b'{"second":true}\r\n')

    def test_pass_inherits_the_only_collector_lock(self):
        state = State(self.root)
        self.addCleanup(state.close)
        args = argparse.Namespace(sync_home=self.root, all=False, days=30, providers=['codex'])
        direct = [sys.executable, '-m', 'csl_sync', 'sync', '--home', str(self.root), 'run']
        with exclusive(self.root) as fd:
            blocked = subprocess.run(direct, capture_output=True, text=True, check=False)
            self.assertIn('"collector_busy"', blocked.stderr)
            code, _, stderr, _ = run_pass(args, fd)
        self.assertEqual(code, 2)
        self.assertIn('"sign_in_required"', stderr)


    def test_native_claude_and_codex_crlf_append_and_rotation(self):
        state = State(self.root)
        self.addCleanup(state.close)
        peer = Peer()
        self.addCleanup(peer.close)
        codex, claude = self.root / 'codex', self.root / 'claude'
        codex_id, claude_id = str(uuid.uuid4()), str(uuid.uuid4())
        codex_path = codex / 'sessions' / f'rollout-{codex_id}.jsonl'
        # Claude encodes the full project path in the directory name. This
        # makes the snapshot path exceed MAX_PATH once the cache key is added.
        project = 'C--Users-vitraltest-AppData-Local-' + 'workspace-' * 9
        claude_path = claude / 'projects' / project / f'{claude_id}.jsonl'
        codex_path.parent.mkdir(parents=True)
        claude_path.parent.mkdir(parents=True)

        def line(value):
            return json.dumps(value).encode() + b'\r\n'

        def codex_message(text):
            return line({'type': 'response_item', 'payload': {'type': 'message', 'role': 'user',
                         'content': [{'type': 'input_text', 'text': text}]}})

        def claude_message(text):
            return line({'type': 'user', 'sessionId': claude_id, 'uuid': str(uuid.uuid4()),
                         'parentUuid': None, 'message': {'role': 'user', 'content': text}})

        header = line({'type': 'session_meta', 'payload': {'id': codex_id, 'cwd': str(self.root)}})
        codex_path.write_bytes(header + codex_message('codex first'))
        claude_first = claude_message('claude first')
        claude_path.write_bytes(claude_first)
        providers = Providers(state.root)
        collector = Collector(state, Registry(peer.url, FakeTokens()), providers)
        fixtures = (('codex', codex_id, codex_path), ('claude', claude_id, claude_path))

        def collect():
            result = collector.run(days=None, providers=('codex', 'claude'))
            self.assertEqual(result['pending'], 0, result)
            self.assertEqual(result['synced'], 2)

        def latest(provider, source_id):
            row = state.session(provider, source_id)
            key = next(name for name in peer.sessions if name.endswith('/' + source_id))
            file = peer.files[(key, 'main', row['files']['main']['revision'])]
            events = peer.views[(key, row['view']['generation'])]
            return row, bytes(file), [event['text'] for event in events]

        with patch.dict(os.environ, CODEX_HOME=str(codex), CLAUDE_CONFIG_DIR=str(claude)):
            collect()
            old = {provider: latest(provider, source_id)[0] for provider, source_id, _ in fixtures}
            for path, record in ((codex_path, codex_message('codex tail')),
                                 (claude_path, claude_message('claude tail'))):
                with path.open('ab') as output:
                    output.write(record[:-1])  # Incomplete CRLF is not a complete record.
            collect()
            for provider, source_id, _ in fixtures:
                self.assertEqual(latest(provider, source_id)[2], [f'{provider} first'])
            for _, _, path in fixtures:
                with path.open('ab') as output:
                    output.write(b'\n')
            collect()
            collect()
            for provider, source_id, path in fixtures:
                row, raw, view = latest(provider, source_id)
                self.assertEqual(raw, path.read_bytes())
                self.assertEqual(view, [f'{provider} first', f'{provider} tail'])
                self.assertEqual(row['files']['main']['revision'], old[provider]['files']['main']['revision'])
            for path, content in ((codex_path, header + codex_message('codex new')),
                                  (claude_path, claude_message('claude new'))):
                with os.fdopen(open_read(path), 'rb') as opened:
                    path.rename(path.with_suffix('.rotated'))
                    path.write_bytes(content)
                    self.assertNotEqual(opened.read(), content)
            collect()
            collect()
            self.assertEqual(len(peer.sessions), 2)
            for provider, source_id, path in fixtures:
                row, raw, view = latest(provider, source_id)
                self.assertNotEqual(row['files']['main']['revision'], old[provider]['files']['main']['revision'])
                self.assertEqual(raw, path.read_bytes())
                self.assertEqual(view, [f'{provider} new'])

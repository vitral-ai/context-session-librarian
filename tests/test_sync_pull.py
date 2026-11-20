import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
import zipfile
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from csl_sync import pull
from csl_sync_http import Registry
from csl_sync_state import SyncError

PREFIX = '/session-registry/v1'


def package():
    out = io.BytesIO()
    with zipfile.ZipFile(out, 'w') as z:
        z.writestr('session.jsonl', '{"synthetic":true}\n')
    return out.getvalue()


class Tokens:
    def __init__(self, value): self.value = value
    def access_token(self, force_refresh=False): return self.value
    def revoke_locally(self): pass


class PullTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='csl-pull-'); self.root = Path(self.tmp.name)
        self.seen = []
        # token -> owner; session -> owner. The registry derives the owner from the token only.
        owners = {'token-a': 'owner-a', 'token-b': 'owner-b'}
        sessions = {'sess-a': 'owner-a'}
        test = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_GET(self):
                token = (self.headers.get('Authorization') or '').removeprefix('Bearer ')
                test.seen.append((self.path, bool(token)))
                owner = owners.get(token)
                parts = self.path.removeprefix(PREFIX + '/').split('/')
                if owner is None:
                    status, body, kind = 401, json.dumps({'error': {'code': 'unauthenticated'}}).encode(), 'application/json'
                elif len(parts) == 3 and parts[0] == 'sessions' and parts[2] == 'package' and sessions.get(parts[1]) == owner:
                    status, body, kind = 200, package(), 'application/zip'
                else:
                    status, body, kind = 404, json.dumps({'error': {'code': 'not_found'}}).encode(), 'application/json'
                self.send_response(status); self.send_header('Content-Type', kind); self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.endpoint = 'http://127.0.0.1:%d%s' % (self.server.server_port, PREFIX)

    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.tmp.cleanup()

    def registry(self, token): return Registry(self.endpoint, Tokens(token))

    def test_owner_pulls_package_to_private_file_with_digest(self):
        result = pull(self.registry('token-a'), 'sess-a', self.root / 'out')
        path = Path(result['path'])
        self.assertEqual(path.read_bytes(), package()); self.assertEqual(result['sha256'], hashlib.sha256(package()).hexdigest())
        self.assertEqual(path.stat().st_mode & 0o777, 0o600); self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)
        self.assertNotIn('token-a', json.dumps(result)); self.assertEqual(list(path.parent.glob('.pull-*')), [])
        self.assertEqual(zipfile.ZipFile(path).namelist(), ['session.jsonl'])

    def test_other_account_is_refused_and_nothing_is_written(self):
        with self.assertRaises(SyncError) as error: pull(self.registry('token-b'), 'sess-a', self.root / 'out')
        self.assertEqual(error.exception.code, 'not_found')
        self.assertEqual(list((self.root / 'out').iterdir()), [])

    def test_unauthenticated_and_unsafe_ids_and_existing_output(self):
        with self.assertRaises(SyncError) as error: pull(self.registry('bad'), 'sess-a', self.root / 'out')
        self.assertEqual(error.exception.code, 'unauthenticated')
        for bad in ('../x', 'a/b', '', 'a b', '-x', 'x' * 129):
            with self.assertRaises(SyncError) as error: pull(self.registry('token-a'), bad, self.root / 'out')
            self.assertEqual(error.exception.code, 'invalid_session_id')
        self.assertFalse(any('..' in path for path, _ in self.seen))
        pull(self.registry('token-a'), 'sess-a', self.root / 'out')
        with self.assertRaises(SyncError) as error: pull(self.registry('token-a'), 'sess-a', self.root / 'out')
        self.assertEqual(error.exception.code, 'output_exists')

    def test_existing_directory_keeps_its_mode_and_new_one_is_private(self):
        existing = self.root / 'Downloads'; existing.mkdir(mode=0o755); existing.chmod(0o755)
        result = pull(self.registry('token-a'), 'sess-a', existing)
        self.assertEqual(existing.stat().st_mode & 0o777, 0o755); self.assertEqual(Path(result['path']).stat().st_mode & 0o777, 0o600)
        fresh = self.root / 'new' / 'dir'
        pull(self.registry('token-a'), 'sess-a', fresh)
        self.assertEqual(fresh.stat().st_mode & 0o777, 0o700)

    def test_pull_runs_while_sync_collector_lock_is_held(self):
        import argparse
        from unittest.mock import patch
        from csl_sync import command
        from csl_sync_state import State, exclusive
        home = self.root / 'sync'; state = State(home); state.set('settings', {'enabled': True, 'endpoint': self.endpoint}); state.close()
        class Signed(Tokens):
            def status(self): return {'signed_in': True}
        args = argparse.Namespace(sync_home=home, sync_action='pull', session_id='sess-a', out_dir=self.root / 'out')
        out = io.StringIO()
        with exclusive(home), patch('csl_sync.Tokens', lambda root, settings: Signed('token-a')):
            from contextlib import redirect_stdout
            with redirect_stdout(out): command(args, None)
        self.assertEqual(json.loads(out.getvalue())['session_id'], 'sess-a')
        self.assertTrue((self.root / 'out' / 'sess-a.zip').exists())

    def test_backend_composed_line_parses_and_pull_output_is_the_import_input(self):
        # Mirrors vitralai fastapi-backend/tests/fixtures/resume-command-v1.json (the line the
        # backend composes for "Resume on machine"). Fails if the flags or the ZIP path change.
        import shlex
        from unittest.mock import patch
        import codex_sessions
        line = 'csl sync pull sess-a --out-dir ~/.csl-resume && csl import ~/.csl-resume/sess-a.zip --cwd /work/project --open'
        pull_line, import_line = [shlex.split(part) for part in line.split(' && ')]
        home = self.root / 'home'; home.mkdir()
        with patch.dict(os.environ, {'HOME': str(home)}):
            parser = codex_sessions.build_parser()
            pulled = parser.parse_args(pull_line[1:]); imported = parser.parse_args(import_line[1:])
            self.assertEqual((pulled.command, pulled.sync_action, pulled.session_id), ('sync', 'pull', 'sess-a'))
            self.assertEqual((imported.command, imported.cwd, imported.open), ('import', Path('/work/project'), True))
            result = pull(self.registry('token-a'), pulled.session_id, pulled.out_dir)
            self.assertEqual(Path(result['path']), Path(imported.bundle).expanduser())
            self.assertEqual(Path(result['path']), home / '.csl-resume' / 'sess-a.zip')

    def test_non_zip_body_and_oversize_leave_no_file(self):
        import csl_sync
        original = csl_sync.MAX_PULL
        try:
            csl_sync.MAX_PULL = 10
            with self.assertRaises(SyncError) as error: pull(self.registry('token-a'), 'sess-a', self.root / 'out')
            self.assertEqual(error.exception.code, 'response_limit_exceeded')
        finally: csl_sync.MAX_PULL = original
        self.assertEqual(list((self.root / 'out').iterdir()), [])


if __name__ == '__main__': unittest.main()

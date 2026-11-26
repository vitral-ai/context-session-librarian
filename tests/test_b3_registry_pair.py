"""Safety of the opt-in local pairing harness; synthetic localhost only."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import os
import threading
import unittest
from unittest.mock import patch

from b3_registry_pair import FixtureRegistry, Tokens, require_stale_writer_refused
from csl_sync_state import SyncError


class PairHarnessTests(unittest.TestCase):
    def test_current_snapshot_requires_writer_fence_not_snapshot_mismatch(self):
        class API:
            def call(self, method, path, **kwargs):
                self.body = kwargs['body']
                raise SyncError(self.error)
        api = API()
        api.error = 'writer_fenced'
        require_stale_writer_refused(api, 'registry-id', 'current-snapshot')
        self.assertEqual(api.body, dict(generation=0, expected_snapshot_id='current-snapshot'))
        api.error = 'writer_snapshot_changed'
        with self.assertRaisesRegex(AssertionError, 'writer_snapshot_changed'):
            require_stale_writer_refused(api, 'registry-id', 'current-snapshot')

    def test_fixture_requests_and_downloads_ignore_environment_proxy(self):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_GET(self):
                body = b'{"ok":true}'
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            env = dict(http_proxy='http://127.0.0.1:1', HTTP_PROXY='http://127.0.0.1:1',
                       all_proxy='http://127.0.0.1:1', ALL_PROXY='http://127.0.0.1:1',
                       no_proxy='', NO_PROXY='')
            with patch.dict(os.environ, env):
                api = FixtureRegistry('http://127.0.0.1:'+str(server.server_port), Tokens('synthetic'))
                self.assertEqual(api.call('GET', '/session'), dict(ok=True))
                sink = io.BytesIO()
                api.download('/package', sink, 1024)
                self.assertEqual(sink.getvalue(), b'{"ok":true}')
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

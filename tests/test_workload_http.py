"""Exact-origin and real loopback TLS fixtures, no remote endpoints."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from csl_sync_state import SyncError
from csl_workload_http import Origin, WorkloadHTTP


def discovery(origin):
    issuer = origin + '/native-auth/v1'
    return {'issuer': issuer, 'token_endpoint': issuer+'/token', 'jwks_uri': issuer+'/jwks',
        'revocation_endpoint': issuer+'/revoke',
        'grant_types_supported': ['urn:ietf:params:oauth:grant-type:device_code', 'refresh_token'],
        'scopes_supported': ['session_sync', 'offline_access'], 'token_endpoint_auth_methods_supported': ['none']}


class OriginTests(unittest.TestCase):
    def test_canonical_https_origin_and_every_request_namespace(self):
        policy = Origin('https://identity.example.test')
        for path in ('/native-auth/v1/token', '/session-registry/v1/limits',
                     '/session-registry/v1/sessions/gemini/fixture/files?limit=100'):
            self.assertEqual(policy.endpoint(policy.value+path), policy.value+path)
        for value in ('http://identity.example.test', 'https://identity.example.test/',
                      'https://user@identity.example.test', 'https://identity.example.test?x=1',
                      'https://identity.example.test#x', 'https://Identity.example.test',
                      'https://identity.example.test:000443', 'https://identity..test',
                      'https://identity.example.test\\evil', 'https://identity.example.test\n'):
            with self.subTest(value=value), self.assertRaises(SyncError): Origin(value)
        for value in ('https://elsewhere.example.test/native-auth/v1/token',
                      'https://identity.example.test:444/native-auth/v1/token',
                      'http://identity.example.test/native-auth/v1/token',
                      'https://identity.example.test/session-registry/authorize',
                      'https://identity.example.test/native-auth/v1/../other',
                      'https://identity.example.test/native-auth/v1/%2e%2e/other',
                      'https://identity.example.test/native-auth/v1//token',
                      'https://identity.example.test/native-auth/v1/token#fragment'):
            with self.subTest(value=value), self.assertRaises(SyncError): policy.endpoint(value)

    def test_discovery_never_delegates_machine_endpoints_or_issuer(self):
        policy = Origin('https://identity.example.test')
        value = discovery(policy.value)
        checked = policy.discovery(value)
        self.assertEqual(checked['device_authorization_endpoint'], policy.issuer+'/device_authorization')
        for name in checked:
            with self.subTest(name=name), self.assertRaises(SyncError):
                policy.discovery({**value, name: 'https://elsewhere.example.test/native-auth/v1/token'})
        for name in ('grant_types_supported', 'scopes_supported', 'token_endpoint_auth_methods_supported'):
            with self.assertRaises(SyncError): policy.discovery({**value, name: []})

    def test_all_methods_refuse_off_origin_before_any_opener_call(self):
        client = WorkloadHTTP('https://identity.example.test')
        client.opener = Mock()
        for method in ('GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'OPTIONS'):
            with self.assertRaises(SyncError):
                client.request('https://elsewhere.example.test/native-auth/v1/token', method, data=b'fixture')
        for headers in ({'Host': 'elsewhere.test'}, {'hOsT': 'elsewhere.test'},
                        {'Host ': 'elsewhere.test'}, {':authority': 'elsewhere.test'},
                        {'Proxy-Authorization': 'fixture'}, {'X-Header': 'x\r\nHost: elsewhere.test'}):
            with self.assertRaises(SyncError):
                client.request('https://identity.example.test/native-auth/v1/token', headers=headers)
        with self.assertRaises(SyncError):
            client.request('https://identity.example.test/native-auth/v1/token', 'CONNECT')
        client.opener.open.assert_not_called()


@unittest.skipUnless(shutil.which('openssl'), 'OpenSSL required for local fixture CA')
class WorkloadTLSTests(unittest.TestCase):
    def test_ca_hostname_get_put_headers_and_refused_redirect(self):
        with tempfile.TemporaryDirectory(prefix='workload-tls-') as directory:
            root = Path(directory); cert, key = root/'ca.crt', root/'key.pem'
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost',
                '-keyout', str(key), '-out', str(cert)], check=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
            seen = []
            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *args): pass
                def do_GET(self): self.reply()
                def do_PUT(self): self.reply()
                def reply(self):
                    body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                    seen.append((self.command, self.path, self.headers.get('Authorization'), body))
                    if self.path.endswith('/redirect'):
                        self.send_response(302)
                        self.send_header('Location', 'https://127.0.0.1:'+str(server.server_port)+'/native-auth/v1/target')
                        self.send_header('Content-Length', '0'); self.end_headers(); return
                    data = json.dumps({'accepted': True}).encode()
                    self.send_response(200); self.send_header('Content-Length', str(len(data)))
                    self.end_headers(); self.wfile.write(data)
            with ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(cert, key)
                server.socket = context.wrap_socket(server.socket, server_side=True)
                thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=.01), daemon=True)
                thread.start()
                try:
                    origin = 'https://localhost:'+str(server.server_port)
                    client = WorkloadHTTP(origin, ca_file=str(cert), attempts=1, timeout=2)
                    self.assertEqual(client.request(origin+'/native-auth/v1/jwks'), {'accepted': True})
                    self.assertEqual(client.request(origin+'/session-registry/v1/fixture?revision=one', 'PUT',
                        data=b'complete-sanitized-record', headers={'Authorization': 'Bearer fixture'}), {'accepted': True})
                    self.assertEqual(seen[-1], ('PUT', '/session-registry/v1/fixture?revision=one',
                                              'Bearer fixture', b'complete-sanitized-record'))
                    with self.assertRaisesRegex(SyncError, '^http_302$'):
                        client.request(origin+'/native-auth/v1/redirect')
                    self.assertEqual(len(seen), 3)
                    with self.assertRaises(SyncError):
                        WorkloadHTTP(origin, attempts=1, timeout=2).request(origin+'/native-auth/v1/jwks')
                    wrong = 'https://127.0.0.1:'+str(server.server_port)
                    with self.assertRaises(SyncError):
                        WorkloadHTTP(wrong, ca_file=str(cert), attempts=1, timeout=2).request(wrong+'/native-auth/v1/jwks')
                    self.assertEqual(len(seen), 3)
                finally:
                    server.shutdown(); thread.join(3)


if __name__ == '__main__': unittest.main()

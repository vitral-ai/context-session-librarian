"""Offline packaging checks. The HTTPS fixture is not a CSL or W7 implementation."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
from urllib.error import HTTPError, URLError
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / 'packaging/workload-collector'
HELM = os.environ.get('VITRAL_FIXTURE_HELM') or shutil.which('helm')
DIGEST = '1' * 64


@unittest.skipUnless(HELM, 'Helm required for chart rendering')
class WorkloadChartTests(unittest.TestCase):
    def render(self, **settings):
        values = {'enabled': 'true', 'identity.origin': 'https://vitral.fixture.test',
                  'identity.caConfigMap': 'test-ca', 'transcripts.provider': 'codex',
                  'location.cluster': 'test:cluster', 'location.label': 'Fixture',
                  'location.workload': 'local/workload', 'collector.image': f'test/csl@sha256:{DIGEST}',
                  'writer.image': f'test/writer@sha256:{DIGEST}', 'writer.command': '{python3}'}
        command = [HELM, 'template', 'fixture', str(CHART), '--kube-version', '1.35.0']
        for name, value in (values | settings).items():
            command.extend(('--set', f'{name}={value}'))
        return subprocess.run(command, capture_output=True, text=True, timeout=10)

    def test_default_opt_out_renders_nothing(self):
        result = subprocess.run([HELM, 'template', 'fixture', str(CHART), '--kube-version', '1.35.0'],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(result.stdout.strip())

    def test_sidecar_reads_transcripts_without_shared_identity_state(self):
        result = self.render()
        self.assertEqual(result.returncode, 0, result.stderr)
        manifest = result.stdout
        self.assertIn('initContainers:', manifest)
        self.assertIn('restartPolicy: Always', manifest)
        self.assertIn('mountPath: /transcripts\n              readOnly: true', manifest)
        self.assertIn('value: vitral-workload-collector', manifest)
        self.assertIn('value: "required"', manifest)
        self.assertIn('fieldPath: metadata.uid', manifest)
        self.assertIn('automountServiceAccountToken: false', manifest)
        self.assertIn('value: /collector-trust/ca.crt', manifest)
        self.assertEqual(manifest.count('mountPath: /collector-state'), 1)
        for forbidden in ('kind: Service', 'hostPath:', 'kind: Role', 'kind: Ingress'):
            self.assertNotIn(forbidden, manifest)

    def test_unqualified_inputs_fail_closed(self):
        failures = ({'collector.image': 'test/csl:latest'}, {'identity.origin': 'https://test/login'},
                    {'transcripts.provider': 'antigravity'}, {'mode': 'daemonSet'},
                    {'terminationGracePeriodSeconds': '29'}, {'collector.flushSeconds': '21'},
                    {'collector.uid': '0'})
        for values in failures:
            with self.subTest(values=values):
                self.assertNotEqual(self.render(**values).returncode, 0)

    def test_daemonset_uses_only_explicit_readonly_claim_and_private_state(self):
        result = self.render(mode='daemonSet', **{'daemonSet.narrowSameOwnerExport': 'true',
                                                'daemonSet.existingClaim': 'approved-transcripts'})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('kind: DaemonSet', result.stdout)
        self.assertIn('claimName: "approved-transcripts"\n            readOnly: true', result.stdout)
        self.assertIn('name: collector-state\n          emptyDir:', result.stdout)
        self.assertNotIn('hostPath:', result.stdout)
        self.assertNotIn('name: writer', result.stdout)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


class FixtureRoutes(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def respond(self, status, body=b'', **headers):
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == '/native-auth/v1/.well-known/oauth-authorization-server':
            origin = f'https://localhost:{self.server.server_port}'
            self.respond(200, json.dumps({'token_endpoint': origin + '/native-auth/v1/token'}).encode())
        elif self.path == '/session-registry/v1/fixture':
            self.respond(200, b'{"view_only":true}')
        elif self.path == '/redirect':
            self.respond(302, Location='https://off-origin.invalid/session-registry/v1/fixture')
        else:
            self.respond(404)


    def do_PUT(self):
        if self.path != '/session-registry/v1/fixture?revision=one':
            self.respond(404)
            return
        if self.headers.get('Authorization') != 'Bearer fixture-approved-replica':
            self.respond(403)
            return
        length = int(self.headers['Content-Length'])
        if length > 8 * 1024 * 1024:
            self.respond(413)
            return
        body = self.rfile.read(length)
        digest = hashlib.sha256(body).hexdigest()
        if (self.headers.get('X-Start') != '0' or self.headers.get('X-End') != str(length)
                or self.headers.get('X-Content-SHA256') != digest
                or self.headers.get('X-Prefix-SHA256') != hashlib.sha256(b'').hexdigest()
                or self.headers.get('Idempotency-Key') != 'fixture-attempt'):
            self.respond(400)
            return
        self.respond(200, json.dumps({'length': length, 'sha256': digest}).encode())


@unittest.skipUnless(shutil.which('openssl'), 'OpenSSL required for fixture TLS')
class FixtureOriginTests(unittest.TestCase):
    def test_local_ca_hostname_and_no_redirect(self):
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            cert, key = root / 'fixture.crt', root / 'fixture.key'
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                            '-days', '1', '-subj', '/CN=localhost',
                            '-addext', 'subjectAltName=DNS:localhost',
                            '-keyout', str(key), '-out', str(cert)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True, timeout=10)
            with ThreadingHTTPServer(('127.0.0.1', 0), FixtureRoutes) as server:
                server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                server_context.load_cert_chain(cert, key)
                server.socket = server_context.wrap_socket(server.socket, server_side=True)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    origin = f'https://localhost:{server.server_port}'
                    client_context = ssl.create_default_context(cafile=str(cert))
                    client = build_opener(ProxyHandler({}), NoRedirect(), HTTPSHandler(context=client_context))
                    with client.open(origin + '/native-auth/v1/.well-known/oauth-authorization-server',
                                     timeout=3) as response:
                        self.assertEqual(json.load(response)['token_endpoint'], origin + '/native-auth/v1/token')
                    with client.open(origin + '/session-registry/v1/fixture', timeout=3) as response:
                        self.assertEqual(json.load(response), {'view_only': True})
                    body = b'x' * (8 * 1024 * 1024)
                    digest = hashlib.sha256(body).hexdigest()
                    headers = {'X-Start': '0', 'X-End': str(len(body)), 'X-Content-SHA256': digest,
                               'X-Prefix-SHA256': hashlib.sha256(b'').hexdigest(),
                               'Idempotency-Key': 'fixture-attempt'}
                    destination = origin + '/session-registry/v1/fixture?revision=one'
                    with self.assertRaises(HTTPError) as denied:
                        client.open(Request(destination, b'fixture', method='PUT'), timeout=3)
                    self.assertEqual(denied.exception.code, 403)
                    denied.exception.close()
                    headers['Authorization'] = 'Bearer fixture-approved-replica'
                    request = Request(destination, body, headers, method='PUT')
                    with client.open(request, timeout=3) as response:
                        self.assertEqual(json.load(response), {'length': len(body), 'sha256': digest})
                    with self.assertRaises(HTTPError) as rejected:
                        client.open(origin + '/redirect', timeout=3)
                    self.assertEqual(rejected.exception.code, 302)
                    rejected.exception.close()
                    with self.assertRaises(URLError):
                        client.open(f'https://127.0.0.1:{server.server_port}/session-registry/v1/fixture',
                                    timeout=3)
                finally:
                    server.shutdown()
                    thread.join(timeout=3)

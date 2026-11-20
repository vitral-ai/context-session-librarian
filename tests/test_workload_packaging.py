"""Offline packaging checks. The HTTPS fixture is not a CSL or W7 implementation."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
import runpy
from pathlib import Path
import shutil
import signal
import ssl
import subprocess
import sys
import tempfile
import time
import threading
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener


from csl_workload_config import Configuration


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / 'packaging/workload-collector'
HELM = os.environ.get('VITRAL_FIXTURE_HELM') or shutil.which('helm')
DIGEST = '1' * 64
PROFILES = {
    'codex': 'codex-0.169.0-text-v1',
    'claude': 'claude-2.1.308-text-v1',
    'gemini': 'gemini-cli-0.68.0-jsonl-text-v1',
}


@unittest.skipUnless(HELM, 'Helm required for chart rendering')
class WorkloadChartTests(unittest.TestCase):
    def render(self, *, release='fixture', chart=CHART, values_file=None, omit=(),
               string_settings=None, **settings):
        values = {'enabled': 'true', 'identity.origin': 'https://vitral.fixture.test',
                  'identity.caConfigMap': 'test-ca', 'transcripts.provider': 'codex',
                  'transcripts.profile': PROFILES['codex'],
                  'location.cluster': 'test:cluster', 'location.label': 'Fixture',
                  'location.workload': 'local/workload', 'collector.image': f'test/csl@sha256:{DIGEST}',
                  'writer.image': f'test/writer@sha256:{DIGEST}', 'writer.command': '{python3}'}
        command = [HELM, 'template', release, str(chart), '--kube-version', '1.35.0']
        if values_file is not None:
            command.extend(('--values', str(values_file)))
        for name, value in (values | settings).items():
            if name not in omit:
                command.extend(('--set', f'{name}={value}'))
        for name, value in (string_settings or {}).items():
            command.extend(('--set-string', f'{name}={value}'))
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

    def test_writer_budget_is_passed_to_mounted_supervisor(self):
        result = self.render(**{'writer.shutdownSeconds': '0'})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('command: ["python3", "/vitral-writer/supervisor.py"]', result.stdout)
        self.assertIn('args: ["0","--","python3"]', result.stdout)
        self.assertIn('mountPath: /vitral-writer\n              readOnly: true', result.stdout)
        default = self.render()
        self.assertIn('args: ["15","--","python3"]', default.stdout)
        self.assertIn('terminationGracePeriodSeconds: 40', result.stdout)

    def test_supervisor_name_is_release_scoped_and_content_versioned(self):
        def supervisor_name(result):
            self.assertEqual(result.returncode, 0, result.stderr)
            match = re.search(r'^kind: ConfigMap\nmetadata:\n  name: ([^\n]+)$',
                              result.stdout, re.MULTILINE)
            self.assertIsNotNone(match)
            name = match.group(1)
            self.assertLessEqual(len(name), 63)
            self.assertIn(f'configMap:\n            name: {name}\n', result.stdout)
            return name

        prefix = 'a' * 43
        alpha = supervisor_name(self.render(release=prefix + '-alpha'))
        self.assertEqual(supervisor_name(self.render(release=prefix + '-alpha')), alpha)
        self.assertNotEqual(supervisor_name(self.render(release=prefix + '-beta')), alpha)
        with tempfile.TemporaryDirectory() as scratch:
            chart = Path(scratch) / 'chart'
            shutil.copytree(CHART, chart)
            with (chart / 'writer-supervisor.py').open('a') as stream:
                stream.write('\n# different supervisor content\n')
            self.assertNotEqual(supervisor_name(self.render(release=prefix + '-alpha', chart=chart)),
                                alpha)

    def test_opt_in_gates_refuse_non_boolean_values_files_and_set_string(self):
        with tempfile.TemporaryDirectory() as scratch:
            values = Path(scratch) / 'values.json'
            for wrong in ('false', 'true', '0', 0, 1, [], {}):
                for gate in ('enabled', 'daemonSet'):
                    with self.subTest(gate=gate, wrong=wrong):
                        entry = ({'enabled': wrong} if gate == 'enabled' else
                                 {'daemonSet': {'narrowSameOwnerExport': wrong,
                                                'existingClaim': 'unattested'}})
                        values.write_text(json.dumps(entry))
                        mode = 'sidecar' if gate == 'enabled' else 'daemonSet'
                        output = self.render(values_file=values, omit=('enabled',) if gate == 'enabled' else (),
                                             mode=mode)
                        self.assertNotEqual(output.returncode, 0, output.stdout)
            for wrong in ('false', 'true', '0'):
                with self.subTest(wrong=wrong):
                    self.assertNotEqual(self.render(string_settings={'enabled': wrong}).returncode, 0)
                    self.assertNotEqual(self.render(mode='daemonSet', string_settings={
                        'daemonSet.narrowSameOwnerExport': wrong,
                        'daemonSet.existingClaim': 'unattested'}).returncode, 0)
            values.write_text('{"enabled":true,"collector":{"flushSeconds":"20"}}')
            self.assertNotEqual(self.render(values_file=values).returncode, 0)

    def test_both_modes_require_and_export_the_exact_provider_profile(self):
        for mode in ('sidecar', 'daemonSet'):
            options = {'daemonSet.narrowSameOwnerExport': 'true',
                       'daemonSet.existingClaim': 'approved-transcripts'} if mode == 'daemonSet' else {}
            for provider, profile in PROFILES.items():
                with self.subTest(mode=mode, provider=provider):
                    result = self.render(mode=mode, **options, **{
                        'transcripts.provider': provider, 'transcripts.profile': profile})
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn(f'name: VITRAL_COLLECTOR_PROFILE\n              value: "{profile}"',
                                  result.stdout)
            for wrong in ('auto', PROFILES['gemini'], 'false', '42'):
                result = self.render(mode=mode, **options, **{'transcripts.profile': wrong})
                self.assertNotEqual(result.returncode, 0, result.stdout)
            result = self.render(mode=mode, omit=('transcripts.profile',), **options)
            self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_rendered_integer_values_are_accepted_by_runner_configuration(self):
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            source, state, ca = root / 'transcripts', root / 'state', root / 'ca.crt'
            source.mkdir()
            state.mkdir()
            ca.touch()
            for mode in ('sidecar', 'daemonSet'):
                options = ({'daemonSet.narrowSameOwnerExport': 'true',
                            'daemonSet.existingClaim': 'approved-transcripts'}
                           if mode == 'daemonSet' else {})
                for provider, profile in PROFILES.items():
                    for spool, flush in ((None, None), ('536870912', '1')):
                        with self.subTest(mode=mode, provider=provider, spool=spool):
                            values = {'transcripts.provider': provider, 'transcripts.profile': profile}
                            if spool is not None:
                                values.update({'collector.spoolLimitBytes': spool,
                                               'collector.flushSeconds': flush})
                            render = self.render(mode=mode, **options, **values)
                            self.assertEqual(render.returncode, 0, render.stderr)
                            rows = re.findall(r'^ +?- name: (VITRAL_[A-Z_]+)\n +value: (.+)$',
                                              render.stdout, re.MULTILINE)
                            env = {key: json.loads(val) if val.startswith('"') else val for key, val in rows}
                            env.update(VITRAL_LOCATION_NAMESPACE='test',
                                       VITRAL_LOCATION_REPLICA='synthetic-replica',
                                       VITRAL_COLLECTOR_TRANSCRIPT_ROOT=str(source),
                                       VITRAL_COLLECTOR_STATE=str(state),
                                       VITRAL_COLLECTOR_TRUST_CA=str(ca))
                            config = Configuration.from_environment(env)
                            self.assertEqual(config.spool_bytes, int(spool or '134217728'))
                            self.assertEqual(config.flush_seconds, int(flush or '20'))
                            self.assertEqual((config.provider, config.profile), (provider, profile))

    def sole(self, **settings):
        sid = '0b9f6b2a-1c3d-4e5f-8a7b-9c0d1e2f3a4b'
        values = {'mode': 'soleProducer', 'transcripts.provider': 'claude', 'transcripts.profile': PROFILES['claude'],
                  'soleProducer.image': f'test/producer@sha256:{DIGEST}', 'soleProducer.expectSession': sid,
                  'soleProducer.credentialSecret': 'claude-token', 'soleProducer.args': '{-p,hello,--session-id,' + sid + '}'}
        return self.render(**(values | settings))

    def test_sole_producer_job_gates_claude_with_rw_and_ro_alias_of_one_volume(self):
        result = self.sole()
        self.assertEqual(result.returncode, 0, result.stderr)
        out = result.stdout
        self.assertEqual(re.findall(r'^kind: (\S+)$', out, re.M), ['Job'])
        for line in ('  backoffLimit: 0', '      restartPolicy: Never', '      automountServiceAccountToken: false',
                     '          command: ["/usr/local/bin/csl-claude-prepare"]',
                     '          args: ["--prepare-seconds","900","--","-p","hello","--session-id","0b9f6b2a-1c3d-4e5f-8a7b-9c0d1e2f3a4b"]',
                     '            - name: transcript\n              mountPath: /agent-private/.claude/projects\n'
                     '            - name: transcript\n              mountPath: /transcripts\n              readOnly: true\n',
                     '            - name: VITRAL_COLLECTOR_EXPECT_SESSION\n              value: "0b9f6b2a-1c3d-4e5f-8a7b-9c0d1e2f3a4b"',
                     '                secretKeyRef:\n                  name: "claude-token"\n                  key: "token"',
                     '            - name: VITRAL_COLLECTOR_PROVIDER\n              value: "claude"'):
            self.assertIn(line, out)
        self.assertEqual(out.count('mountPath: /agent-private/.claude/projects\n              readOnly'), 0)
        self.assertNotIn('supervisor', out)

    def test_sole_producer_refuses_other_providers_bad_session_missing_secret_or_args(self):
        for settings in ({'transcripts.provider': 'codex', 'transcripts.profile': PROFILES['codex']},
                         {'soleProducer.expectSession': 'not-a-uuid'}, {'soleProducer.expectSession': '0B9F6B2A-1C3D-4E5F-8A7B-9C0D1E2F3A4B'},
                         {'soleProducer.credentialSecret': ''}, {'soleProducer.args': '{}'}, {'soleProducer.args': '{-p,hello}'},
                         {'soleProducer.args': '{-p,hello,--session-id,1b9f6b2a-1c3d-4e5f-8a7b-9c0d1e2f3a4b}'},
                         {'soleProducer.image': 'test/producer:latest'}):
            with self.subTest(settings=settings):
                self.assertNotEqual(self.sole(**settings).returncode, 0)

    def test_daemonset_uses_only_explicit_readonly_claim_and_private_state(self):
        result = self.render(mode='daemonSet', **{'daemonSet.narrowSameOwnerExport': 'true',
                                                'daemonSet.existingClaim': 'approved-transcripts'})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('kind: DaemonSet', result.stdout)
        self.assertIn('claimName: "approved-transcripts"\n            readOnly: true', result.stdout)
        self.assertIn('name: collector-state\n          emptyDir:', result.stdout)
        self.assertNotIn('hostPath:', result.stdout)
        self.assertNotIn('name: writer', result.stdout)


class ContainerAppsProfileTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        launcher = root / 'require-profile.py'
        shutil.copyfile(ROOT / 'packaging/container-apps/require-profile.py', launcher)
        shutil.copyfile(CHART / 'profiles.json', root / 'profiles.json')
        self.launcher = runpy.run_path(str(launcher))['main']

    def test_launcher_executes_only_reviewed_pairs_and_preserves_argv_and_environment(self):
        argv = ['require-profile.py', '--', 'a b', '$(literal)', 'semi;colon']
        collector = '/usr/local/bin/csl-workload-collector'
        for provider, profile in PROFILES.items():
            with self.subTest(provider=provider):
                settings = {'VITRAL_COLLECTOR_PROVIDER': provider, 'VITRAL_COLLECTOR_PROFILE': profile,
                            'VITRAL_COLLECTOR_STATE': '/collector-state',
                            'VITRAL_COLLECTOR_TRUST_CA': '/collector-trust/ca.crt'}
                with patch.dict(os.environ, settings, clear=True), patch.object(sys, 'argv', argv), \
                        patch.object(os, 'execv') as execute:
                    self.launcher()
                    execute.assert_called_once_with(collector, [collector, *argv[1:]])
                    self.assertEqual(dict(os.environ), settings)

    def test_launcher_refuses_missing_auto_mismatch_and_unknown_provider(self):
        for provider, profile in (('codex', None), ('codex', 'auto'), ('codex', PROFILES['claude']),
                                  ('antigravity', PROFILES['gemini']), (None, PROFILES['codex'])):
            with self.subTest(provider=provider, profile=profile):
                settings = {}
                if provider is not None:
                    settings['VITRAL_COLLECTOR_PROVIDER'] = provider
                if profile is not None:
                    settings['VITRAL_COLLECTOR_PROFILE'] = profile
                with patch.dict(os.environ, settings, clear=True), patch.object(os, 'execv') as execute:
                    with self.assertRaisesRegex(SystemExit, 'must match'):
                        self.launcher()
                    execute.assert_not_called()


@unittest.skipUnless(hasattr(time, 'CLOCK_BOOTTIME'), 'Linux writer supervisor')
class WriterSupervisorTests(unittest.TestCase):
    supervisor = CHART / 'writer-supervisor.py'

    def test_writer_exit_status_is_preserved(self):
        result = subprocess.run([sys.executable, self.supervisor, '1', '--',
                                 sys.executable, '-c', 'raise SystemExit(7)'], timeout=5)
        self.assertEqual(result.returncode, 7)

    def test_term_ignoring_writer_leaves_drain_window_and_unacked_fixture_is_pending(self):
        with tempfile.TemporaryDirectory() as scratch:
            completed = Path(scratch) / 'completed.jsonl'
            writer = (
                'import os, signal, sys, time; from pathlib import Path; '
                'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
                'Path(sys.argv[1]).write_text(str(os.getpid()) + "\\n"); time.sleep(60)'
            )
            process = subprocess.Popen([sys.executable, self.supervisor, '1', '--',
                                        sys.executable, '-c', writer, str(completed)])
            try:
                ready_deadline = time.monotonic() + 5
                while not completed.exists() and time.monotonic() < ready_deadline:
                    time.sleep(0.02)
                self.assertTrue(completed.exists())
                started = time.clock_gettime(time.CLOCK_BOOTTIME)
                process.send_signal(signal.SIGTERM)
                self.assertEqual(process.wait(timeout=4), 137)
                remaining = 30 - (time.clock_gettime(time.CLOCK_BOOTTIME) - started)
                self.assertGreaterEqual(remaining, 20)

                # Stand-in for the sidecar receipt gate. No ACK arrives
                # within the fixture deadline, so it cannot claim completion.
                ack = threading.Event()
                outcome = 'complete' if ack.wait(timeout=0.05) else 'pending'
                self.assertEqual(outcome, 'pending')
                self.assertTrue(completed.read_bytes().endswith(b'\n'))
            finally:
                if process.poll() is None:
                    if completed.exists():
                        try:
                            os.killpg(int(completed.read_text()), signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    process.kill()
                    process.wait(timeout=5)


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

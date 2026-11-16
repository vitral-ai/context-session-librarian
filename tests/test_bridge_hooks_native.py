"""Opt-in native CLI hook gate; fabricated loopback Responses, no auth/inference.

Run with CSL_RUN_CODEX_NATIVE_TESTS=1. This tests Codex's actual hook engine,
not model behavior. All homes, transcripts and registration are disposable.
"""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid


SOURCE = Path(__file__).resolve().parents[1]
EVENTS = {
    'SessionStart': 'session_start',
    'UserPromptSubmit': 'user_prompt_submit',
    'PostToolUse': 'post_tool_use',
    'Stop': 'stop',
}

# Test-only bootstrap: the owner fixture explicitly associates the first native
# UUID with a fresh endpoint. Production hooks must never auto-register callers.
HOOK_FIXTURE = '''
import json, pathlib, sys, uuid
from types import SimpleNamespace
sys.path.insert(0, SOURCE)
from csl_bridge_store import Store, encode
from csl_bridge_mailbox import Mailbox, now
from csl_bridge_hooks import surface
root = pathlib.Path(__file__).parent
payload = json.load(sys.stdin)
event = payload['hook_event_name']
home = root / 'bridge'
store = Store(home, create=not (home / 'bridge.sqlite').exists())
try:
    if not store.agents():
        with store.transaction() as db:
            db.executemany('INSERT INTO config VALUES(?,?)', [
                ('identity', encode('native')), ('runtime_socket', encode(None))])
            db.execute('INSERT INTO agents VALUES(?,?)', ('native', payload['session_id']))
            db.execute('INSERT INTO peers VALUES(?,?)', ('sender', encode({
                'transport': 'receive-only', 'allow_agents': ['native'],
                'allow_sources': ['sender'], 'allow_ops': ['mail']})))
    mailbox = Mailbox(SimpleNamespace(store=store, identity='native',
        agents=store.agents(), peers=store.peers()))
    message_id = str(uuid.uuid4())
    receipt = mailbox.incoming('sender', {
        'version': 1, 'op': 'mail', 'message_version': 1, 'action': 'send',
        'id': message_id, 'source': 'sender', 'target': 'native',
        'envelope': {'kind': 'message', 'body': 'SYNTHETIC_BODY_' + event,
            'in_reply_to': None, 'grant_ref': None, 'created_at': now(), 'expires_at': None}})
finally:
    store.close()
output = surface(home, 'native', event, payload)
with (root / 'hook-calls.jsonl').open('a') as stream:
    stream.write(json.dumps({'event': event, 'payload': payload,
        'message_id': message_id, 'receipt': receipt, 'output': output}) + '\\n')
if output:
    print(output)
'''


@unittest.skipUnless(os.environ.get('CSL_RUN_CODEX_NATIVE_TESTS') == '1',
                     'Set CSL_RUN_CODEX_NATIVE_TESTS=1 for the isolated native hook gate')
@unittest.skipUnless(os.name == 'posix', 'Native hook fixture uses POSIX commands')
class NativeHookTests(unittest.TestCase):
    def test_native_four_boundaries_headers_stop_guard_without_ack_or_task(self):
        executable = shutil.which('codex')
        if executable is None:
            self.skipTest('Codex CLI is not installed')
        with tempfile.TemporaryDirectory(prefix='csl-native-hooks-') as temp:
            root = Path(temp)
            root.chmod(0o700)
            for folder in ('home', 'codex', 'sqlite', 'project'):
                (root / folder).mkdir(mode=0o700)
            env = {
                'HOME': str(root / 'home'), 'CODEX_HOME': str(root / 'codex'),
                'CODEX_SQLITE_HOME': str(root / 'sqlite'),
                'CLAUDE_CONFIG_DIR': str(root / 'unused-claude'),
                'XDG_CONFIG_HOME': str(root / 'home/config'),
                'XDG_CACHE_HOME': str(root / 'home/cache'),
                'PATH': '/usr/bin:/bin', 'SHELL': '/bin/sh', 'LANG': 'C.UTF-8',
                'NO_PROXY': '127.0.0.1,localhost', 'DISABLE_TELEMETRY': '1',
                'DISABLE_ERROR_REPORTING': '1', 'PYTHONDONTWRITEBYTECODE': '1',
            }
            version = subprocess.run([executable, '--version'], env=env, cwd=root,
                                     capture_output=True, text=True, timeout=10)
            self.assertEqual(version.returncode, 0, version.stderr)
            print('\nNative hook gate CLI: ' + version.stdout.strip(), flush=True)
            requests = []

            class Backend(BaseHTTPRequestHandler):
                def log_message(self, *_):
                    pass

                def do_GET(self):
                    self.send_error(404, 'No model discovery in this fixture')

                def do_POST(self):
                    size = int(self.headers['Content-Length'])
                    if not 0 < size <= 4_000_000:
                        self.send_error(413)
                        return
                    requests.append(json.loads(self.rfile.read(size)))
                    number = len(requests)
                    if number > 3:
                        self.send_error(503, 'Unexpected continuation')
                        return
                    if number == 1:
                        item = {'id': 'fc_' + uuid.uuid4().hex, 'type': 'function_call',
                                'name': 'exec_command', 'call_id': 'call_' + uuid.uuid4().hex,
                                'arguments': json.dumps({'cmd': '/usr/bin/printf CSL_SYNTHETIC_TOOL',
                                    'max_output_tokens': 100, 'yield_time_ms': 1000})}
                    else:
                        item = {'id': 'msg_' + uuid.uuid4().hex, 'type': 'message',
                                'role': 'assistant', 'status': 'completed', 'content': [
                                    {'type': 'output_text', 'text': 'SYNTHETIC FINAL ' + str(number),
                                     'annotations': []}]}
                    events = [
                        {'type': 'response.output_item.added', 'output_index': 0,
                         'item': {**item, 'status': 'in_progress'}},
                        {'type': 'response.output_item.done', 'output_index': 0, 'item': item},
                        {'type': 'response.completed', 'response': {
                            'id': 'resp_' + uuid.uuid4().hex, 'status': 'completed',
                            'output': [item], 'usage': {
                                'input_tokens': 1, 'output_tokens': 1, 'total_tokens': 2}}},
                    ]
                    raw = ''.join('event: ' + e['type'] + '\ndata: ' + json.dumps(e) + '\n\n'
                                  for e in events).encode()
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/event-stream')
                    self.send_header('Content-Length', str(len(raw)))
                    self.end_headers()
                    try:
                        self.wfile.write(raw)
                    except (BrokenPipeError, ConnectionResetError):
                        pass

            server = ThreadingHTTPServer(('127.0.0.1', 0), Backend)
            server.daemon_threads = True
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                script = root / 'owner_test_hook.py'
                script.write_text(HOOK_FIXTURE.replace('SOURCE', repr(str(SOURCE))))
                command = shlex.join([sys.executable, '-B', str(script)])
                hooks = {'hooks': {event: [{'hooks': [{
                    'type': 'command', 'command': command, 'timeout': 3}]}] for event in EVENTS}}
                hook_path = root / 'codex/hooks.json'
                hook_path.write_text(json.dumps(hooks))
                config = '''model_provider="hook_fixture"
model="synthetic-never-generate"
web_search="disabled"
[analytics]
enabled=false
[features]
hooks=true
plugins=false
[model_providers.hook_fixture]
name="Synthetic fixture; no inference"
base_url="http://127.0.0.1:PORT/v1"
wire_api="responses"
requires_openai_auth=false
supports_websockets=false
request_max_retries=0
'''.replace('PORT', str(server.server_port))
                # Match native discovery.rs normalized identity and fingerprint.rs
                # canonical JSON hashing. Trust only this disposable fixture;
                # never bypass hook trust, approvals or sandbox policy.
                for label in EVENTS.values():
                    identity = {'event_name': label, 'hooks': [{
                        'type': 'command', 'command': command, 'timeout': 3, 'async': False}]}
                    canonical = json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()
                    digest = 'sha256:' + hashlib.sha256(canonical).hexdigest()
                    key = str(hook_path) + ':' + label + ':0:0'
                    config += '\n[hooks.state.' + json.dumps(key) + ']\ntrusted_hash=' + json.dumps(digest) + '\n'
                (root / 'codex/config.toml').write_text(config)
                completed = subprocess.run([
                    executable, '--no-daemon', 'exec', '--skip-git-repo-check', '--json',
                    '--cd', str(root / 'project'),
                    'Synthetic native hook fixture. Backend supplies fabricated controls only.',
                ], env=env, cwd=root / 'project', capture_output=True, text=True, timeout=35)
                detail = version.stdout.strip() + '\n' + completed.stdout[-3000:] + completed.stderr[-3000:]
                self.assertEqual(completed.returncode, 0, detail)
                log = root / 'hook-calls.jsonl'
                self.assertTrue(log.exists(), detail)
                calls = [json.loads(line) for line in log.read_text().splitlines()]
                self.assertEqual([c['event'] for c in calls], [*EVENTS, 'Stop'], detail)
                self.assertEqual(len(requests), 3, detail)
                encoded = [json.dumps(request) for request in requests]
                for event, index in [('SessionStart', 0), ('UserPromptSubmit', 0),
                                     ('PostToolUse', 1), ('Stop', 2)]:
                    call = next(c for c in calls if c['event'] == event)
                    self.assertIn(call['message_id'], encoded[index], (event, detail))
                stops = [c for c in calls if c['event'] == 'Stop']
                self.assertIs(stops[0]['payload']['stop_hook_active'], False)
                self.assertIs(stops[1]['payload']['stop_hook_active'], True)
                self.assertEqual(stops[1]['output'], '')
                self.assertTrue(all('SYNTHETIC_BODY_' not in value for value in encoded))
                self.assertIn('CSL_SYNTHETIC_TOOL', encoded[1])
                with sqlite3.connect(root / 'bridge/bridge.sqlite') as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM mail_messages WHERE acknowledged_at IS NOT NULL').fetchone()[0], 0)
                    self.assertEqual(db.execute('SELECT count(*) FROM tasks').fetchone()[0], 0)
            finally:
                server.shutdown()
                server.server_close()
                worker.join(timeout=3)


if __name__ == '__main__':
    unittest.main()

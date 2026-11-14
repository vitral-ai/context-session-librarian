"""Opt-in real CLI queue gate against a disposable synthetic native runtime.

No real model, credentials, personal home or shared daemon is used. The native
app-server and queue CLI are real; Responses output is a fabricated control.
"""
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
import uuid

from csl_bridge_mailbox import Mailbox, grant, now
from csl_bridge_rpc import Runtime
from csl_bridge_store import Store, encode
from experiments.bridge.runtime_gate import Backend, Plan


SOURCE = Path(__file__).resolve().parents[1]


def completed(runtime, thread, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            event = runtime.events.get(timeout=max(.01, deadline - time.monotonic()))
        except queue.Empty:
            break
        if event.get('method') == 'turn/completed' and event['params']['threadId'] == thread:
            return event
    raise AssertionError('No native completed turn observed within the fixture deadline')


@unittest.skipUnless(os.environ.get('CSL_RUN_CODEX_NATIVE_TESTS') == '1',
                     'Set CSL_RUN_CODEX_NATIVE_TESTS=1 for the isolated native queue gate')
@unittest.skipUnless(os.name == 'posix', 'Fixture requires Unix sockets')
class NativeNotifyTests(unittest.TestCase):
    def test_real_queue_helper_wakes_only_the_selected_existing_runtime(self):
        executable = shutil.which('codex')
        if executable is None:
            self.skipTest('Codex CLI is not installed')
        with tempfile.TemporaryDirectory(prefix='csl-native-queue-') as temp:
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
            print('\nNative queue gate CLI: ' + version.stdout.strip(), flush=True)
            backend = Backend()
            runtime = None
            process = None
            store = None
            try:
                config = '''model_provider="queue_fixture"
model="synthetic-no-inference"
web_search="disabled"
[analytics]
enabled=false
[features]
plugins=false
[model_providers.queue_fixture]
name="Synthetic queue control"
base_url="http://127.0.0.1:PORT/v1"
wire_api="responses"
requires_openai_auth=false
supports_websockets=false
request_max_retries=0
'''.replace('PORT', str(backend.server.server_port))
                (root / 'codex/config.toml').write_text(config)
                endpoint = root / 'runtime.sock'
                with (root / 'app-server.log').open('w') as log:
                    process = subprocess.Popen([
                        executable, 'app-server', '--listen', 'unix://' + str(endpoint),
                    ], env=env, cwd=root / 'project', stdin=subprocess.DEVNULL,
                        stdout=log, stderr=log)
                    deadline = time.monotonic() + 12
                    while not endpoint.exists():
                        self.assertIsNone(process.poll(), (root / 'app-server.log').read_text())
                        self.assertLess(time.monotonic(), deadline, 'Private app-server startup timed out')
                        time.sleep(.05)
                    runtime = Runtime(endpoint)
                    thread = runtime.call('thread/start', {
                        'cwd': str(root / 'project'), 'approvalPolicy': 'on-request',
                        'sandbox': 'read-only',
                    })['thread']['id']
                    backend.add(Plan(text='SYNTHETIC SEED'))
                    runtime.call('turn/start', {'threadId': thread,
                        'input': [{'type': 'text', 'text': 'Synthetic seed, no real inference'}]})
                    completed(runtime, thread)
                    self.assertEqual(runtime.call('thread/read', {'threadId': thread})['thread']['status']['type'], 'idle')
                    store = Store(root / 'bridge', create=True)
                    with store.transaction() as db:
                        db.executemany('INSERT INTO config VALUES(?,?)', [
                            ('identity', encode('recipient')), ('runtime_socket', encode(None))])
                        db.execute('INSERT INTO agents VALUES(?,?)', ('recipient', thread))
                        db.execute('INSERT INTO peers VALUES(?,?)', ('sender', encode({
                            'transport': 'receive-only', 'allow_agents': ['recipient'],
                            'allow_sources': ['sender'], 'allow_ops': ['mail']})))
                    grant(store, 'wake-only', 'sender', 'sender', 'recipient', ['activate'],
                          'Synthetic header notification only; no work authority')
                    mailbox = Mailbox(SimpleNamespace(store=store, identity='recipient',
                        agents=store.agents(), peers=store.peers()))
                    message_id = str(uuid.uuid4())
                    mailbox.incoming('sender', {'version': 1, 'op': 'mail',
                        'message_version': 1, 'action': 'send', 'id': message_id,
                        'source': 'sender', 'target': 'recipient', 'envelope': {
                            'kind': 'message', 'body': 'SYNTHETIC_BODY_MUST_NOT_QUEUE',
                            'in_reply_to': None, 'grant_ref': 'wake-only',
                            'created_at': now(), 'expires_at': None}})
                    backend.add(Plan(text='SYNTHETIC QUEUE COMPLETION'))
                    command = [sys.executable, str(SOURCE / 'csl-bridge-notify'),
                        '--home', str(root / 'bridge'), '--agent', 'recipient',
                        '--codex', executable, '--runtime-socket', str(endpoint), '--wake-codex']
                    result = subprocess.run(command, env=env, cwd=root / 'project',
                                            capture_output=True, text=True, timeout=30)
                    detail = version.stdout + result.stdout + result.stderr
                    self.assertEqual(result.returncode, 0, detail)
                    receipt = json.loads(result.stdout)
                    self.assertEqual(receipt['state'], 'submitted_to_native_queue', detail)
                    self.assertEqual(receipt['message_ids'], [message_id])
                    self.assertIs(receipt['model_turn_observed'], False)
                    event = completed(runtime, thread)
                    self.assertEqual(event['params']['turn']['status'], 'completed')
                    encoded = [json.dumps(request) for request in backend.requests]
                    matching = [request for request in encoded if message_id in request]
                    self.assertEqual(len(matching), 1)
                    self.assertTrue(all('SYNTHETIC_BODY_MUST_NOT_QUEUE' not in request for request in encoded))
                    self.assertIsNone(process.poll(), 'Queue must retain the selected native runtime')
                    self.assertEqual(runtime.call('thread/loaded/list', {})['data'].count(thread), 1)
                    self.assertEqual(store.rows('SELECT count(*) AS n FROM tasks')[0]['n'], 0)
                    self.assertIsNone(store.rows('SELECT acknowledged_at FROM mail_messages WHERE id=?', (message_id,))[0]['acknowledged_at'])
                    again = subprocess.run(command, env=env, cwd=root / 'project',
                                           capture_output=True, text=True, timeout=30)
                    self.assertEqual(again.returncode, 0, again.stderr)
                    self.assertIs(json.loads(again.stdout)['activation_attempted'], False)
            finally:
                if store is not None:
                    store.close()
                if runtime is not None:
                    runtime.close()
                if process is not None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=3)
                backend.close()


if __name__ == '__main__':
    unittest.main()

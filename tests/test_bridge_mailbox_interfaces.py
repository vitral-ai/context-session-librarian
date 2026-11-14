"""Actual CLI/IPC and MCP mailbox surfaces, fresh client-only homes only."""
import json
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from csl_bridge_mcp import Adapter, serve
from csl_bridge_store import BridgeError, encode
from csl_bridge_transport import exchange
import test_bridge_mailbox as helpers
uid = helpers.uid

ROOT = Path(__file__).resolve().parents[1]


class MailboxInterfaces(unittest.TestCase):
    setUp = helpers.MailboxTests.setUp
    tearDown = helpers.MailboxTests.tearDown
    exchange = helpers.MailboxTests.exchange
    send = helpers.MailboxTests.send
    flush = helpers.MailboxTests.flush
    receive = helpers.MailboxTests.receive
    def test_actual_cli_and_mcp_use_bound_registered_identity(self):
        self.a.start(); self.b.start()
        lab = self.root/'process-home'; lab.mkdir()
        env = {'PATH': os.environ['PATH'], 'HOME': str(lab), 'CODEX_HOME': str(lab/'codex'),
               'CLAUDE_CONFIG_DIR': str(lab/'claude'), 'LANG': 'C.UTF-8'}
        text = self.root/'selected.txt'; text.write_text('Synthetic CLI message')
        message_id = uid()
        result = subprocess.run([sys.executable, str(ROOT/'csl'), 'bridge', '--home', str(self.a.store.home),
                                 'send', '--from', 'alice', '--peer', 'bob', '--to', 'bob',
                                 '--message-id', message_id, '--text-file', str(text)], env=env,
                                capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['id'], message_id)
        for _ in range(40):
            inbox = Adapter(self.b.store.home, 'bob').call('receive', {})['data']['messages']
            if inbox:
                break
            time.sleep(.05)
        self.assertEqual(inbox[0]['from'], {'peer': 'alice', 'agent': 'alice'})
        adapter = Adapter(self.b.store.home, 'bob')
        replied = adapter.call('reply', {'in_reply_to': message_id, 'body': 'Synthetic MCP reply', 'id': uid()})['data']
        self.assertTrue(replied['facts']['queued'])
        adapter.call('ack', {'message_id': message_id})
        for _ in range(40):
            view = Adapter(self.a.store.home, 'alice').call('receive', {'outbox': True})['data']['messages'][0]
            if view['facts']['acknowledged'] and view['facts']['replied']:
                break
            time.sleep(.05)
        self.assertTrue(view['facts']['acknowledged']); self.assertTrue(view['facts']['replied'])
        self.assertFalse(self.a.store.rows('SELECT * FROM tasks'))

    def test_mcp_collision_is_explicit_rejection_not_uncertainty(self):
        self.a.start()
        adapter = Adapter(self.a.store.home, 'alice')
        arguments = {'peer': 'bob', 'target': 'bob', 'body': 'First', 'id': uid()}
        original = adapter.call('send', arguments)['data']
        duplicate = adapter.call('send', arguments)['data']
        self.assertEqual(original['id'], duplicate['id'])
        rejected = adapter.call('send', {**arguments, 'body': 'Different'})['data']
        self.assertEqual(rejected, {'state': 'rejected', 'reason': 'id_collision'})
        tools = {tool['name'] for tool in adapter.catalog()}
        self.assertTrue({'send', 'receive', 'ack', 'reply'} <= tools)
        self.assertFalse({'grant', 'grant-revoke'} & tools)

    def test_stdio_mcp_collision_returns_typed_mail_rejection(self):
        self.a.start()
        adapter = Adapter(self.a.store.home, 'alice')
        args = {'peer': 'bob', 'target': 'bob', 'body': 'Original', 'id': uid()}
        adapter.call('send', args)
        inputs = [{'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {'protocolVersion': '2024-11-05'}},
                  {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call', 'params': {'name': 'send', 'arguments': {**args, 'body': 'Changed'}}}]
        output = io.StringIO()
        serve(adapter, io.BytesIO(('\n'.join(encode(item) for item in inputs)+'\n').encode()), output)
        response = next(json.loads(line) for line in output.getvalue().splitlines() if json.loads(line)['id'] == 2)
        data = json.loads(response['result']['content'][0]['text'])['data']
        self.assertEqual(data, {'state': 'rejected', 'reason': 'id_collision'})

    def test_mail_only_peers_do_not_poll_legacy_watch(self):
        self.a.peer_loop = lambda *_: self.fail('Mailbox-only peer must not start legacy event watch')
        self.a.start()
        self.assertEqual(len(self.a.threads), 2)  # mailbox delivery and local IPC only

    def test_older_peer_negative_response_is_confirmed_rejection_not_timeout(self):
        original = self.send()
        wire = json.loads(self.a.store.rows('SELECT request FROM operations WHERE id=?', (original['id'],))[0]['request'])
        negative = SimpleNamespace(returncode=1, stdout=b'{"ok":false,"data":{"error":"Bridge receiver rejected request"}}')
        with patch('csl_bridge_transport.subprocess.run', return_value=negative):
            result = exchange({'transport': 'operator', 'helper': '/synthetic/helper'}, 'alice', wire)
        self.assertEqual(result, {'state': 'rejected', 'reason': 'peer_rejected_reason_unknown'})
        fixture = ROOT/'tests/fixtures/bridge_v030_negative_peer.py'
        completed = subprocess.run([sys.executable, str(fixture)], input=encode(wire)+'\n', capture_output=True, text=True)
        self.assertEqual(completed.returncode, 1)
        self.assertFalse(json.loads(completed.stdout)['ok'])
        with patch('csl_bridge_transport.subprocess.run', side_effect=subprocess.TimeoutExpired('synthetic', 25)):
            with self.assertRaises(BridgeError):
                exchange({'transport': 'operator', 'helper': '/synthetic/helper'}, 'alice', wire)

    def test_receive_pages_are_byte_bounded_without_losing_messages(self):
        for index in range(10):
            self.send(body=str(index)+'x'*11990)
        self.flush(self.a, 'bob')
        found, cursor = [], 0
        for _ in range(10):
            result = self.receive(after=cursor, limit=100)
            self.assertLess(len(encode(result).encode()), 61000)
            if not result['messages']:
                break
            found.extend(m['id'] for m in result['messages']); cursor = result['cursor']
        self.assertEqual(len(found), 10); self.assertEqual(len(set(found)), 10)

    def test_recipient_sqlite_failure_is_uncertain_and_recovers_over_actual_ipc(self):
        self.b.mailbox
        self.b.store.db.execute("CREATE TRIGGER transient_receiver BEFORE INSERT ON mail_messages BEGIN SELECT RAISE(ABORT,'synthetic temporary storage failure'); END")
        self.b.start()
        self.send()
        self.a.mailbox.flush('bob')  # actual lab transport -> private recipient IPC
        view = self.receive(self.a, agent='alice', outbox=True)['messages'][0]
        self.assertEqual(view['delivery']['state'], 'uncertain')
        self.assertEqual(self.receive()['messages'], [])
        self.b.store.db.execute('DROP TRIGGER transient_receiver')
        self.a.mailbox.flush('bob')  # receipt says absent, then same immutable send succeeds
        self.assertEqual(len(self.receive()['messages']), 1)
        self.assertTrue(self.receive(self.a, agent='alice', outbox=True)['messages'][0]['facts']['persisted'])


if __name__ == '__main__':
    unittest.main()

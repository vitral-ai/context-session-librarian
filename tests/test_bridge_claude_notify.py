"""Focused native notification tests; no real native sessions or tokens."""
import json
import os
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import uuid

from csl_bridge_engine import Connector
from csl_bridge_store import BridgeError, Store, encode
from csl_bridge_claude_notify import notify, owned_json


class NotifyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = Path(self.temp.name) / 'home'
        self.session = str(uuid.uuid4())
        self.mid = str(uuid.uuid4())
        store = Store(self.home, create=True)
        with store.transaction() as db:
            db.executemany('INSERT INTO config VALUES(?,?)', [('identity', encode('local')), ('runtime_socket', encode(None))])
            db.execute('INSERT INTO agents VALUES(?,?)', ('target', self.session))
            db.execute('INSERT INTO peers VALUES(?,?)', ('remote', encode({'transport': 'receive-only',
                'allow_agents': ['target'], 'allow_sources': ['sender'], 'allow_ops': ['mail']})))
        store.close()
        self.connector = Connector(self.home)
        # Use the actual mailbox validator and durable incoming operation.
        other = Store(Path(self.temp.name) / 'other', create=True)
        with other.transaction() as db:
            db.executemany('INSERT INTO config VALUES(?,?)', [('identity', encode('remote')), ('runtime_socket', encode(None))])
            db.execute('INSERT INTO agents VALUES(?,?)', ('sender', str(uuid.uuid4())))
            db.execute('INSERT INTO peers VALUES(?,?)', ('local', encode({'transport': 'receive-only',
                'allow_agents': ['sender'], 'allow_sources': ['target'], 'allow_ops': ['mail']})))
        other.close()
        source = Connector(Path(self.temp.name) / 'other')
        source.mailbox.queue('sender', 'local', 'target', self.mid, 'message', 'SECRET REMOTE BODY')
        req = source.store.rows('SELECT request FROM operations')[0]['request']
        self.connector.local({'local_op': '_incoming', 'trusted_peer': 'remote', 'request': json.loads(req)})
        source.close()
        self.record = {'pid': os.getpid(), 'procStart': '123'}
        self.sock = MagicMock()
        self.sock.getsockopt.return_value = struct.pack('3i', os.getpid(), os.getuid(), 0)
        self.sock.__enter__.return_value = self.sock

    def tearDown(self):
        self.connector.close()
        self.temp.cleanup()

    def run_notify(self, agent='target', mid=None):
        with patch('csl_bridge_claude_notify.endpoint', return_value=(self.record, Path('/test/socket'), '0' * 32)), \
             patch('csl_bridge_claude_notify.socket.socket', return_value=self.sock), \
             patch('csl_bridge_claude_notify.start_ticks', return_value='123'):
            return notify(self.home, agent, mid or self.mid)

    def test_compact_preview_bound_session_and_no_ack(self):
        self.assertTrue(self.run_notify())
        raw = self.sock.sendall.call_args.args[0].decode()
        self.assertIn('SECRET REMOTE BODY', raw)
        frames = [json.loads(line) for line in raw.splitlines()]
        self.assertEqual(frames[1]['session_id'], self.session)
        self.assertEqual(frames[1]['priority'], 'next')
        self.assertEqual(frames[1]['from'], 'csl-mailbox')
        self.assertEqual('CSL from sender (' + self.mid[:8] + '):\nSECRET REMOTE BODY', frames[1]['message']['content'])
        self.assertIsNone(self.connector.mailbox.message(self.mid)['acknowledged_at'])

    def test_duplicate_pull_attempts_native_once(self):
        self.assertTrue(self.run_notify())
        self.assertFalse(self.run_notify())
        self.assertEqual(self.sock.sendall.call_count, 1)

    def test_unknown_message_or_wrong_endpoint_cannot_wake(self):
        self.assertFalse(self.run_notify(mid=str(uuid.uuid4())))
        with self.assertRaises(BridgeError):
            self.run_notify(agent='wrong')
        self.sock.sendall.assert_not_called()

    def test_server_pid_change_refuses_before_write(self):
        self.sock.getsockopt.return_value = struct.pack('3i', os.getpid() + 1, os.getuid(), 0)
        with self.assertRaises(BridgeError):
            self.run_notify()
        self.sock.sendall.assert_not_called()

    def fallback(self):
        from csl_bridge_hooks import surface
        return surface(self.home, 'target', 'UserPromptSubmit', {'session_id': self.session})

    def test_native_success_suppresses_hook_and_hook_suppresses_native(self):
        self.assertTrue(self.run_notify())
        self.assertEqual(self.fallback(), '')
        self.assertFalse(self.run_notify())

    def test_hook_first_suppresses_native(self):
        self.assertIn('SECRET REMOTE BODY', self.fallback())
        self.assertFalse(self.run_notify())
        self.sock.sendall.assert_not_called()

    def test_failed_write_keeps_hook_eligible_and_mail_retained(self):
        self.sock.sendall.side_effect = OSError('lost connection')
        with self.assertRaises(OSError):
            self.run_notify()
        self.assertIn('SECRET REMOTE BODY', self.fallback())
        self.assertEqual(self.fallback(), '')
        self.assertIsNotNone(self.connector.mailbox.message(self.mid))

    def test_metadata_refuses_symlink_and_writable_file(self):
        path = Path(self.temp.name) / 'metadata'
        path.write_text('{}')
        path.chmod(0o666)
        with self.assertRaises(BridgeError):
            owned_json(path)
        path.chmod(0o600)
        link = path.with_name('link')
        link.symlink_to(path)
        with self.assertRaises(OSError):
            owned_json(link)

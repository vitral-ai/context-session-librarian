"""Disk-backed outbound-only SSH mailbox transport, no model or SSH required."""
from pathlib import Path
import io
import json
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
import uuid

from csl_bridge_engine import Connector
from csl_bridge_store import BridgeError, Store, encode
from csl_bridge_ssh_pull import cached_result, client, main, pending_mail, server, ssh_command


class PullMailTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.connectors = {}
        for own, other in [('laptop', 'remote'), ('remote', 'laptop')]:
            store = Store(self.root / own, create=True)
            with store.transaction() as db:
                db.executemany('INSERT INTO config VALUES(?,?)',
                               [('identity', encode(own)), ('runtime_socket', encode(None))])
                db.execute('INSERT INTO agents VALUES(?,?)', (own, str(uuid.uuid4())))
                db.execute('INSERT INTO peers VALUES(?,?)', (other, encode({
                    'transport': 'receive-only', 'allow_agents': [own],
                    'allow_sources': [other], 'allow_ops': ['mail', 'agents']})))
            store.close()
            self.connectors[own] = Connector(self.root / own, runtime_factory=Mock(side_effect=AssertionError))
        self.cache = self.root / 'cache'
        self.mid = str(uuid.uuid4())
        self.remote = self.connectors['remote']
        self.laptop = self.connectors['laptop']
        self.remote.mailbox.queue('remote', 'laptop', 'laptop', self.mid, 'message', 'return mail')
        self.request = {'version': 1, 'op': 'agents', 'id': str(uuid.uuid4())}

    def tearDown(self):
        for connector in self.connectors.values():
            connector.close()
        self.temp.cleanup()

    def local(self, home, request, **kwargs):
        return self.connectors[Path(home).name].local(request)

    def exchange(self, packet):
        return server(self.root / 'remote', 'laptop', self.cache, packet)

    def pull(self, exchange=None):
        with patch('csl_bridge_ssh_pull.local_call', side_effect=self.local):
            return client(self.root / 'laptop', 'remote', self.request, exchange or self.exchange)

    def flush(self):
        with patch('csl_bridge_transport.exchange', side_effect=lambda config, peer, req: cached_result(self.cache, req)):
            self.remote.mailbox.flush('laptop')

    def test_ssh_rejects_stale_silent_streams_and_reopens_on_demand(self):
        command = ssh_command("remote", "/private/control", "fixed-command")
        for option in ("-oServerAliveInterval=5", "-oServerAliveCountMax=2",
                       "-oConnectTimeout=15", "-oControlMaster=auto", "-oControlPersist=60"):
            self.assertIn(option, command)
        self.assertEqual(command[-3:], ["--", "remote", "fixed-command"])

    def test_link_timeout_reports_reconciliation_without_losing_operation_id(self):
        output = io.StringIO()
        stdin = Mock(buffer=io.BytesIO((encode(self.request) + '\n').encode()))
        argv = ['pull', 'client', '--ssh-alias', 'remote', '--control-path', '/private/control',
                '--remote-command', 'fixed-command']
        with patch('sys.argv', argv), patch('sys.stdin', stdin), patch('sys.stdout', output), \
                patch('csl_bridge_ssh_pull.subprocess.run', side_effect=subprocess.TimeoutExpired('ssh', 22)):
            self.assertEqual(main(), 0)
        result = json.loads(output.getvalue())
        self.assertTrue(result['ok'])
        self.assertEqual(result['data']['state'], 'uncertain')
        self.assertEqual(result['data']['reason'], 'ssh_pull_unavailable')
        self.assertIn('operation ID', result['data']['recovery'])

    def test_real_outbox_persistence_and_normal_flush(self):
        self.pull()
        self.assertEqual(self.laptop.mailbox.message(self.mid)['direction'], 'in')
        self.flush()
        self.assertIsNotNone(self.remote.mailbox.message(self.mid)['persisted_at'])
        self.assertEqual(pending_mail(self.remote.store, 'laptop'), [])

    def test_disconnect_after_recipient_persistence_replays_once(self):
        def disconnected(packet):
            if packet['receipts']:
                raise OSError('connection lost before acknowledgement')
            return self.exchange(packet)
        result = self.pull(disconnected)
        self.assertIsInstance(result, dict)
        self.assertTrue(pending_mail(self.remote.store, 'laptop'))
        self.pull()
        self.flush()
        self.assertEqual(len(self.laptop.store.rows('SELECT * FROM mail_messages WHERE id=?', (self.mid,))), 1)
        self.assertIsNotNone(self.remote.mailbox.message(self.mid)['persisted_at'])

    def test_local_mail_storage_failure_preserves_known_forward_result(self):
        def unavailable(home, request, **kwargs):
            if Path(home).name == 'laptop':
                raise BridgeError('local storage unavailable')
            return self.local(home, request, **kwargs)
        with patch('csl_bridge_ssh_pull.local_call', side_effect=unavailable):
            result = client(self.root / 'laptop', 'remote', self.request, self.exchange)
        self.assertIsInstance(result, dict)
        self.assertTrue(pending_mail(self.remote.store, 'laptop'))
        self.assertIsNone(self.laptop.mailbox.message(self.mid))
        self.pull()
        self.flush()
        self.assertIsNotNone(self.laptop.mailbox.message(self.mid))

    def test_opted_notification_runs_only_after_real_persistence(self):
        notifier = Mock()
        with patch('csl_bridge_ssh_pull.local_call', side_effect=self.local):
            client(self.root / 'laptop', 'remote', self.request, self.exchange, notifier)
        self.assertEqual(notifier.call_args.args[0]['id'], self.mid)
        self.assertIsNotNone(self.laptop.mailbox.message(self.mid))

    def test_native_notification_failure_keeps_mail_and_forward_result(self):
        with patch('csl_bridge_ssh_pull.local_call', side_effect=self.local), \
             patch('sys.stderr', io.StringIO()):
            result = client(self.root / 'laptop', 'remote', self.request, self.exchange,
                            Mock(side_effect=OSError('native unavailable')))
        self.assertIsInstance(result, dict)
        self.assertIsNotNone(self.laptop.mailbox.message(self.mid))
        self.flush()
        self.assertIsNotNone(self.remote.mailbox.message(self.mid)['persisted_at'])

    def test_cached_receipt_survives_adapter_restart(self):
        self.pull()
        request = pending_mail(self.remote.store, 'laptop')[0]
        self.assertEqual(cached_result(self.cache, request)['state'], 'persisted')
        self.flush()

    def test_normal_source_acl_still_rejects_forgery(self):
        self.laptop.peers['remote']['allow_sources'] = []
        self.pull()
        self.assertIsNone(self.laptop.mailbox.message(self.mid))
        self.flush()
        operation = self.remote.store.rows('SELECT state FROM operations WHERE id=?', (self.mid,))[0]
        self.assertEqual(operation['state'], 'rejected')

    def test_receipt_cannot_change_message_or_endpoint(self):
        self.pull()
        request = dict(pending_mail(self.remote.store, 'laptop')[0])
        request['target'] = 'someone-else'
        with self.assertRaises(BridgeError):
            cached_result(self.cache, request)

    def test_receipt_must_reference_existing_exact_outbox_request(self):
        request = pending_mail(self.remote.store, 'laptop')[0]
        request = {**request, 'id': str(uuid.uuid4())}
        with self.assertRaises(BridgeError):
            self.exchange({'request': None, 'receipts': [{'request': request, 'result': {'state': 'persisted'}}]})


if __name__ == '__main__':
    unittest.main()

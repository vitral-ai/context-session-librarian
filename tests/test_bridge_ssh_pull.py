"""Disk-backed outbound-only SSH mailbox transport, no model or SSH required."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import uuid

from csl_bridge_engine import Connector
from csl_bridge_store import BridgeError, Store, encode
from csl_bridge_ssh_pull import cached_result, client, pending_mail, server


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

    def local(self, home, request):
        return self.connectors[Path(home).name].local(request)

    def exchange(self, packet):
        return server(self.root / 'remote', 'laptop', self.cache, packet)

    def pull(self, exchange=None):
        with patch('csl_bridge_ssh_pull.local_call', side_effect=self.local):
            return client(self.root / 'laptop', 'remote', self.request, exchange or self.exchange)

    def flush(self):
        with patch('csl_bridge_transport.exchange', side_effect=lambda config, peer, req: cached_result(self.cache, req)):
            self.remote.mailbox.flush('laptop')

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
        with self.assertRaises(OSError):
            self.pull(disconnected)
        self.pull()
        self.flush()
        self.assertEqual(len(self.laptop.store.rows('SELECT * FROM mail_messages WHERE id=?', (self.mid,))), 1)
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

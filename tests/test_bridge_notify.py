from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_bridge_mailbox as fixtures
from csl_bridge_mailbox import grant
from csl_bridge_notify import notify
from csl_bridge_store import BridgeError


class NotifyTests(unittest.TestCase):
    def setUp(self):
        import socket
        fixtures.MailboxTests.setUp(self)
        self.listener = socket.socket(socket.AF_UNIX)
        self.endpoint = self.root/'native.sock'
        self.listener.bind(str(self.endpoint))

    def tearDown(self):
        self.listener.close()
        fixtures.MailboxTests.tearDown(self)
    send = fixtures.MailboxTests.send
    flush = fixtures.MailboxTests.flush
    receive = fixtures.MailboxTests.receive
    exchange = fixtures.MailboxTests.exchange

    def notify(self, executable, wake=True):
        return notify(self.root/'bob', 'bob', executable, wake, self.endpoint)

    def test_activation_requires_both_owner_opt_in_and_live_local_grant(self):
        import sys
        executable = sys.executable
        self.send(); self.flush(self.a, 'bob')
        with self.assertRaises(BridgeError):
            self.notify(executable, False)
        with patch('csl_bridge_notify.subprocess.run') as run:
            self.assertFalse(self.notify(executable)['activation_attempted'])
            run.assert_not_called()
        grant(self.b.store, 'wake-test', 'alice', 'alice', 'bob', ['communicate', 'activate'], 'Synthetic notification only')
        sent = self.send(body='Never include this body in the queue', grant_ref='wake-test')
        self.flush(self.a, 'bob')
        with patch('csl_bridge_notify.subprocess.run', return_value=SimpleNamespace(returncode=0)) as run:
            value = self.notify(executable)
            self.assertEqual(value['state'], 'submitted_to_native_queue')
            self.assertFalse(value['model_turn_observed'])
            self.assertEqual(value['message_ids'], [sent['id']])
            self.assertNotIn('Never include', run.call_args.args[0][-1])
            self.assertIn('--resume', run.call_args.args[0])
            self.assertFalse(self.notify(executable)['activation_attempted'])
            self.assertEqual(run.call_count, 1)
        self.assertFalse(self.receive()['messages'][1]['facts']['acknowledged'])

    def test_uncertain_attempt_is_not_blindly_retried_and_revocation_is_honored(self):
        import sys
        grant(self.b.store, 'wake-test', 'alice', 'alice', 'bob', ['activate'], 'Synthetic notification only')
        self.send(grant_ref='wake-test'); self.flush(self.a, 'bob')
        with patch('csl_bridge_notify.subprocess.run', side_effect=OSError('synthetic')) as run:
            self.assertEqual(self.notify(sys.executable)['state'], 'unconfirmed')
            retried = self.notify(sys.executable)
            self.assertFalse(retried['activation_attempted'])
            self.assertEqual(retried['state'], 'unconfirmed')
            self.assertEqual(len(retried['previous_unconfirmed_ids']), 1)
            self.assertEqual(run.call_count, 1)
        self.send(grant_ref='wake-test'); self.flush(self.a, 'bob')
        grant(self.b.store, 'wake-test', None, None, None, None, None, revoke=True)
        with patch('csl_bridge_notify.subprocess.run') as run:
            self.assertFalse(self.notify(sys.executable)['activation_attempted'])
            run.assert_not_called()

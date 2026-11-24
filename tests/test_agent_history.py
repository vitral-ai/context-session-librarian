"""Pair history with synthetic identities only; no native process or network."""
import uuid
import argparse
import contextlib
import io
from unittest.mock import patch
import csl_agents
import test_agent_hub
import unittest
from csl_agent_hub import Hub
from csl_bridge_store import BridgeError


class HistoryTests(unittest.TestCase):
    setUp = test_agent_hub.HubTests.setUp
    tearDown = test_agent_hub.HubTests.tearDown
    do = test_agent_hub.HubTests.do
    allow = test_agent_hub.HubTests.allow
    send = test_agent_hub.HubTests.send
    def history(self, source='client', partner='claude', machine='remote', **kwargs):
        return self.do(machine, action='history', source=source, **({'with': partner} if partner else {}), **kwargs)['messages']

    def test_both_directions_order_delivery_and_full_id(self):
        self.allow()
        first = self.send(text='a' * 10000)['id']
        second = self.send('client', 'claude', 'remote', text='reply')['id']
        self.do('remote', action='ack', id=first, receipt={'state': 'delivered'})
        rows = self.history()
        self.assertEqual([r['id'] for r in rows], [first, second])
        self.assertEqual([r['state'] for r in rows], ['delivered', 'queued'])
        self.assertEqual(rows[0]['body'], 'a' * 10000)
        self.assertTrue(rows[0]['time'])
        self.assertEqual(self.history(partner=None, id=first), rows[:1])

    def test_private_pair_and_owner(self):
        self.allow(); mid = self.send()['id']
        with self.assertRaises(BridgeError):
            self.history('other', None, 'third', id=mid)
        with self.assertRaises(BridgeError):
            self.history('claude', 'client', 'remote')
        self.assertEqual(self.history('other', None, 'laptop', id=mid)[0]['id'], mid)
        self.assertEqual(self.history('other', 'client', 'third'), [])

    def test_refused_is_durable_without_delivery(self):
        mid = str(uuid.uuid4())
        with self.assertRaises(BridgeError):
            self.send(id=mid)
        self.hub = Hub(self.path, self.machines)
        self.assertEqual(self.history()[0]['state'], 'refused')
        self.assertEqual(self.do('remote', action='pull')['envelopes'], [])
        self.allow(); self.send(id=mid)
        self.assertEqual(self.history()[0]['state'], 'queued')
        self.assertEqual(len(self.history()), 1)

    def test_retention_does_not_erase_delivery_or_idempotency(self):
        self.hub = Hub(self.path, self.machines, history_retention=2)
        self.allow()
        ids = [self.send(text=str(i))['id'] for i in range(4)]
        self.assertEqual([r['id'] for r in self.history(last=2)], ids[-2:])
        self.assertEqual(self.send(id=ids[0], text='0')['state'], 'queued')
        self.assertEqual(self.history(partner=None, id=ids[0]), [])
        self.assertEqual(self.do('remote', action='pull')['envelopes'][0]['id'], ids[0])
        self.hub = Hub(self.path, self.machines, history_retention=2)
        self.assertEqual([r['id'] for r in self.history(last=2)], ids[-2:])
        with self.assertRaises(BridgeError):
            self.history(last=3)

    def test_rebinding_does_not_expose_previous_machine_history(self):
        self.allow(); self.send()
        self.do('laptop', action='register', name='client', cli='codex', machine='third', session=str(uuid.uuid4()))
        with self.assertRaises(BridgeError):
            self.history('client', 'claude', 'third')

    def test_backfill_existing_envelopes_once(self):
        self.allow(); mid = self.send()['id']
        with self.hub.connect() as db:
            db.execute('DELETE FROM history'); db.execute('DELETE FROM history_migrations')
        self.hub = Hub(self.path, self.machines)
        self.assertEqual(self.history()[0]['id'], mid)
        with self.hub.connect() as db:
            db.execute('DELETE FROM history')
        self.hub = Hub(self.path, self.machines)
        self.assertEqual(self.history(), [])


class HistoryCLITests(unittest.TestCase):
    def parse(self, *argv):
        parser = argparse.ArgumentParser()
        csl_agents.add_commands(parser.add_subparsers(), None)
        return parser.parse_args(argv)

    def test_env_identity_default_count_and_full_id(self):
        for selection, expected in [(('--with', 'partner'), {'with': 'partner'}),
                                    (('--id', str(uuid.uuid4())), None)]:
            args = self.parse('agents', 'history', *selection)
            with patch.dict('os.environ', {'CSL_AGENT': 'reader'}), \
                 patch('csl_agents.config', return_value={}), \
                 patch('csl_agents.call', return_value={'messages': []}) as call, \
                 contextlib.redirect_stdout(io.StringIO()):
                csl_agents._execute(args)
            request = call.call_args.args[1]
            self.assertEqual(request, {'action': 'history', 'source': 'reader',
                                      **(expected or {'id': selection[1]})})

    def test_explicit_identity_and_count(self):
        args = self.parse('agents', 'history', '--with', 'partner', '--from', 'reader', '--last', '20')
        with patch('csl_agents.config', return_value={}), \
             patch('csl_agents.call', return_value={'messages': []}) as call, \
             contextlib.redirect_stdout(io.StringIO()):
            csl_agents._execute(args)
        self.assertEqual(call.call_args.args[1],
                         {'action': 'history', 'source': 'reader', 'with': 'partner', 'last': 20})

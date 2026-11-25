"""Coordinator metadata cannot alter delivery bindings or leak refusal channels."""
import argparse
import unittest
from unittest.mock import patch
import csl_agents
import test_agent_hub
from csl_bridge_store import BridgeError


class OwnerTests(unittest.TestCase):
    setUp = test_agent_hub.HubTests.setUp
    tearDown = test_agent_hub.HubTests.tearDown
    do = test_agent_hub.HubTests.do
    allow = test_agent_hub.HubTests.allow
    send = test_agent_hub.HubTests.send
    def set_owner(self, key, owner, machine='laptop'):
        record = self.do('laptop', action='show', name=key)
        record.pop('owner', None)
        return self.do(machine, action='register', **record, owner=owner)

    def notices(self):
        with self.hub.connect() as db:
            return [dict(r) for r in db.execute(
                'SELECT target,id FROM envelopes WHERE source="hub-notifier" ORDER BY target')]

    def test_refusal_goes_to_sender_and_its_owner_only_and_deduplicates(self):
        self.set_owner('claude', 'claude')
        self.set_owner('client', 'other')
        import uuid
        mid = str(uuid.uuid4())
        for _ in range(2):
            with self.assertRaisesRegex(BridgeError, 'Notified.*client, other'):
                self.send(source='client', target='builder', machine='remote', id=mid)
        self.assertEqual([r['target'] for r in self.notices()], ['client', 'other'])
        self.assertEqual(len(self.do('remote', action='pull')['envelopes']), 1)
        self.assertEqual(len(self.do('third', action='pull')['envelopes']), 1)
        self.assertEqual(self.do('laptop', action='pull')['envelopes'], [])

    def test_self_owner_gets_one_notice_and_unknown_target_uses_sender_owner(self):
        self.set_owner('client', 'client')
        with self.assertRaises(BridgeError):
            self.send('client', 'missing', 'remote')
        self.assertEqual([r['target'] for r in self.notices()], ['client'])

    def test_owner_change_preserves_approval_queued_packet_and_idempotent_retry(self):
        self.allow()
        sent = self.send()
        before = self.do('remote', action='pull')
        approvals = self.do('laptop', action='permissions')
        self.set_owner('client', 'other')
        self.set_owner('claude', 'builder')
        self.assertEqual(self.do('remote', action='pull'), before)
        self.assertEqual(self.do('laptop', action='permissions'), approvals)
        self.assertEqual(self.send(id=sent['id'])['state'], 'queued')
        # Old clients re-registering without owner retain it.
        binding = self.do('laptop', action='show', name='client')
        binding.pop('owner')
        self.do('laptop', action='register', **binding)
        self.assertEqual(self.do('laptop', action='show', name='client')['owner'], 'other')

    def test_owner_is_admin_managed_and_must_be_registered(self):
        with self.assertRaisesRegex(BridgeError, 'owner machine'):
            self.set_owner('client', 'client', machine='remote')
        with self.assertRaisesRegex(BridgeError, 'Unknown agent'):
            self.set_owner('client', 'absent')
        self.set_owner('client', 'other')
        self.assertEqual([r['name'] for r in self.do('remote', action='list', owner='other')['agents']], ['client'])
        self.assertEqual(self.do('laptop', action='status', owner='other'),
                         self.do('laptop', action='list', owner='other'))
        with self.assertRaisesRegex(BridgeError, 'selected owner'):
            self.do('laptop', action='show', name='client', owner='claude')
        self.do('laptop', action='remove', name='other')
        self.assertIsNone(self.do('laptop', action='show', name='client')['owner'])

    def test_service_refusal_notifies_only_known_target_owner(self):
        import uuid
        self.do('laptop', action='register', name='ci-notices', cli='codex',
                machine='laptop', session=str(uuid.uuid4()))
        with self.assertRaisesRegex(BridgeError, 'Service agents'):
            self.set_owner('ci-notices', 'claude')
        self.set_owner('client', 'other')
        with self.assertRaises(BridgeError):
            self.send('ci-notices', 'client')
        self.assertEqual([r['target'] for r in self.notices()], ['other'])

    def test_cli_owner_filters_use_current_hub_without_changing_nats_status(self):
        parser = argparse.ArgumentParser()
        csl_agents.add_commands(parser.add_subparsers(), None)
        for argv in (['agents', 'list', '--owner', 'claude'],
                     ['agents', 'status', '--owner', 'claude'],
                     ['agents', 'show', 'client', '--owner', 'claude']):
            args = parser.parse_args(argv)
            with patch.object(csl_agents, 'config', return_value={}), \
                 patch.object(csl_agents, 'call', return_value={}) as call:
                csl_agents._execute(args)
            self.assertEqual(call.call_args.args[1]['owner'], 'claude')
        args = parser.parse_args(['agents', 'status'])
        with patch('csl_nats_tools.command', return_value={}) as command:
            csl_agents._execute(args)
        command.assert_called_once_with(args)

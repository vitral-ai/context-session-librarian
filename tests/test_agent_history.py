"""Pair history with synthetic identities only; no native process or network."""
import uuid
import argparse
import contextlib
import io
import shlex
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import csl_agents
from csl_nats import ControlRejected, send_command as nats_send_command
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

class PrefixTests(HistoryTests):
    def test_prefix_resolution_ambiguity_and_canonical_reply(self):
        self.allow()
        ids = ['12345678-1111-4111-8111-111111111111', '12345678-2222-4222-8222-222222222222']
        for mid in ids:
            self.send(id=mid)
        with self.assertRaisesRegex(BridgeError, 'Ambiguous'):
            self.history(partner=None, id='12345678')
        with self.assertRaisesRegex(BridgeError, 'Ambiguous'):
            self.send('client', 'claude', 'remote', in_reply_to='12345678')
        self.assertEqual(self.history(partner=None, id='12345678-1')[0]['id'], ids[0])
        sent = self.send('client', 'claude', 'remote', in_reply_to='12345678-1')
        with self.hub.connect() as db:
            from csl_bridge_store import decode
            packet = decode(db.execute('SELECT packet FROM envelopes WHERE id=?', (sent['id'],)).fetchone()[0])
        self.assertEqual(packet['in_reply_to'], ids[0])
        # A retry using the full UUID has the same durable fingerprint.
        self.assertEqual(self.send('client', 'claude', 'remote', id=sent['id'], in_reply_to=ids[0])['id'], sent['id'])
        for bad in ('1234567', '12345678%', '12345678_1', '12345678-abcz', 'x' * 100):
            with self.assertRaises(BridgeError):
                self.history(partner=None, id=bad)

    def test_hidden_history_does_not_change_prefix_ambiguity(self):
        self.allow(); self.allow('builder', 'other')
        visible = '12345678-1111-4111-8111-111111111111'
        hidden = '12345678-2222-4222-8222-222222222222'
        self.send(id=visible)
        self.send('builder', 'other', 'laptop', id=hidden)
        self.assertEqual(self.history(partner=None, id='12345678')[0]['id'], visible)
        self.assertEqual(self.history(partner=None, id=hidden[:10]), [])
        with self.assertRaisesRegex(BridgeError, 'private'):
            self.history(partner=None, id=hidden)

    def test_explicit_reply_survives_history_retention(self):
        self.hub = Hub(self.path, self.machines, history_retention=1)
        self.allow()
        old = self.send()['id']; self.send()
        self.assertEqual(self.history(partner=None, id=old[:8]), [])
        self.assertEqual(self.send('client', 'claude', 'remote', in_reply_to=old[:8])['state'], 'queued')

    def test_reply_pins_survive_history_expiry_and_name_reassignment(self):
        self.hub = Hub(self.path, self.machines, history_retention=1)
        self.allow()
        old = self.send()['id']; self.send()
        # Today's name approval must not confer access to an old machine's mail.
        self.do('laptop', action='register', name='client', cli='codex', machine='third', session=str(uuid.uuid4()))
        self.allow()
        for selector in (old, old[:8]):
            with self.assertRaisesRegex(BridgeError, 'reverse'):
                self.send('client', 'claude', 'third', in_reply_to=selector)
        # The original source pin matters too, even if the replying machine stayed.
        self.do('laptop', action='register', name='client', cli='codex', machine='remote', session=str(uuid.uuid4()))
        self.do('laptop', action='register', name='claude', cli='claude', machine='third', session=str(uuid.uuid4()))
        self.allow()
        with self.assertRaisesRegex(BridgeError, 'reverse'):
            self.send('client', 'claude', 'remote', in_reply_to=old[:8])


class ReplyCLITests(HistoryCLITests):
    def test_positional_prefix_infers_target_without_touching_body(self):
        args = self.parse('reply', '12345678', '--from', 'client', '--text', 'quoted reply')
        full = '12345678-1111-4111-8111-111111111111'
        with patch('csl_agents.config', return_value={}), \
             patch('csl_agents.call', side_effect=[{'messages': [{'id': full, 'source': 'claude', 'target': 'client'}]}, {'state': 'queued'}]) as call, \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            csl_agents._execute(args)
        self.assertEqual(call.call_args_list[0].args[1], {'action': 'history', 'source': 'client', 'id': '12345678'})
        sent = call.call_args_list[1].args[1]
        self.assertEqual((sent['source'], sent['target'], sent['in_reply_to'], sent['text']), ('client', 'claude', full, 'quoted reply'))

    def test_explicit_prefix_and_file_remain_supported(self):
        args = self.parse('reply', '--from', 'client', '--to', 'claude', '--in-reply-to', '12345678')
        with patch('csl_agents.config', return_value={}), patch('csl_bridge.reviewed_text', return_value='file reply'), \
             patch('csl_agents.call', return_value={'state': 'queued'}) as call, \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            csl_agents._execute(args)
        self.assertEqual(call.call_args.args[1]['in_reply_to'], '12345678')
        self.assertEqual(call.call_args.args[1]['text'], 'file reply')


class SenderRoutingTests(unittest.TestCase):
    parse = HistoryCLITests.parse
    parent = '12345678-1111-4111-8111-111111111111'

    def setUp(self):
        self.routes = {'senders': {'client': '/private/vi.json'}, 'nats_recipients': ['worker']}
        for target, key in [('csl_bridge_claude_notify.owned_json', 'read_routes'),
                            ('csl_agents.config', 'config'), ('csl_agents.call', 'hub'),
                            ('csl_nats_tools.call', 'nats_history'), ('csl_nats.send_command', 'nats_send')]:
            mock = patch(target)
            setattr(self, key, mock.start())
            self.addCleanup(mock.stop)
        self.read_routes.side_effect = lambda path: self.routes
        self.config.return_value = {}
        self.hub.return_value = {'state': 'queued', 'messages': []}
        self.nats_history.return_value = {'messages': []}

    def execute(self, *args, source='client'):
        parsed = self.parse(*args, '--from', source, '--text', 'test body')
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as stderr:
            csl_agents._execute(parsed)
        return stderr.getvalue()

    def test_default_send_uses_nats_for_enrolled_target(self):
        self.assertIn('transport=nats', self.execute('send', '--to', 'worker'))
        self.assertEqual(self.nats_send.call_args.args[0].nats_config, Path('/private/vi.json'))
        self.hub.assert_not_called()

    def test_hub_only_and_unopted_source_still_use_hub(self):
        self.assertIn('transport=hub', self.execute('send', '--to', 'legacy'))
        self.assertEqual(self.hub.call_args.args[1]['target'], 'legacy')
        self.assertNotIn('transport=', self.execute('send', '--to', 'worker', source='other'))
        self.assertEqual(self.hub.call_args.args[1]['source'], 'other')
        self.nats_send.assert_not_called()

    def test_explicit_pins_override_default(self):
        self.execute('send', '--to', 'worker', '--hub-config', '/private/hub.json')
        self.config.assert_called_once_with(Path('/private/hub.json'))
        self.execute('send', '--to', 'legacy', '--nats-config', '/private/alternate.json')
        self.assertEqual(self.nats_send.call_args.args[0].nats_config, Path('/private/alternate.json'))
        self.read_routes.assert_not_called()

    def test_nats_unconfirmed_never_posts_to_hub(self):
        self.nats_send.side_effect = BridgeError('NATS send unconfirmed')
        with self.assertRaisesRegex(BridgeError, 'NATS send unconfirmed'):
            self.execute('send', '--to', 'worker')
        self.hub.assert_not_called()

    def test_same_id_retry_requires_pin_even_after_sender_routes_are_removed(self):
        message_id = str(uuid.uuid4())
        parsed = self.parse('send', '--to', 'worker', '--from', 'client', '--text', 'test body')
        settings = {'tenant': 'vi', 'connector_home': Path('/private/bridge'),
                    'agents': {'client': {'session': self.parent}}}
        self.nats_send.side_effect = nats_send_command

        def interrupted(operation):
            operation.close()
            raise TimeoutError

        with patch('csl_nats.config', return_value=settings), patch('csl_nats.Store'), \
                patch('csl_nats.uuid.uuid4', return_value=uuid.UUID(message_id)), \
                patch('csl_nats.run_cli', side_effect=interrupted), \
                contextlib.redirect_stderr(io.StringIO()) as stderr, \
                self.assertRaisesRegex(BridgeError, 'NATS send unconfirmed.*printed config pin'):
            csl_agents._execute(parsed)
        self.assertIn('retry_pin=--nats-config /private/vi.json', stderr.getvalue())
        self.assertIn('message_id=' + message_id, stderr.getvalue())
        self.nats_send.reset_mock(side_effect=True)

        self.routes['nats_recipients'] = []
        for command in [('send', '--to', 'worker'), ('reply', self.parent)]:
            with self.subTest(command=command), self.assertRaisesRegex(BridgeError, 'original --nats-config or --hub-config'):
                self.execute(*command, '--id', message_id)
        self.read_routes.side_effect = FileNotFoundError
        with self.assertRaisesRegex(BridgeError, 'original --nats-config or --hub-config'):
            self.execute('send', '--to', 'worker', '--id', message_id)
        self.nats_send.assert_not_called()
        self.hub.assert_not_called()

        self.execute('send', '--to', 'worker', '--nats-config', '/private/vi.json', '--id', message_id)
        sent = self.nats_send.call_args.args[0]
        self.assertEqual((sent.id, sent.nats_config), (message_id, Path('/private/vi.json')))
        self.hub.assert_not_called()

    def test_hub_retry_stays_on_hub_after_enrollment(self):
        output = self.execute('send', '--to', 'legacy')
        self.assertIn('retry_pin=--hub-config ' + str(csl_agents.DEFAULT_CONFIG), output)
        message_id = self.hub.call_args.args[1]['id']
        self.routes['nats_recipients'].append('legacy')
        self.execute('send', '--to', 'legacy', '--hub-config', str(csl_agents.DEFAULT_CONFIG), '--id', message_id)
        self.assertEqual(self.hub.call_args.args[1]['id'], message_id)
        self.nats_send.assert_not_called()

    def test_printed_retry_options_resend_inferred_nats_reply(self):
        message_id = str(uuid.uuid4())
        self.nats_history.return_value = {'messages': [{'id': self.parent, 'from': 'worker', 'to': 'client'}]}
        self.nats_send.side_effect = nats_send_command
        parsed = self.parse('reply', self.parent[:8], '--from', 'client', '--text', 'test body')
        settings = {'tenant': 'vi', 'connector_home': Path('/private/bridge'),
                    'agents': {'client': {'session': self.parent}}}
        client = MagicMock()
        client.connection.close = AsyncMock()
        with patch('csl_nats.config', return_value=settings), patch('csl_nats.Store'), \
                patch('csl_nats.uuid.uuid4', return_value=uuid.UUID(message_id)), \
                patch('csl_nats.remember_send', side_effect=lambda store, tenant, envelope, thread: envelope), \
                patch('csl_nats.Client.connect_active', new=AsyncMock(return_value=client)), \
                patch('csl_nats.post_saved', side_effect=[TimeoutError, {'id': message_id, 'state': 'queued'}]) as post:
            with contextlib.redirect_stderr(io.StringIO()) as stderr, \
                    self.assertRaisesRegex(BridgeError, 'NATS send unconfirmed'):
                csl_agents._execute(parsed)
            options = dict(line.split('=', 1) for line in stderr.getvalue().splitlines())
            self.routes['nats_recipients'] = []
            self.execute('reply', *shlex.split(options['retry_reply']), *shlex.split(options['retry_pin']),
                         '--id', options['message_id'])
        self.hub.assert_called_once()
        self.nats_history.assert_called_once()
        self.assertEqual(post.await_count, 2)
        for attempt in post.await_args_list:
            envelope = attempt.args[4]
            self.assertEqual((envelope['id'], envelope['to'], envelope['reply_to']),
                             (message_id, 'worker', self.parent))

    def test_full_id_hub_only_reply_does_not_depend_on_nats(self):
        self.hub.side_effect = [{'messages': [{'id': self.parent, 'source': 'legacy', 'target': 'client'}]},
                                {'state': 'queued'}]
        self.assertIn('transport=hub', self.execute('reply', self.parent))
        sent = self.hub.call_args.args[1]
        self.assertEqual((sent['target'], sent['in_reply_to']), ('legacy', self.parent))
        self.nats_history.assert_not_called()

    def test_short_nats_reply_uses_canonical_parent(self):
        self.nats_history.return_value = {'messages': [{'id': self.parent, 'from': 'worker', 'to': 'client'}]}
        self.assertIn('transport=nats', self.execute('reply', self.parent[:8]))
        args = self.nats_send.call_args.args[0]
        self.assertEqual((args.to, args.message_id), ('worker', self.parent))
        self.hub.assert_called_once()

    def test_short_hub_reply_with_missing_nats_parent(self):
        self.hub.side_effect = [{'messages': [{'id': self.parent, 'source': 'legacy', 'target': 'client'}]},
                                {'state': 'queued'}]
        self.nats_history.side_effect = ControlRejected('not_found', 'message unavailable')
        self.assertIn('transport=hub', self.execute('reply', self.parent[:8]))
        self.assertEqual(self.hub.call_args.args[1]['in_reply_to'], self.parent)

    def test_hub_parent_keeps_hub_route_for_enrolled_peer(self):
        hub_parent = {'id': self.parent, 'source': 'worker', 'target': 'client'}
        self.nats_history.return_value = {'messages': [{'id': self.parent, 'from': 'worker', 'to': 'client'}]}
        for selector in (self.parent[:8], self.parent):
            with self.subTest(selector=selector):
                self.hub.side_effect = [{'messages': [hub_parent]}, {'state': 'queued'}]
                self.assertIn('transport=hub', self.execute('reply', selector, '--to', 'worker'))
                self.assertEqual(self.hub.call_args.args[1]['in_reply_to'], self.parent)
        self.nats_send.assert_not_called()

    def test_nats_parent_keeps_nats_route_for_peer_removed_from_enrollment(self):
        self.routes['nats_recipients'] = []
        self.nats_history.return_value = {'messages': [{'id': self.parent, 'from': 'worker', 'to': 'client'}]}
        self.assertIn('transport=nats', self.execute('reply', self.parent, '--to', 'worker'))
        self.nats_send.assert_called_once()
        self.hub.assert_called_once()

    def test_pinned_nats_reply_resolves_prefix_without_hub(self):
        self.nats_history.return_value = {'messages': [{'id': self.parent, 'from': 'worker', 'to': 'client'}]}
        self.execute('reply', self.parent[:24], '--nats-config', '/private/vi.json')
        sent = self.nats_send.call_args.args[0]
        self.assertEqual((sent.message_id, sent.to), (self.parent, 'worker'))
        self.hub.assert_not_called()

    def test_cross_transport_prefix_collision_refuses_to_send(self):
        self.hub.return_value = {'messages': [{'id': self.parent, 'source': 'legacy', 'target': 'client'}]}
        other = '12345678-2222-4222-8222-222222222222'
        self.nats_history.return_value = {'messages': [{'id': other, 'from': 'worker', 'to': 'client'}]}
        with self.assertRaisesRegex(BridgeError, 'one matching message'):
            self.execute('reply', self.parent[:8])
        self.nats_send.assert_not_called()
        self.hub.assert_called_once()

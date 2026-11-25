from functools import partial
import io
import json
from pathlib import Path
import shlex
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Event, RLock
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
import uuid

from csl_agents import deliver as deliver_hub
from csl_bridge_hooks import surface_nats
from csl_bridge_store import BridgeError, Store, encode, neutralize_headers
from csl_ci_notice import notice_request
from codex_sessions import main
from csl_nats import (MAX_DIGEST, ClaudeInbox, Client, Ledger, agent_name, body_text, deliver_codex, delivery_parts,
                      receive_one, recover_acks, remember_send)


class NatsAdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.home = Path(directory.name)
        self.config_path = self.home / "adapter config.json"
        self.deliver_codex = partial(deliver_codex, config_path=self.config_path)
        self.store = Store(self.home, create=True)
        self.addCleanup(self.store.close)
        self.thread = str(uuid.uuid4())
        self.turn = str(uuid.uuid4())
        self.id = str(uuid.uuid4())
        self.ledger = Ledger(self.store, 'vi', 'alice', self.thread)
        self.connection = SimpleNamespace(request=AsyncMock())
        self.client = Client(self.connection, 'vi', 'alice')
        self.calls = []

        async def receipt(message_id, state):
            self.assertEqual(message_id, self.id)
            self.calls.append(state)
            return state

        async def ack_sync(**_):
            self.assertEqual(self.rows()[0]['ack_state'], 'ack_intent')
            self.calls.append('broker ACK')

        self.client.receipt = receipt
        self.message = SimpleNamespace(
            subject='csl.vi.agent.alice.inbox',
            reply='$JS.ACK.CSL_MSG.agent-alice.1.42.2.1234.0',
            data=json.dumps({'v': 1, 'id': self.id, 'from': 'bob', 'to': 'alice',
                             'body': 'x' * 49152, 'kind': 'message'}).encode(),
            ack_sync=AsyncMock(side_effect=ack_sync), nak=AsyncMock(),
        )
        self.deliver = Mock(return_value=self.turn)

    def rows(self):
        return self.store.rows('SELECT * FROM nats_delivered')

    async def test_ack_order_and_replay_after_reopening_ledger(self):
        self.assertTrue(await receive_one(self.client, self.ledger, self.message, self.deliver))
        self.assertEqual(self.calls, ['delivered', 'broker ACK', 'acked'])
        self.assertEqual(self.rows()[0]['ack_state'], 'acked')
        reopened = Store(self.home)
        self.addCleanup(reopened.close)
        ledger = Ledger(reopened, 'vi', 'alice', self.thread)
        self.message.ack_sync.side_effect = None
        self.assertTrue(await receive_one(self.client, ledger, self.message, self.deliver))
        self.deliver.assert_called_once()
        self.assertEqual(self.message.ack_sync.await_count, 2)

    async def test_unavailable_native_does_not_ack(self):
        self.deliver.return_value = None
        self.assertFalse(await receive_one(self.client, self.ledger, self.message, self.deliver))
        self.assertEqual(self.rows(), [])
        self.message.nak.assert_awaited_once_with(delay=60)
        self.message.ack_sync.assert_not_awaited()

    async def test_digest_keeps_member_order_and_recovers_every_receipt(self):
        items = [dict(json.loads(self.message.data), id=str(uuid.uuid4()), body=body)
                 for body in ('a' * 49000 + ' first end', 'b' * 49000 + ' second end')]
        self.message.data = json.dumps(dict(items[0], id=self.id, kind='digest', **{'from': 'csl-control'},
                                            body='', items=items)).encode()
        self.client.receipt = AsyncMock()
        self.message.ack_sync.side_effect = TimeoutError
        with self.assertRaises(TimeoutError):
            await receive_one(self.client, self.ledger, self.message, self.deliver)
        text = '\n\n'.join(delivery_parts(self.deliver.call_args.args[0], self.config_path))
        self.assertLess(text.index(items[0]['body']), text.index(items[1]['body']))
        self.assertEqual({call.args for call in self.client.receipt.await_args_list},
                         {(entry['id'], 'delivered') for entry in items} | {(self.id, 'delivered')})
        self.client.receipt.reset_mock()
        reopened = Store(self.home)
        self.addCleanup(reopened.close)
        await recover_acks(self.client, Ledger(reopened, 'vi', 'alice', self.thread), (TimeoutError,))
        self.assertEqual({call.args for call in self.client.receipt.await_args_list},
                         {(entry['id'], 'acked') for entry in items} | {(self.id, 'acked')})
        self.deliver.assert_called_once()

    async def test_digest_with_duplicate_or_foreign_child_is_rejected_before_handoff(self):
        item = dict(json.loads(self.message.data), id=str(uuid.uuid4()), body='message')
        for items in ([item, item], [dict(item, to='charlie')]):
            self.message.data = json.dumps(dict(item, id=self.id, kind='digest', **{'from': 'csl-control'},
                                                body='', items=items)).encode()
            with self.assertRaises(BridgeError):
                await receive_one(self.client, self.ledger, self.message, self.deliver)
        self.deliver.assert_not_called()
        self.message.ack_sync.assert_not_awaited()

    async def test_claude_wake_is_batched_and_ack_waits_for_hook_output(self):
        with self.store.transaction() as db:
            db.execute('INSERT INTO agents VALUES (?,?)', ('alice', self.thread))
        inbox = ClaudeInbox(self.store, 'vi', 'alice', self.thread)
        self.assertFalse(await receive_one(self.client, self.ledger, self.message,
                                           partial(inbox.stage, config_path=self.config_path)))
        second = dict(json.loads(self.message.data), id=str(uuid.uuid4()), body='second body')
        inbox.stage(second, 'second-fingerprint', config_path=self.config_path)
        with patch('csl_bridge_claude_notify.wake') as wake:
            wake.side_effect = OSError('closed socket')
            with self.assertRaises(OSError):
                inbox.wake_waiting('/fixture')
            wake.side_effect = None
            inbox.wake_waiting('/fixture')
            inbox.wake_waiting('/fixture')
            self.assertEqual(wake.call_count, 2)
            wake.assert_called_with(self.thread, '/fixture')
        self.assertEqual(self.rows(), [])

        event = 'PostToolUse'
        payload = {'session_id': self.thread, 'hook_event_name': event}
        output = Mock(side_effect=BrokenPipeError)
        with self.assertRaises(BrokenPipeError):
            surface_nats(self.home, 'vi', 'alice', event, payload, 10, output)
        self.assertEqual(self.rows(), [])
        output = Mock()
        surface_nats(self.home, 'other', 'alice', event, payload, 10, output)
        output.assert_not_called()
        with self.assertRaises(BridgeError):
            surface_nats(self.home, 'vi', 'alice', event, dict(payload, session_id=self.turn), 10, output)
        surface_nats(self.home, 'vi', 'alice', event, payload, 10, output)
        surface_nats(self.home, 'vi', 'alice', event, payload, 10, output)
        output.assert_called_once()
        body = json.loads(output.call_args.args[0])['hookSpecificOutput']['additionalContext']
        self.assertIn('x' * 49152 + '\n\nCSL from bob', body)
        self.assertTrue(body.endswith('second body'))
        self.assertTrue(await receive_one(self.client, self.ledger, self.message,
                                           partial(inbox.stage, config_path=self.config_path)))
        self.assertEqual(self.rows()[0]['turn'], self.thread)
        self.assertEqual(self.calls, ['delivered', 'broker ACK', 'acked'])

    async def test_lost_ack_confirmation_recovers_without_second_handoff(self):
        self.message.ack_sync.side_effect = TimeoutError
        with self.assertRaises(TimeoutError):
            await receive_one(self.client, self.ledger, self.message, self.deliver)
        self.assertEqual(self.rows()[0]['ack_state'], 'ack_intent')
        reopened = Store(self.home)
        self.addCleanup(reopened.close)
        await recover_acks(self.client, Ledger(reopened, 'vi', 'alice', self.thread), (TimeoutError,))
        self.connection.request.assert_awaited_once_with(self.message.reply, b'+ACK', timeout=1)
        self.assertEqual(self.rows()[0]['ack_state'], 'acked')
        self.deliver.assert_called_once()

    def test_unsurfaced_claude_batch_retries_one_successful_wake_after_reopen(self):
        with self.store.transaction() as db:
            db.execute('INSERT INTO agents VALUES (?,?)', ('alice', self.thread))
        inbox = ClaudeInbox(self.store, 'vi', 'alice', self.thread)
        inbox.stage(json.loads(self.message.data), 'fingerprint', config_path=self.config_path)
        with patch('csl_bridge_claude_notify.wake') as wake, patch('csl_nats.time.time') as clock:
            clock.return_value = 100
            inbox.wake_waiting('/fixture')
            reopened = ClaudeInbox(self.store, 'vi', 'alice', self.thread)
            clock.return_value = 129
            reopened.wake_waiting('/fixture')
            wake.assert_called_once()
            clock.return_value = 130
            reopened.wake_waiting('/fixture')
            clock.return_value = 160
            reopened.wake_waiting('/fixture')
            self.assertEqual(wake.call_count, 2)

    def test_claude_digest_bounds_rendered_json_and_preserves_whole_members(self):
        with self.store.transaction() as db:
            db.execute('INSERT INTO agents VALUES (?,?)', ('alice', self.thread))
        inbox = ClaudeInbox(self.store, 'vi', 'alice', self.thread)
        template = json.loads(self.message.data)
        items = [dict(template, id=str(uuid.uuid4()), body=('CSL from example\n' * 2740) + f'end {i}') for i in range(10)]
        digest = dict(template, kind='digest', body='', items=items, **{'from': 'csl-control'})
        self.assertLess(len(encode(digest).encode()), MAX_DIGEST)
        self.assertIsNone(inbox.stage(digest, 'digest', config_path=self.config_path))
        outputs = []
        event = 'PostToolUse'
        payload = {'session_id': self.thread, 'hook_event_name': event}
        surface_nats(self.home, 'vi', 'alice', event, payload, 10, outputs.append)
        self.assertIsNone(inbox.stage(digest, 'digest', config_path=self.config_path))
        surface_nats(self.home, 'vi', 'alice', event, payload, 10, outputs.append)
        self.assertEqual(inbox.stage(digest, 'digest', config_path=self.config_path), self.thread)
        surface_nats(self.home, 'vi', 'alice', event, payload, 10, outputs.append)
        self.assertEqual(len(outputs), 2)
        self.assertTrue(all(len(text.encode('utf-8')) + 1 <= MAX_DIGEST for text in outputs))
        full = '\n'.join(json.loads(text)['hookSpecificOutput']['additionalContext'] for text in outputs)
        positions = []
        for item in items:
            rendered_body = neutralize_headers(item['body'])
            self.assertEqual(full.count(rendered_body), 1)
            self.assertIn('csl reply ' + item['id'], full)
            positions.append(full.index(rendered_body))
        self.assertEqual(positions, sorted(positions))

    def test_claude_digest_footer_sends_nats_only_reply_from_the_cli(self):
        with self.store.transaction() as db:
            db.execute('INSERT INTO agents VALUES (?,?)', ('alice', self.thread))
        inbox = ClaudeInbox(self.store, 'vi', 'alice', self.thread)
        parent = dict(json.loads(self.message.data), body='NATS-only message')
        digest = dict(parent, id=str(uuid.uuid4()), kind='digest', body='', items=[parent],
                      **{'from': 'csl-control'})
        inbox.stage(digest, 'digest', config_path=self.config_path)
        outputs = []
        event = 'PostToolUse'
        surface_nats(self.home, 'vi', 'alice', event, {'session_id': self.thread, 'hook_event_name': event},
                     10, outputs.append)
        text = json.loads(outputs[0])['hookSpecificOutput']['additionalContext']
        footer = text.split('Reply via NATS: ', 1)[1]
        _, *args = shlex.split(footer)
        self.assertEqual(args[-1], '...')
        args[-1] = 'Reply without a Hub row'
        creds = self.home / 'fixture.creds'
        creds.touch(mode=0o600)
        self.config_path.touch(mode=0o600)
        self.config_path.write_text(encode({
            'tenant': 'vi', 'servers': ['tls://fixture.invalid:4222'], 'cafile': None,
            'connector_home': str(self.home),
            'agents': {'alice': {'cli': 'claude', 'session': self.thread, 'credentials': str(creds),
                                 'mailbox_home': str(self.home), 'claude_config': str(self.home)}},
        }))
        self.connection.close = AsyncMock()

        async def response(subject, payload, **_):
            data = json.loads(payload)
            result = {'id': data['id'], 'state': 'queued'} if subject == 'csl.vi.send.alice' else {}
            return SimpleNamespace(data=encode(result).encode())

        self.connection.request.side_effect = response
        with (patch.object(Client, 'connect', new=AsyncMock(return_value=self.client)),
              patch('csl_agents.call', side_effect=AssertionError('unexpected Hub request')) as hub,
              patch('sys.stdout', new_callable=io.StringIO), patch('sys.stderr', new_callable=io.StringIO)):
            self.assertEqual(main(args), 0)
        hub.assert_not_called()
        (subject, payload), kwargs = self.connection.request.await_args
        self.assertEqual(subject, 'csl.vi.send.alice')
        reply = json.loads(payload)
        self.assertEqual((reply['kind'], reply['from'], reply['to'], reply['reply_to'], reply['body']),
                         ('reply', 'alice', 'bob', parent['id'], 'Reply without a Hub row'))
        self.assertNotEqual(reply['id'], parent['id'])
        self.assertEqual(kwargs['headers'], {'Nats-Msg-Id': reply['id']})

    async def test_failed_control_receipt_retries_without_second_handoff(self):
        self.client.receipt = AsyncMock(side_effect=TimeoutError)
        with self.assertRaises(TimeoutError):
            await receive_one(self.client, self.ledger, self.message, self.deliver)
        self.assertEqual(self.rows()[0]['ack_state'], 'delivered')
        self.message.ack_sync.assert_not_awaited()
        self.client.receipt.side_effect = None
        await receive_one(self.client, self.ledger, self.message, self.deliver)
        self.deliver.assert_called_once()

    async def test_wrong_lane_and_same_id_different_body_are_never_acked(self):
        valid = self.message.reply
        self.message.reply = '$JS.ACK.CSL_MSG.agent-charlie.1.42.2.1234.0'
        with self.assertRaises(BridgeError):
            await receive_one(self.client, self.ledger, self.message, self.deliver)
        self.deliver.assert_not_called()
        self.message.reply = valid
        await receive_one(self.client, self.ledger, self.message, self.deliver)
        self.message.data = self.message.data.replace(b'xxx', b'abc', 1)
        with self.assertRaises(BridgeError):
            await receive_one(self.client, self.ledger, self.message, self.deliver)
        self.message.ack_sync.assert_awaited_once()

    async def test_send_subject_and_message_id(self):
        self.connection.request.return_value = SimpleNamespace(
            data=json.dumps({'id': self.id, 'state': 'queued'}).encode())
        await self.client.send({'id': self.id})
        args, kwargs = self.connection.request.await_args
        self.assertEqual(args[0], 'csl.vi.send.alice')
        self.assertEqual(kwargs['headers'], {'Nats-Msg-Id': self.id})

    def test_native_operation_fingerprint_and_complete_body(self):
        with self.store.transaction() as db:
            db.execute("INSERT INTO agents VALUES ('alice', ?)", (self.thread,))

        def invoke(_peer, request, *, native_text):
            self.assertTrue(native_text.endswith('\nend'))
            self.assertGreater(len(native_text), 12000)
            result = {'state': 'delivered', 'request_id': request['id'],
                      'thread_id': self.thread, 'turn_id': self.turn}
            self.store.operation(request['id'], 'delivered', result)
            return result

        native = SimpleNamespace(
            stop=Event(), client_only=False, agents={'alice': self.thread},
            locks={'alice': RLock()}, store=self.store, invoke=Mock(side_effect=invoke))
        env = {'id': self.id, 'from': 'bob', 'to': 'alice', 'body': ('\t' * 48000) + '\nend'}
        self.assertEqual(self.deliver_codex(native, self.thread, env, 'hash'), self.turn)
        self.assertEqual(self.deliver_codex(native, self.thread, env, 'hash'), self.turn)
        native.invoke.assert_called_once()
        with self.assertRaises(BridgeError):
            self.deliver_codex(native, self.thread, env, 'different')

    def dual_transport(self):
        with self.store.transaction() as db:
            db.execute("INSERT INTO agents VALUES ('alice', ?)", (self.thread,))
        notice = notice_request({'repository': 'example/project', 'source': 'ci-notices'}, {
            'action': 'notice', 'repository': 'example/project', 'number': 100,
            'branch': 'codex/alice/test', 'workflow': 'Tests', 'run_id': 10,
            'attempt': 1, 'sha': 'a' * 40, 'results': {'completion': 'success'},
        })
        self.id = notice['id']
        packet = {
            'id': self.id, 'text': notice['text'], 'in_reply_to': None,
            'source': {'name': notice['source'], 'session': str(uuid.uuid4()), 'machine': 'ci'},
            'target': {'name': 'alice', 'session': self.thread, 'machine': 'local', 'cli': 'codex'},
        }
        envelope = {'v': 1, 'id': self.id, 'from': notice['source'], 'to': 'alice',
                    'kind': 'ci', 'body': notice['text']}
        self.message.data = json.dumps(envelope).encode()
        config = patch('csl_agents.config', return_value={'machine': 'local'})
        config.start()
        self.addCleanup(config.stop)

        def invoke(_peer, request, **_):
            result = {'state': 'delivered', 'request_id': request['id'],
                      'thread_id': self.thread, 'turn_id': self.turn}
            self.store.operation(request['id'], 'delivered', result)
            return result

        native = SimpleNamespace(
            stop=Event(), client_only=False, agents={'alice': self.thread}, peers={},
            locks={'alice': RLock()}, store=self.store, current=lambda _: {'status': 'idle'},
            invoke=Mock(side_effect=invoke),
        )

        def incoming(peer, request):
            self.store.reserve(peer, request)
            return native.invoke(peer, request)

        native.incoming = incoming
        return native, packet

    async def test_hub_first_acks_the_native_turn_from_nats(self):
        native, packet = self.dual_transport()
        receipt = deliver_hub(native, packet)
        adapter = lambda envelope, fingerprint: self.deliver_codex(native, self.thread, envelope, fingerprint)
        self.assertTrue(await receive_one(self.client, self.ledger, self.message, adapter))
        self.assertEqual(self.rows()[0]['turn'], receipt['turn_id'])
        self.assertEqual(self.rows()[0]['ack_state'], 'acked')
        native.invoke.assert_called_once()

    async def test_nats_first_reuses_turn_and_refuses_collisions_from_hub(self):
        native, packet = self.dual_transport()
        adapter = lambda envelope, fingerprint: self.deliver_codex(native, self.thread, envelope, fingerprint)
        await receive_one(self.client, self.ledger, self.message, adapter)
        self.assertEqual(deliver_hub(native, packet)['turn_id'], self.turn)
        for update in ({'text': 'different'}, {'in_reply_to': str(uuid.uuid4())},
                       {'source': {**packet['source'], 'name': 'someone-else'}}):
            with self.assertRaises(BridgeError):
                deliver_hub(native, {**packet, **update})
        replacement = str(uuid.uuid4())
        native.agents['alice'] = replacement
        with self.store.transaction() as db:
            db.execute("UPDATE agents SET thread=? WHERE name='alice'", (replacement,))
        with self.assertRaisesRegex(BridgeError, 'original thread differs'):
            deliver_hub(native, {**packet, 'target': {**packet['target'], 'session': replacement}})
        native.invoke.assert_called_once()

    async def test_other_transport_cannot_take_over_a_pending_handoff(self):
        native, packet = self.dual_transport()
        native.invoke.side_effect = None
        native.invoke.return_value = {'state': 'pending'}
        deliver_hub(native, packet)
        adapter = lambda envelope, fingerprint: self.deliver_codex(native, self.thread, envelope, fingerprint)
        self.assertFalse(await receive_one(self.client, self.ledger, self.message, adapter))
        self.message.ack_sync.assert_not_awaited()
        native.invoke.assert_called_once()

    def test_grouped_ci_reconciles_both_arrival_orders(self):
        native, packet = self.dual_transport()
        ci = json.loads(self.message.data)
        other = dict(ci, id=str(uuid.uuid4()), body='ordinary grouped body', kind='message', **{'from': 'bob'})
        digest = dict(ci, id=str(uuid.uuid4()), kind='digest', body='', items=[ci, other], **{'from': 'csl-control'})
        deliver_hub(native, packet)
        self.assertEqual(self.deliver_codex(native, self.thread, digest, 'digest-one'), self.turn)
        grouped_text = native.invoke.call_args.kwargs['native_text']
        self.assertNotIn(ci['body'], grouped_text)
        self.assertIn(other['body'], grouped_text)
        already_delivered_digest = dict(digest, id=str(uuid.uuid4()), items=[ci])
        self.assertEqual(self.deliver_codex(native, self.thread, already_delivered_digest, 'already'), self.turn)
        self.assertEqual(native.invoke.call_count, 2)

        second = dict(ci, id=str(uuid.uuid4()), body='second CI test')
        second_digest = dict(digest, id=str(uuid.uuid4()), items=[second])
        self.assertEqual(self.deliver_codex(native, self.thread, second_digest, 'digest-two'), self.turn)
        self.assertIn(second['body'], native.invoke.call_args.kwargs['native_text'])
        second_packet = dict(packet, id=second['id'], text=second['body'])
        receipt = deliver_hub(native, second_packet)
        self.assertEqual((receipt['request_id'], receipt['turn_id']), (second['id'], self.turn))
        self.assertEqual(self.deliver_codex(native, self.thread, second, 'standalone'), self.turn)
        with self.assertRaisesRegex(BridgeError, 'CI completion does not match'):
            deliver_hub(native, dict(second_packet, text='mismatch'))
        self.assertEqual(self.deliver_codex(native, self.thread, second_digest, 'digest-two'), self.turn)
        self.assertEqual(native.invoke.call_count, 3)

    def test_hub_waits_for_pending_digest_member(self):
        native, packet = self.dual_transport()
        ci = json.loads(self.message.data)
        digest = dict(ci, id=str(uuid.uuid4()), kind='digest', body='', items=[ci], **{'from': 'csl-control'})
        complete = native.invoke.side_effect
        native.invoke.side_effect = None
        native.invoke.return_value = {'state': 'pending'}
        self.assertIsNone(self.deliver_codex(native, self.thread, digest, 'digest'))
        self.assertEqual(deliver_hub(native, packet), {'state': 'queued'})
        native.invoke.assert_called_once()
        native.invoke.side_effect = complete
        self.assertEqual(self.deliver_codex(native, self.thread, digest, 'digest'), self.turn)
        self.assertEqual(deliver_hub(native, packet)['turn_id'], self.turn)
        self.assertEqual(native.invoke.call_count, 2)

    def test_extended_names_can_be_registered_and_legacy_rules_still_apply(self):
        longest = '2' + 'a' * 62
        for valid in ('01', longest):
            self.assertEqual(agent_name(valid, registration=True), valid)
        for invalid in ('a', longest + 'a', 'a.b', 'ci-notices'):
            with self.assertRaises(BridgeError):
                agent_name(invalid, registration=True)
        cli = Path(__file__).resolve().parents[1] / 'csl'
        command = [sys.executable, str(cli), 'bridge', '--home', str(self.home),
                   'agent', longest, '--thread', self.thread]
        self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
        subprocess.run([*command, '--nats'], check=True, capture_output=True)
        self.assertEqual(self.store.agents()[longest], self.thread)

    def test_unsure_send_keeps_creation_and_build_identity(self):
        env = {'id': self.id, 'from': 'alice', 'to': 'bob', 'client_version': 'before', 'created_at': 'before'}
        self.assertEqual(remember_send(self.store, 'vi', env), env)
        retry = {**env, 'created_at': 'after', 'client_version': 'after'}
        self.assertEqual(remember_send(self.store, 'vi', retry), env)
        with self.assertRaises(BridgeError):
            remember_send(self.store, 'vi', {**env, 'to': 'charlie'})

    def test_byte_limit(self):
        self.assertEqual(body_text('é' * 24576), 'é' * 24576)
        with self.assertRaises(BridgeError):
            body_text('é' * 24577)

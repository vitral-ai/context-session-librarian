import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Event, RLock
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
import uuid

from csl_agents import deliver as deliver_hub
from csl_bridge_store import BridgeError, Store
from csl_ci_notice import notice_request
from csl_nats import Client, Ledger, agent_name, body_text, deliver_codex, receive_one, recover_acks, remember_send


class NatsAdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.home = Path(directory.name)
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
        self.assertEqual(deliver_codex(native, self.thread, env, 'hash'), self.turn)
        self.assertEqual(deliver_codex(native, self.thread, env, 'hash'), self.turn)
        native.invoke.assert_called_once()
        with self.assertRaises(BridgeError):
            deliver_codex(native, self.thread, env, 'different')

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
        adapter = lambda envelope, fingerprint: deliver_codex(native, self.thread, envelope, fingerprint)
        self.assertTrue(await receive_one(self.client, self.ledger, self.message, adapter))
        self.assertEqual(self.rows()[0]['turn'], receipt['turn_id'])
        self.assertEqual(self.rows()[0]['ack_state'], 'acked')
        native.invoke.assert_called_once()

    async def test_nats_first_reuses_turn_and_refuses_collisions_from_hub(self):
        native, packet = self.dual_transport()
        adapter = lambda envelope, fingerprint: deliver_codex(native, self.thread, envelope, fingerprint)
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
        adapter = lambda envelope, fingerprint: deliver_codex(native, self.thread, envelope, fingerprint)
        self.assertFalse(await receive_one(self.client, self.ledger, self.message, adapter))
        self.message.ack_sync.assert_not_awaited()
        native.invoke.assert_called_once()

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

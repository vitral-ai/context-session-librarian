import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, RLock
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock
import uuid

from csl_bridge_store import BridgeError, Store
from csl_nats import Client, Ledger, body_text, deliver_codex, receive_one, recover_acks, remember_send


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
            result = {'state': 'delivered', 'thread_id': self.thread, 'turn_id': self.turn}
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

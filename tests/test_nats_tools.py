import base64
from contextlib import redirect_stdout
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
import uuid

from csl_bridge_store import BridgeError
from codex_sessions import main
from csl_nats import Client, ControlRejected, delivery_parts, validate_envelope
from csl_nats_tools import CHUNK_BYTES, fetch, history, put


class NatsToolsTests(unittest.IsolatedAsyncioTestCase):
    async def test_history_fills_across_pages_in_order(self):
        messages = [{'id': str(uuid.uuid4()), 'body': str(i)} for i in range(3)]
        cursor = messages[-1]['id']
        request = AsyncMock(side_effect=[
            {'messages': messages[-1:], 'retention': 200, 'next_before': cursor},
            {'messages': messages[:-1], 'retention': 200},
        ])
        result = await history(SimpleNamespace(request=request), partner='bob', last=3)
        self.assertEqual(result['messages'], messages)
        request.assert_awaited_with('ctl.history', {'with': 'bob', 'last': 2, 'before': cursor})

    async def test_history_since_is_resolved_once_and_reused_on_each_page(self):
        cursor = str(uuid.uuid4())
        request = AsyncMock(side_effect=[
            {'messages': [{'id': cursor}], 'retention': 200, 'next_before': cursor},
            {'messages': [], 'retention': 200},
        ])
        with patch('csl_nats_tools.datetime', wraps=datetime) as clock:
            clock.now.return_value = datetime(2026, 11, 27, 12, tzinfo=timezone.utc)
            await history(SimpleNamespace(request=request), partner='bob', since='30m')
            clock.now.assert_called_once_with(timezone.utc)
        for call in request.await_args_list:
            self.assertEqual(call.args[1]['since'], '2026-11-27T11:30:00+00:00')
        with self.assertRaisesRegex(BridgeError, 'requires --with'):
            await history(SimpleNamespace(request=request), message_id=cursor, since='30m')

    async def test_only_latest_accepts_a_bare_array_and_preserves_control_errors(self):
        request = AsyncMock(side_effect=[SimpleNamespace(data=body) for body in
                           (b'[]', b'[]', b'{"error":"forbidden","message":"Not approved"}')])
        client = Client(SimpleNamespace(request=request), 'vi', 'alice')
        self.assertEqual(await client.request('ctl.latest', {}), [])
        with self.assertRaisesRegex(BridgeError, 'Invalid response'):
            await client.request('ctl.watch', {})
        with self.assertRaisesRegex(ControlRejected, 'Not approved'):
            await client.request('ctl.latest', {})

    def test_pairs_and_watches_use_the_control_contract(self):
        watch = str(uuid.uuid4())
        cases = [
            (['allow', 'alice', 'bob'], 'pairs', {'action': 'allow', 'a': 'alice', 'b': 'bob'},
             {'allowed': True}, {'allowed': True}),
            (['deny', 'alice', 'bob'], 'pairs', {'action': 'deny', 'a': 'alice', 'b': 'bob'},
             {'allowed': False}, {'allowed': False}),
            (['pairs', '--pending'], 'pairs', {'action': 'list', 'pending': True},
             {'pending': [], 'pairs': [{'a': 'alice', 'b': 'bob'}]}, {'pending': []}),
            (['watch', '--agent', 'bob', '--after', '2h', '--note', 'Delivery report'], 'watch',
             {'action': 'create', 'agent': 'bob', 'after_seconds': 7200, 'note': 'Delivery report'},
             {'id': watch}, {'id': watch}),
            (['watch', '--list'], 'watch', {'action': 'list'}, {'watches': []}, {'watches': []}),
            (['watch', '--cancel', watch], 'watch', {'action': 'cancel', 'id': watch},
             {'id': watch, 'cancelled': True}, {'id': watch, 'cancelled': True}),
        ]
        for args, verb, body, response, expected in cases:
            with self.subTest(args=args), redirect_stdout(io.StringIO()) as stdout:
                client = SimpleNamespace(request=AsyncMock(return_value=response),
                                         connection=SimpleNamespace(close=AsyncMock()))
                with (patch('csl_nats_tools.config', return_value={}),
                      patch.object(Client, 'connect_active', new=AsyncMock(return_value=client))):
                    self.assertEqual(main(['agents', *args, '--nats-config', '/private/config',
                                           '--from', 'alice']), 0)
                client.request.assert_awaited_once_with('ctl.' + verb, body)
                self.assertEqual(json.loads(stdout.getvalue()), expected)

    def test_latest_prints_newest_first_and_keeps_previews_on_one_line(self):
        first, second = str(uuid.uuid4()), str(uuid.uuid4())
        rows = [
            {'id': first, 'with': 'bob', 'at': '2026-11-27T10:00:00Z', 'from': 'alice', 'preview': 'older'},
            {'id': second, 'with': 'carol', 'at': '2026-11-27T10:30:00Z',
             'from': 'carol', 'preview': 'newer\nmessage' + 'x' * 120},
        ]
        client = SimpleNamespace(request=AsyncMock(return_value=rows), connection=SimpleNamespace(close=AsyncMock()))
        with (patch('csl_nats_tools.config', return_value={}),
              patch.object(Client, 'connect_active', new=AsyncMock(return_value=client)),
              redirect_stdout(io.StringIO()) as stdout):
            self.assertEqual(main(['agents', 'latest', '--nats-config', '/private/config', '--from', 'alice']), 0)
        first_line, second_line = stdout.getvalue().splitlines()
        self.assertIn('newer message', first_line)
        self.assertTrue(first_line.endswith(second[:8]))
        self.assertTrue(second_line.endswith(first[:8]))
        self.assertNotIn('x' * 120, first_line)

    async def test_history_rejects_a_repeated_cursor(self):
        cursor = str(uuid.uuid4())
        request = AsyncMock(return_value={'messages': [{'id': cursor}], 'retention': 200, 'next_before': cursor})
        with self.assertRaisesRegex(BridgeError, 'cursor'):
            await history(SimpleNamespace(request=request), partner='bob', last=3)
        self.assertEqual(request.await_count, 2)

    async def test_chunked_transfer_acl_and_atomic_verified_fetch(self):
        payload = b'a' * CHUNK_BYTES + b'b'
        digest = hashlib.sha256(payload).hexdigest()
        info = {'id': digest, 'name': 'report.txt', 'size': len(payload)}
        calls = []

        async def rpc(verb, request):
            offset = request['offset']
            chunk = payload[offset:offset + CHUNK_BYTES]
            data = base64.b64encode(chunk).decode('ascii')
            calls.append((verb, offset))
            if verb == 'ctl.put':
                self.assertEqual(request, {**info, 'allowed': ['alice', 'bob'], 'offset': offset, 'data': data})
            else:
                self.assertEqual(request, {'id': digest, 'offset': offset})
            return {**info, 'data': data, 'next_offset': CHUNK_BYTES if offset == 0 else None}

        client = SimpleNamespace(request=rpc)
        self.assertEqual(await put(client, io.BytesIO(payload), info['name'], ['bob', 'alice', 'bob']), info)
        with TemporaryDirectory() as directory:
            path = Path(directory) / info['name']
            self.assertEqual(await fetch(client, digest, directory=path.parent), {**info, 'path': str(path)})
            self.assertEqual(path.read_bytes(), payload)
            with self.assertRaises(FileExistsError):
                await fetch(client, digest, path)
            self.assertEqual(path.read_bytes(), payload)
        self.assertEqual(calls[:4], [('ctl.put', 0), ('ctl.put', CHUNK_BYTES),
                                     ('ctl.fetch', 0), ('ctl.fetch', CHUNK_BYTES)])

    async def test_bad_digest_never_publishes_a_partial_file(self):
        digest = '0' * 64
        client = SimpleNamespace(request=AsyncMock(return_value={
            'id': digest, 'name': 'report.txt', 'size': 1, 'data': 'YQ==', 'next_offset': None,
        }))
        with TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(BridgeError, 'SHA-256'):
                await fetch(client, digest, directory=root)
            self.assertEqual(list(root.iterdir()), [])

    def test_attachment_renders_fetch_in_direct_message_and_digest(self):
        filename = 'a report.md'
        envelope = {'v': 1, 'id': str(uuid.uuid4()), 'from': 'bob', 'to': 'alice', 'body': 'Report',
                    'kind': 'message', 'attachments': [{'id': 'a' * 64, 'name': filename, 'size': 42}]}
        validate_envelope(envelope, 'alice')
        for item in (envelope, {'kind': 'digest', 'items': [envelope]}):
            [rendered] = delivery_parts(item, Path('private config'))
            self.assertIn(filename, rendered)
            self.assertIn("csl fetch " + "a" * 64 + " --from alice --nats-config 'private config'", rendered)
        envelope['attachments'][0]['name'] = '../escape'
        with self.assertRaisesRegex(BridgeError, 'file name'):
            validate_envelope(envelope, 'alice')

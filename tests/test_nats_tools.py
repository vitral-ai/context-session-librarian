import base64
import hashlib
import io
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
import uuid

from csl_bridge_store import BridgeError
from csl_nats import delivery_parts, validate_envelope
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

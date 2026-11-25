from contextlib import redirect_stderr
from io import StringIO
import json
from pathlib import Path
import sqlite3
import tempfile
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from uuid import UUID

from csl_agent_hub import Hub
from scripts.import_hub_history import apply_history, load_history, pair


class HubHistoryImportTests(unittest.TestCase):
    def setUp(self):
        work = tempfile.TemporaryDirectory()
        self.addCleanup(work.cleanup)
        self.database = Path(work.name) / 'hub.sqlite'
        self.hub = Hub(self.database, {}, history_retention=215)
        self.selected = pair('worker-b,worker-a')
        self.agents = {
            name: {'name': name, 'machine': machine}
            for name, machine in [('worker-a', 'laptop'), ('worker-b', 'threadripper'), ('worker-c', 'laptop')]
        }
        with self.hub.connect() as db:
            db.execute("INSERT INTO agents VALUES ('worker-a', '{\"machine\":\"laptop\"}')")
            db.execute("INSERT INTO agents VALUES ('worker-b', '{\"machine\":\"threadripper\"}')")

    def record(self, number, *, status='delivered', source='worker-a', target='worker-b', parent=None):
        mid = str(UUID(int=number))
        packet = {'id': mid, 'source': self.agents[source], 'target': self.agents[target],
                  'text': f'exact body {number}\n', 'created_at': '2026-11-25T12:00:00+00:00'}
        with self.hub.connect() as db:
            self.hub.remember(db, packet, status)
            if parent:
                packet['in_reply_to'] = parent
                db.execute('INSERT INTO envelopes VALUES(?,?,?,?,?,?,NULL,0)',
                           (mid, source, target, self.agents[target]['machine'], json.dumps(packet), status))
        return mid

    def test_pairs_use_last_200_in_both_directions(self):
        for number in range(1, 202):
            parent = self.record(number)
        last = self.record(202, source='worker-b', target='worker-a', parent=parent)
        self.record(203, target='worker-c')
        rows = load_history(self.database, [self.selected, self.selected])
        self.assertEqual(len(rows), 200)
        self.assertEqual(rows[0]['id'], str(UUID(int=3)))
        self.assertEqual(rows[-1], {
            'id': last, 'from': 'worker-b', 'to': 'worker-a', 'from_machine': 'threadripper',
            'to_machine': 'laptop', 'state': 'delivered', 'created_at': '2026-11-25T12:00:00+00:00',
            'body': 'exact body 202\n', 'reply_to': parent,
        })

    def test_queued_and_machine_rebinding_fail_before_apply(self):
        self.record(1, status='queued')
        with self.assertRaisesRegex(ValueError, 'settle it before import'):
            load_history(self.database, [self.selected])
        with sqlite3.connect(self.database) as db:
            db.execute("UPDATE history SET state='dropped',target_machine='stale'")
        with self.assertRaisesRegex(ValueError, 'does not match the current machines'):
            load_history(self.database, [self.selected])


class ArchiveCapacityTests(unittest.IsolatedAsyncioTestCase):
    async def test_batch_reports_capacity_before_all_writes(self):
        actions = []

        async def request(_subject, raw, *, timeout):
            data = json.loads(raw)
            actions.append(data['action'])
            return SimpleNamespace(data=json.dumps({
                'id': data['history']['id'], 'payload_bytes': 1024,
                'reserve_bytes': 2048, 'available_bytes': 3072,
            }).encode())

        connection = SimpleNamespace(request=request, close=AsyncMock())
        nats = SimpleNamespace(connect=AsyncMock(return_value=connection))
        args = SimpleNamespace(tenant='vi', server='tls://example.test:4222', owner_creds='unused', ca=None)
        report = StringIO()
        with patch.dict(sys.modules, {'nats': nats}), redirect_stderr(report):
            with self.assertRaisesRegex(ValueError, 'no rows were written'):
                await apply_history(args, [{'id': 'first'}, {'id': 'second'}])
        self.assertEqual(actions, ['plan-hub-history', 'plan-hub-history'])
        self.assertEqual(json.loads(report.getvalue()), {
            'rows': 2, 'payload_bytes': 2048, 'reserve_bytes': 4096,
            'available_bytes': 3072, 'applied': False,
        })
        connection.close.assert_awaited_once()

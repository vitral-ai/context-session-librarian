"""Message scheduling through a retained, disk-backed watch backlog."""
import tempfile
import time
import unittest
import uuid
from pathlib import Path

from csl_light import Journal


class OutboundPriorityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='csl-outbound-priority-')
        self.addCleanup(self.temp.cleanup)
        self.journal = Journal(Path(self.temp.name) / 'light.sqlite')
        self.peer = 'synthetic-guest'

    def put(self, request=None, *, kind='request', peer=None):
        identifier = str(uuid.uuid4())
        frame = {'id': identifier, 'type': kind}
        if request is not None:
            frame['request'] = request
        self.journal.put('out', peer or self.peer, identifier, frame)
        return identifier

    def test_messages_and_events_precede_more_than_one_window_of_old_watches(self):
        watches = [self.put({'op': 'watch'}) for _ in range(300)]
        messages = [self.put({'op': 'followup'}),
                    self.put({'op': 'mail', 'action': 'send'}),
                    self.put({'op': 'send_message'}), self.put(kind='event')]
        rows = self.journal.ready_out(self.peer)
        identifiers = [row['id'] for row in rows]
        self.assertEqual(identifiers[:4], messages)
        self.assertEqual(identifiers[4:], watches[:252])
        self.assertEqual(len(rows), 256)
        with self.journal.db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM rpc').fetchone()[0], 304)

    def test_retry_deadlines_are_applied_before_the_window(self):
        waiting = [self.put({'op': 'followup'}) for _ in range(300)]
        eligible = self.put({'op': 'followup'})
        deadlines = dict.fromkeys(waiting, time.monotonic() + 60)
        self.assertEqual([row['id'] for row in self.journal.ready_out(self.peer, deadlines)],
                         [eligible])

    def test_completed_and_other_peer_rows_stay_excluded(self):
        completed = self.put({'op': 'followup'})
        self.journal.finish('out', self.peer, completed, {'ok': True})
        self.put({'op': 'followup'}, peer='other-guest')
        watch = self.put({'op': 'watch'})
        message = self.put({'op': 'mail', 'action': 'send'})
        self.assertEqual([row['id'] for row in self.journal.ready_out(self.peer)],
                         [message, watch])
        self.assertEqual(self.journal.get('out', self.peer, completed)['state'], 'done')

    def test_order_and_payloads_are_stable_without_delivery_mutations(self):
        watch = self.put({'op': 'watch'})
        first = self.put({'op': 'followup'})
        second = self.put({'op': 'send_message'})
        before = {identifier: self.journal.get('out', self.peer, identifier)
                  for identifier in (watch, first, second)}
        for _ in range(2):
            self.assertEqual([row['id'] for row in self.journal.ready_out(self.peer)],
                             [first, second, watch])
        self.assertEqual(before, {identifier: self.journal.get('out', self.peer, identifier)
                                  for identifier in before})


if __name__ == '__main__':
    unittest.main()

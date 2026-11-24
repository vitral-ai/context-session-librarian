import json
from datetime import datetime, timedelta, timezone
import uuid
import unittest

import test_bridge_mailbox as fixtures
from csl_bridge_hooks import surface
from csl_bridge_store import BridgeError


class HookTests(unittest.TestCase):
    setUp = fixtures.MailboxTests.setUp
    tearDown = fixtures.MailboxTests.tearDown
    send = fixtures.MailboxTests.send
    flush = fixtures.MailboxTests.flush
    receive = fixtures.MailboxTests.receive
    exchange = fixtures.MailboxTests.exchange
    def notify(self, event='UserPromptSubmit', session=None, **kwargs):
        session = session or self.b.store.agents()['bob']
        return surface(self.root/'bob', 'bob', event,
                       {'session_id': session, 'hook_event_name': event, **kwargs.pop('payload', {})}, **kwargs)

    def test_headers_only_once_per_session_without_ack_or_runtime(self):
        sent = self.send(body='Private synthetic body')
        self.flush(self.a, 'bob')
        first = self.notify()
        self.assertIn(sent['id'][:8], first)
        self.assertIn('Private synthetic body', first)
        self.assertNotIn('authority', first.split('\n', 1)[1])
        self.assertEqual(self.notify(), '')
        other = str(uuid.uuid4())
        with self.assertRaises(BridgeError):
            self.notify(session=other)
        self.assertIn(sent['id'][:8], self.notify(session=other, session_binding=other))
        self.assertFalse(self.receive()['messages'][0]['facts']['acknowledged'])

    def test_body_opt_in_uses_local_authority_not_sender_assertion(self):
        self.send(body='Synthetic message with no authority')
        self.flush(self.a, 'bob')
        value = self.notify(include_body=True)
        self.assertIn('Synthetic message with no authority', value)
        self.assertNotIn('CSL mailbox boundary notification', value)

    def test_event_formats_stop_guard_and_new_message(self):
        self.send(); self.flush(self.a, 'bob')
        self.assertEqual(self.notify(event='Stop', payload={'stop_hook_active': True}), '')
        stopped = json.loads(self.notify(event='Stop'))
        self.assertEqual(stopped['decision'], 'block')
        self.assertEqual(self.notify(event='Stop'), '')
        self.send(body='A second message'); self.flush(self.a, 'bob')
        post = json.loads(self.notify(event='PostToolUse'))
        self.assertEqual(post['hookSpecificOutput']['hookEventName'], 'PostToolUse')
        other = str(uuid.uuid4())
        self.assertIn('A second message', self.notify(session=other, session_binding=other, include_body=True))

    def test_expired_messages_cannot_starve_live_notifications(self):
        from csl_bridge_store import decode, encode
        for _ in range(12):
            self.send()
        self.flush(self.a, 'bob')
        past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        with self.b.store.transaction() as db:
            for row in db.execute('SELECT id,envelope FROM mail_messages').fetchall():
                envelope = decode(row['envelope']); envelope['expires_at'] = past
                db.execute('UPDATE mail_messages SET envelope=? WHERE id=?', (encode(envelope), row['id']))
        live = self.send(body='Live after expired rows'); self.flush(self.a, 'bob')
        self.assertIn(live['id'][:8], self.notify(limit=1))

    def test_final_json_unicode_payload_is_bounded_with_explicit_body_omission(self):
        self.send(body='😀' * 12000); self.flush(self.a, 'bob')
        value = self.notify(event='PostToolUse', include_body=True)
        self.assertLessEqual(len(value.encode()), 48000)
        content = json.loads(value)['hookSpecificOutput']['additionalContext']
        self.assertIn(' [truncated, csl receive for full text]', content)
        self.assertEqual(content.count('😀'), 600)

    def test_page_bound_reports_more_without_consuming_remaining_messages(self):
        first, second = self.send(), self.send(body='Next page')
        self.flush(self.a, 'bob')
        one = self.notify(limit=1)
        self.assertIn('More CSL messages waiting', one)
        self.assertIn(first['id'][:8], one)
        two = self.notify(limit=1)
        self.assertNotIn('More CSL messages waiting', two)
        self.assertIn(second['id'][:8], two)

    def test_boundaries_validate_without_transcript_reads(self):
        self.send(); self.flush(self.a, 'bob')
        output = self.notify(payload={'transcript_path': '/nonexistent/never-read', 'cwd': '/nonexistent'})
        self.assertTrue(output)
        with self.assertRaises(BridgeError):
            surface(self.root/'bob', 'bob', 'Unknown', {'session_id': 'x'})
        with self.assertRaises(BridgeError):
            surface(self.root/'bob', 'bob', 'Stop', {'session_id': 'x', 'hook_event_name': 'SessionStart'})


if __name__ == '__main__':
    unittest.main()

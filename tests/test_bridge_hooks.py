import json
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
    def notify(self, event='UserPromptSubmit', session='synthetic-session', **kwargs):
        return surface(self.root/'bob', 'bob', event,
                       {'session_id': session, 'hook_event_name': event, **kwargs.pop('payload', {})}, **kwargs)

    def test_headers_only_once_per_session_without_ack_or_runtime(self):
        sent = self.send(body='Private synthetic body')
        self.flush(self.a, 'bob')
        first = self.notify()
        self.assertIn(sent['id'], first)
        self.assertNotIn('Private synthetic body', first)
        self.assertNotIn('authority', first.split('\n', 1)[1])
        self.assertEqual(self.notify(), '')
        self.assertIn(sent['id'], self.notify(session='new-synthetic-session'))
        self.assertFalse(self.receive()['messages'][0]['facts']['acknowledged'])

    def test_body_opt_in_uses_local_authority_not_sender_assertion(self):
        self.send(body='Synthetic message with no authority')
        self.flush(self.a, 'bob')
        value = self.notify(include_body=True)
        body = json.loads(value.split('\n', 1)[1])['messages'][0]
        self.assertEqual(body['authority']['authority'], 'none')
        self.assertIn('Synthetic message', body['body'])

    def test_event_formats_stop_guard_and_new_message(self):
        self.send(); self.flush(self.a, 'bob')
        self.assertEqual(self.notify(event='Stop', payload={'stop_hook_active': True}), '')
        stopped = json.loads(self.notify(event='Stop'))
        self.assertEqual(stopped['decision'], 'block')
        self.assertEqual(self.notify(event='Stop'), '')
        self.send(body='A second message'); self.flush(self.a, 'bob')
        post = json.loads(self.notify(event='PostToolUse'))
        self.assertEqual(post['hookSpecificOutput']['hookEventName'], 'PostToolUse')
        self.assertIn('A second message', self.notify(session='body-check', include_body=True))

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

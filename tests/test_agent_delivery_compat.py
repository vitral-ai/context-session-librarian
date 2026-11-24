"""Durable replay and direct-vs-Hub presentation integration regressions."""
import unittest
import uuid
from unittest.mock import patch
from csl_agents import deliver, frame_message
from csl_bridge_hooks import surface
from csl_bridge_store import BridgeError, message_header
import test_bridge


class DeliveryCompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.bridge = test_bridge.BridgeTests()
        self.bridge.setUp()
        self.addCleanup(self.bridge.tearDown)
        self.setting = patch('csl_agents.config', return_value={'machine': 'remote'})
        self.setting.start()
        self.addCleanup(self.setting.stop)

    def packet(self, cli='claude', text='Synthetic durable body'):
        return {'id': str(uuid.uuid4()), 'text': text, 'created_at': '2026-11-24T00:00:00Z',
                'source': {'name': 'sender', 'session': str(uuid.uuid4()), 'cli': 'codex', 'machine': 'laptop'},
                'target': {'name': 'worker', 'session': self.bridge.thread, 'cli': cli, 'machine': 'remote'}}

    def test_replay_of_preupgrade_mail_keeps_original_fingerprint(self):
        packet = self.packet()
        with patch('csl_agents.frame_message', side_effect=lambda a,b,c,d: message_header(a,b,c)+d), \
             patch('csl_bridge_claude_notify.notify', return_value=True):
            self.assertEqual(deliver(self.bridge.connector, packet)['state'], 'delivered')
        with patch('csl_bridge_claude_notify.notify', return_value=False):
            self.assertEqual(deliver(self.bridge.connector, packet)['state'], 'delivered')
        self.assertEqual(len(self.bridge.connector.store.rows('SELECT * FROM mail_messages')), 1)
        self.assertEqual(frame_message('s', packet['id'], None, 'body'), message_header('s', packet['id'], None)+'body')

    def test_failed_push_settles_durable_mail_and_hook_emits_once(self):
        packet = self.packet()
        with patch('csl_bridge_claude_notify.notify', side_effect=BridgeError('No live native endpoint')):
            self.assertEqual(deliver(self.bridge.connector, packet)['state'], 'delivered')
            output = surface(self.bridge.connector.store.home, 'worker', 'UserPromptSubmit',
                             {'session_id': self.bridge.thread})
            self.assertIn('CSL from sender (' + packet['id'][:8] + '):\nSynthetic durable body', output)
            self.assertEqual(deliver(self.bridge.connector, packet)['state'], 'delivered')
        self.assertEqual(surface(self.bridge.connector.store.home, 'worker', 'UserPromptSubmit',
                                 {'session_id': self.bridge.thread}), '')

    def test_direct_followup_has_full_body_without_mailbox(self):
        text = 'x' * 900 + '\nImportant final instruction'
        self.bridge.connector.mailbox  # Initialize an empty durable mailbox for the assertion.
        result = self.bridge.connector.incoming('alice', self.bridge.request(text=text))
        self.assertEqual(result['state'], 'delivered')
        native = next(p['input'][0]['text'] for m,p in self.bridge.runtime.calls if m=='turn/start')
        self.assertTrue(native.endswith(text))
        self.assertNotIn('[truncated', native)
        self.assertEqual(self.bridge.connector.store.rows('SELECT * FROM mail_messages'), [])

    def test_hub_followup_only_has_preview_with_full_durable_request(self):
        text = 'x' * 900 + '\nImportant final instruction'
        packet = self.packet('codex', text)
        self.assertEqual(deliver(self.bridge.connector, packet)['state'], 'delivered')
        native = next(p['input'][0]['text'] for m,p in self.bridge.runtime.calls if m=='turn/start')
        self.assertTrue(native.endswith(' [truncated, csl receive for full text]'))
        self.assertNotIn('Important final instruction', native)
        import json
        request = self.bridge.connector.store.rows('SELECT request FROM operations WHERE id=?', (packet['id'],))[0]
        self.assertEqual(json.loads(request['request'])['text'], text)

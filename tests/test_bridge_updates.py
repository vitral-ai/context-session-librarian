import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from csl_bridge_store import BridgeError
from csl_bridge_updates import updates


class UpdatesTests(unittest.TestCase):
    def test_cursor_persists_both_channels_and_resumes(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'cursor.json'
            replies = [{'events': [{'cursor': 8, 'event': {'text': 'done'}}]}, {'events': []},
                       {'messages': [{'cursor': 12, 'body': 'mail'}]}]
            out = io.StringIO()
            with patch('csl_bridge_updates.local_call', side_effect=replies):
                updates(root, 'alice', path, out)
            self.assertEqual(len(out.getvalue().splitlines()), 2)
            self.assertEqual(json.loads(path.read_text())['mail'], 12)
            with patch('csl_bridge_updates.local_call', side_effect=[{'events': []}, {'messages': []}]) as call:
                updates(root, 'alice', path, io.StringIO())
            self.assertEqual(call.call_args_list[0].args[1]['after'], 8)
            self.assertEqual(call.call_args_list[1].args[1]['after'], 12)
            with self.assertRaises(BridgeError):
                updates(root, 'bob', path, io.StringIO())

    def test_output_failure_does_not_advance_cursor(self):
        class Broken(io.StringIO):
            def flush(self):
                raise BrokenPipeError()
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'cursor.json'
            with patch('csl_bridge_updates.local_call', side_effect=[{'events': []}, {'messages': [{'cursor': 3}]}]):
                with self.assertRaises(BrokenPipeError):
                    updates(root, 'alice', path, Broken())
            self.assertFalse(path.exists())

    def test_bounded_event_drain(self):
        with tempfile.TemporaryDirectory() as root:
            with patch('csl_bridge_updates.local_call', side_effect=[{'events': [{'cursor': 1}]}, {'messages': []}]) as call:
                updates(root, 'alice', Path(root) / 'cursor.json', io.StringIO(), limit=1)
            self.assertEqual(call.call_count, 2)

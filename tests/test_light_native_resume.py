"""Actual Native.inject with a synthetic RPC boundary; no native daemon/model."""
import unittest
import uuid

import csl_light as light
from csl_bridge_rpc import RuntimeRejected


class NativeResumeTests(unittest.TestCase):
    def exercise(self, loaded=False, duplicate=False, reject=False, read_error=False, active=False):
        thread, message_id, turn = (str(uuid.uuid4()) for _ in range(3))
        calls = []

        class Node:
            def agent(self, agent):
                if agent != 'worker':
                    raise ValueError('Unknown registered fixture agent')
                return '/synthetic', thread

        class Client:
            def __init__(self):
                self.loaded = loaded

            def call(self, method, params):
                calls.append((method, params))
                if method == 'thread/read':
                    if read_error:
                        raise RuntimeRejected('Synthetic read rejection')
                    return {'thread': {'id': thread, 'status': {
                        'type': 'idle' if self.loaded else 'notLoaded'}}}
                if method == 'thread/resume':
                    if reject:
                        raise RuntimeRejected('Synthetic resume rejection')
                    self.loaded = True
                    return {'thread': {'id': thread, 'status': {'type': 'idle'}}}
                if method == 'thread/turns/list':
                    turns = [{'id': turn, 'status': 'completed' if duplicate else 'inProgress',
                              'items': [{'type': 'userMessage', 'clientId': message_id}] if duplicate else []}]
                    return {'data': turns if duplicate or active else []}
                if method in ('turn/start', 'turn/steer'):
                    if not self.loaded:
                        raise RuntimeRejected('Synthetic thread not loaded')
                    return {'turn': {'id': turn}} if method == 'turn/start' else {'turnId': turn}
                raise AssertionError('Unexpected RPC: ' + method)

        client = Client()
        native = light.Native(Node())
        native.client = lambda home: client
        try:
            result = native.inject('worker', {'id': message_id, 'source': 'coordinator',
                                             'text': 'Synthetic message'}, str(uuid.uuid4()))
            error = None
        except Exception as caught:
            result, error = None, caught
        return result, error, calls, thread

    def test_not_loaded_resumes_registered_uuid_before_delivery(self):
        result, error, calls, thread = self.exercise()
        self.assertIsNone(error)
        self.assertEqual(result['state'], 'delivered')
        self.assertEqual([name for name, _ in calls],
                         ['thread/read', 'thread/resume', 'thread/turns/list', 'turn/start'])
        self.assertEqual(calls[1][1], {'threadId': thread, 'excludeTurns': True})
        self.assertTrue(all(params['threadId'] == thread for _, params in calls))
        self.assertEqual(calls[2][1]['limit'], 20)

    def test_loaded_thread_does_not_resume(self):
        result, error, calls, _ = self.exercise(loaded=True)
        self.assertIsNone(error)
        self.assertEqual(result['state'], 'delivered')
        self.assertNotIn('thread/resume', [name for name, _ in calls])

    def test_duplicate_after_resume_recovers_without_another_turn(self):
        result, error, calls, _ = self.exercise(duplicate=True)
        self.assertIsNone(error)
        self.assertTrue(result['reconciled'])
        self.assertFalse(any(name in ('turn/start', 'turn/steer') for name, _ in calls))
        self.assertEqual(next(params['limit'] for name, params in calls
                              if name == 'thread/turns/list'), 20)

    def test_resume_failure_does_not_start_or_retry_resume(self):
        result, error, calls, _ = self.exercise(reject=True)
        self.assertIsNone(result)
        self.assertIsInstance(error, RuntimeRejected)
        self.assertEqual([name for name, _ in calls], ['thread/read', 'thread/resume'])

    def test_existing_read_exception_fallback_is_unchanged(self):
        result, error, calls, _ = self.exercise(read_error=True)
        self.assertIsNone(error)
        self.assertEqual(result['state'], 'delivered')
        self.assertEqual([name for name, _ in calls].count('thread/resume'), 1)

    def test_loaded_active_turn_is_steered_without_a_new_turn(self):
        result, error, calls, _ = self.exercise(loaded=True, active=True)
        self.assertIsNone(error)
        self.assertEqual(result['state'], 'delivered')
        self.assertIn('turn/steer', [name for name, _ in calls])
        self.assertNotIn('turn/start', [name for name, _ in calls])

    def test_resumed_active_turn_is_steered_on_the_same_uuid(self):
        result, error, calls, thread = self.exercise(active=True)
        self.assertIsNone(error)
        self.assertEqual(result['state'], 'delivered')
        self.assertEqual([name for name, _ in calls],
                         ['thread/read', 'thread/resume', 'thread/turns/list', 'turn/steer'])
        self.assertEqual(calls[1][1], {'threadId': thread, 'excludeTurns': True})
        self.assertTrue(all(params['threadId'] == thread for _, params in calls))

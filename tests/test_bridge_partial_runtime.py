"""Partial native availability must not disconnect sibling agents."""
import threading
import time
import unittest

import test_bridge as fixtures


class PartialRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.BridgeTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.connector = self.fixture.connector
        self.runtime = self.fixture.runtime
        self.late = fixtures.uid()
        self.connector.agents['late'] = self.late
        self.connector.locks['late'] = threading.RLock()
        self.connector.waiters['late'] = 0
        self.connector.peers['alice']['allow_agents'].append('late')
        self.loaded = {self.fixture.thread}
        original = self.runtime.call

        def call(method, params, **kwargs):
            if method == 'thread/loaded/list':
                return {'data': list(self.loaded), 'nextCursor': None}
            return original(method, params, **kwargs)

        self.runtime.call = call

    def request(self, target='worker', op='followup'):
        return self.fixture.request(op, target=target)

    def test_missing_agent_does_not_block_loaded_sibling(self):
        self.connector.refresh_loaded(self.runtime)
        self.assertEqual(self.connector.current('late')['status'], 'agent_not_loaded')
        request = self.request('late')
        pending = self.connector.incoming('alice', request)
        self.assertEqual((pending['state'], pending['reason']), ('pending', 'agent_not_loaded'))
        self.assertEqual(self.connector.incoming('alice', self.request())['state'], 'delivered')
        resumes = [p['threadId'] for m, p in self.runtime.calls if m == 'thread/resume']
        self.assertEqual(resumes, [self.fixture.thread])

    def test_late_load_drains_same_id_once_without_reconnect(self):
        self.connector.refresh_loaded(self.runtime)
        request = self.request('late')
        self.assertEqual(self.connector.incoming('alice', request)['state'], 'pending')
        self.loaded.add(self.late)
        self.assertEqual(self.connector.refresh_loaded(self.runtime), {self.late})
        self.connector.reconcile()
        receipt = self.connector.incoming('alice', request)
        self.assertEqual(receipt['state'], 'delivered')
        self.assertEqual(self.connector.refresh_loaded(self.runtime), set())
        self.connector.reconcile()
        effects = [(m, p) for m, p in self.runtime.calls
                   if m in ('turn/start', 'turn/steer') and p['clientUserMessageId'] == request['id']]
        self.assertEqual(len(effects), 1)
        self.assertTrue(self.runtime.connected)

    def test_unload_keeps_messages_pending_without_hosting_thread(self):
        self.connector.refresh_loaded(self.runtime)
        self.loaded.clear()
        self.connector.refresh_loaded(self.runtime)
        request = self.request(op='send_message')
        result = self.connector.incoming('alice', request)
        self.assertEqual(result['reason'], 'agent_not_loaded')
        self.assertFalse(any(m.startswith('turn/') for m, _ in self.runtime.calls))

    def test_existing_loop_discovers_late_load_without_restart(self):
        request = self.request('late')
        self.connector.refresh_loaded(self.runtime)
        self.connector.incoming('alice', request)
        self.connector.factory = lambda _: self.runtime
        thread = threading.Thread(target=self.connector.runtime_loop)
        thread.start()
        try:
            self.loaded.add(self.late)
            deadline = time.monotonic() + 6
            while time.monotonic() < deadline:
                row = self.connector.store.rows('SELECT state FROM operations WHERE id=?', (request['id'],))[0]
                if row['state'] == 'delivered':
                    break
                time.sleep(.05)
            self.assertEqual(row['state'], 'delivered')
        finally:
            self.connector.stop.set()
            thread.join(3)
        self.assertFalse(thread.is_alive())

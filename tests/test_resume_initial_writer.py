"""Initial same-device acquisition with durable local state and synthetic HTTP."""
import copy
from contextlib import ExitStack
from unittest.mock import patch, Mock
import unittest

from test_resume_action import LocalTests, request, NATIVE, REQUEST, SNAP, DEVICE, ACCOUNT, REGISTRY
import csl_resume_action as action
from csl_sync_state import State, SyncError
from csl_sync_writer import Writer, read_fence


class InitialWriterTests(LocalTests):
    def setUp(self):
        super().setUp()
        self.state = State(self.profile); self.addCleanup(self.state.close)
        self.state.set('device_id', DEVICE)
        self.state.set('settings', dict(endpoint='https://registry.example'))
        self.file = dict(file_key='main', mode='append', complete_length=12, size=12, sha256='a'*64)
        self.state.save_session('codex', NATIVE, dict(status='synced', snapshot_id=SNAP,
            files=dict(main=dict(mode='append', length=12, sha256='a'*64, offset=12))))
        self.remote = dict(id=REGISTRY, provider='codex', native_id=NATIVE, snapshot_id=SNAP,
            writer=dict(state='legacy', device_id=DEVICE, generation=0), sync=dict(complete=True))
        self.identity = dict(account_id=ACCOUNT, device_id=DEVICE)
        self.registry = Mock(); self.registry.call.side_effect = self.http
        self.posts = []
        self.q = request(action='run', expected_device_id=DEVICE); self.q.pop('intent')
        self.local = action.Local(self.q)
        self.writer = Writer(self.state, self.registry)
        self.stack = ExitStack(); self.addCleanup(self.stack.close)
        self.stop = self.stack.enter_context(patch.object(action, 'require_stopped'))
        self.stack.enter_context(patch('csl_sync_providers.raw_files', side_effect=lambda *a: [dict(self.file)]))

    def http(self, method, path, **kw):
        if method == 'GET': return copy.deepcopy(self.remote)
        self.assertEqual(read_fence('codex', NATIVE)['state'], 'pending')
        self.assertEqual(self.state.session('codex', NATIVE)['writer']['request_id'], REQUEST)
        self.posts.append(kw['body'])
        self.remote['writer'] = dict(state='active', device_id=DEVICE, generation=1)
        return dict(self.remote['writer'])

    def acquire(self, q=None):
        action.initial_in_place(self.writer, self.local, q or self.q, self.identity, self.remote)

    def test_readonly_check_then_original_device_acquires_and_replays(self):
        self.acquire(dict(self.q, action='check', intent='in_place'))
        self.assertIsNone(read_fence('codex', NATIVE)); self.assertFalse(self.posts)
        self.assertNotIn('writer', self.state.session('codex', NATIVE))
        self.acquire()
        self.assertEqual(read_fence('codex', NATIVE)['writer']['generation'], 1)
        self.assertEqual(self.state.session('codex', NATIVE)['writer']['state'], 'active')
        self.stop.side_effect = SyncError('writer_unsupported_source')  # live exact launch replay needs no old receipt
        self.acquire(); self.assertEqual(len(self.posts), 1)
        self.assertEqual(self.posts[0], dict(request_id=REQUEST, expected_snapshot_id=SNAP, expected_generation=0))

    def test_unmanaged_live_wrong_device_changed_bytes_and_incomplete_refuse(self):
        for mutation, error in (
            (lambda: setattr(self.stop, 'side_effect', SyncError('writer_unsupported_source')), 'resume_stop_proof_required'),
            (lambda: self.remote['writer'].update(device_id=NATIVE), 'writer_fenced'),
            (lambda: self.file.update(sha256='b'*64), 'writer_snapshot_changed'),
            (lambda: self.file.update(size=13), 'writer_snapshot_changed'),
            (lambda: self.remote['sync'].update(complete=False), 'source_incomplete')):
            with self.subTest(error=error):
                self.stop.side_effect = None; self.remote['writer']['device_id'] = DEVICE
                self.remote['sync']['complete'] = True; self.file.update(sha256='a'*64, size=12)
                mutation()
                with self.assertRaisesRegex(SyncError, '^'+error+'$'): self.acquire()
                self.assertFalse(self.posts); self.assertIsNone(read_fence('codex', NATIVE))

    def test_lost_acquire_ack_reuses_exact_request(self):
        def lost(*a, **kw):
            self.http(*a, **kw); raise SyncError('network_error', retryable=True)
        self.registry.call.side_effect = lost
        with self.assertRaises(SyncError): self.acquire()
        self.assertEqual(read_fence('codex', NATIVE)['state'], 'pending')
        with self.assertRaisesRegex(SyncError, 'writer_request_conflict'):
            self.acquire(dict(self.q, request_id=NATIVE))
        self.registry.call.side_effect = self.http
        self.acquire(); self.assertEqual(len(self.posts), 2)
        self.assertEqual(self.posts[0], self.posts[1])

    def test_journal_failure_does_not_fence_and_fence_failure_recovers(self):
        with patch.object(self.state, 'save_session', side_effect=OSError('disk')):
            with self.assertRaises(OSError): self.acquire()
        self.assertIsNone(read_fence('codex', NATIVE)); self.assertFalse(self.posts)
        with patch.object(action, 'save_fence', side_effect=OSError('disk')):
            with self.assertRaises(OSError): self.acquire()
        self.assertEqual(self.state.session('codex', NATIVE)['writer']['state'], 'pending')
        self.assertIsNone(read_fence('codex', NATIVE)); self.assertFalse(self.posts)
        self.acquire(); self.assertEqual(len(self.posts), 1)

    def test_active_sqlite_publishing_fence_retry_does_not_acquire_twice(self):
        from csl_sync_writer import save_fence as real_save
        def fail_active(provider, identifier, status, binding, **kw):
            if status == 'active': raise OSError('disk')
            return real_save(provider, identifier, status, binding, **kw)
        with patch('csl_sync_writer.save_fence', side_effect=fail_active):
            with self.assertRaises(OSError): self.acquire()
        self.assertEqual(self.state.session('codex', NATIVE)['writer']['state'], 'active')
        self.assertEqual(read_fence('codex', NATIVE)['state'], 'publishing')
        before = self.state.session('codex', NATIVE)
        self.acquire(dict(self.q, action='check', intent='in_place'))
        self.assertEqual(self.state.session('codex', NATIVE), before)
        self.assertEqual(read_fence('codex', NATIVE)['state'], 'publishing')
        self.acquire(); self.assertEqual(len(self.posts), 1)
        self.assertEqual(read_fence('codex', NATIVE)['state'], 'active')

    def test_execute_run_reports_actual_generation_and_unlocks_before_host(self):
        with patch('csl_sync.attest_installation', return_value=self.identity), \
             patch('csl_sync_http.Registry', return_value=self.registry), \
             patch('csl_sync_providers._resolve'), \
             patch.object(action.Local, 'run_host', return_value=dict(state='launched')) as launch:
            checked = action.execute(dict(self.q, action='check', intent='in_place'))
            self.assertNotIn('generation', checked)
            self.assertEqual(checked['state'], 'eligible')
            result = action.execute(self.q)
            self.assertEqual(result['generation'], 1)
            self.assertEqual(launch.call_args.args[0]['expected_writer']['generation'], 1)
            self.assertEqual(action.execute(self.q)['generation'], 1)
            self.assertEqual(len(self.posts), 1)


if __name__ == '__main__': unittest.main()

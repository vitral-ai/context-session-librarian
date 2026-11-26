"""Synthetic writer handoffs; no native processes or personal profiles are used."""
import json
import hashlib
import io
import os
from pathlib import Path
import tempfile
import unittest
import uuid
import zipfile
from types import SimpleNamespace
from contextlib import redirect_stdout
from unittest.mock import patch

from csl_sync_http import HTTP, Registry
from csl_sync_state import State, SyncError
from csl_sync_writer import Writer, native_lock, write_headers, read_fence, fence_path
from csl_sync_engine import Collector
from sync_support import FakeProvider, FakeTokens, Peer


class WriterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.env = patch.dict(os.environ, CODEX_HOME=str(self.root/'codex'), CLAUDE_CONFIG_DIR=str(self.root/'claude'))
        self.env.start(); self.addCleanup(self.env.stop)
        self.state = State(self.root/'state'); self.addCleanup(self.state.close)
        self.device = self.state.get('device_id')
        self.state.set('settings', dict(kind='local', endpoint='https://registry.example/v1',
                                       token_endpoint='https://identity.example/native-auth/v1/token'))
        self.state.set('registered', True)
        owner = self
        class Transport:
            def request(self, url, method, **kwargs):
                owner.request = (url, method, kwargs)
                return dict(kind='collector', device_id=owner.device, account_id='opaque-owner')
        class API:
            http = Transport()
            tokens = FakeTokens()
            def call(self, method, path, **kwargs):
                owner.calls.append((method, path, kwargs))
                return owner.response
        self.calls = []; self.api = API(); self.writer = Writer(self.state, self.api)
        self.response = dict(device_id=self.device, generation=1, state='active')
        self.binding = dict(registry_id='ss_fixture', snapshot='snapshot-fixture', generation=0,
                            request_id=str(uuid.uuid4()), expected_device=self.device,
                            package_sha256='a'*64, account_id='opaque-owner', destinations=['/synthetic/native.jsonl'])

    def test_attestation_uses_trusted_token_origin_root_path_and_same_token(self):
        self.assertEqual(self.writer.attest(self.device), 'opaque-owner')
        url, method, options = self.request
        self.assertEqual(url, 'https://identity.example/session-registry/authorize')
        self.assertEqual(method, 'POST')
        self.assertEqual(json.loads(options['data']), {'action':'write'})
        self.assertEqual(options['headers']['Authorization'], 'Bearer fixture-access')
        self.assertFalse(options['retry'])

    def test_missing_device_workload_and_mismatched_attestation_refuse(self):
        with self.assertRaisesRegex(SyncError, 'writer_device_required'): self.writer.attest('foreign-device')
        self.state.set('settings', {**self.state.get('settings'), 'kind':'workload'})
        with self.assertRaisesRegex(SyncError, 'writer_device_required'): self.writer.attest(self.device)
        self.assertFalse(self.calls)

    def test_lost_acquire_response_retries_exact_durable_request(self):
        original = self.api.call
        def lose(method, path, **kwargs):
            self.assertEqual(self.state.session('codex', 'native')['writer']['state'], 'pending')
            self.calls.append((method, path, kwargs))
            raise SyncError('registry_unavailable')
        self.api.call = lose
        with self.assertRaisesRegex(SyncError, 'registry_unavailable'):
            self.writer.acquire('codex', 'native', **self.binding)
        self.api.call = original
        result = self.writer.acquire('codex', 'native', **self.binding)
        self.assertEqual(result['state'], 'publishing')
        self.assertEqual(self.calls[0], self.calls[1])
        self.assertEqual(self.calls[1][2]['body'], dict(request_id=self.binding['request_id'],
                         expected_snapshot_id='snapshot-fixture', expected_generation=0))
        self.assertEqual(self.calls[1][2]['key'], self.binding['request_id'])
        self.assertNotIn('access_token', json.dumps(self.state.session('codex', 'native')))

    def test_pending_blocks_upload_and_changed_request_account_or_destination_refuse(self):
        self.writer.acquire('codex', 'native', **self.binding)
        with self.assertRaisesRegex(SyncError, 'writer_fenced'): write_headers(self.state, 'codex', 'native', self.api)
        for key, value in [('request_id',str(uuid.uuid4())), ('account_id','other'), ('destinations',['/different'])]:
            with self.subTest(key=key), self.assertRaisesRegex(SyncError,'writer_request_conflict'):
                self.writer.acquire('codex', 'native', **{**self.binding,key:value})
        self.assertEqual(len(self.calls),1)

    def test_publish_reuses_acquire_and_attests_account_before_headers(self):
        self.writer.acquire('codex', 'native', **self.binding)
        self.writer.acquire('codex', 'native', **self.binding)
        self.assertEqual(len(self.calls), 1)
        self.writer.published('codex', 'native')
        self.assertEqual(write_headers(self.state,'codex','native',self.api), {'X-Vitral-Session-Writer':'1'})
        local = self.state.session('codex','native'); local['writer']['account_id']='other-account'
        self.state.save_session('codex','native',local)
        with self.assertRaisesRegex(SyncError,'writer_device_required'): write_headers(self.state,'codex','native',self.api)

    def test_sign_in_checkpoint_reset_preserves_writer_fence(self):
        self.writer.acquire('codex', 'native', **self.binding)
        record = self.state.session('codex','native')['writer']
        self.state.reset_checkpoints()
        self.assertEqual(self.state.session('codex','native')['writer'], record)
        with self.assertRaisesRegex(SyncError, 'writer_fenced'): write_headers(self.state,'codex','native',self.api)
        self.assertEqual(write_headers(self.state,'codex','legacy'), {})

    def test_lock_is_shared_by_profiles_and_scoped_to_native_id(self):
        with native_lock('codex','native'):
            with self.assertRaisesRegex(SyncError,'collector_busy'):
                with native_lock('codex','native'): self.fail('duplicate lock')
            with native_lock('codex','unrelated'): pass
            with native_lock('claude','native'): pass

    def test_shared_fence_blocks_other_profiles_even_without_their_own_journal(self):
        other = State(self.root/'other'); self.addCleanup(other.close)
        self.writer.acquire('codex', 'native', **self.binding)
        self.assertEqual(read_fence('codex', 'native')['state'], 'publishing')
        with self.assertRaisesRegex(SyncError, 'writer_fenced'):
            write_headers(other, 'codex', 'native', self.api)
        self.writer.published('codex', 'native')
        self.assertEqual(read_fence('codex', 'native')['state'], 'active')
        with self.assertRaisesRegex(SyncError, 'writer_fenced'):
            write_headers(other, 'codex', 'native', self.api)

    def test_invalid_shared_fence_is_not_treated_as_legacy(self):
        path = fence_path('codex', 'native'); path.parent.mkdir(parents=True)
        path.write_text('{'); path.chmod(0o600)
        with self.assertRaisesRegex(SyncError, 'invalid_writer_fence'):
            write_headers(self.state, 'codex', 'native', self.api)

    def test_keeper_receipt_requires_positive_end_of_matching_native_process(self):
        from csl_resume import require_stopped
        from types import SimpleNamespace
        receipt = dict(schema_version=1, provider='codex', native_session_id='native', keeper_session_id='keeper',
                       process=dict(pid=42, start_identity='fixture-incarnation'),
                       end=dict(exited_at='2026-11-26T12:00:00Z', exit_status=0))
        def execute(command, **options):
            self.assertEqual(command, ['vitral-host','keeper-receipt','--provider','codex','--native-id','native'])
            self.assertEqual(options['timeout'], 10)
            options['stdout'].write(json.dumps(receipt).encode())
            return SimpleNamespace(returncode=0)
        with patch('subprocess.run', execute):
            self.assertEqual(require_stopped('codex','native'), receipt)
            for replacement in (dict(end=None), dict(native_session_id='other'),
                                dict(process=dict(pid=42,start_identity='')), dict(schema_version=2)):
                with patch.dict(receipt, replacement), self.assertRaisesRegex(SyncError, 'writer_unsupported_source'):
                    require_stopped('codex','native')

    def test_lost_release_response_fences_source_and_reconciles_without_second_post(self):
        self.response = dict(id='ss_fixture',provider='codex',native_id='native',snapshot_id='snapshot-fixture',
            writer=dict(state='legacy',generation=0,device_id=self.device),
            sync=dict(complete=True),package=dict(available=True))
        def calls(method, path, **kwargs):
            self.calls.append((method,path,kwargs))
            if method == 'POST':
                self.assertEqual(read_fence('codex','native')['state'], 'releasing')
                self.assertEqual(self.state.session('codex','native')['writer']['state'], 'releasing')
                self.response['writer']['state'] = 'released'
                raise SyncError('registry_unavailable')
            return self.response
        self.api.call = calls
        options = dict(registry_id='ss_fixture', snapshot='snapshot-fixture',generation=0,expected_device=self.device)
        with self.assertRaisesRegex(SyncError,'registry_unavailable'):
            self.writer.release('codex','native',**options)
        with self.assertRaisesRegex(SyncError,'writer_fenced'):
            write_headers(self.state,'codex','native',self.api)
        self.assertEqual(self.writer.release('codex','native',**options), dict(state='released',generation=0))
        self.assertEqual(sum(method == 'POST' for method,_,_ in self.calls), 1)
        self.assertEqual(read_fence('codex','native')['state'], 'released')

    def test_release_requires_exact_complete_owned_snapshot_before_mutation(self):
        self.response = dict(id='ss_fixture',provider='codex',native_id='native',snapshot_id='snapshot-fixture',
            writer=dict(state='legacy',generation=0,device_id=self.device),
            sync=dict(complete=False),package=dict(available=True))
        with self.assertRaisesRegex(SyncError,'writer_snapshot_incomplete'):
            self.writer.release('codex','native',registry_id='ss_fixture',snapshot='snapshot-fixture',
                                generation=0,expected_device=self.device)
        self.assertIsNone(read_fence('codex','native'))
        self.assertTrue(all(method == 'GET' for method,_,_ in self.calls))

    def import_fixture(self, provider='claude'):
        identifier = str(uuid.uuid4())
        raw = (json.dumps(dict(type='user', uuid='fixture-user', sessionId=identifier,
                              message=dict(role='user', content='Synthetic handoff'))) + '\n').encode()
        if provider == 'codex':
            raw = (json.dumps(dict(type='session_meta', payload=dict(id=identifier, cwd=str(self.root)))) + '\n').encode()
        member = 'sessions/' + identifier + '/main.jsonl'
        manifest = dict(format='codex-session-librarian',version=3,mode='raw',created_at='2026-11-26T12:00:00Z',
            sessions=[dict(id=identifier, provider=provider, primary=member, files=[
                dict(file=member,relative_path='session.jsonl',file_key='main',role='transcript',
                     bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())])])
        bundle = self.root/'fixture.zip'
        with zipfile.ZipFile(bundle, 'w') as archive:
            archive.writestr('manifest.json',json.dumps(manifest));archive.writestr(member,raw)
        row = dict(id='ss_fixture', provider=provider,native_id=identifier,snapshot_id='snapshot-fixture',
                   writer=dict(state='released',generation=0,device_id='source-device'),
                   sync=dict(complete=True),package=dict(available=True))
        def call(method, path, **kwargs):
            self.calls.append((method,path,kwargs))
            if method == 'POST':
                row['writer'] = dict(state='active',device_id=self.device,generation=1)
                return row['writer']
            return row
        self.api.call = call
        def download(path, output, limit): output.write(bundle.read_bytes())
        self.api.download = download
        args = SimpleNamespace(bundle=bundle, sync_home=self.state.root, cwd=self.root, open=False,
            provider=None, list=False, name=None, target=None, leaf=None, repair_interrupted=False,
            history_policy='active',registry_session_id='ss_fixture',expected_snapshot_id='snapshot-fixture',
            expected_generation=0, request_id=str(uuid.uuid4()),expected_device_id=self.device)
        return args, identifier, raw

    def test_fresh_destination_keeps_native_id_and_bytes_without_prior_process_receipt(self):
        from csl_resume import import_same_id
        import csl_claude
        args, identifier, raw = self.import_fixture()
        with patch('csl_sync_http.Registry', return_value=self.api), patch('csl_resume.require_stopped') as stopped, redirect_stdout(io.StringIO()):
            self.assertEqual(import_same_id(args, None), [identifier])
        stopped.assert_not_called()
        self.assertEqual(csl_claude.target_path(identifier,self.root).read_bytes(), raw)
        self.assertEqual(self.state.session('claude',identifier)['writer']['state'], 'active')
        self.assertEqual(read_fence('claude',identifier)['state'], 'active')
        self.assertEqual(sum(method == 'POST' for method,_,_ in self.calls), 1)

    def test_existing_native_id_requires_positive_stop_and_byte_identity(self):
        from csl_resume import import_same_id
        import csl_claude
        args, identifier, raw = self.import_fixture()
        target = csl_claude.target_path(identifier,self.root);target.parent.mkdir(parents=True);target.write_bytes(raw)
        with patch('csl_sync_http.Registry',return_value=self.api), patch('csl_resume.require_stopped',side_effect=SyncError('writer_unsupported_source')):
            with self.assertRaisesRegex(SyncError,'writer_unsupported_source'): import_same_id(args,None)
        self.assertFalse(self.calls)
        target.write_bytes(raw + b'{}\n')
        with patch('csl_sync_http.Registry',return_value=self.api), patch('csl_resume.require_stopped'), redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(SyncError,'native_destination_conflict'): import_same_id(args,None)
        self.assertFalse(any(method == 'POST' for method,_,_ in self.calls))

    def test_failed_publish_keeps_fence_and_exact_retry_does_not_reacquire(self):
        from csl_resume import import_same_id
        args, identifier, raw = self.import_fixture()
        with patch('csl_sync_http.Registry',return_value=self.api), patch('csl_transfer.publish_session_file',side_effect=OSError('synthetic disk failure')):
            with self.assertRaises(OSError): import_same_id(args,None)
        self.assertEqual(read_fence('claude',identifier)['state'], 'publishing')
        with patch('csl_sync_http.Registry',return_value=self.api), redirect_stdout(io.StringIO()):
            import_same_id(args,None)
        self.assertEqual(sum(method == 'POST' for method,_,_ in self.calls), 1)
        self.assertEqual(read_fence('claude',identifier)['state'], 'active')

    def test_released_requires_exact_snapshot_generation_and_complete_package(self):
        self.response = dict(id='ss_fixture',provider='codex',native_id='native',snapshot_id='snapshot-fixture',
                             writer=dict(state='released',generation=0),sync=dict(complete=True),package=dict(available=True))
        self.writer.released('ss_fixture','codex','native','snapshot-fixture',0)
        for field, value, code in [('snapshot_id','old','writer_snapshot_changed'),
                                  ('writer',dict(state='active',generation=0),'writer_in_use'),
                                  ('sync',dict(complete=False),'source_incomplete')]:
            with patch.dict(self.response,{field:value}), self.assertRaisesRegex(SyncError,code):
                self.writer.released('ss_fixture','codex','native','snapshot-fixture',0)

    def test_generation_header_reaches_real_snapshot_raw_and_view_requests(self):
        peer = Peer(); self.addCleanup(peer.close)
        state_settings = self.state.get('settings'); state_settings['endpoint'] = peer.url
        self.state.set('settings', state_settings)
        api = Registry(peer.url, FakeTokens(), http=HTTP(sleep=lambda _:None))
        requests = []; request = api.http.request
        def recording(url, method='GET', **kwargs):
            if url.endswith('/session-registry/authorize'):
                return dict(kind='collector', device_id=self.device, account_id='opaque-owner')
            if method == 'PUT': requests.append((url, kwargs.get('headers',{})))
            return request(url, method, **kwargs)
        api.http.request = recording
        writer = Writer(self.state, self.api)
        writer.acquire('codex','fake-session',**self.binding); writer.published('codex','fake-session')
        path = self.root/'synthetic.jsonl';path.write_text('one\ntwo\n')
        provider = FakeProvider(path)
        collector = Collector(self.state, api, provider)
        collector.sync_session('codex','fake-session',provider.list_sessions('codex')[0])
        self.assertEqual(len(requests),3)
        self.assertTrue(any('/content?' in url for url,_ in requests))
        self.assertTrue(any(url.endswith('/view') for url,_ in requests))
        self.assertTrue(all(headers.get('X-Vitral-Session-Writer') == '1' for _,headers in requests))

    def test_other_provider_upload_keeps_legacy_path(self):
        peer = Peer(); self.addCleanup(peer.close)
        api = Registry(peer.url, FakeTokens(), http=HTTP(sleep=lambda _:None))
        path = self.root/'other.jsonl'; path.write_text('one\ntwo\n')
        provider = FakeProvider(path)
        view_records = provider.view_records
        def paired_page(*args, **kwargs):
            page = view_records(*args, **kwargs)
            page['source_files'] = [page['source_file']]
            return page
        provider.view_records = paired_page
        Collector(self.state, api, provider).sync_session('antigravity', 'fake-session', provider.list_sessions('codex')[0])
        self.assertTrue(peer.snapshots)

    def test_missing_manifest_is_a_clean_input_error(self):
        from csl_resume import import_same_id
        args, _, _ = self.import_fixture()
        with zipfile.ZipFile(args.bundle, 'w') as archive:
            archive.writestr('other', 'fixture')
        with self.assertRaisesRegex(SyncError, 'invalid_native_manifest'):
            import_same_id(args, None)

    def test_codex_destination_keeps_exact_raw_session_metadata(self):
        from csl_resume import import_same_id
        from csl_sync_providers import _resolve
        args, identifier, raw = self.import_fixture('codex')
        with patch('csl_sync_http.Registry', return_value=self.api), redirect_stdout(io.StringIO()):
            self.assertEqual(import_same_id(args, None), [identifier])
        _, path = _resolve('codex', identifier, None)
        self.assertEqual(path.read_bytes(), raw)
        self.assertEqual(read_fence('codex', identifier)['state'], 'active')

    def test_source_command_proves_end_then_flushes_under_same_lock_before_release(self):
        from csl_sync import command
        settings = self.state.get('settings'); settings['enabled'] = True
        self.state.set('settings', settings)
        args = SimpleNamespace(sync_home=self.state.root, sync_action='release-writer', provider='codex',
            native_id='native', registry_session_id='ss_fixture', expected_snapshot_id='snapshot-fixture',
            expected_generation=0, expected_device_id=self.device)
        events = []
        def stopped(*unused): events.append('stopped')
        def flushed(*unused):
            with self.assertRaisesRegex(SyncError, 'collector_busy'):
                with native_lock('codex', 'native'): pass
            events.append('flushed')
        row = dict(id='ss_fixture',provider='codex',native_id='native',snapshot_id='snapshot-fixture',
                   writer=dict(state='legacy',generation=0,device_id=self.device),
                   sync=dict(complete=True),package=dict(available=True))
        def call(method, path, **kwargs):
            if method == 'POST':
                self.assertEqual(events, ['stopped', 'flushed'])
                events.append('released')
                row['writer']['state'] = 'released'
            return row
        self.api.call = call
        with patch('csl_sync.Tokens') as tokens, patch('csl_sync.Registry', return_value=self.api), \
             patch('csl_sync_writer.require_stopped', side_effect=stopped), \
             patch('csl_sync_providers._resolve', return_value=(self.root, self.root/'fixture')), \
             patch('csl_sync_providers._metadata', return_value={}), \
             patch('csl_sync.Collector._sync_session', side_effect=flushed), redirect_stdout(io.StringIO()):
            tokens.return_value.status.return_value = dict(signed_in=True)
            command(args, None)
        self.assertEqual(events, ['stopped', 'flushed', 'released'])
        self.assertEqual(read_fence('codex', 'native')['state'], 'released')

    def test_active_journal_with_publishing_fence_recovers_without_receipt_or_acquire(self):
        from csl_resume import import_same_id
        from csl_sync_writer import save_fence
        args, identifier, _ = self.import_fixture()
        def interrupted(provider, identifier, state, binding):
            if state == 'active': raise OSError('synthetic crash before active fence')
            return save_fence(provider, identifier, state, binding)
        with patch('csl_sync_http.Registry', return_value=self.api), \
             patch('csl_sync_writer.save_fence', side_effect=interrupted), redirect_stdout(io.StringIO()), \
             self.assertRaises(OSError):
            import_same_id(args, None)
        self.assertEqual(self.state.session('claude', identifier)['writer']['state'], 'active')
        self.assertEqual(read_fence('claude', identifier)['state'], 'publishing')
        with patch('csl_sync_http.Registry', return_value=self.api), \
             patch('csl_resume.require_stopped') as stopped, redirect_stdout(io.StringIO()):
            import_same_id(args, None)
        stopped.assert_not_called()
        self.assertEqual(sum(method == 'POST' for method,_,_ in self.calls), 1)
        self.assertEqual(read_fence('claude', identifier)['state'], 'active')

    def test_completed_release_allows_new_attested_acquisition_on_return(self):
        from csl_sync_writer import save_fence, profile_binding
        self.writer.acquire('codex', 'native', **self.binding)
        self.writer.published('codex', 'native')
        local = self.state.session('codex', 'native')
        local['writer']['state'] = 'released'
        self.state.save_session('codex', 'native', local)
        save_fence('codex', 'native', 'released', profile_binding(self.state))
        returned = dict(self.binding, generation=2, snapshot='later-snapshot', request_id=str(uuid.uuid4()))
        self.response = dict(state='active', device_id=self.device, generation=3)
        with patch.object(self.writer, 'attest', return_value='foreign-owner'), \
             self.assertRaisesRegex(SyncError, 'writer_fenced'):
            self.writer.acquire('codex', 'native', **returned)
        self.assertEqual(len(self.calls), 1)
        result = self.writer.acquire('codex', 'native', **returned)
        self.assertEqual(result['generation'], 3)
        self.assertEqual(result['request_id'], returned['request_id'])
        self.assertEqual(len(self.calls), 2)
        self.writer.acquire('codex', 'native', **returned)
        self.assertEqual(len(self.calls), 2)
        with self.assertRaisesRegex(SyncError, 'writer_request_conflict'):
            self.writer.acquire('codex', 'native', **dict(returned, request_id=str(uuid.uuid4())))

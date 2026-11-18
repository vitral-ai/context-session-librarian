"""Synthetic Gemini 0.68.0 projection and existing-uploader tests; no harness."""
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from csl_gemini_workload import WorkloadGeminiProvider, SOURCE_COMMIT
from csl_sync_engine import Collector, PROVIDERS
from csl_sync_http import Registry, HTTP
from csl_sync_state import State, SyncError
from csl_workload_stream import Bounds, Redactor, read_complete, verify_prefix
from sync_support import FakeTokens, Peer

SID = '11111111-2222-4333-8444-555555555555'
CANARY = 'sk-proj-' + 'Z'*32


def records():
    return [dict(sessionId=SID, projectHash='fixture-project', startTime='2026-11-18T01:00:00Z',
                 lastUpdated='2026-11-18T01:01:00Z', kind='main', summary='api_key='+CANARY),
            dict(id='u1', type='user', timestamp='2026-11-18T01:00:00Z', content='hello '+CANARY),
            dict(id='a1', type='gemini', timestamp='2026-11-18T01:01:00Z', content=[{'text':'answer'}],
                 model='fixture-model', toolCalls=[dict(id='t1', name='read_file', args={'password':CANARY},
                                                      result=[{'text':'tool '+CANARY}], status='success')])]


class GeminiWorkloadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='csl-workload-')
        self.root = Path(self.tmp.name)
        self.inputs = self.root/'inputs'; self.inputs.mkdir()
        self.path = self.inputs/'session.jsonl'
        self.rows = records()
        self.write(self.rows)
        self.provider = self.make()

    def tearDown(self):
        self.tmp.cleanup()

    def make(self, **kwargs):
        return WorkloadGeminiProvider(((self.inputs,'session.jsonl'),), self.root/'projection', **kwargs)

    def write(self, rows, tail=b''):
        self.path.write_bytes(b''.join(json.dumps(row,ensure_ascii=False).encode()+b'\n' for row in rows)+tail)

    def prepare(self):
        return self.provider.list_sessions('gemini')[0]

    def view(self, **kwargs):
        return self.provider.view_records('gemini', SID, **kwargs)

    def test_pin_redacted_metadata_view_spool_and_no_resume_package(self):
        self.assertEqual(SOURCE_COMMIT, '5a5c7180ff3f0a02511f54b54db7f228d4982a30')
        metadata = self.prepare(); page = self.view()
        self.assertEqual(metadata['byte_count'], self.provider.raw_files('gemini',SID)[0]['complete_length'])
        self.assertEqual([r['kind'] for r in page['records']], ['text','text','tool_call','tool_result'])
        self.assertIsNone(metadata['cwd']); self.assertIsNone(metadata['repository'])
        for file in (self.root/'projection').rglob('*'):
            if file.is_file():
                self.assertNotIn(CANARY.encode(), file.read_bytes())
                self.assertEqual(file.stat().st_mode & 0o777, 0o600)
        self.assertNotIn(CANARY, json.dumps(metadata)+json.dumps(page))
        self.assertFalse(self.provider.package_descriptor('gemini',SID)['available'])
        self.assertEqual(self.path.read_bytes().count(CANARY.encode()),4)
        cursor = json.loads(next((self.root/'projection/source-cursors').glob('*.json')).read_text())
        self.assertEqual(cursor['projection_version'],1)
        self.assertEqual(len(cursor['policy_sha256']),64)
        self.assertNotEqual(cursor['source_cursor']['sha256'], cursor['sanitized_sha256'])
        raw = Path(self.provider.raw_files('gemini',SID)[0]['path']).read_bytes()
        for row in page['records']:
            offset = row['raw_offset']['offset']
            self.assertTrue(offset==0 or raw[offset-1:offset]==b'\n')
            json.loads(raw[offset:].splitlines()[0])

    def test_partial_record_is_never_published_then_completes_once(self):
        self.prepare(); before = self.view()
        tail = json.dumps(dict(id='u2',type='user',content='new '+CANARY),ensure_ascii=False).encode()
        self.write(self.rows,tail[:len(tail)//2]); self.prepare()
        self.assertEqual(self.view()['records'],before['records'])
        self.assertTrue(self.provider.prepared[SID]['source_cursor']['partial'])
        self.write(self.rows,tail+b'\n'); self.prepare()
        page=self.view(position=before['position'])
        self.assertFalse(page['reset_required']); self.assertEqual(len(page['records']),1)
        self.assertNotIn(CANARY,json.dumps(page))

    def test_patch_rewinds_checkpoint_and_order_follow_pinned_semantics(self):
        rows = self.rows + [dict(id='u2',type='user',content='tail'),
            {'$patch':{'updates':[{'id':'a1','content':'changed','toolCalls':[{'id':'t1','result':'patched'}]}],
                       'orderIds':['a1','u1','u2']}}]
        self.write(rows); self.prepare(); page=self.view()
        self.assertEqual([r['text'] for r in page['records'] if r['kind']=='text'],['changed','hello [CSL REDACTED]','tail'])
        self.assertEqual([r['text'] for r in page['records'] if r['kind']=='tool_result'],['patched'])
        self.write(rows+[{'$rewindTo':'u1'}]); self.prepare()
        self.assertEqual([r['native_pointer'] for r in self.view()['records']],['/messages/a1']*3)
        self.write(rows+[{'$rewindTo':'missing'}]); self.prepare()
        self.assertEqual(self.view()['records'],[])
        self.write(self.rows+[{'$set':{'messages':[dict(id='n1',type='user',content='checkpoint')]}}])
        self.prepare(); self.assertEqual([r['text'] for r in self.view()['records']],['checkpoint'])

    def test_invalid_opaque_identity_and_unknown_control_refuse_before_projection(self):
        changes = [b'{bad}\n', b'{"id":"a","id":"b"}\n',
            json.dumps({'$unknown':{'x':1}}).encode()+b'\n',
            json.dumps(dict(id='opaque',type='gemini',content=[{'inlineData':{'data':'hidden'}}])).encode()+b'\n',
            json.dumps(dict(id='malformed',type={},content='not qualified')).encode()+b'\n',
            json.dumps({'messages':[dict(id='u2',type='user',content='unqualified')]}).encode()+b'\n',
            json.dumps(dict(id=CANARY,type='user',content='bad identity')).encode()+b'\n']
        for tail in changes:
            with self.subTest(tail=tail[:12]):
                self.write(self.rows,tail)
                with self.assertRaises(SyncError): self.prepare()
                self.assertEqual(list(self.provider.store.root.iterdir()),[])

    def test_source_record_output_structure_and_spool_limits_refuse(self):
        for bounds in [Bounds(source_bytes=8),Bounds(record_bytes=8),Bounds(output_bytes=8),
                       Bounds(records=1),Bounds(values=2),Bounds(depth=1),Bounds(spool_bytes=8)]:
            with self.subTest(bounds=bounds), self.assertRaises(SyncError):
                self.make(bounds=bounds).list_sessions('gemini')

    def test_source_size_limit_is_checked_before_any_payload_read(self):
        from csl_workload_stream import opened_source
        class Unreadable:
            def __init__(self): self.source = opened_source(self_inputs, 'session.jsonl')
            def __enter__(self): return self
            def __exit__(self, *args): self.source.close()
            def fileno(self): return self.source.fileno()
            def readline(self, *args): raise AssertionError('payload read before source bound')
        self_inputs = self.inputs
        with patch('csl_workload_stream.opened_source', return_value=Unreadable()):
            with self.assertRaisesRegex(SyncError, '^workload_source_limit$'):
                read_complete(self.inputs, 'session.jsonl', Bounds(source_bytes=8))

    def test_symlink_and_parent_escape_refuse_without_reading_other_files(self):
        other=self.root/'not-a-transcript';other.write_bytes(b'PRIVATE_NON_TRANSCRIPT')
        self.path.unlink();self.path.symlink_to(other)
        with self.assertRaises(SyncError):self.prepare()
        with self.assertRaises(SyncError): read_complete(self.inputs,'../not-a-transcript',Bounds())
        with self.assertRaises(SyncError): read_complete(self.inputs,'.',Bounds())
        self.path.unlink();self.path.write_bytes(b'')
        with self.assertRaises(SyncError):self.prepare()

    def test_spool_overlap_refuses_before_writing_source_and_symlinked_state_refuses(self):
        store = self.inputs/'state'
        with self.assertRaises(SyncError):
            WorkloadGeminiProvider(((self.inputs,'session.jsonl'),), store)
        self.assertFalse(store.exists())
        elsewhere = self.root/'elsewhere'; elsewhere.mkdir()
        link = self.root/'link'; link.symlink_to(elsewhere, target_is_directory=True)
        with self.assertRaises(SyncError):
            WorkloadGeminiProvider(((self.inputs,'session.jsonl'),), link/'state')
        self.assertFalse((elsewhere/'state').exists())

    def test_atomic_replacement_temporary_counts_toward_spool_limit(self):
        from csl_workload_stream import ProjectionStore
        store = ProjectionStore(self.root/'bounded-spool', Bounds(spool_bytes=10))
        original = store.publish('source', b'123456')
        with self.assertRaisesRegex(SyncError, '^workload_spool_limit$'):
            store.publish('source', b'654321')
        self.assertEqual(original.read_bytes(), b'123456')
        store.publish('source', b'12345678')
        self.assertEqual(original.read_bytes(), b'12345678')

    def test_append_during_capture_does_not_starve_verified_prefix(self):
        original = verify_prefix
        def growing(root, relative, cursor):
            with self.path.open('ab') as stream:
                stream.write(b'{"id":"u2","type":"user","content":"later"}\n')
            original(root, relative, cursor)
        with patch('csl_workload_provider.verify_prefix', side_effect=growing):
            self.prepare()
        self.assertEqual(len(self.view()['records']), 4)
        self.prepare()
        self.assertEqual(self.view()['records'][-1]['text'], 'later')
        _, cursor = read_complete(self.inputs, 'session.jsonl', Bounds())
        self.path.write_bytes(b'changed\n')
        with self.assertRaisesRegex(SyncError, '^source_changed$'):
            verify_prefix(self.inputs, 'session.jsonl', cursor)

    def test_duplicate_order_and_message_ids_keep_first_insertion_position(self):
        self.write(self.rows + [dict(id='u2', type='user', content='tail'),
            {'$patch': {'orderIds': ['a1', 'u1', 'a1']}},
            dict(id='u1', type='user', content='replacement')])
        self.prepare()
        self.assertEqual([r['native_pointer'] for r in self.view()['records']],
            ['/messages/u2', '/messages/a1', '/messages/a1', '/messages/a1', '/messages/u1'])

    def test_scalar_and_nested_credential_fields_are_removed(self):
        cleaned = Redactor(Bounds()).clean({'password': 123456, 'client_secret': {'unknown': 'value'},
            'args': {'access_token': ['private'], 'type': {}, 'text': CANARY}})
        self.assertEqual(cleaned, {'password': '[CSL REDACTED]', 'client_secret': '[CSL REDACTED]',
            'args': {'access_token': '[CSL REDACTED]', 'type': {}, 'text': '[CSL REDACTED]'}})

    def test_invalid_view_positions_fail_with_fixed_code(self):
        self.prepare()
        for value in ([], 'private', {'next_seq': True}, {'next_seq': 0}, {'next_seq': 1}):
            with self.subTest(value=value), self.assertRaisesRegex(SyncError, '^invalid_view_position$'):
                self.view(position=value)

    def test_incomplete_utf8_is_held_but_complete_invalid_utf8_refuses(self):
        self.write(self.rows, b'\xc3')
        self.prepare()
        self.assertTrue(self.provider.prepared[SID]['source_cursor']['partial'])
        self.write(self.rows, b'\xc3\n')
        with self.assertRaises(SyncError): self.prepare()

    def test_invalid_projection_never_uploads_source_content(self):
        peer = Peer(); state = State(self.root/'invalid-state')
        try:
            self.write(self.rows, b'{"id":"bad","type":"user","content":[{"inlineData":{}}]}\n')
            collector = Collector(state, Registry(peer.url, FakeTokens()), self.provider)
            outcome = collector.run(providers=['gemini'], days=None)
            self.assertEqual(outcome['synced'], 0)
            self.assertEqual(outcome['pending'], 1)
            self.assertEqual(outcome['sessions'][0]['error'], 'workload_opaque_or_key_refused')
            self.assertEqual(peer.sessions, {})
            self.assertEqual(peer.files, {})
            self.assertEqual(peer.views, {})
        finally:
            peer.close(); state.close()

    def test_existing_uploader_resumes_sanitized_bytes_and_resets_view_on_patch(self):
        self.peer=Peer();self.state=State(self.root/'state')
        try:
            collector=Collector(self.state,Registry(self.peer.url,FakeTokens(),http=HTTP(sleep=lambda _:None)),
                                self.provider,chunk_size=17)
            self.peer.drop_once=True;self.peer.drop_view_once=True
            self.assertEqual(collector.run(providers=['gemini'],days=None)['synced'],1)
            prior=self.state.session('gemini',SID)
            self.write(self.rows+[{'$patch':{'id':'a1','content':'changed '+CANARY}}])
            self.assertEqual(collector.run(providers=['gemini'],days=None)['synced'],1)
            current=self.state.session('gemini',SID)
            self.assertEqual(prior['files']['main']['revision'],current['files']['main']['revision'])
            self.assertNotEqual(prior['view']['generation'],current['view']['generation'])
            self.assertEqual(collector.run(providers=['gemini'],days=None)['synced'],1)
            for value in self.peer.files.values(): self.assertNotIn(CANARY.encode(),bytes(value))
            self.assertNotIn(CANARY,json.dumps(self.peer.sessions)+json.dumps(list(self.peer.views.values())))
            self.assertFalse(next(iter(self.peer.sessions.values()))['package']['available'])
            self.assertEqual(PROVIDERS,('codex','claude','antigravity'))
        finally:
            self.peer.close();self.state.close()

    def test_policy_changes_keep_identical_bytes_but_rewrite_changes_revision(self):
        self.prepare(); first=self.provider.raw_files('gemini',SID)[0]
        inode=Path(first['path']).stat().st_ino
        changed=self.make(extra_literals=('not-present',));changed.list_sessions('gemini')
        self.assertEqual(Path(first['path']).stat().st_ino,inode)
        self.assertEqual(changed.raw_files('gemini',SID)[0]['sha256'],first['sha256'])
        self.rows[1]['content']='history replaced';self.write(self.rows)
        self.prepare()
        self.assertNotEqual(Path(first['path']).stat().st_ino,inode)

    def test_restart_and_policy_replacement_create_coherent_raw_and_view_generations(self):
        peer = Peer(); state = State(self.root/'restart-state')
        try:
            def run(provider):
                collector = Collector(state, Registry(peer.url, FakeTokens(), http=HTTP(sleep=lambda _:None)), provider)
                self.assertEqual(collector.run(providers=['gemini'], days=None)['synced'], 1)
                return copy.deepcopy(state.session('gemini', SID))
            peer.drop_metadata_once = peer.drop_once = peer.drop_view_once = True
            first = run(self.provider)
            state.close(); state = State(self.root/'restart-state')
            again = run(self.make())
            self.assertEqual(first['files'], again['files'])
            self.assertEqual(first['view'], again['view'])
            changed = run(self.make(extra_literals=('answer',)))
            self.assertNotEqual(first['files']['main']['revision'], changed['files']['main']['revision'])
            self.assertNotEqual(first['view']['generation'], changed['view']['generation'])
            descriptor = peer.sessions['sessions/gemini/'+SID]
            content = peer.files[('sessions/gemini/'+SID, 'main', changed['files']['main']['revision'])]
            self.assertNotIn(b'answer', content)
            self.assertFalse(descriptor['package']['available'])
        finally:
            peer.close(); state.close()

    def test_unrendered_byte_rewrites_pair_raw_and_view_after_restart(self):
        for kind in ('summary', 'thought', 'empty', 'policy', 'legacy-binding'):
            with self.subTest(kind=kind):
                rows = records()
                rows[0]['summary'] = 'before'
                rows[-1]['thoughts'] = [{'subject': 'fixture', 'description': 'aaaa'}]
                if kind == 'empty': rows = rows[:1]
                self.write(rows)
                peer = Peer(); state_root = self.root/('rewrite-state-'+kind)
                state = State(state_root)
                try:
                    def run(provider, attempts=3):
                        collector = Collector(state, Registry(peer.url, FakeTokens(),
                            http=HTTP(attempts=attempts, sleep=lambda _: None)), provider)
                        return collector.run(providers=['gemini'], days=None)
                    self.assertEqual(run(self.make())['synced'], 1)
                    first = copy.deepcopy(state.session('gemini', SID))
                    first_rows = copy.deepcopy(peer.views.get(
                        ('sessions/gemini/'+SID, first['view']['generation']), []))
                    if kind == 'legacy-binding':
                        first['view'].pop('source_revisions')
                        state.save_session('gemini', SID, first)
                    state.close(); state = State(state_root)
                    if kind in ('summary', 'empty', 'legacy-binding'):
                        rows[0]['summary'] = 'after!'
                    elif kind == 'thought':
                        rows[-1]['thoughts'][0]['description'] = 'bbbb'
                    self.write(rows)
                    policy = ('aaaa',) if kind == 'policy' else ()
                    # Local capture checkpoints are not delivery/lineage authority.
                    for checkpoint in (self.root/'projection/source-cursors').glob('*.json'):
                        checkpoint.unlink()
                    loss = {'thought': 'drop_once', 'policy': 'drop_view_once'}.get(kind, 'drop_metadata_once')
                    setattr(peer, loss, True)
                    self.assertEqual(run(self.make(extra_literals=policy), attempts=1)['pending'], 1)
                    planned = copy.deepcopy(state.session('gemini', SID))
                    self.assertNotEqual(first['files']['main']['revision'], planned['files']['main']['revision'])
                    self.assertNotEqual(first['view']['generation'], planned['view']['generation'])
                    snapshot_id = peer.sessions['sessions/gemini/'+SID]['snapshot_id']
                    state.close(); state = State(state_root)
                    self.assertEqual(run(self.make(extra_literals=policy))['synced'], 1)
                    current = state.session('gemini', SID)
                    self.assertEqual(planned['view']['generation'], current['view']['generation'])
                    self.assertEqual(snapshot_id, peer.sessions['sessions/gemini/'+SID]['snapshot_id'])
                    self.assertEqual(first_rows, peer.views.get(
                        ('sessions/gemini/'+SID, current['view']['generation']), []))
                    self.assertEqual(current['view']['source_revisions'],
                        {'main': current['files']['main']['revision']})
                    if kind == 'policy':
                        self.assertEqual(run(self.make(extra_literals=('aaaa', 'absent')))['synced'], 1)
                        identical = state.session('gemini', SID)
                        self.assertEqual(current['files']['main']['revision'], identical['files']['main']['revision'])
                        self.assertEqual(current['view']['generation'], identical['view']['generation'])
                finally:
                    peer.close(); state.close()

    def test_bare_metadata_merges_but_set_messages_replaces(self):
        bare = {'sessionId': SID, 'projectHash': 'fixture-project', 'messages': [
            dict(id='u2', type='user', content='later'),
            dict(id='u1', type='user', content='replacement')]}
        self.write(self.rows + [bare, {**bare, 'messages': []}])
        self.prepare()
        view = self.view()['records']
        self.assertEqual([r['native_pointer'] for r in view],
            ['/messages/u1', '/messages/a1', '/messages/a1', '/messages/a1', '/messages/u2'])
        self.assertEqual(view[0]['text'], 'replacement')
        self.assertEqual(view[-1]['text'], 'later')
        self.write(self.rows + [bare, {'$set': {'messages': bare['messages']}},
            {'$set': {'summary': 'no reset'}},
            {**bare, 'messages': [dict(id='u3', type='user', content='merged')]}])
        self.prepare()
        self.assertEqual([r['text'] for r in self.view()['records']],
            ['later', 'replacement', 'merged'])

    def test_remote_revision_recovery_also_rebinds_view_generation(self):
        peer = Peer(); state = State(self.root/'remote-mismatch-state')
        try:
            def run():
                collector = Collector(state, Registry(peer.url, FakeTokens(),
                    http=HTTP(sleep=lambda _: None)), self.make())
                return collector.run(providers=['gemini'], days=None)
            self.assertEqual(run()['synced'], 1)
            before = copy.deepcopy(state.session('gemini', SID))
            peer.files[('sessions/gemini/'+SID, 'main', before['files']['main']['revision'])] = bytearray(b'wrong')
            self.assertEqual(run()['pending'], 1)
            self.assertEqual(run()['synced'], 1)
            after = state.session('gemini', SID)
            self.assertNotEqual(before['files']['main']['revision'], after['files']['main']['revision'])
            self.assertNotEqual(before['view']['generation'], after['view']['generation'])
            self.assertEqual(after['view']['source_revisions'], {'main': after['files']['main']['revision']})
        finally:
            peer.close(); state.close()


if __name__=='__main__': unittest.main()

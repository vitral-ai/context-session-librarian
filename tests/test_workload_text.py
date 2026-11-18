"""Synthetic pinned text profiles and the existing real loopback uploader."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from csl_sync_engine import Collector, PROVIDERS
from csl_sync_http import HTTP, Registry
from csl_sync_state import State, SyncError
from csl_text_workload import WorkloadTextProvider, PROFILE_MARKER, MARKER
from csl_workload_stream import Bounds, Redactor
from sync_support import Peer, FakeTokens

SID = '11111111-2222-4333-8444-555555555555'
CANARY = 'sk-proj-'+'Q'*32
OPAQUE = 'SYNTHETIC-OPAQUE-PRIVATE-REASONING'


def native(provider):
    if provider == 'codex':
        return [
            {'type':'session_meta','timestamp':'2026-11-18T01:00:00Z',
             'payload':{'id':SID,'cli_version':'0.169.0','cwd':CANARY,'instructions':CANARY}},
            {'type':'turn_context','payload':{'model':'fixture-model'}},
            {'type':'response_item','payload':{'type':'message','role':'user',
             'content':[{'type':'input_text','text':'hello '+CANARY}]}},
            {'type':'response_item','payload':{'type':'function_call','call_id':'call-1','name':'read',
             'arguments':json.dumps({'password':'tiny','count':1,'api_key':CANARY})}},
            {'type':'response_item','payload':{'type':'function_call_output','call_id':'call-1','output':'result '+CANARY}},
            {'type':'response_item','payload':{'type':'reasoning','encrypted_content':OPAQUE}},
            {'type':'response_item','payload':{'type':'message','role':'assistant',
             'content':[{'type':'output_text','text':'answer'}]}},
        ]
    return [
        {'type':'user','version':'2.1.308','sessionId':SID,'uuid':'u1','parentUuid':None,
         'timestamp':'2026-11-18T01:00:00Z','cwd':CANARY,'message':{'role':'user','content':'hello '+CANARY}},
        {'type':'assistant','version':'2.1.308','sessionId':SID,'uuid':'a1','parentUuid':'u1',
         'message':{'role':'assistant','model':'fixture-model','content':[
             {'type':'text','text':'answer'}, {'type':'tool_use','id':'call-1','name':'Read',
              'input':{'password':'tiny','count':1,'api_key':CANARY}},
             {'type':'thinking','thinking':OPAQUE,'signature':OPAQUE}]}},
        {'type':'user','version':'2.1.308','sessionId':SID,'uuid':'u2','parentUuid':'a1',
         'message':{'role':'user','content':[{'type':'tool_result','tool_use_id':'call-1','content':'result '+CANARY}]}}
    ]


class WorkloadTextTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='workload-text-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def provider(self, provider, records=None, **options):
        source = self.root/provider/'input'; source.mkdir(parents=True, exist_ok=True)
        path = source/'main.jsonl'
        if records is not None:
            path.write_bytes(b''.join(json.dumps(row).encode()+b'\n' for row in records))
        return WorkloadTextProvider(provider, ((source,'main.jsonl'),), self.root/provider/'projection', **options), path

    def test_both_profiles_sanitize_all_artifacts_and_explicitly_omit_reasoning(self):
        for provider in ('codex','claude'):
            with self.subTest(provider=provider):
                adapter, path = self.provider(provider, native(provider))
                original = path.read_bytes()
                with patch('csl_sync_providers._repository', side_effect=AssertionError('no Git discovery')):
                    metadata = adapter.list_sessions(provider)[0]
                view = adapter.view_records(provider, SID)
                descriptor = adapter.raw_files(provider,SID)[0]
                projected = Path(descriptor['path']).read_bytes()
                self.assertIsNone(metadata['cwd']); self.assertIsNone(metadata['repository'])
                self.assertEqual(metadata['native_id'],SID)
                self.assertEqual(metadata['model'],'fixture-model')
                self.assertEqual(metadata['byte_count'],len(projected))
                self.assertIn('tool_call',[row['kind'] for row in view['records']])
                self.assertIn('tool_result',[row['kind'] for row in view['records']])
                self.assertTrue(any(row['kind']=='notice' and 'reasoning omitted' in row['text'] for row in view['records']))
                for artifact in (projected, json.dumps(metadata).encode(), json.dumps(view).encode()):
                    for secret in (CANARY.encode(),OPAQUE.encode(),b'tiny'):
                        self.assertNotIn(secret,artifact)
                for artifact in (self.root/provider/'projection').rglob('*'):
                    if artifact.is_file():
                        self.assertNotIn(CANARY.encode(),artifact.read_bytes())
                        self.assertNotIn(OPAQUE.encode(),artifact.read_bytes())
                for row in view['records']:
                    offset = row['raw_offset']['offset']
                    self.assertTrue(offset==0 or projected[offset-1:offset]==b'\n')
                    self.assertEqual(json.loads(projected[offset:].splitlines()[0])[PROFILE_MARKER],adapter.profile)
                self.assertEqual(view['source_file']['sha256'],hashlib.sha256(projected).hexdigest())
                self.assertFalse(adapter.package_descriptor(provider,SID)['available'])
                self.assertEqual(path.read_bytes(),original)

    def test_complete_record_and_utf8_tail_are_held_until_newline(self):
        for provider in ('codex','claude'):
            adapter,path = self.provider(provider,native(provider)); adapter.list_sessions(provider)
            before = adapter.raw_files(provider,SID)[0]['sha256']
            if provider=='codex':
                extra={'type':'response_item','payload':{'type':'message','role':'user','content':'tail \u00e9 '+CANARY}}
            else:
                extra={'type':'user','version':'2.1.308','sessionId':SID,'uuid':'u3','parentUuid':'u2',
                       'message':{'role':'user','content':'tail \u00e9 '+CANARY}}
            tail=json.dumps(extra,ensure_ascii=False).encode()
            original=path.read_bytes();cut=tail.index(b'\xc3')+1
            path.write_bytes(original+tail[:cut]);adapter.list_sessions(provider)
            self.assertEqual(adapter.raw_files(provider,SID)[0]['sha256'],before)
            self.assertTrue(adapter.prepared[SID]['source_cursor']['partial'])
            path.write_bytes(original+tail+b'\n');adapter.list_sessions(provider)
            self.assertEqual(sum(row['text'].startswith('tail \u00e9') for row in adapter.view_records(provider,SID)['records']),1)

    def test_unknown_media_version_controls_and_reserved_projection_input_refuse(self):
        for provider in ('codex','claude'):
            rows=native(provider)
            bad=copy.deepcopy(rows)
            if provider=='codex':bad[2]['payload']['content']=[{'type':'input_image','image_url':CANARY}]
            else:bad[0]['message']['content']=[{'type':'image','source':{'data':CANARY}}]
            variants=[bad,rows+[{'type':'unknown','payload':{}}]]
            reserved=copy.deepcopy(rows);reserved[0][MARKER]='fake';variants.append(reserved)
            version=copy.deepcopy(rows)
            if provider=='codex':version[0]['payload']['cli_version']='unknown'
            else:version[0]['version']='unknown'
            variants.append(version)
            missing=copy.deepcopy(rows)
            if provider=='codex':del missing[0]['payload']['cli_version']
            else:del missing[0]['version']
            variants.append(missing)
            for index,variant in enumerate(variants):
                with self.subTest(provider=provider,variant=index):
                    adapter,_=self.provider(provider,variant)
                    with self.assertRaises(SyncError):adapter.list_sessions(provider)
                    self.assertEqual(list(adapter.store.root.iterdir()),[])

    def test_external_codex_history_and_ambiguous_claude_branches_refuse(self):
        codex=native('codex');codex[0]['payload']['history_base']={'path':'../../not-read'}
        adapter,_=self.provider('codex',codex)
        with self.assertRaisesRegex(SyncError,'^workload_external_history_refused$'):adapter.list_sessions('codex')
        for shape in ('sidechain','fork','duplicate','compact','external'):
            rows=native('claude')
            if shape=='sidechain':rows[1]['isSidechain']=True
            elif shape=='fork':rows[2]['parentUuid']='u1'
            elif shape=='duplicate':rows[2]['uuid']='u1'
            elif shape=='compact':rows[2]['isCompactSummary']=True
            else:rows[0]['parentUuid']='missing'
            adapter,_=self.provider('claude',rows)
            with self.subTest(shape=shape),self.assertRaises(SyncError):adapter.list_sessions('claude')

    def test_unqualified_timeline_controls_refuse_before_publication(self):
        for provider in ('codex','claude'):
            rows=native(provider)
            if provider=='codex':rows.append({'type':'event_msg','payload':{'type':'thread_rolled_back'}})
            else:rows.append({'type':'system','subtype':'compact_boundary','uuid':'boundary','parentUuid':None})
            adapter,_=self.provider(provider,rows)
            with self.assertRaisesRegex(SyncError,'^workload_text_format_refused$'):
                adapter.list_sessions(provider)
            self.assertEqual(list(adapter.store.root.iterdir()),[])

    def test_fresh_paginated_codex_requires_exact_ordinals_and_one_header(self):
        rows=native('codex');rows[0]['payload']['history_mode']='paginated'
        for ordinal,row in enumerate(rows):row['ordinal']=ordinal
        adapter,_=self.provider('codex',rows);adapter.list_sessions('codex')
        self.assertTrue(adapter.view_records('codex',SID)['records'])
        for index,value in ((0,False),(0,1),(3,9),(3,None)):
            broken=copy.deepcopy(rows);broken[index]['ordinal']=value
            adapter,_=self.provider('codex',broken)
            with self.subTest(index=index,value=value),self.assertRaisesRegex(SyncError,'^workload_codex_ordinal_refused$'):
                adapter.list_sessions('codex')
        broken=rows+[copy.deepcopy(rows[0])];adapter,_=self.provider('codex',broken)
        with self.assertRaisesRegex(SyncError,'^workload_text_format_refused$'):adapter.list_sessions('codex')

    def test_claude_parent_can_pass_through_non_message_metadata(self):
        rows=native('claude')
        rows.insert(2,{'type':'system','uuid':'sys1','parentUuid':'a1','sessionId':SID})
        rows[-1]['parentUuid']='sys1'
        adapter,_=self.provider('claude',rows);adapter.list_sessions('claude')
        self.assertEqual(sum(row['kind']=='tool_result' for row in adapter.view_records('claude',SID)['records']),1)

    def test_unselected_raw_structure_is_bounded_before_projection(self):
        for provider in ('codex','claude'):
            rows=native(provider);rows[0]['discarded_metadata']=[None]*200
            adapter,_=self.provider(provider,rows,bounds=Bounds(values=180))
            with self.assertRaisesRegex(SyncError,'^workload_structure_limit$'):adapter.list_sessions(provider)

    def test_omitted_reasoning_and_metadata_rewrite_keeps_identical_projected_revision(self):
        for provider in ('codex','claude'):
            rows=native(provider);adapter,path=self.provider(provider,rows);adapter.list_sessions(provider)
            before=adapter.raw_files(provider,SID)[0];before_view=adapter.view_records(provider,SID)['records']
            inode=Path(before['path']).stat().st_ino
            if provider=='codex':rows[-2]['payload']['encrypted_content']='changed-private-state'
            else:rows[1]['message']['content'][-1]['signature']='changed-private-state'
            rows[0]['cwd']='unselected-metadata-changed'
            self.provider(provider,rows);adapter.list_sessions(provider)
            after=adapter.raw_files(provider,SID)[0]
            self.assertEqual(before['sha256'],after['sha256'])
            self.assertEqual(Path(after['path']).stat().st_ino,inode)
            self.assertEqual(adapter.view_records(provider,SID)['records'],before_view)

    def test_native_identifiers_and_json_arguments_fail_closed(self):
        for provider in ('codex','claude'):
            rows=native(provider)
            if provider=='codex':rows[3]['payload']['call_id']=CANARY
            else:rows[1]['uuid']=CANARY
            adapter,_=self.provider(provider,rows)
            with self.assertRaises(SyncError):adapter.list_sessions(provider)
            rows=native(provider)
            if provider=='codex':rows[0]['payload']['id']='..'
            else:
                for row in rows:row['sessionId']='..'
            adapter,_=self.provider(provider,rows)
            with self.assertRaises(SyncError):adapter.list_sessions(provider)
        for arguments in ('{bad}', '{"password":"one","password":"two"}', '[]', '{"n":1e999}', '{"n":"\\ud800"}'):
            rows=native('codex');rows[3]['payload']['arguments']=arguments
            adapter,_=self.provider('codex',rows)
            with self.subTest(arguments=arguments),self.assertRaises(SyncError):
                adapter.list_sessions('codex')
        for provider in ('codex','claude'):
            rows=native(provider);rows[0]['discarded_metadata']='\ud800'
            adapter,_=self.provider(provider,rows)
            with self.assertRaisesRegex(SyncError,'^workload_record_refused$'):
                adapter.list_sessions(provider)

    def test_imported_or_forked_sources_do_not_become_native(self):
        for provider in ('codex','claude'):
            rows=native(provider);rows[0]['csl_source_session_id']='source-id'
            adapter,_=self.provider(provider,rows)
            with self.assertRaisesRegex(SyncError,'^workload_text_provenance_refused$'):
                adapter.list_sessions(provider)

    def assert_no_upload(self, adapter):
        prior_spool = {path.name: path.read_bytes() for path in adapter.store.root.iterdir()}
        peer = Peer()
        state = State(self.root/'refusal-state')
        try:
            result = Collector(state, Registry(peer.url, FakeTokens(), http=HTTP(attempts=1)), adapter).run(
                providers=['codex'], days=None)
            self.assertEqual(result['synced'], 0)
            self.assertEqual(result['pending'], 1)
            self.assertEqual(peer.sessions, {})
            self.assertEqual(peer.files, {})
            self.assertEqual(peer.views, {})
            self.assertEqual({path.name: path.read_bytes() for path in adapter.store.root.iterdir()}, prior_spool)
        finally:
            peer.close(); state.close()

    def test_codex_main_provenance_is_checked_before_selection_and_upload(self):
        variants = [
            {'attached_thread': True}, {'parent_thread_id': SID},
            {'source': {'subagent': {'thread_spawn': {'parent_thread_id': SID, 'depth': 1}}}},
            {'source': {'subagent': 'review'}}, {'source': {'subagent': 'compact'}},
            {'source': {'internal': 'guardian'}}, {'source': {'custom': 'import'}},
            {'source': 'unknown'}, {'forked_from_id': SID},
            {'forked_from_ordinal_exclusive': 0}, {'csl_conversion': {}},
            {'csl_source_provider': 'claude'}, {'csl_source_session_id': SID}, {'forkedFrom': SID},
        ]
        for paginated in (False, True):
            for variant in variants:
                for location in ('payload', 'record'):
                    with self.subTest(paginated=paginated, variant=variant, location=location):
                        rows = native('codex')
                        if paginated:
                            rows[0]['payload']['history_mode'] = 'paginated'
                            for ordinal, row in enumerate(rows): row['ordinal'] = ordinal
                        target = rows[0]['payload'] if location == 'payload' else rows[0]
                        target.update(variant)
                        adapter, _ = self.provider('codex', rows)
                        self.assert_no_upload(adapter)
        for source in (None, 'cli', 'vscode', 'exec'):
            rows = native('codex')
            if source is not None: rows[0]['payload']['source'] = source
            rows[0]['payload']['attached_thread'] = False
            adapter, _ = self.provider('codex', rows)
            self.assertEqual(adapter.list_sessions('codex')[0]['native_id'], SID)

    def test_decoded_argument_descendants_are_validated_before_redaction(self):
        invalid = ('1e999', '"\\ud800"', '['*40+'0'+']'*40,
                   '['+','.join(['0']*300)+']')
        for key in ('password', 'ordinary', 'pass\\u0077ord'):
            for value in invalid:
                for nested in (False, True):
                    arguments = '{"'+key+'":'+value+'}'
                    if nested: arguments = '{"outer":'+arguments+'}'
                    with self.subTest(key=key, value=value[:30], nested=nested):
                        rows = native('codex'); rows[3]['payload']['arguments'] = arguments
                        adapter, _ = self.provider('codex', rows, bounds=Bounds(values=250))
                        self.assert_no_upload(adapter)

    def test_argument_budget_is_cumulative_across_calls_and_captured_source(self):
        for key in ('password', 'ordinary'):
            rows = [native('codex')[0], {'type': 'response_item', 'payload': {
                'type': 'function_call', 'call_id': 'call-1', 'name': 'read',
                'arguments': json.dumps({key: [None]*140})}}]
            adapter, _ = self.provider('codex', rows, bounds=Bounds(values=250))
            self.assertEqual(adapter.list_sessions('codex')[0]['native_id'], SID)
            another = copy.deepcopy(rows[-1]); another['payload']['call_id'] = 'call-2'
            adapter, _ = self.provider('codex', rows+[another], bounds=Bounds(values=250))
            with self.subTest(key=key): self.assert_no_upload(adapter)
            rows[0]['discarded_metadata'] = [None]*140
            adapter, _ = self.provider('codex', rows, bounds=Bounds(values=250))
            with self.subTest(key=key, discarded=True): self.assert_no_upload(adapter)

    def test_redactor_validates_masked_subtrees_even_when_called_directly(self):
        deep = 0
        for _ in range(40): deep = [deep]
        for value in (float('inf'), '\ud800', deep, [None]*300, {'\ud800': 0}):
            with self.subTest(value_type=type(value).__name__), self.assertRaises(SyncError):
                Redactor(Bounds(values=250)).clean({'password': value})

    def test_view_cursor_and_policy_change_do_not_use_native_offsets(self):
        for provider in ('codex','claude'):
            adapter,path=self.provider(provider,native(provider));adapter.list_sessions(provider)
            first=adapter.view_records(provider,SID,limit=1)
            second=adapter.view_records(provider,SID,position=first['position'])
            self.assertEqual(second['records'][0]['seq'],2)
            old=adapter.raw_files(provider,SID)[0];old_inode=Path(old['path']).stat().st_ino
            changed,_=self.provider(provider,extra_literals=('absent-from-source',));changed.list_sessions(provider)
            self.assertEqual(Path(old['path']).stat().st_ino,old_inode)
            self.assertEqual(changed.raw_files(provider,SID)[0]['sha256'],old['sha256'])
            changed,_=self.provider(provider,extra_literals=('hello',));changed.list_sessions(provider)
            self.assertTrue(changed.view_records(provider,SID,position=first['position'])['reset_required'])
            self.assertNotEqual(Path(old['path']).stat().st_ino,old_inode)

    def test_uploader_append_rewrite_loss_and_restart_keep_sanitized_lineage(self):
        for provider in ('codex','claude'):
            with self.subTest(provider=provider):
                rows=native(provider);adapter,path=self.provider(provider,rows)
                peer=Peer();state_root=self.root/provider/'state';state=State(state_root)
                try:
                    def run():
                        adapter,_=self.provider(provider)
                        return Collector(state,Registry(peer.url,FakeTokens(),http=HTTP(attempts=1)),adapter,chunk_size=23).run(
                            providers=[provider],days=None)
                    peer.drop_once=True
                    self.assertEqual(run()['pending'],1)
                    state.close();state=State(state_root)
                    self.assertEqual(run()['synced'],1)
                    first=copy.deepcopy(state.session(provider,SID))
                    if provider=='codex':
                        rows.append({'type':'event_msg','payload':{'type':'thread_name_updated','thread_name':'new title'}})
                    else:rows.append({'type':'custom-title','sessionId':SID,'customTitle':'new title'})
                    self.provider(provider,rows)
                    self.assertEqual(run()['synced'],1)
                    appended=copy.deepcopy(state.session(provider,SID))
                    self.assertEqual(first['files']['main']['revision'],appended['files']['main']['revision'])
                    self.assertEqual(first['view']['generation'],appended['view']['generation'])
                    if provider=='codex':rows[-1]['payload']['thread_name']='old title'
                    else:rows[-1]['customTitle']='old title'
                    self.provider(provider,rows);peer.drop_metadata_once=True
                    self.assertEqual(run()['pending'],1)
                    pending=copy.deepcopy(state.session(provider,SID))
                    self.assertNotEqual(appended['files']['main']['revision'],pending['files']['main']['revision'])
                    self.assertNotEqual(appended['view']['generation'],pending['view']['generation'])
                    state.close();state=State(state_root)
                    self.assertEqual(run()['synced'],1)
                    self.assertEqual(pending['view']['generation'],state.session(provider,SID)['view']['generation'])
                    for data in peer.files.values():
                        self.assertNotIn(CANARY.encode(),data);self.assertNotIn(OPAQUE.encode(),data)
                    self.assertEqual(PROVIDERS,('codex','claude','antigravity'))
                finally:peer.close();state.close()


if __name__=='__main__':unittest.main()

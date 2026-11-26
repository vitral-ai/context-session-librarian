import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import subprocess
import shutil
import unittest
import uuid
from unittest.mock import patch
import zipfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import csl_sync_providers as p
import csl_provider_bundle as bundle


class ProviderTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.home=self.root/'h';self.sid=str(uuid.uuid4());self.cwd=self.root/'project';self.cwd.mkdir()
        self.path=self.home/'projects'/'project'/f'{self.sid}.jsonl'
        self.rows=[{'type':'user','sessionId':self.sid,'cwd':str(self.cwd),'uuid':str(uuid.uuid4()),'parentUuid':None,'timestamp':'2026-01-01T00:00:00Z','message':{'role':'user','content':'Decide A'}},
                   {'type':'assistant','sessionId':self.sid,'uuid':str(uuid.uuid4()),'timestamp':'2026-01-02T00:00:00Z','message':{'role':'assistant','model':'synthetic-model','content':[{'type':'text','text':'Use B'},{'type':'tool_use','id':'call-a','name':'Read','input':{'path':'fake'} }]}},
                   {'type':'user','sessionId':self.sid,'uuid':str(uuid.uuid4()),'timestamp':'2026-01-03T00:00:00Z','message':{'role':'user','content':[{'type':'tool_result','tool_use_id':'call-a','content':'EVIDENCE'}]}}]
        self.write(self.path,self.rows)

    def write(self,path,rows):
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(b''.join((json.dumps(r,ensure_ascii=False)+'\n').encode() for r in rows))

    def test_claude_full_metadata_and_own_session_not_subagent(self):
        child=self.path.with_suffix('')/'subagents/agent-one.jsonl';self.write(child,self.rows)
        meta=p.list_sessions('claude',home=self.home)
        self.assertEqual(len(meta),1);r=meta[0]
        self.assertEqual(r['native_id'],self.sid);self.assertEqual(r['record_count'],3)
        self.assertEqual(r['model'],'synthetic-model');self.assertEqual(r['updated_at'],'2026-01-03T00:00:00+00:00')
        self.assertEqual(r['cwd'],str(self.cwd));self.assertEqual(r['state'],'active')
        self.assertEqual({f['role'] for f in p.raw_files('claude',self.sid,home=self.home)},{'transcript','subagent'})

    def test_title_uses_first_real_user_after_bootstrap_and_tool_results(self):
        bootstrap = {**self.rows[0], 'isMeta': True, 'message': {'role': 'user', 'content': 'bootstrap'}}
        instructions = {**self.rows[0], 'message': {'role': 'user', 'content': '# AGENTS.md instructions\nsetup'}}
        self.write(self.path, [bootstrap, instructions, self.rows[2], self.rows[0]])
        row = p.list_sessions('claude', home=self.home)[0]
        self.assertEqual(row['title'], 'Decide A')

    def test_partial_tail_never_in_hash_or_view(self):
        original=self.path.read_bytes()
        with self.path.open('ab') as f:f.write(b'{"type":"assistant"')
        main=p.raw_files('claude',self.sid,home=self.home)[0]
        self.assertEqual(main['complete_length'],len(original));self.assertEqual(main['sha256'],hashlib.sha256(original).hexdigest())
        self.assertEqual(main['size'],len(original)+19)
        self.assertEqual(len(p.view_records('claude',self.sid,home=self.home)['records']),4)

    def test_pagination_append_and_rewrite_reset(self):
        first=p.view_records('claude',self.sid,home=self.home,limit=2)
        self.assertEqual([r['seq'] for r in first['records']],[1,2]);self.assertFalse(first['complete'])
        second=p.view_records('claude',self.sid,home=self.home,position=first['position'])
        self.assertEqual([r['seq'] for r in second['records']],[3,4]);self.assertTrue(second['complete'])
        self.assertEqual(second['records'][-1]['kind'],'tool_result');self.assertEqual(second['records'][-1]['call_id'],'call-a')
        extra={**self.rows[0],'uuid':str(uuid.uuid4()),'message':{'role':'user','content':'Next'}}
        with self.path.open('ab') as f:f.write((json.dumps(extra)+'\n').encode())
        third=p.view_records('claude',self.sid,home=self.home,position=second['position'])
        self.assertEqual([r['seq'] for r in third['records']],[5])
        self.rows[0]['message']['content']='Changed';self.write(self.path,self.rows)
        reset=p.view_records('claude',self.sid,home=self.home,position=second['position'])
        self.assertTrue(reset['reset_required']);self.assertFalse(reset['records'])
        self.assertFalse(reset['complete']);self.assertEqual(reset['position']['next_seq'],1)
        restarted=p.view_records('claude',self.sid,home=self.home,position=reset['position'])
        self.assertEqual(restarted['records'][0]['text'],'Changed')

    def test_view_truncates_only_view_and_keeps_offsets(self):
        raw=self.path.read_bytes();result=p.view_records('claude',self.sid,home=self.home,text_limit=3)
        self.assertTrue(result['records'][0]['truncated']);self.assertEqual(result['records'][0]['text'],'Dec')
        self.assertEqual(set(result['records'][0]['raw_offset']),{'file_key','offset'})
        self.assertEqual(self.path.read_bytes(),raw)

    def test_symlink_companion_is_rejected_not_followed(self):
        child=self.path.with_suffix('')/'tool-results/out.txt';child.parent.mkdir(parents=True)
        external=self.root/'credential';external.write_text('DO-NOT-COLLECT');child.symlink_to(external)
        with self.assertRaisesRegex(p.ProviderError,'symlink_source'):p.raw_files('claude',self.sid,home=self.home)



    def test_codex_metadata_and_views(self):
        path=self.home/'sessions/2026/01/rollout-test-'/f'{self.sid}.jsonl'
        self.write(path,[{'type':'session_meta','timestamp':'2026-01-01T00:00:00Z','payload':{'id':self.sid,'cwd':str(self.cwd),'forked_from_id':'parent'}},{'type':'turn_context','payload':{'model':'test-model'}},{'type':'response_item','payload':{'type':'message','role':'user','content':[{'type':'input_text','text':'question'}]}},{'type':'response_item','payload':{'type':'function_call','call_id':'c1','name':'read','arguments':'{}'}},{'type':'response_item','payload':{'type':'function_call_output','call_id':'c1','output':'result'}}])
        row=p.list_sessions('codex',home=self.home)[0];self.assertEqual(row['native_id'],self.sid);self.assertEqual(row['parent']['native_id'],'parent')
        self.assertEqual([r['kind'] for r in p.view_records('codex',self.sid,home=self.home)['records']],['text','tool_call','tool_result'])

    def test_v3_claude_package_exact_raw_with_companions_and_import(self):
        companion=self.path.with_suffix('')
        self.write(companion/'subagents/agent-one.jsonl',self.rows)
        result=companion/'tool-results/one.txt';result.parent.mkdir(parents=True);result.write_text('bounded synthetic tool result')
        old_primary=self.path.read_bytes();out=self.root/'native.zip'
        bundle.export_native('claude',self.sid,out,home=self.home)
        with zipfile.ZipFile(out) as z:
            manifest=bundle.validate(z);entry=manifest['sessions'][0]
            self.assertEqual(z.read(entry['primary']),old_primary);self.assertEqual(len(entry['files']),3)
            target_home=self.root/'target'
            with patch.dict(os.environ,{'CLAUDE_CONFIG_DIR':str(target_home)}):rows=bundle.import_native(z,manifest,self.cwd)
        installed=Path(rows[0]['path']);self.assertNotEqual(rows[0]['id'],self.sid)
        self.assertTrue(installed.with_suffix('').joinpath('subagents/agent-one.jsonl').is_file())
        self.assertEqual(installed.with_suffix('').joinpath('tool-results/one.txt').read_text(),result.read_text())
        self.assertEqual(self.path.read_bytes(),old_primary)

    def test_descriptor_has_stable_file_keys_and_opaque_mapping(self):
        companion=self.path.with_suffix('')/'tool-results/one.txt';companion.parent.mkdir(parents=True);companion.write_text('one')
        a=p.raw_files('claude',self.sid,home=self.home);b=p.raw_files('claude',self.sid,home=self.home)
        self.assertEqual([f['file_key'] for f in a],[f['file_key'] for f in b])
        desc=p.package_descriptor('claude',self.sid,home=self.home)
        self.assertEqual(desc['manifest']['version'],3);self.assertEqual(len(desc['members']),2)
        self.assertTrue(all(m['member'].startswith('sessions/'+self.sid+'/') and '..' not in m['member'].split('/') for m in desc['members']))

    def test_invalid_record_errors_do_not_echo_text(self):
        self.path.write_text('{private-transcript-content}\n')
        try:p.list_sessions('claude',home=self.home)
        except p.ProviderError as e:self.assertNotIn('private-transcript-content',str(e))
        else:self.fail('invalid record accepted')

    def test_view_position_and_raw_snapshot_binding(self):
        with self.assertRaisesRegex(p.ProviderError,'invalid_view_position'):
            p.view_records('claude',self.sid,home=self.home,position=[])
        result=p.view_records('claude',self.sid,home=self.home)
        main=p.raw_files('claude',self.sid,home=self.home)[0]
        self.assertEqual(result['source_file'],{'file_key':'main','length':main['complete_length'],'sha256':main['sha256']})

    def test_package_unavailable_for_metadata_only_and_missing_codex_base(self):
        path=self.home/'sessions'/f'{self.sid}.jsonl'
        rows=[{'type':'session_meta','payload':{'id':self.sid,'cwd':str(self.cwd)}}]
        self.write(path,rows)
        self.assertEqual(p.package_descriptor('codex',self.sid,home=self.home)['reason'],'no_conversation')
        rows[0]['payload'].update(history_mode='paginated',history_base={'thread_id':str(uuid.uuid4()),'end_ordinal_exclusive':1,'end_byte_offset':100})
        rows[0]['ordinal']=1
        rows.append({'type':'response_item','ordinal':2,'payload':{'type':'message','role':'user','content':[{'type':'input_text','text':'visible only'}]}})
        path.unlink();path=self.home/'sessions'/f'rollout-2026-01-01T00-00-00-{self.sid}.jsonl'
        self.write(path,rows)
        self.assertEqual(p.package_descriptor('codex',self.sid,home=self.home)['reason'],'history_base_missing')
        with self.assertRaisesRegex(p.ProviderError,'history_base_missing'):p.raw_files('codex',self.sid,home=self.home)

    def codex_roundtrip(self):
        source=self.home/'sessions/2026/01/01'/f'rollout-2026-01-01T00-00-00-{self.sid}.jsonl'
        stamp='2026-01-01T00:00:00Z'
        self.write(source,[{'type':'session_meta','timestamp':stamp,'payload':{'id':self.sid,'timestamp':stamp,'cwd':str(self.cwd),'originator':'csl_test','source':'cli','cli_version':'0.169.0','model_provider':'openai','history_mode':'legacy'}},
                           {'type':'event_msg','timestamp':stamp,'payload':{'type':'task_started','turn_id':'22222222-2222-4222-8222-222222222222'}},
                           {'type':'response_item','timestamp':stamp,'payload':{'type':'message','role':'user','content':[{'type':'input_text','text':'NATIVE-RAW-USER'}]}},
                           {'type':'event_msg','timestamp':stamp,'payload':{'type':'item_completed','thread_id':self.sid,'turn_id':'22222222-2222-4222-8222-222222222222','item':{'type':'UserMessage','id':'u','client_id':None,'content':[{'type':'text','text':'NATIVE-RAW-USER','text_elements':[]}]}}},
                           {'type':'response_item','timestamp':stamp,'payload':{'type':'reasoning','encrypted_content':'synthetic-opaque'}},
                           {'type':'response_item','timestamp':stamp,'payload':{'type':'message','role':'assistant','content':[{'type':'output_text','text':'NATIVE-RAW-ANSWER'}]}},
                           {'type':'event_msg','timestamp':stamp,'payload':{'type':'item_completed','thread_id':self.sid,'turn_id':'22222222-2222-4222-8222-222222222222','item':{'type':'AgentMessage','id':'a','phase':'final_answer','content':[{'type':'Text','text':'NATIVE-RAW-ANSWER'}]}}},
                           {'type':'event_msg','timestamp':stamp,'payload':{'type':'task_complete','turn_id':'22222222-2222-4222-8222-222222222222','last_agent_message':'NATIVE-RAW-ANSWER'}}])
        original=source.read_bytes();out=self.root/'codex.zip'
        bundle.export_native('codex',self.sid,out,home=self.home)
        target=self.root/'codex-target'
        with zipfile.ZipFile(out) as z,patch.dict(os.environ,{'CODEX_HOME':str(target)}):
            manifest=bundle.validate(z);self.assertEqual(z.read(manifest['sessions'][0]['primary']),original)
            installed=bundle.import_native(z,manifest,self.cwd)[0]
        self.assertNotEqual(installed['id'],self.sid)
        self.assertIn(b'synthetic-opaque',Path(installed['path']).read_bytes())
        self.assertEqual(source.read_bytes(),original)
        return target,installed

    def test_codex_v3_raw_preserves_encrypted_bytes_and_origin(self):
        target,installed=self.codex_roundtrip()
        meta=p.list_sessions('codex',home=target)[0]
        self.assertEqual(meta['origin'],'imported');self.assertEqual(meta['parent']['native_id'],self.sid)

    @unittest.skipUnless(os.environ.get('CSL_RUN_CODEX_NATIVE_TESTS')=='1' and shutil.which('codex'),'Opt in to offline Codex native loading')
    def test_codex_v3_native_loader(self):
        from test_native_display import read_native_items
        target,installed=self.codex_roundtrip()
        env={'PATH':os.environ['PATH'],'HOME':str(self.root),'CODEX_HOME':str(target),'CLAUDE_CONFIG_DIR':str(self.root/'isolated-claude')}
        items=read_native_items(self,self.root,env,installed['id'])
        self.assertIn('NATIVE-RAW-USER',json.dumps(items));self.assertIn('NATIVE-RAW-ANSWER',json.dumps(items))




    def test_v3_tampering_and_traversal_rejected_before_install(self):
        out=self.root/'valid.zip';bundle.export_native('claude',self.sid,out,home=self.home)
        with zipfile.ZipFile(out) as z:members={n:z.read(n) for n in z.namelist()}
        for label,mutate in [('checksum',False),('traversal',True)]:
            manifest=json.loads(members['manifest.json']);entry=manifest['sessions'][0];main=entry['primary']
            if mutate:entry['files'][0]['relative_path']='../escape'
            body=members[main] if mutate else members[main]+b' '
            damaged=self.root/(label+'.zip')
            with zipfile.ZipFile(damaged,'w') as z:
                z.writestr(main,body);z.writestr('manifest.json',json.dumps(manifest))
            with zipfile.ZipFile(damaged) as z,self.assertRaises(ValueError):bundle.validate(z)

    def test_raw_scrub_request_cannot_silently_keep_secret(self):
        with patch('csl_secrets.SecretReview.resolve'),patch('csl_secrets.SecretReview.scrubbed',new_callable=lambda:property(lambda s:True),create=True):
            with self.assertRaisesRegex(ValueError,'cannot_be_scrubbed'):
                bundle.export_native('claude',self.sid,self.root/'raw.zip',home=self.home)
        self.assertFalse((self.root/'raw.zip').exists())

    def test_failed_publication_rolls_back_all_files(self):
        child=self.path.with_suffix('')/'tool-results/one.txt';child.parent.mkdir(parents=True);child.write_text('synthetic')
        out=self.root/'valid.zip';bundle.export_native('claude',self.sid,out,home=self.home)
        target=self.root/'target';real_link=os.link;calls=[]
        def fail_second(source,dest):
            calls.append(dest)
            if len(calls)==2:raise OSError('synthetic interruption')
            return real_link(source,dest)
        with zipfile.ZipFile(out) as z,patch.dict(os.environ,{'CLAUDE_CONFIG_DIR':str(target)}),patch('csl_provider_bundle.os.link',side_effect=fail_second):
            with self.assertRaises(OSError):bundle.import_native(z,bundle.validate(z),self.cwd)
        self.assertFalse(any(f.is_file() for f in target.rglob('*')))

    def test_provider_cli_claude_metadata_and_v3_inspection(self):
        env={**os.environ,'CLAUDE_CONFIG_DIR':str(self.home),'CODEX_HOME':str(self.root/'codex')}
        cli=Path(__file__).resolve().parents[1]/'csl'
        result=subprocess.run([sys.executable,str(cli),'sessions','--provider','claude','--json'],env=env,capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        rows=json.loads(result.stdout)['sessions'];self.assertEqual(rows[0]['model'],'synthetic-model')
        out=self.root/'valid.zip';bundle.export_native('claude',self.sid,out,home=self.home)
        result=subprocess.run([sys.executable,str(cli),'import',str(out),'--list'],env=env,capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr);self.assertIn(self.sid,result.stdout)
        original=self.path.read_bytes()
        result=subprocess.run([sys.executable,str(cli),'import',str(out),'--cwd',str(self.cwd)],env=env,capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr);self.assertIn('continuity:',result.stdout)
        self.assertIn('history is not execution authority',result.stdout);self.assertEqual(self.path.read_bytes(),original)


if __name__=='__main__':unittest.main()

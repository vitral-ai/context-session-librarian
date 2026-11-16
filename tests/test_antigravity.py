"""Antigravity native SQLite profile; generated fixtures contain no real history."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid
import zipfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import csl_antigravity as a
import csl_provider_bundle as bundle
import csl_sync_providers as providers


def pb(*rows):
    return a.encode([(n,2,v.encode() if isinstance(v,str) else v) if isinstance(v,(str,bytes)) else (n,0,v) for n,v in rows])


def make_session(root,sid,cwd):
    path=root/'conversations'/(sid+'.db');path.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(path)
    db.executescript('''
        CREATE TABLE trajectory_meta (trajectory_id TEXT PRIMARY KEY,cascade_id TEXT,trajectory_type INTEGER,source INTEGER);
        CREATE TABLE steps (idx INTEGER PRIMARY KEY,step_type INTEGER,status INTEGER,step_payload BLOB,step_format INTEGER,metadata BLOB);
        CREATE TABLE trajectory_metadata_blob (id TEXT PRIMARY KEY,data BLOB);
        CREATE TABLE executor_metadata (idx INTEGER PRIMARY KEY,data BLOB);
        CREATE TABLE parent_references (idx INTEGER PRIMARY KEY,data BLOB);
        CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY,data BLOB);
    ''')
    trajectory=str(uuid.uuid4());db.execute('INSERT INTO trajectory_meta VALUES (?,?,4,17)',(trajectory,sid))
    meta=pb((1,pb((1,Path(cwd).as_uri()))),(2,pb((1,1700000000))),(6,sid),(7,Path(cwd).as_uri()),(18,'source-project'))
    db.execute("INSERT INTO trajectory_metadata_blob VALUES ('main',?)",(meta,))
    step_meta=pb((1,pb((1,1700000000))),(20,pb((1,trajectory),(4,sid))))
    user=pb((1,14),(4,3),(5,step_meta),(19,pb((2,'Remember SYNTHETIC-4827'),(12,pb((1,'SOURCE-POLICY'))))))
    answer=pb((1,15),(4,3),(5,step_meta),(20,pb((1,'Use decision B'),(3,'opaque thinking preserved raw'))))
    for i,kind,body in [(0,14,user),(1,15,answer)]:
        db.execute('INSERT INTO steps VALUES (?,?,3,?,0,?)',(i,kind,body,step_meta))
    db.execute('INSERT INTO executor_metadata VALUES (0,?)',(pb((1,'source-authority')),))
    db.commit();db.close()
    return path


class AntigravityTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.home=self.root/'agy';self.cwd=self.root/'project';self.cwd.mkdir()
        self.sid=str(uuid.uuid4());self.path=make_session(self.home,self.sid,self.cwd)

    def test_metadata_view_and_no_index_or_credentials_required(self):
        before=self.path.read_bytes()
        row=providers.list_sessions('antigravity',home=self.home)[0]
        self.assertEqual(row['native_id'],self.sid);self.assertEqual(row['cwd'],str(self.cwd))
        self.assertEqual(row['record_count'],2);self.assertIsNone(row['model'])
        view=providers.view_records('antigravity',self.sid,home=self.home)
        self.assertEqual([r['role'] for r in view['records']],['user','assistant'])
        self.assertEqual(view['records'][0]['raw_offset'],{'file_key':'main','offset':0})
        self.assertEqual(view['records'][1]['native_pointer'],'/steps/1')
        self.assertEqual(view['source_files'],[view['source_file']]);self.assertEqual(self.path.read_bytes(),before)
        self.assertFalse(Path(str(self.path)+'-shm').exists())

    def test_wal_is_collected_and_read_without_checkpointing_origin(self):
        db=sqlite3.connect(self.path);self.addCleanup(db.close)
        db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA wal_autocheckpoint=0')
        body=pb((1,14),(4,3),(19,pb((2,'WAL-ONLY'))))
        db.execute('INSERT INTO steps VALUES (2,14,3,?,0,NULL)',(body,));db.commit()
        wal=Path(str(self.path)+'-wal');before={p.name:p.read_bytes() for p in (self.path,wal)}
        inventory=providers.raw_files('antigravity',self.sid,home=self.home)
        self.assertEqual([f['file_key'] for f in inventory],['main','sqlite-wal'])
        self.assertTrue(all(f['mode']=='rewrite' for f in inventory))
        result=providers.view_records('antigravity',self.sid,home=self.home)
        self.assertEqual(result['records'][-1]['text'],'WAL-ONLY');self.assertEqual(len(result['source_files']),2)
        self.assertEqual(before,{p.name:p.read_bytes() for p in (self.path,wal)})

    def test_wal_partial_tail_excluded_from_complete_length(self):
        db=sqlite3.connect(self.path);self.addCleanup(db.close)
        db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA wal_autocheckpoint=0')
        db.execute('UPDATE steps SET status=3');db.commit()
        # Force a WAL write even if SQLite optimized away the first update.
        db.execute('INSERT INTO gen_metadata VALUES (1,?)',(b'synthetic',));db.commit()
        wal=Path(str(self.path)+'-wal');good=wal.read_bytes()
        with wal.open('ab') as f:f.write(b'partial-frame')
        f=providers.raw_files('antigravity',self.sid,home=self.home)[1]
        self.assertEqual(f['complete_length'],len(good));self.assertEqual(f['size'],len(good)+13)
        self.assertEqual(f['sha256'],hashlib.sha256(good).hexdigest())

    def test_pagination_appended_steps_and_rewrite_reset(self):
        one=providers.view_records('antigravity',self.sid,home=self.home,limit=1)
        two=providers.view_records('antigravity',self.sid,home=self.home,position=one['position'])
        self.assertEqual(two['records'][0]['seq'],2)
        db=sqlite3.connect(self.path)
        db.execute('UPDATE steps SET step_payload=? WHERE idx=0',(pb((1,14),(19,pb((2,'changed')))),));db.commit();db.close()
        reset=providers.view_records('antigravity',self.sid,home=self.home,position=two['position'])
        self.assertTrue(reset['reset_required']);self.assertEqual(reset['position']['next_seq'],1)
        restarted=providers.view_records('antigravity',self.sid,home=self.home,position=reset['position'])
        self.assertEqual(restarted['records'][0]['text'],'changed')

    def test_view_file_tool_correlation_error_and_unknown_notice(self):
        call=pb((1,'call-one'),(2,'view_file'),(3,'{"path":"fixture.txt"}'))
        meta=pb((4,call))
        response=pb((1,15),(4,3),(20,pb((7,call))))
        tool=pb((1,8),(4,7),(5,meta),(14,pb((4,'TOOL-ONLY-EVIDENCE'))))
        db=sqlite3.connect(self.path)
        for i,kind,status,body in [(2,15,3,response),(3,8,7,tool),(4,999,3,pb((1,999)))]:
            db.execute('INSERT INTO steps VALUES (?,?,?,?,0,NULL)',(i,kind,status,body))
        db.commit();db.close()
        rows=providers.view_records('antigravity',self.sid,home=self.home)['records']
        self.assertEqual([r['kind'] for r in rows[2:]],['tool_call','tool_result','notice'])
        self.assertEqual(rows[3]['call_id'],'call-one');self.assertIn('native_step_status=error',rows[3]['text'])
        self.assertIn('TOOL-ONLY-EVIDENCE',rows[3]['text'])

    def test_raw_package_and_independent_import_remove_source_permission_configuration(self):
        before=self.path.read_bytes();out=self.root/'session.zip'
        bundle.export_native('antigravity',self.sid,out,home=self.home)
        target=self.root/'target'
        with zipfile.ZipFile(out) as z,patch.dict(os.environ,{'CSL_ANTIGRAVITY_HOME':str(target)}):
            manifest=bundle.validate(z);entry=manifest['sessions'][0]
            self.assertEqual(z.read(entry['primary']),before)
            imported=bundle.import_native(z,manifest,self.cwd)[0]
            another=bundle.import_native(z,manifest,self.cwd)[0]
        self.assertNotEqual(imported['id'],self.sid);self.assertNotEqual(imported['id'],another['id'])
        self.assertEqual(self.path.read_bytes(),before)
        with a.snapshot(target,imported['id']) as (db,_):
            row=a.metadata(db,imported['id']);self.assertEqual(row['origin'],'imported')
            self.assertEqual(row['parent']['native_id'],self.sid)
            self.assertEqual(db.execute('SELECT count(*) FROM executor_metadata').fetchone()[0],0)
            payload=db.execute('SELECT step_payload FROM steps WHERE idx=0').fetchone()[0]
            self.assertFalse(a.first(a.first(payload,19),12))
            self.assertEqual(db.execute('SELECT cascade_id FROM trajectory_meta').fetchone()[0],imported['id'])

    def test_run_command_negative_exit_code_keeps_signed_meaning(self):
        call=pb((1,'command-id'),(2,'run_command'),(3,'{"command_line":"synthetic"}'))
        body=pb((1,21),(4,7),(5,pb((4,call))),(28,pb((6,2**64-1),(21,pb((1,'error output'))))))
        db=sqlite3.connect(self.path);db.execute('INSERT INTO steps VALUES (2,21,7,?,0,NULL)',(body,));db.commit();db.close()
        last=providers.view_records('antigravity',self.sid,home=self.home)['records'][-1]
        self.assertIn('exit_code=-1',last['text']);self.assertIn('native_step_status=error',last['text'])

    def test_wal_package_restore_keeps_wal_only_messages(self):
        db=sqlite3.connect(self.path);self.addCleanup(db.close)
        db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA wal_autocheckpoint=0')
        db.execute('INSERT INTO steps VALUES (2,14,3,?,0,NULL)',(pb((1,14),(19,pb((2,'WAL-ONLY')))),));db.commit()
        out=self.root/'wal.zip';bundle.export_native('antigravity',self.sid,out,home=self.home)
        target=self.root/'target'
        with zipfile.ZipFile(out) as z,patch.dict(os.environ,{'CSL_ANTIGRAVITY_HOME':str(target)}):
            manifest=bundle.validate(z);self.assertEqual(len(manifest['sessions'][0]['files']),2)
            imported=bundle.import_native(z,manifest,self.cwd)[0]
        rows=providers.view_records('antigravity',imported['id'],home=target)['records']
        self.assertEqual(rows[-1]['text'],'WAL-ONLY')

    def test_unsupported_schema_and_step_format_fail_explicitly(self):
        db=sqlite3.connect(self.path);db.execute('UPDATE steps SET step_format=99');db.commit();db.close()
        with self.assertRaisesRegex(providers.ProviderError,'unsupported_antigravity_step_format'):
            providers.list_sessions('antigravity',home=self.home)

    def test_trigger_schema_rejected(self):
        db=sqlite3.connect(self.path);db.execute('CREATE TRIGGER hidden AFTER UPDATE ON steps BEGIN DELETE FROM steps; END;');db.commit();db.close()
        with self.assertRaisesRegex(providers.ProviderError,'unsupported_antigravity_schema'):
            providers.list_sessions('antigravity',home=self.home)

    def test_parent_dependency_marks_package_unavailable(self):
        db=sqlite3.connect(self.path)
        value=db.execute("SELECT data FROM trajectory_metadata_blob WHERE id='main'").fetchone()[0]+pb((5,str(uuid.uuid4())))
        db.execute("UPDATE trajectory_metadata_blob SET data=? WHERE id='main'",(value,));db.commit();db.close()
        d=providers.package_descriptor('antigravity',self.sid,home=self.home)
        self.assertFalse(d['available']);self.assertEqual(d['reason'],'antigravity_parent_context_not_packaged')

    def test_running_session_refuses_native_package_but_keeps_raw_inventory(self):
        db=sqlite3.connect(self.path);db.execute('UPDATE steps SET status=2 WHERE idx=1');db.commit();db.close()
        d=providers.package_descriptor('antigravity',self.sid,home=self.home)
        self.assertFalse(d['available']);self.assertEqual(d['reason'],'antigravity_session_not_quiescent')
        self.assertEqual(len(providers.raw_files('antigravity',self.sid,home=self.home)),1)

    def test_subtrajectory_dependency_refuses_incomplete_native_package(self):
        db=sqlite3.connect(self.path);db.execute('ALTER TABLE steps ADD COLUMN has_subtrajectory INTEGER DEFAULT 0');db.execute('UPDATE steps SET has_subtrajectory=1 WHERE idx=1');db.commit();db.close()
        self.assertEqual(providers.package_descriptor('antigravity',self.sid,home=self.home)['reason'],'antigravity_subtrajectory_not_packaged')

    def test_wal_changed_during_snapshot_is_rejected(self):
        original=a.inventory;calls=[]
        def changing(*args):
            result=original(*args);calls.append(1)
            if len(calls)>1:result[0]['sha256']='0'*64
            return result
        with patch('csl_antigravity.inventory',side_effect=changing):
            with self.assertRaisesRegex(providers.ProviderError,'antigravity_source_changing'):
                providers.view_records('antigravity',self.sid,home=self.home)

    def test_symlink_and_source_identity_mismatch_rejected(self):
        link=self.home/'conversations'/(str(uuid.uuid4())+'.db');link.symlink_to(self.path)
        with self.assertRaises(providers.ProviderError):providers.list_sessions('antigravity',home=self.home)
        link.unlink();db=sqlite3.connect(self.path);db.execute('UPDATE trajectory_meta SET cascade_id=?',(str(uuid.uuid4()),));db.commit();db.close()
        with self.assertRaisesRegex(providers.ProviderError,'antigravity_identity_mismatch'):
            providers.list_sessions('antigravity',home=self.home)

    def test_protobuf_malformed_input_does_not_echo_content(self):
        for value in (b'\x00PRIVATE',b'\x12\xff',b'\x0a\x80\x80'):
            with self.assertRaises(a.FormatError) as caught:list(a.fields(value))
            self.assertNotIn('PRIVATE',str(caught.exception))

    def test_cli_listing_export_inspection_and_import(self):
        cli=Path(__file__).resolve().parents[1]/'csl'
        env={**os.environ,'HOME':str(self.root),'CSL_ANTIGRAVITY_HOME':str(self.home),'CODEX_HOME':str(self.root/'codex'),'CLAUDE_CONFIG_DIR':str(self.root/'claude')}
        def run(*args):
            r=subprocess.run([sys.executable,str(cli),*args],env=env,capture_output=True,text=True,timeout=15)
            self.assertEqual(r.returncode,0,r.stderr);return r.stdout
        self.assertEqual(json.loads(run('sessions','--provider','antigravity','--json'))['sessions'][0]['native_id'],self.sid)
        out=self.root/'cli.zip';run('export',self.sid,'--provider','antigravity','--raw','--to',str(out))
        self.assertIn(self.sid,run('import',str(out),'--list'))
        result=run('import',str(out),'--cwd',str(self.cwd));self.assertIn('agy --mode plan --conversation',result)
        self.assertIn('history is not execution authority',result)


@unittest.skipUnless(os.environ.get('CSL_RUN_ANTIGRAVITY_NATIVE_TESTS')=='1' and os.environ.get('CSL_ANTIGRAVITY_BINARY'),'Opt in with a private Antigravity CLI binary; localhost only')
class NativeAntigravityTests(unittest.TestCase):
    def test_native_package_resume_reconstructs_same_messages_without_model(self):
        # Native Antigravity storage and model API identifiers are required by
        # this executable. They do not register another CSL session provider.
        native_storage=Path('.gemini/antigravity-cli')
        captured=[]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                count=int(self.headers.get('Content-Length',0))
                if count>16*1024*1024:self.send_error(413);return
                data=json.loads(self.rfile.read(count));captured.append(data.get('contents',[]))
                body=('data: '+json.dumps({'candidates':[{'content':{'parts':[{'text':'SYNTHETIC-ACK-4827'}],'role':'model'},'finishReason':'STOP','index':0}],
                    'usageMetadata':{'promptTokenCount':10,'candidatesTokenCount':2,'totalTokenCount':12}})+'\n\n').encode()
                self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);cwd=root/'project';cwd.mkdir()
            server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            def run(home,*args):
                settings=home/native_storage;settings.mkdir(parents=True,exist_ok=True)
                (settings/'settings.json').write_text(json.dumps({'modelProvider':'gemini','enableTelemetry':False}))
                env={'PATH':os.environ['PATH'],'HOME':str(home),'XDG_CONFIG_HOME':str(home/'.config'),
                    'XDG_DATA_HOME':str(home/'.local/share'),'XDG_CACHE_HOME':str(home/'.cache'),
                    'GEMINI_API_KEY':'synthetic-not-a-real-key','GOOGLE_GEMINI_BASE_URL':f'http://127.0.0.1:{server.server_port}',
                    'BROWSER':'/bin/false','NO_BROWSER':'1','NO_PROXY':'127.0.0.1,localhost'}
                result=subprocess.run([os.environ['CSL_ANTIGRAVITY_BINARY'],'--mode','plan','--output-format','json','--print-timeout','20s',*args],cwd=cwd,env=env,capture_output=True,text=True,timeout=35)
                self.assertEqual(result.returncode,0,result.stderr[:500]);return json.loads(result.stdout)
            try:
                source_home=root/'source';native=run(source_home,'-p','Remember SYNTHETIC-USER-4827. Do not use tools. Reply ACK.')
                sid=native['conversation_id'];provider_root=source_home/native_storage
                before=providers.raw_files('antigravity',sid,home=provider_root)
                archive=root/'raw.zip';bundle.export_native('antigravity',sid,archive,home=provider_root,allow_secrets=True)
                baseline=root/'baseline';baseline_root=baseline/native_storage
                for f in before:
                    dest=baseline_root/'conversations'/(sid+'.db'+('-wal' if f['file_key']=='sqlite-wal' else ''))
                    dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(Path(f['path']).read_bytes()[:f['complete_length']])
                restored=root/'restored';restored_root=restored/native_storage
                with zipfile.ZipFile(archive) as z,patch.dict(os.environ,{'CSL_ANTIGRAVITY_HOME':str(restored_root)}):
                    imported=bundle.import_native(z,bundle.validate(z),cwd)[0]
                captured.clear();run(baseline,'--conversation',sid,'-p','What code appeared earlier? Do not use tools.')
                baseline_request=captured[-1]
                time.sleep(1.1)  # Require the comparison to survive native clock changes.
                captured.clear();result=run(restored,'--conversation',imported['id'],'-p','What code appeared earlier? Do not use tools.')
                imported_request=captured[-1]
                self.assertEqual(result['conversation_id'],imported['id'])
                self.assertIn('SYNTHETIC-USER-4827',json.dumps(imported_request))
                self.assertIn('SYNTHETIC-ACK-4827',json.dumps(imported_request))
                # Source settings are deliberately not permission authority at import.
                # The runtime may relocate its settings-change reminder. Compare that
                # known synthetic difference separately rather than claim exact bytes.
                def without_settings_notice(value):
                    if isinstance(value,dict):return {k:without_settings_notice(v) for k,v in value.items()}
                    if isinstance(value,list):return [without_settings_notice(v) for v in value]
                    if isinstance(value,str):return re.sub(r'<USER_SETTINGS_CHANGE>.*?</USER_SETTINGS_CHANGE>','',value,flags=re.S).strip()
                    return value
                self.assertGreaterEqual(len(imported_request),3)
                self.assertEqual([m['role'] for m in imported_request[:2]],['user','model'])
                # The newly generated turn has current clocks/background notices;
                # compare the two historical messages separately, not all future
                # runtime metadata as though it had to be byte-identical.
                baseline_normalized=without_settings_notice(baseline_request[:2])
                imported_normalized=without_settings_notice(imported_request[:2])
                if baseline_normalized!=imported_normalized:
                    left=json.dumps(baseline_normalized);right=json.dumps(imported_normalized)
                    offset=next((i for i,(x,y) in enumerate(zip(left,right)) if x!=y),0)
                    self.fail('Synthetic request mismatch: '+repr((left[max(0,offset-150):offset+100],right[max(0,offset-150):offset+100])))
                self.assertEqual(before,providers.raw_files('antigravity',sid,home=provider_root))
                print('antigravity_native_evidence='+json.dumps({'cli':'1.5.9','source_steps':2,
                    'new_identity':imported['id']!=sid,'historical_user_and_assistant_in_request':True,
                    'raw_messages_identical':baseline_request==imported_request,
                    'historical_messages_equal_excluding_settings_notice':True,
                    'source_inventory_unchanged':True,'paid_model_calls':0,'account_authenticated':False}))
            finally:
                server.shutdown();server.server_close();thread.join(timeout=3)


if __name__=='__main__':unittest.main()

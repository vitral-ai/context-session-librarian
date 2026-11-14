import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from csl_sync_engine import Collector
from csl_sync_http import HTTP, Registry, checked_url
from csl_sync_state import State, SyncError, exclusive
from sync_support import FakeProvider, FakeTokens, Peer


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='csl-sync-test-');self.root=Path(self.tmp.name)
        self.source=self.root/'private-home-history.jsonl';self.source.write_bytes(b'one\ntwo\npartial-secret')
        self.provider=FakeProvider(self.source);self.state=State(self.root/'state');self.peer=Peer();self.tokens=FakeTokens()
        self.registry=Registry(self.peer.url,self.tokens,http=HTTP(attempts=3,sleep=lambda _:None))
        self.collector=Collector(self.state,self.registry,self.provider,chunk_size=4)
    def tearDown(self):self.peer.close();self.state.close();self.tmp.cleanup()
    def run_pass(self):return self.collector.run(providers=['codex'])
    def data(self):
        plan=self.peer.sessions['sessions/codex/fake-session'];f=plan['files'][0]
        return bytes(self.peer.files[('sessions/codex/fake-session','main',f['revision'])])

    def test_complete_prefix_idempotence_append_and_status(self):
        self.assertEqual(self.run_pass()['synced'],1);self.assertEqual(self.data(),b'one\ntwo\n')
        local=self.state.session('codex','fake-session');rev=local['files']['main']['revision'];gen=local['view']['generation'];snapshot=local['snapshot_id']
        self.run_pass();self.assertEqual(len(self.peer.writes),2);self.assertEqual(self.state.session('codex','fake-session')['snapshot_id'],snapshot)
        self.source.write_bytes(b'one\ntwo\nthree\n')
        self.run_pass();new=self.state.session('codex','fake-session')
        self.assertEqual(new['files']['main']['revision'],rev);self.assertEqual(new['view']['generation'],gen)
        self.assertEqual(self.data(),b'one\ntwo\nthree\n');self.assertEqual(len(self.peer.views[('sessions/codex/fake-session',gen)]),3)
        status=json.dumps(self.state.status());self.assertNotIn(str(self.source),status);self.assertNotIn('one',status)

    def test_lost_upload_response_is_idempotent(self):
        self.peer.drop_once=True;self.run_pass()
        self.assertEqual(self.data(),b'one\ntwo\n');self.assertEqual(len(self.peer.writes),2)

    def test_view_from_another_source_snapshot_is_not_published(self):
        render = self.provider.view_records
        def changed(*args, **kwargs):
            page = render(*args, **kwargs)
            page['source_file']['sha256'] = '0' * 64
            return page
        self.provider.view_records = changed
        self.assertEqual(self.run_pass()['pending'], 1)
        self.assertEqual(self.state.session('codex', 'fake-session')['last_error'], 'source_changed')
        self.assertEqual(self.peer.sessions, {})
        self.assertEqual(self.peer.writes, [])
        self.provider.view_records = render
        self.assertEqual(self.run_pass()['synced'], 1)

    def test_view_pagination_and_lost_response_preserve_exact_sequence(self):
        self.source.write_bytes(b''.join((str(i)+'\n').encode() for i in range(251)))
        self.collector.chunk_size=65536;self.peer.drop_view_once=True;self.run_pass()
        local=self.state.session('codex','fake-session')
        rows=self.peer.views[('sessions/codex/fake-session',local['view']['generation'])]
        self.assertEqual([r['seq'] for r in rows],list(range(1,252)));self.assertEqual(local['view']['last_seq'],251)

    def test_remote_prefix_conflict_uses_a_new_revision_on_next_pass(self):
        self.run_pass();old=self.state.session('codex','fake-session');rev=old['files']['main']['revision']
        self.peer.files[('sessions/codex/fake-session','main',rev)][:]=b'bad\n'
        self.assertEqual(self.run_pass()['pending'],1)
        self.assertEqual(self.state.session('codex','fake-session')['last_error'],'remote_prefix_mismatch')
        self.assertEqual(self.run_pass()['synced'],1);self.assertEqual(self.data(),b'one\ntwo\n')
        self.assertNotEqual(self.state.session('codex','fake-session')['files']['main']['revision'],rev)

    def test_eight_mebibyte_chunk_boundary_is_enforced(self):
        self.source.write_bytes(b'x'*(8*1024*1024+17)+b'\n');self.collector.chunk_size=8*1024*1024
        self.run_pass();self.assertEqual(len(self.peer.writes),2)
        self.assertTrue(all(end-start<=8*1024*1024 for _,start,end in self.peer.writes))
        self.assertEqual(hashlib.sha256(self.data()).hexdigest(),hashlib.sha256(self.source.read_bytes()).hexdigest())

    def test_truncation_and_replacement_start_new_revisions_and_view_generation(self):
        self.run_pass();old=self.state.session('codex','fake-session');rev=old['files']['main']['revision'];gen=old['view']['generation']
        self.source.write_bytes(b'new\n');self.run_pass();new=self.state.session('codex','fake-session')
        self.assertNotEqual(new['files']['main']['revision'],rev);self.assertNotEqual(new['view']['generation'],gen)
        rev=new['files']['main']['revision'];other=self.root/'replacement';other.write_bytes(b'new\n');os.replace(other,self.source)
        self.run_pass();self.assertNotEqual(self.state.session('codex','fake-session')['files']['main']['revision'],rev)

    def test_rewrite_file_uploads_whole_revision_in_chunks_and_invalid_json_is_not_uploaded(self):
        self.provider.mode='rewrite';self.source.write_text(json.dumps({'messages':['old','decision']}));self.run_pass();old=self.state.session('codex','fake-session')['files']['main']['revision']
        self.source.write_text(json.dumps({'messages':['changed','decision']}));self.run_pass()
        self.assertNotEqual(self.state.session('codex','fake-session')['files']['main']['revision'],old)
        self.assertEqual(self.data(),self.source.read_bytes());before=len(self.peer.writes)
        self.source.write_bytes(b'{"messages":[');self.assertEqual(self.run_pass()['pending'],1);self.assertEqual(len(self.peer.writes),before)

    def test_default_window_include_exclude_and_following(self):
        self.provider.updated_at=(datetime.now(timezone.utc)-timedelta(days=40)).isoformat()
        self.assertEqual(self.run_pass()['sessions'],[])
        self.state.select('codex','fake-session',True);self.assertEqual(self.run_pass()['synced'],1)
        self.state.db.execute('delete from selections');self.state.db.commit()
        self.assertEqual(self.run_pass()['synced'],1)  # previously selected sessions remain followed
        self.state.select('codex','fake-session',False);before=len(self.peer.writes);self.source.write_bytes(b'private change\n')
        self.assertEqual(self.run_pass()['sessions'],[]);self.assertEqual(len(self.peer.writes),before)
        self.assertFalse(any(method=='DELETE' for method,path in self.peer.calls))

    def test_unavailable_bounded_retries_and_safe_diagnostics(self):
        self.peer.error=(503,'unavailable');delays=[];self.registry.http.sleep=delays.append
        with self.assertRaises(SyncError) as error:self.run_pass()
        self.assertTrue(error.exception.retryable);self.assertEqual(len(self.peer.calls),3);self.assertEqual(len(delays),2)
        self.assertNotIn('PRIVATE_TRANSCRIPT_SENTINEL',str(error.exception))

    def test_revoked_credentials_are_not_retried_as_a_regular_network_failure(self):
        self.peer.error=(401,'session_revoked')
        with self.assertRaises(SyncError) as error:self.run_pass()
        self.assertEqual(error.exception.code,'session_revoked');self.assertTrue(self.tokens.revoked);self.assertEqual(len(self.peer.calls),1)

    def test_limit_refusal_is_visible_without_silent_success(self):
        self.peer.error=(413,'limit_exceeded')
        with self.assertRaises(SyncError) as error:self.run_pass()
        self.assertEqual(error.exception.code,'limit_exceeded');self.assertFalse(error.exception.retryable)

    def test_two_processes_cannot_collect_same_installation(self):
        code='import sys,time;from csl_sync_state import exclusive;from pathlib import Path\nwith exclusive(Path(sys.argv[1])):\n print("READY",flush=True);time.sleep(60)'
        process=subprocess.Popen([sys.executable,'-c',code,str(self.state.root)],cwd=Path(__file__).resolve().parents[1],stdout=subprocess.PIPE,text=True)
        try:
            self.assertEqual(process.stdout.readline().strip(),'READY')
            with self.assertRaises(SyncError) as error:
                with exclusive(self.state.root):pass
            self.assertEqual(error.exception.code,'collector_busy')
        finally:process.terminate();process.wait(timeout=5);process.stdout.close()
        with exclusive(self.state.root):pass

    def test_killed_mid_upload_resumes_registry_offset_and_removes_private_orphan_spool(self):
        self.peer.first_chunk=threading.Event();self.peer.release=threading.Event()
        code=('import sys;from pathlib import Path;from csl_sync_state import State,exclusive;from csl_sync_engine import Collector;'
              'from csl_sync_http import Registry,HTTP;from sync_support import FakeProvider,FakeTokens;'
              's=State(Path(sys.argv[1]));p=FakeProvider(Path(sys.argv[2]));'
              'c=Collector(s,Registry(sys.argv[3],FakeTokens(),http=HTTP(sleep=lambda _:None)),p,chunk_size=4);'
              'c.run(providers=["codex"])')
        env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parent)+os.pathsep+str(Path(__file__).resolve().parents[1]))
        process=subprocess.Popen([sys.executable,'-c',code,str(self.state.root),str(self.source),self.peer.url],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        try:
            self.assertTrue(self.peer.first_chunk.wait(5));process.kill();process.wait(timeout=5);self.peer.release.set()
            self.assertTrue(list((self.state.root/'snapshots').iterdir()))
            self.run_pass();self.assertEqual(self.data(),b'one\ntwo\n');self.assertEqual(len(self.peer.writes),2)
            self.assertEqual(list((self.state.root/'snapshots').iterdir()),[])
        finally:
            self.peer.release.set()
            if process.poll() is None:process.kill();process.wait(timeout=5)
            process.stderr.close()

    def test_foreign_or_cleartext_endpoints_are_rejected(self):
        for url in ['http://example.test','https://user:secret@example.test','https://example.test/?token=secret']:
            with self.assertRaises(SyncError):checked_url(url)
        self.assertEqual(checked_url('http://127.0.0.1:1234/v1'),'http://127.0.0.1:1234/v1')

    def test_authenticated_http_never_forwards_credentials_to_a_redirect(self):
        other=Peer()
        try:
            self.peer.redirect_url=other.url+'/limits'
            with self.assertRaises(SyncError) as error:self.registry.call('GET','/limits')
            self.assertEqual(error.exception.status,302);self.assertEqual(other.calls,[])
        finally:other.close()


if __name__=='__main__':unittest.main()

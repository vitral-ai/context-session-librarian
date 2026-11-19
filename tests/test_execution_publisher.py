"""Actual private publication in disposable homes; no native model or login."""
import contextlib
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid
import zipfile

from csl_backup_core import canonical
import csl_codex_history as history
import csl_durable as durable
import csl_execution as execution
import csl_import_execution as publisher
import csl_provider_bundle as bundle
from tests.test_codex_history import write_rollout


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path.home(),prefix='.publisher-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.root.chmod(0o700)
        self.home,self.journal,self.cwd,self.source=[self.root/x for x in ('codex','journal','project','source')]
        for p in (self.home,self.journal,self.cwd,self.source):p.mkdir(mode=0o700)
        self.jfd=os.open(self.journal,os.O_RDONLY|os.O_DIRECTORY)
        self.hfd=os.open(self.home,os.O_RDONLY|os.O_DIRECTORY)
        self.addCleanup(os.close,self.jfd)
        self.addCleanup(os.close,self.hfd)
        self.roots={'journal':publisher.identity(self.jfd),'home':publisher.identity(self.hfd)}
        self.boot,self.start=execution.linux_clock()
        self.set_package()

    def set_package(self,graph=False,shared=False,levels=3):
        # Each call uses new source storage, keeping prior installed fixtures intact.
        source=self.source/str(uuid.uuid4());source.mkdir(mode=0o700)
        base=None;owner=None
        for i in range(levels if graph else 1):
            path,owner,base=write_rollout(source,base=base,label='SYNTHETIC-'+str(i),
                                        owner=owner if shared else None,
                                        rollout=str(uuid.uuid4()) if shared and i else None,
                                        archived=i<(levels-1) if graph else False)
        self.source_root=source
        self.source_path=path
        self.sid=owner
        package=self.root/(str(uuid.uuid4())+'.zip')
        with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
            bundle.export_native('codex',self.sid,package,home=source,allow_secrets=True)
        body=package.read_bytes()
        self.package=package
        self.value=dict(schema_version=1,operation='import',execution_id='re_'+'1'*64,import_key='ri_'+'2'*64,
                        provider='codex',source_id=self.sid,package_size=len(body),package_sha256=hashlib.sha256(body).hexdigest(),
                        owner='1'*24,host_id='ses_fixture',assignment_id='33333333-3333-4333-8333-333333333333',generation='1',boot_id=self.boot,
                        cwd=str(self.cwd),cwd_device=str(self.cwd.stat().st_dev),cwd_inode=str(self.cwd.stat().st_ino),
                        authority=dict(boot_id=self.boot,start_boottime_ns=str(self.start),end_boottime_ns=str(self.start+300_000_000_000)))
        self.req=execution.request(canonical(self.value))

    def guard(self,req=None):
        return execution.Guard(req or self.req,lambda:None,execution.linux_clock)

    def publish(self,*,fault=None,check=None):
        check=check or self.guard()
        with self.package.open('rb') as src:
            capture=execution.capture(src.fileno(),self.req,check,10*1024*1024)
        try:
            return publisher.publish(self.jfd,self.hfd,self.req,capture,check,roots=self.roots,fault=fault)
        finally:capture.close()

    def row(self):
        return json.loads((self.journal/(self.value['import_key']+'.json')).read_bytes())

    def query(self,req=None):
        req=req or self.req
        return publisher.lookup(self.jfd,self.hfd,req,self.guard(req),roots=self.roots)

    def installed(self):
        return sorted(self.home.rglob('*.jsonl'))

    def test_singleton_actual_receipt_and_repeat_no_new_ids(self):
        receipt=self.publish()
        self.assertEqual(receipt['validation'],'native_unverified')
        self.assertEqual(receipt['state'],'imported')
        self.assertNotEqual(receipt['imported_native_id'],self.sid)
        self.assertEqual(len(self.installed()),1)
        before=self.installed()[0].read_bytes()
        self.assertEqual(receipt,self.publish())
        self.assertEqual(self.query(),{'state':'complete','receipt':receipt})
        self.assertEqual(self.installed()[0].read_bytes(),before)
        self.assertEqual(self.installed()[0].stat().st_nlink,1)
        self.assertFalse(list(self.home.rglob('.csl-*')))
        rows=[json.loads(line) for line in before.splitlines()]
        self.assertEqual(rows[0]['payload']['id'],receipt['imported_native_id'])
        self.assertEqual(rows[0]['payload']['cwd'],str(self.cwd))
        self.assertEqual(rows[0]['csl_source_session_id'],self.sid)

    def test_complete_graph_and_shared_owner_preserve_coordinates(self):
        for shared in (False,True):
            with self.subTest(shared=shared):
                # Keep separate execution keys when installing both independent graphs.
                self.set_package(graph=True,shared=shared)
                if shared:
                    self.value['import_key']='ri_'+'3'*64
                    self.value['execution_id']='re_'+'3'*64
                    self.req=execution.request(canonical(self.value))
                receipt=self.publish()
                row=self.row()
                self.assertEqual(len(row['members']),3)
                self.assertEqual(len(row['plan']['mapping']['rollouts']),3)
                root=self.home/row['members'][-1]['path']
                nodes=history.lineage(self.home,root)
                self.assertEqual(len(nodes),3)
                self.assertEqual(nodes[-1].owner,receipt['imported_native_id'])
                self.assertEqual([m['role'] for m in row['members']],['ancestor','ancestor','root'])
                self.assertEqual(receipt,self.publish())

    def test_missing_and_fresh_bound_lookup_are_read_only(self):
        self.assertEqual(self.query(),{'state':'not_prepared','receipt':None})
        self.assertEqual(list(self.journal.iterdir()),[])
        self.publish()
        before={p.relative_to(self.root): (p.stat().st_ino,p.read_bytes()) for p in [*self.journal.glob('*'),*self.installed()] if p.is_file()}
        value=copy.deepcopy(self.value);value['operation']='lookup'
        value['authority']['start_boottime_ns']=str(self.start+1)
        value['authority']['end_boottime_ns']=str(self.start+310_000_000_000)
        req=execution.request(canonical(value))
        self.assertEqual(self.query(req)['state'],'complete')
        after={p.relative_to(self.root): (p.stat().st_ino,p.read_bytes()) for p in [*self.journal.glob('*'),*self.installed()] if p.is_file()}
        self.assertEqual(before,after)
        with self.assertRaisesRegex(execution.ExecutionError,'unsupported'):
            publisher.publish(self.jfd,self.hfd,req,None,self.guard(req),roots=self.roots)
        value['operation']='import';req=execution.request(canonical(value))
        with self.assertRaisesRegex(execution.ExecutionError,'original_bound_changed'):
            publisher.publish(self.jfd,self.hfd,req,None,self.guard(req),roots=self.roots)

    def test_legacy_reservation_mapping_preserved_and_claude_unsupported(self):
        with self.package.open('rb') as src:
            captured=execution.capture(src.fileno(),self.req,self.guard(),10*1024*1024)
        try:
            plan=execution.plan(captured,self.req,self.guard(),self.jfd)
            old=execution.reserve(self.jfd,self.req,plan,self.guard())
        finally:captured.close()
        receipt=self.publish()
        self.assertEqual(receipt['imported_native_id'],old['plan']['mapping']['owners'][self.sid])
        value=copy.deepcopy(self.value);value['provider']='claude'
        req=execution.request(canonical(value))
        before=list(self.home.rglob('*'))
        with self.assertRaisesRegex(execution.ExecutionError,'unsupported'):
            publisher.publish(self.jfd,self.hfd,req,None,self.guard(req),roots=self.roots)
        self.assertEqual(before,list(self.home.rglob('*')))

    def kill_at(self,point,occurrence=1):
        pid=os.fork()
        if pid==0:
            try:
                seen=0
                def cut(where):
                    nonlocal seen
                    if where==point:
                        seen+=1
                        if seen==occurrence:os.kill(os.getpid(),signal.SIGKILL)
                self.publish(fault=cut)
            except BaseException:os._exit(97)
            os._exit(98)  # Fails if selected actual boundary wasn't reached.
        end=time.monotonic()+15
        while True:
            got,status=os.waitpid(pid,os.WNOHANG)
            if got:break
            if time.monotonic()>=end:
                os.kill(pid,signal.SIGKILL);os.waitpid(pid,0)
                self.fail('owned SIGKILL fixture did not reach its selected cut')
            time.sleep(.01)
        self.assertEqual(got,pid)
        self.assertTrue(os.WIFSIGNALED(status),(point,status))
        self.assertEqual(os.WTERMSIG(status),signal.SIGKILL)

    def test_real_sigkill_cold_matrix_complete_graph(self):
        points=['reserved','metadata_reserved_renamed','metadata_preparing_renamed','keeper_created_0',
                'keeper_fsynced_0','prepared','linked_0','linked_fsynced_0','linked_1','linked_2',
                'metadata_receipted_renamed','metadata_receipted_fsynced','receipted',
                'metadata_retiring_keepers_renamed','keeper_unlinked_0','keeper_unlinked_1','keeper_unlinked_2']
        for n,point in enumerate(points):
            with self.subTest(point=point):
                self.set_package(graph=True)
                self.value['import_key']='ri_'+format(n+100,'064x')
                self.value['execution_id']='re_'+format(n+100,'064x')
                self.req=execution.request(canonical(self.value))
                self.kill_at(point)
                cold=self.row()
                # Actual new process readback, not an in-memory callback/assertion.
                read_fd,write_fd=os.pipe();pid=os.fork()
                if pid==0:
                    os.close(read_fd)
                    try:
                        value=self.query()
                        os.write(write_fd,canonical(value))
                    except BaseException:os._exit(96)
                    finally:os.close(write_fd)
                    os._exit(0)
                os.close(write_fd);raw=os.read(read_fd,execution.MAX_MESSAGE+1);os.close(read_fd)
                self.assertEqual(os.waitpid(pid,0)[1],0)
                status=json.loads(raw)
                self.assertEqual(status['receipt'],cold['receipt'])
                root_id=cold['plan']['mapping']['owners'][self.sid]
                if point=='keeper_created_0':
                    with self.assertRaisesRegex(publisher.Pending,'uncommitted'):
                        self.publish()
                    self.assertEqual(self.row()['plan']['mapping'],cold['plan']['mapping'])
                    continue
                receipt=self.publish()
                self.assertEqual(receipt['imported_native_id'],root_id)
                self.assertEqual(len(self.row()['members']),3)
                self.assertEqual(self.query()['receipt'],receipt)
                self.assertEqual(self.row()['state'],'complete')

    def test_foreign_same_bytes_and_nlink3_never_adopted(self):
        def stop(point):
            if point=='prepared':raise RuntimeError('pause')
        with self.assertRaisesRegex(RuntimeError,'pause'):self.publish(fault=stop)
        row=self.row();m=row['members'][0];target=self.home/m['path'];keeper=target.parent/m['keeper']
        target.write_bytes(keeper.read_bytes());target.chmod(0o600)
        foreign_inode=target.stat().st_ino
        with self.assertRaises(publisher.Pending):self.publish()
        self.assertEqual(target.stat().st_ino,foreign_inode)
        target.unlink()  # Only this test's explicitly owned foreign fixture.
        os.link(keeper,target);extra=target.parent/'extra-owned-link';os.link(keeper,extra)
        with self.assertRaises(publisher.Pending):self.publish()
        self.assertTrue(extra.exists())

    def test_expiry_before_link_retains_intent_and_fresh_lookup_cannot_publish(self):
        expired=False
        def guard():
            if expired:raise execution.ExecutionError('known_revoked')
            self.guard()()
        def stop(point):
            nonlocal expired
            if point=='prepared':expired=True
        with self.assertRaisesRegex(execution.ExecutionError,'known_revoked'):
            self.publish(check=guard,fault=stop)
        self.assertEqual(self.installed(),[])
        row=self.row();self.assertEqual(row['state'],'prepared')
        self.assertEqual(self.query()['receipt'],None)
        self.assertEqual(row,self.row())

    def test_fsync_unknown_and_receipt_never_regresses_on_retirement_failure(self):
        def fail(point):
            if point=='metadata_receipted_renamed':raise OSError('synthetic fsync unknown')
        with self.assertRaises(OSError):self.publish(fault=fail)
        receipt=self.query()['receipt'];self.assertIsNotNone(receipt)
        def retirement(point):
            if point=='keeper_unlinked_0':raise PermissionError('synthetic directory failure')
        with self.assertRaises(PermissionError):self.publish(fault=retirement)
        self.assertEqual(self.query()['receipt'],receipt)
        self.assertEqual(self.publish(),receipt)

    def test_strict_record_receipt_path_and_semantic_corruption_refuse(self):
        self.publish();path=self.journal/(self.value['import_key']+'.json');row=self.row()
        cases=[]
        for field,value in [('revision',True),('v',True),('state','imported'),('receipt',None),('members',[]),('unknown',1)]:
            bad=copy.deepcopy(row);bad[field]=value;cases.append(bad)
        for field,value in [('path','../foreign'),('identity',None),('phase','kept'),('size',True),('native_id',str(uuid.uuid4())),('keeper','../foreign')]:
            bad=copy.deepcopy(row);bad['members'][0][field]=value;cases.append(bad)
        bad=copy.deepcopy(row);bad['receipt']['imported_native_id']=self.sid;cases.append(bad)
        bad=copy.deepcopy(row);bad['receipt']['schema_version']=True;cases.append(bad)
        for i,bad in enumerate(cases):
            with self.subTest(case=i):
                path.write_bytes(canonical(bad)+b'\n')
                with self.assertRaises((execution.ExecutionError,ValueError)):self.query()
        path.write_bytes(canonical(row)+b'\n')
        # Existing old link-create gap is refused, not adopted by filename/hash.
        os.link(path,self.journal/'.execution-old-fixture')
        with self.assertRaises(durable.DurableError):self.query()

    def test_kill_before_first_rename_retains_unknown_stage_without_second_mapping(self):
        self.kill_at('metadata_planning_written')
        self.assertEqual(self.query(),{'state':'not_prepared','receipt':None})
        self.assertEqual(self.installed(),[])
        with patch.object(history,'reserve_mapping',side_effect=AssertionError('new mapping')),patch.object(execution,'plan',side_effect=AssertionError('new plan')):
            with self.assertRaisesRegex(publisher.Pending,'staging_unknown'):self.publish()
        self.assertTrue((self.journal/('.'+self.value['import_key']+'.1.tmp')).is_file())

    def test_native_denial_during_real_link_retains_committed_custody(self):
        revoked=False
        def check():
            if revoked:raise execution.ExecutionError('known_assignment_revoked')
            self.guard()()
        def cut(point):
            nonlocal revoked
            if point=='linked_0':revoked=True
        with self.assertRaisesRegex(execution.ExecutionError,'known_assignment_revoked'):
            self.publish(check=check,fault=cut)
        self.assertEqual(len(self.installed()),1)
        row=self.row();m=row['members'][0]
        self.assertEqual(m['phase'],'publishing')
        target=self.home/m['path'];keeper=target.parent/m['keeper']
        self.assertEqual(target.stat().st_ino,keeper.stat().st_ino)
        self.assertEqual(target.stat().st_nlink,2)
        self.assertIsNone(self.query()['receipt'])
        self.assertEqual(self.publish()['imported_native_id'],m['owner_id'])

    def test_concurrent_processes_have_one_mapping_and_one_complete_result(self):
        go_read,go_write=os.pipe();children=[]
        for i in range(2):
            pid=os.fork()
            if pid==0:
                os.close(go_write)
                os.read(go_read,1);os.close(go_read)
                try:self.publish()
                except BlockingIOError:os._exit(20)
                except BaseException:os._exit(99)
                os._exit(0)
            children.append(pid)
        os.close(go_read);os.write(go_write,b'xx');os.close(go_write)
        codes=[os.waitstatus_to_exitcode(os.waitpid(pid,0)[1]) for pid in children]
        self.assertIn(0,codes)
        self.assertTrue(set(codes)<={0,20},codes)
        self.assertEqual(len(self.installed()),1)
        self.assertEqual(self.publish(),self.query()['receipt'])

    def test_changed_binding_package_keeper_and_roots_refuse(self):
        def stop(point):
            if point=='prepared':raise RuntimeError('pause')
        with self.assertRaises(RuntimeError):self.publish(fault=stop)
        original=self.row()
        for key,value in [('owner','2'*24),('host_id','ses_other'),('generation','2'),('package_sha256','3'*64),('source_id',str(uuid.uuid4()))]:
            with self.subTest(key=key):
                v=copy.deepcopy(self.value);v[key]=value;req=execution.request(canonical(v))
                with self.assertRaises(execution.ExecutionError):
                    publisher.publish(self.jfd,self.hfd,req,None,self.guard(req),roots=self.roots)
                self.assertEqual(original,self.row())
        roots=copy.deepcopy(self.roots);roots['home']['inode']=str(int(roots['home']['inode'])+1)
        with self.assertRaisesRegex(execution.ExecutionError,'root_changed'):
            publisher.lookup(self.jfd,self.hfd,self.req,self.guard(),roots=roots)
        m=original['members'][0];keeper=(self.home/m['path']).parent/m['keeper']
        keeper.write_bytes(b'altered')
        with self.assertRaises(publisher.Pending):self.publish()
        self.assertEqual(self.installed(),[])

    def test_cwd_same_path_replacement_fences_effect(self):
        old=self.cwd.with_name('old-project')
        self.cwd.rename(old);self.cwd.mkdir(mode=0o700)
        with self.assertRaisesRegex(execution.ExecutionError,'cwd_changed'):self.publish()
        self.assertEqual(list(self.journal.iterdir()),[])
        self.assertEqual(self.installed(),[])

    def test_parent_replacement_before_link_is_not_published_elsewhere(self):
        changed=False
        def replace(point):
            nonlocal changed
            if point=='prepared':
                changed=True
                (self.home/'sessions').rename(self.home/'old-sessions')
                (self.home/'sessions').mkdir(mode=0o700)
        with self.assertRaises((FileNotFoundError,publisher.Pending)):self.publish(fault=replace)
        self.assertTrue(changed)
        self.assertEqual(self.installed(),[])

    def test_link_error_checks_known_loss_before_returning_filesystem_failure(self):
        denied=False
        def check():
            if denied:raise execution.ExecutionError('known_revoked_during_syscall')
            self.guard()()
        def link(*args,**kwargs):
            nonlocal denied
            denied=True
            raise OSError('synthetic link error')
        with patch.object(publisher.os,'link',side_effect=link):
            with self.assertRaisesRegex(execution.ExecutionError,'known_revoked_during_syscall'):
                self.publish(check=check)
        self.assertEqual(self.installed(),[])
        self.assertEqual(self.row()['members'][0]['phase'],'publishing')

    def test_original_end_equality_fences_publication_after_prepared(self):
        now=self.start
        guard=execution.Guard(self.req,lambda:None,lambda:(self.boot,now))
        def cut(point):
            nonlocal now
            if point=='prepared':now=int(self.value['authority']['end_boottime_ns'])
        with self.assertRaisesRegex(execution.ExecutionError,'original_bound_lost'):
            self.publish(check=guard,fault=cut)
        self.assertEqual(self.installed(),[])
        self.assertEqual(self.row()['state'],'prepared')
        self.assertIsNone(self.query()['receipt'])

    def test_post_open_authority_loss_closes_owned_descriptor(self):
        denied=False;opened=[]
        def check():
            if denied:raise execution.ExecutionError('known_revoked_after_open')
        real_open=os.open
        def opened_then_lost(*args,**kwargs):
            nonlocal denied
            fd=real_open(*args,**kwargs);opened.append(fd);denied=True
            return fd
        with patch.object(publisher.os,'open',side_effect=opened_then_lost):
            with self.assertRaisesRegex(execution.ExecutionError,'known_revoked_after_open'):
                publisher._open(check,'owned-fixture',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600,dir_fd=self.jfd)
        self.assertEqual(len(opened),1)
        with self.assertRaises(OSError):os.fstat(opened[0])


if __name__=='__main__':unittest.main()

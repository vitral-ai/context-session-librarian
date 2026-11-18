"""Unwired synthetic execution primitives: no histories installed or CLI invoked."""
import copy
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid
import zipfile

import csl_execution as e
import csl_durable as d
from csl_backup_core import canonical
from tests.test_codex_history import write_rollout
import csl_codex_history as history
import csl_provider_bundle as bundle


def allowed():
    pass


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path.home(), prefix='.execution-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.root.chmod(0o700)
        self.fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        self.addCleanup(os.close, self.fd)
        self.body = self.zip('A')
        self.path = self.root/'package.zip'
        self.path.write_bytes(self.body)
        self.v = dict(schema_version=1, operation='import', execution_id='re_'+'1'*64, import_key='ri_'+'2'*64,
                      provider='codex', source_id='11111111-1111-4111-8111-111111111111',
                      package_size=len(self.body), package_sha256=hashlib.sha256(self.body).hexdigest(),
                      owner='1'*24, host_id='ses_fresh0', assignment_id='33333333-3333-4333-8333-333333333333',
                      generation=str((1<<64)-1), boot_id='44444444-4444-4444-8444-444444444444',
                      cwd=str(self.root), cwd_device=str(self.root.stat().st_dev), cwd_inode=str(self.root.stat().st_ino),
                      authority=dict(boot_id='44444444-4444-4444-8444-444444444444',start_boottime_ns='0',end_boottime_ns='100'))
        self.req = e.request(canonical(self.v))

    def zip(self, marker):
        sid='11111111-1111-4111-8111-111111111111'
        body=(json.dumps({'type':'session_meta','payload':{'id':sid,'cwd':'/synthetic','source':'cli'}})+'\n'+marker+'\n').encode()
        manifest=dict(format='codex-session-librarian',version=1,mode='raw',sessions=[dict(id=sid,provider='codex',title='synthetic',file='sessions/'+sid+'.jsonl',bytes=len(body),sha256=hashlib.sha256(body).hexdigest())])
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as archive:
            archive.writestr('manifest.json',json.dumps(manifest))
            archive.writestr('sessions/'+sid+'.jsonl',body)
        return stream.getvalue()

    def capture(self, check=allowed):
        with self.path.open('rb') as stream:
            return e.capture(stream.fileno(),self.req,check,2**20)

    def prepared(self):
        c=self.capture();self.addCleanup(c.close)
        return e.plan(c,self.req,allowed,self.fd)

    def test_closed_codec_refusals_and_exact_ranges(self):
        self.assertEqual(self.req.value,self.v)
        for key,value in [('host_id','22222222-2222-4222-8222-222222222222'),('generation','0'),('generation',str(1<<64)),('generation',True),('package_size',True),('provider','antigravity'),('owner','X'*24),('cwd','//x'),('cwd','/a/../b'),('cwd_inode','0'),('boot_id','X')]:
            with self.subTest(key=key,value=value):
                v=dict(self.v);v[key]=value
                with self.assertRaises(e.ExecutionError):e.request(canonical(v))
        for raw in [b'{}',b'{"schema_version":1,"schema_version":1}',b'[]',b'NaN',b'x'*16385]:
            with self.assertRaises(e.ExecutionError):e.request(raw)
        for field in ['unknown']:
            v=dict(self.v);v[field]=1
            with self.assertRaises(e.ExecutionError):e.request(canonical(v))
        for end in ['0','-1','01',str(1<<63),True]:
            v=copy.deepcopy(self.v);v['authority']['end_boottime_ns']=end
            with self.assertRaises(e.ExecutionError):e.request(canonical(v))

    def test_original_guard_no_renewal_backwards_boot_or_failure(self):
        clock=[self.v['boot_id'],1]
        guard=e.Guard(self.req,allowed,lambda:tuple(clock))
        guard();clock[1]=2;guard()
        for sample in [(clock[0],1),('changed',3),(clock[0],100),(clock[0],True)]:
            clock[:]=sample
            with self.assertRaises(e.ExecutionError):guard()
        error=RuntimeError('authoritative revocation')
        def lost():raise error
        with self.assertRaisesRegex(RuntimeError,'revocation'):e.Guard(self.req,lost,lambda:('bad',999))()

    def test_capture_qualifies_whole_zip_and_is_sealed(self):
        c=self.capture();self.addCleanup(c.close)
        original=c.rewind().read()
        self.path.write_bytes(self.zip('B'))
        self.assertEqual(c.rewind().read(),original)
        with self.assertRaises(OSError):os.write(c.stream.fileno(),b'x')
        with self.assertRaises(OSError):os.ftruncate(c.stream.fileno(),0)
        p=e.plan(c,self.req,allowed,self.fd)
        self.assertEqual(p['package_sha256'],self.v['package_sha256'])
        self.assertEqual(list(p['mapping']['owners']),[self.v['source_id']])

    def test_same_inode_same_size_valid_other_zip_refuses(self):
        before=self.path.stat();self.path.write_bytes(self.zip('B'));after=self.path.stat()
        self.assertEqual((before.st_dev,before.st_ino,before.st_size),(after.st_dev,after.st_ino,after.st_size))
        with self.assertRaises(e.ExecutionError):self.capture()
        self.assertEqual(list(self.root.glob('ri_*.json')),[])

    def test_truncate_append_offset_and_write_access_refuse(self):
        for raw in [self.body[:-1],self.body+b'x']:
            self.path.write_bytes(raw)
            with self.assertRaises(e.ExecutionError):self.capture()
        self.path.write_bytes(self.body)
        with self.path.open('rb') as stream:
            stream.read(1)
            with self.assertRaises(e.ExecutionError):e.capture(stream.fileno(),self.req,allowed,2**20)
        with self.path.open('r+b') as stream:
            with self.assertRaises(e.ExecutionError):e.capture(stream.fileno(),self.req,allowed,2**20)
        with self.assertRaises(e.ExecutionError):
            with self.path.open('rb') as stream:e.capture(stream.fileno(),self.req,allowed,1)

    def test_capture_read_expiry_or_inflight_rewrite_refuse(self):
        for mode in ['expiry','rewrite']:
            self.path.write_bytes(self.body);calls=0
            def check():
                nonlocal calls
                calls+=1
                if calls==3:
                    if mode=='expiry':raise e.ExecutionError('original_expired')
                    self.path.write_bytes(self.zip('B'))
            with self.subTest(mode=mode),self.assertRaises(e.ExecutionError):self.capture(check)
            self.assertEqual(list(self.root.glob('ri_*.json')),[])

    def test_exclusive_durable_reservation_and_lookup_never_import(self):
        with self.assertRaises(FileNotFoundError):e.lookup(self.fd,self.req,allowed)
        p=self.prepared();first=e.reserve(self.fd,self.req,p,allowed)
        second=e.reserve(self.fd,self.req,self.prepared(),allowed)
        self.assertEqual(first,second)
        v=dict(self.v,operation='lookup');v['authority']=dict(v['authority'],end_boottime_ns='200')
        lookup=e.request(canonical(v))
        self.assertEqual(e.lookup(self.fd,lookup,allowed),first)
        with self.assertRaises(e.ExecutionError):e.reserve(self.fd,lookup,p,allowed)
        v['operation']='import'
        with self.assertRaises(e.ExecutionError):e.reserve(self.fd,e.request(canonical(v)),p,allowed)
        self.assertEqual(first['state'],'reserved')
        self.assertFalse((self.root/'sessions').exists())

    def test_durable_metadata_refuses_foreign_mode_links_and_expired_write(self):
        p=self.prepared();name=self.v['import_key']+'.json'
        def expired():raise e.ExecutionError('expired')
        with self.assertRaises(e.ExecutionError):e.reserve(self.fd,self.req,p,expired)
        self.assertFalse((self.root/name).exists())
        row=e.reserve(self.fd,self.req,p,allowed)
        (self.root/name).chmod(0o644)
        with self.assertRaises(d.DurableError):e.lookup(self.fd,self.req,allowed)
        (self.root/name).chmod(0o600)
        os.link(self.root/name,self.root/'foreign')
        with self.assertRaises(d.DurableError):e.lookup(self.fd,self.req,allowed)
        self.assertEqual((self.root/'foreign').read_bytes(),canonical(row)+b'\n')

    def test_symlink_directory_and_journal_refuse_without_chmod(self):
        self.root.chmod(0o755)
        with self.assertRaises(d.DurableError):d.directory(self.fd)
        self.assertEqual(self.root.stat().st_mode&0o777,0o755)
        self.root.chmod(0o700)
        sentinel=self.root/'sentinel';sentinel.write_text('foreign');sentinel.chmod(0o600)
        (self.root/(self.v['import_key']+'.json')).symlink_to(sentinel)
        with self.assertRaises(OSError):e.lookup(self.fd,self.req,allowed)
        self.assertEqual(sentinel.read_text(),'foreign')

    def test_full_graph_reserves_all_and_never_publishes(self):
        home=self.root/'source';home.mkdir()
        base,base_id,position=write_rollout(home)
        child,sid,_=write_rollout(home,base=position,label='CHILD')
        nodes=history.lineage(home,child)
        mapping=history.reserve_mapping(nodes)
        self.assertEqual(set(mapping['rollouts']),{n.rollout_id for n in nodes})
        target=self.root/'destination';target.mkdir()
        root=mapping['rollouts'][nodes[-1].rollout_id]
        inventories=[]
        for run in ['one','two']:
            stage=self.root/run
            plans=history.restore(nodes,self.root,root,stage,target,mapping=mapping)
            inventories.append([(str(dest),path.read_bytes()) for path,dest,_ in plans])
        self.assertEqual(inventories[0],inventories[1])
        self.assertEqual(list(target.iterdir()),[])
        bad=copy.deepcopy(mapping);bad['rollouts'].pop(nodes[0].rollout_id)
        with self.assertRaises(history.HistoryError):history.restore(nodes,self.root,root,self.root/'bad',target,mapping=bad)

    def test_graph_capture_uses_real_full_graph_validator(self):
        import contextlib
        home=self.root/'graph-source';home.mkdir()
        base,base_id,position=write_rollout(home,archived=True)
        child,sid,_=write_rollout(home,base=position,label='CHILD')
        package=self.root/'graph.zip'
        with contextlib.redirect_stdout(io.StringIO()):bundle.export_native('codex',sid,package,home=home)
        raw=package.read_bytes();v=dict(self.v,source_id=sid,package_size=len(raw),package_sha256=hashlib.sha256(raw).hexdigest())
        req=e.request(canonical(v))
        with package.open('rb') as src:captured=e.capture(src.fileno(),req,allowed,2**20)
        self.addCleanup(captured.close)
        prepared=e.plan(captured,req,allowed,self.fd)
        self.assertEqual(len(prepared['mapping']['rollouts']),2)
        row=e.reserve(self.fd,req,prepared,allowed)
        self.assertEqual(row['plan'],prepared)
        self.assertEqual(list(self.root.glob('.graph-*')),[])
        self.assertFalse((self.root/'sessions').exists())

    def test_reservation_unknown_after_link_retains_exact_ids(self):
        from unittest.mock import patch
        original=os.fsync
        def lost(fd):
            if fd==self.fd:raise OSError('synthetic directory fsync uncertainty')
            return original(fd)
        prepared=self.prepared()
        with patch('csl_durable.os.fsync',side_effect=lost):
            with self.assertRaises(OSError):e.reserve(self.fd,self.req,prepared,allowed)
        prior=e.lookup(self.fd,self.req,allowed)
        self.assertEqual(prior['plan'],prepared)
        self.assertEqual(e.reserve(self.fd,self.req,self.prepared(),allowed),prior)

    def test_lookup_rejects_corrupt_reserved_mapping(self):
        row=e.reserve(self.fd,self.req,self.prepared(),allowed)
        row['plan']['mapping']['owners'][self.v['source_id']]=self.v['source_id']
        (self.root/(self.v['import_key']+'.json')).write_bytes(canonical(row))
        with self.assertRaises(e.ExecutionError):e.lookup(self.fd,self.req,allowed)

    def test_post_validation_original_budget_at_capture_and_metadata_link(self):
        from unittest.mock import patch
        expired=False
        def check():
            if expired:raise e.ExecutionError('original_expired_after_read')
        original_stat=os.fstat
        def suspend(fd):
            nonlocal expired
            value=original_stat(fd);expired=True;return value
        with self.path.open('rb') as src, patch('csl_execution.os.fstat',side_effect=suspend), patch('csl_execution.os.memfd_create') as create:
            with self.assertRaisesRegex(e.ExecutionError,'original_expired_after_read'):e.capture(src.fileno(),self.req,check,2**20)
            create.assert_not_called()
        expired=False
        original=os.stat
        def suspend_name(name,*args,**kwargs):
            nonlocal expired
            value=original(name,*args,**kwargs)
            if isinstance(name,str) and name.startswith('.execution-'):expired=True
            return value
        with patch('csl_durable.os.stat',side_effect=suspend_name):
            with self.assertRaisesRegex(e.ExecutionError,'original_expired_after_read'):d.create(self.fd,'bound.json',{'v':1},check,16384)
        self.assertFalse((self.root/'bound.json').exists())

    def test_contended_reservation_refuses_then_reconciles_first_mapping(self):
        import threading
        entered,release=threading.Event(),threading.Event()
        prepared=self.prepared();result=[];calls=0
        def held():
            nonlocal calls
            calls+=1
            if calls==3:
                entered.set()
                if not release.wait(3):raise RuntimeError('fixture release missing')
        def worker():
            try:result.append(e.reserve(self.fd,self.req,prepared,held))
            except Exception as error:result.append(error)
        thread=threading.Thread(target=worker);thread.start()
        try:
            self.assertTrue(entered.wait(3))
            with self.assertRaises(BlockingIOError):e.reserve(self.fd,self.req,prepared,allowed)
        finally:
            release.set();thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result,[e.lookup(self.fd,self.req,allowed)])

    def test_no_production_importer_or_command_uses_foundation(self):
        import ast
        root=Path(__file__).resolve().parents[1]
        for source in list(root.glob('*.py'))+[root/'csl']:
            if source.name in ('csl_execution.py','csl_execution_transport.py'):continue
            tree=ast.parse(source.read_text())
            for node in ast.walk(tree):
                names=[a.name for a in node.names] if isinstance(node,ast.Import) else [node.module] if isinstance(node,ast.ImportFrom) else []
                self.assertFalse(any(name in ('csl_execution','csl_durable','csl_execution_transport') for name in names),(source.name,names))

    def test_graph_open_expiry_cannot_create_member(self):
        from unittest.mock import patch
        import contextlib
        home=self.root/'budget-graph';home.mkdir()
        base,_,position=write_rollout(home,archived=True)
        child,sid,_=write_rollout(home,base=position,label='CHILD')
        package=self.root/'budget.zip'
        with contextlib.redirect_stdout(io.StringIO()):bundle.export_native('codex',sid,package,home=home)
        scratch=self.root/'owned-graph';scratch.mkdir()
        original=zipfile.ZipFile.open;expired=False
        def suspended(archive,*args,**kwargs):
            nonlocal expired
            stream=original(archive,*args,**kwargs);expired=True;return stream
        def check():
            if expired:raise e.ExecutionError('original_expired_in_zip_open')
        with zipfile.ZipFile(package) as archive:
            entry=bundle.validate(archive)['sessions'][0]
            with patch('zipfile.ZipFile.open',new=suspended),self.assertRaisesRegex(e.ExecutionError,'original_expired_in_zip_open'):
                history.unpack(archive,entry,scratch,check=check)
        self.assertEqual(list(scratch.rglob('*.jsonl')),[])

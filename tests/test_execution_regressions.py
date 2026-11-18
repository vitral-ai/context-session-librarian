"""Actual-file/source-G2 regressions. Synthetic owned data; no native importer."""
import contextlib
import copy
import errno
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch
import zipfile

import tests.test_execution as fixture
from tests.test_codex_history import write_rollout
import csl_execution as e
import csl_durable as d
import csl_provider_bundle as bundle
from csl_backup_core import canonical


class ReviewRegressions(unittest.TestCase):
    setUp = fixture.ExecutionTests.setUp
    zip = fixture.ExecutionTests.zip
    capture = fixture.ExecutionTests.capture
    prepared = fixture.ExecutionTests.prepared

    def test_observed_reservation_disappearance_cannot_allocate_another_map(self):
        first=e.reserve(self.fd,self.req,self.prepared(),fixture.allowed)
        name=self.v['import_key']+'.json';path=self.root/name
        with path.open('rb') as old:
            original=os.stat;fired=False
            def gone(value,*args,**kw):
                nonlocal fired
                if value==name and not fired:
                    fired=True;path.unlink()
                return original(value,*args,**kw)
            with patch('csl_durable.os.stat',side_effect=gone):
                with self.assertRaises(d.DurableError):e.reserve(self.fd,self.req,self.prepared(),fixture.allowed)
            self.assertTrue(fired)
            self.assertFalse(path.exists())
            self.assertEqual(json.loads(old.read()),first)

    def test_fifo_metadata_is_refused_before_blocking_read(self):
        name=self.v['import_key']+'.json';os.mkfifo(self.root/name,0o600)
        script="""import os,sys,csl_durable as d
fd=os.open(sys.argv[1],os.O_RDONLY|os.O_DIRECTORY)
print('entered',flush=True)
try:d.read(fd,sys.argv[2],16384)
except d.DurableError:print('refused',flush=True)
else:raise SystemExit(2)
finally:os.close(fd)
"""
        child=subprocess.Popen([sys.executable,'-B','-c',script,str(self.root),name],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            out,err=child.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            child.kill();out,err=child.communicate(timeout=2)
            self.fail('FIFO blocked before descriptor qualification: '+out)
        self.assertEqual(child.returncode,0,err)
        self.assertIn('refused',out)
        self.assertTrue((self.root/name).is_fifo())

    def test_post_seal_and_post_seal_qualification_loss_close_capture(self):
        for point in [fcntl.F_ADD_SEALS,fcntl.F_GET_SEALS]:
            for loss in ['expired','revoked']:
                with self.subTest(point=point,loss=loss):
                    original=fcntl.fcntl;failed=False
                    error=e.ExecutionError('original_expired') if loss=='expired' else RuntimeError('known_native_revocation')
                    def suspend(fd,command,*args):
                        nonlocal failed
                        value=original(fd,command,*args)
                        if command==point:failed=True
                        return value
                    def check():
                        if failed:raise error
                    captured=None
                    try:
                        with patch('csl_execution.fcntl.fcntl',side_effect=suspend):
                            with self.assertRaises(type(error)) as caught:captured=self.capture(check)
                        self.assertIs(caught.exception,error)
                    finally:
                        if captured:captured.close()
                    leaks=[]
                    for fd in Path('/proc/self/fd').iterdir():
                        try:
                            if 'memfd:csl-execution-capture' in os.readlink(fd):leaks.append(str(fd))
                        except FileNotFoundError:pass
                    self.assertEqual(leaks,[])

    def member_archive(self,version,valid):
        sid=self.v['source_id'];body=(json.dumps({'type':'session_meta','payload':{'id':sid,'source':'cli','cwd':'/synthetic'}})+'\n').encode()
        sha=hashlib.sha256(body).hexdigest() if valid else '0'*64
        member='sessions/'+sid+('/main.jsonl' if version==3 else '.jsonl')
        entry=dict(id=sid,provider='codex',title='synthetic')
        if version==3:entry.update(primary=member,files=[dict(file_key='main',file=member,relative_path='session.jsonl',role='transcript',bytes=len(body),sha256=sha)])
        else:entry.update(file=member,bytes=len(body),sha256=sha)
        manifest=dict(format='codex-session-librarian',version=version,mode='raw',sessions=[entry])
        output=io.BytesIO()
        with zipfile.ZipFile(output,'w') as archive:
            archive.writestr('manifest.json',json.dumps(manifest));archive.writestr(member,body)
        raw=output.getvalue();path=self.root/'member.zip';path.write_bytes(raw)
        req=e.request(canonical(dict(self.v,package_size=len(raw),package_sha256=hashlib.sha256(raw).hexdigest())))
        with path.open('rb') as src:captured=e.capture(src.fileno(),req,fixture.allowed,2**20)
        return captured,req

    def test_every_admitted_format_requires_inner_sha_before_plan(self):
        for version in [1,2,3]:
            for valid in [False,True]:
                with self.subTest(version=version,valid=valid):
                    captured,req=self.member_archive(version,valid)
                    try:
                        if valid:
                            value=e.plan(captured,req,fixture.allowed,self.fd)
                            self.assertEqual(value['package_sha256'],req.value['package_sha256'])
                        else:
                            with self.assertRaisesRegex(ValueError,'native_checksum_mismatch'):e.plan(captured,req,fixture.allowed,self.fd)
                            self.assertEqual(list(self.root.glob('ri_*.json')),[])
                    finally:captured.close()

    def test_member_read_rechecks_original_before_reading_more(self):
        for version in [1,2,3]:
            with self.subTest(version=version):
                captured,req=self.member_archive(version,True)
                original=zipfile.ZipExtFile.read;expired=False;reads=0
                def observed(stream,*args,**kw):
                    nonlocal expired,reads
                    data=original(stream,*args,**kw)
                    if stream.name.endswith('.jsonl'):expired=True;reads+=1
                    return data
                def check():
                    if expired:raise e.ExecutionError('expired_after_member_read')
                try:
                    with patch('zipfile.ZipExtFile.read',new=observed):
                        with self.assertRaisesRegex(e.ExecutionError,'expired_after_member_read'):e.plan(captured,req,check,self.fd)
                    self.assertEqual(reads,1)
                finally:captured.close()

    @unittest.skipUnless(shutil.which('zstd'),'fixture compression requires existing zstd')
    def test_private_compressed_graph_never_looks_up_or_launches_decoder(self):
        home=self.root/'compressed-source';home.mkdir()
        base,_,position=write_rollout(home,archived=True)
        child,sid,_=write_rollout(home,base=position,label='CHILD')
        compressed=Path(str(base)+'.zst')
        compressed.write_bytes(subprocess.run([shutil.which('zstd'),'-q','-c'],input=base.read_bytes(),capture_output=True,check=True).stdout)
        base.unlink();package=self.root/'compressed.zip'
        with contextlib.redirect_stdout(io.StringIO()):bundle.export_native('codex',sid,package,home=home)
        raw=package.read_bytes();req=e.request(canonical(dict(self.v,source_id=sid,package_size=len(raw),package_sha256=hashlib.sha256(raw).hexdigest())))
        with package.open('rb') as src:captured=e.capture(src.fileno(),req,fixture.allowed,2**20)
        try:
            with patch('csl_codex_history.shutil.which',side_effect=AssertionError('ambient decoder lookup')),patch('csl_codex_history.subprocess.Popen',side_effect=AssertionError('ambient decoder launch')):
                with self.assertRaisesRegex(e.ExecutionError,'execution_compressed_graph_unsupported'):e.plan(captured,req,fixture.allowed,self.fd)
        finally:captured.close()

    def test_only_initial_open_absence_can_mean_first_use(self):
        name=self.v['import_key']+'.json'
        with self.assertRaises(d.MissingMetadata):d.read(self.fd,name,16384)
        with patch('csl_execution.durable.read',side_effect=FileNotFoundError('root lost after qualification')):
            with self.assertRaises(FileNotFoundError):e.reserve(self.fd,self.req,self.prepared(),fixture.allowed)
        self.assertFalse((self.root/name).exists())

    def test_replaced_observed_record_and_special_objects_preserve_data(self):
        first=e.reserve(self.fd,self.req,self.prepared(),fixture.allowed)
        name=self.v['import_key']+'.json';path=self.root/name
        original=os.stat;fired=False
        def replace(value,*args,**kw):
            nonlocal fired
            if value==name and not fired:
                fired=True;path.unlink();path.write_bytes(b'foreign sentinel');path.chmod(0o600)
            return original(value,*args,**kw)
        with patch('csl_durable.os.stat',side_effect=replace),self.assertRaises(d.DurableError):e.reserve(self.fd,self.req,self.prepared(),fixture.allowed)
        self.assertEqual(path.read_bytes(),b'foreign sentinel')
        special=self.root/'special';special.mkdir()
        with self.assertRaises((OSError,d.DurableError)):d.read(self.fd,'special',16384)
        special.rmdir();os.mkfifo(special,0o600)
        with self.assertRaises(d.DurableError):d.read(self.fd,'special',16384)
        (self.root/'operation.lock').unlink();os.mkfifo(self.root/'operation.lock',0o600)
        with self.assertRaises(d.DurableError):
            with d.locked(self.fd,fixture.allowed):self.fail('FIFO admitted as lock')

    def test_capture_wrap_and_final_qualification_errors_close_owned_fd(self):
        for target in ['wrap','rewind']:
            with self.subTest(target=target):
                original=os.memfd_create;created=[]
                def opened(*args):
                    fd=original(*args);created.append(fd);return fd
                fail=OSError('synthetic owned capture finalization failure')
                key='csl_execution.os.fdopen' if target=='wrap' else 'csl_execution.Capture.rewind'
                with patch('csl_execution.os.memfd_create',side_effect=opened),patch(key,side_effect=fail),self.assertRaises(OSError):self.capture()
                self.assertEqual(len(created),1)
                with self.assertRaises(OSError):fcntl.fcntl(created[0],fcntl.F_GETFD)

    def test_lock_disappearance_or_replacement_after_acquire_is_not_admission(self):
        for replace in [False,True]:
            with self.subTest(replace=replace):
                path=self.root/'operation.lock'
                if path.exists():path.unlink()
                original=fcntl.flock
                def changed(fd,flags):
                    value=original(fd,flags);path.unlink()
                    if replace:path.write_bytes(b'foreign lock');path.chmod(0o600)
                    return value
                with patch('csl_durable.fcntl.flock',side_effect=changed),self.assertRaises(d.DurableError):
                    with d.locked(self.fd,fixture.allowed):self.fail('observed lock loss admitted')
                if replace:self.assertEqual(path.read_bytes(),b'foreign lock')

    def test_authority_checker_missing_error_is_never_journal_absence(self):
        original=d.MissingMetadata('native authority source unavailable')
        calls=0
        def check():
            nonlocal calls
            calls+=1
            if calls==4:raise original  # Actual check at lookup after lock admission.
        with self.assertRaises(d.MissingMetadata) as caught:e.reserve(self.fd,self.req,self.prepared(),check)
        self.assertIs(caught.exception,original)
        self.assertFalse((self.root/(self.v['import_key']+'.json')).exists())

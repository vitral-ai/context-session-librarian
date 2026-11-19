"""R3 author regressions: owned scratch recovery/cleanup, not native recall."""
import os
from pathlib import Path
import signal
import time
import unittest
from unittest.mock import patch

from csl_backup_core import canonical
import csl_execution as execution
import csl_durable as durable
import csl_import_execution as publisher
from tests import test_execution_publisher as fixture


class R3Tests(unittest.TestCase):
    setUp=fixture.PublisherTests.setUp
    set_package=fixture.PublisherTests.set_package
    guard=fixture.PublisherTests.guard
    publish=fixture.PublisherTests.publish
    row=fixture.PublisherTests.row
    query=fixture.PublisherTests.query
    installed=fixture.PublisherTests.installed
    kill_at=fixture.PublisherTests.kill_at

    def key(self,n):
        self.value['execution_id']='re_'+format(n,'064x')
        self.value['import_key']='ri_'+format(n,'064x')
        self.req=execution.request(canonical(self.value))

    def test_umask002_and027_have_explicit_private_components(self):
        self.set_package(graph=True)
        for n,mask in enumerate((0o002,0o027),2000):
            with self.subTest(umask=oct(mask)):
                self.key(n)
                previous=os.umask(mask)
                try:
                    def modes(point):
                        if point=='workspace_rewritten':
                            for spec in (self.row()['planning'],self.row()['workspace']):
                                for root,dirs,files in os.walk(self.journal/spec['name']):
                                    self.assertEqual(os.stat(root).st_mode&0o777,0o700)
                                    for name in files:self.assertEqual(os.stat(Path(root)/name).st_mode&0o777,0o600)
                    receipt=self.publish(fault=modes)
                finally:os.umask(previous)
                self.assertEqual(self.query()['receipt'],receipt)
                self.assertFalse((self.journal/self.row()['planning']['name']).exists())
                self.assertFalse((self.journal/self.row()['workspace']['name']).exists())

    def test_actual_mid_planning_and_rewrite_kills_reset_only_owned_entries(self):
        self.set_package(graph=True)
        points=[('scratch_written_planning',1),('scratch_written_planning',2),
                ('scratch_written_workspace',1),('scratch_written_workspace',4),('scratch_written_workspace',9)]
        for n,(point,occurrence) in enumerate(points,2100):
            with self.subTest(point=point,occurrence=occurrence):
                self.key(n);self.kill_at(point,occurrence)
                prior=self.row()
                receipt=self.publish()
                if prior['plan'] is not None:self.assertEqual(receipt['imported_native_id'],prior['plan']['mapping']['owners'][self.sid])
                self.assertEqual(self.query()['receipt'],receipt)
                for field in ('planning','workspace'):
                    self.assertEqual(self.row()[field]['phase'],'retired')
                    self.assertFalse((self.journal/self.row()[field]['name']).exists())

    def test_unacknowledged_child_or_root_stays_bounded_pending(self):
        self.set_package(graph=True)
        for n,point in enumerate(('scratch_root_created_planning','scratch_created_planning','scratch_created_workspace'),2200):
            with self.subTest(point=point):
                self.key(n);self.kill_at(point)
                before=sorted(p.name for p in self.journal.iterdir())
                with self.assertRaises(publisher.Pending):self.publish()
                with self.assertRaises(publisher.Pending):self.publish()
                self.assertEqual(before,sorted(p.name for p in self.journal.iterdir()))
                self.assertIsNone(self.query()['receipt'])

    def test_partial_reset_rejects_foreign_extra_and_replaced_inode(self):
        self.set_package(graph=True);self.kill_at('scratch_written_workspace',1)
        spec=self.row()['workspace'];root=self.journal/spec['name']
        sentinel=root/'foreign-sentinel';sentinel.write_bytes(b'foreign-owned-fixture');sentinel.chmod(0o600)
        with self.assertRaisesRegex(publisher.Pending,'unknown_entry'):self.publish()
        self.assertEqual(sentinel.read_bytes(),b'foreign-owned-fixture')
        sentinel.unlink()  # This test alone owns the deliberate extra fixture.
        e=next(e for e in spec['entries'] if e['kind']=='file' and e['phase']=='owned')
        selected=root/e['path'];replacement=selected.with_name('foreign-replacement')
        replacement.write_bytes(selected.read_bytes());replacement.chmod(0o600)
        ino=replacement.stat().st_ino;os.replace(replacement,selected)
        with self.assertRaisesRegex(publisher.Pending,'entry_changed'):self.publish()
        self.assertEqual(selected.stat().st_ino,ino)
        self.assertEqual(self.installed(),[])

    def test_receipt_cleanup_sigkill_matrix_preserves_receipt_and_erases_copies(self):
        self.set_package(graph=True)
        cuts=[('scratch_unlinked_planning',1),('scratch_unlinked_planning',3),
              ('scratch_root_unlinked_planning',1),('scratch_unlinked_workspace',1),
              ('scratch_unlinked_workspace',5),('scratch_root_unlinked_workspace',1)]
        for n,(point,occurrence) in enumerate(cuts,2300):
            with self.subTest(point=point,occurrence=occurrence):
                self.key(n);self.kill_at(point,occurrence)
                receipt=self.row()['receipt'];self.assertIsNotNone(receipt)
                self.assertEqual(self.query()['receipt'],receipt)
                self.assertEqual(self.publish(),receipt)
                for field in ('planning','workspace'):
                    spec=self.row()[field]
                    self.assertEqual(spec['phase'],'retired')
                    self.assertFalse((self.journal/spec['name']).exists())

    def test_owned_reset_sigkill_and_authority_loss_do_not_renew_original(self):
        self.set_package(graph=True);self.kill_at('scratch_written_workspace',4)
        self.kill_at('scratch_unlinked_workspace',1)
        receipt=self.publish();self.assertEqual(self.query()['receipt'],receipt)
        self.key(2400);self.kill_at('scratch_written_workspace',1)
        before=self.row()
        with self.assertRaisesRegex(execution.ExecutionError,'known_revoke'):
            self.publish(check=lambda:(_ for _ in ()).throw(execution.ExecutionError('known_revoke')))
        self.assertEqual(self.row(),before)

    def test_postreceipt_foreign_or_failure_never_deletes_or_downgrades(self):
        self.set_package(graph=True)
        def foreign(point):
            if point=='provider_complete':
                p=self.journal/self.row()['planning']['name']/'unrelated'
                p.write_bytes(b'preserve');p.chmod(0o600)
        with self.assertRaisesRegex(publisher.Pending,'unknown_entry'):self.publish(fault=foreign)
        receipt=self.row()['receipt'];self.assertIsNotNone(receipt)
        p=self.journal/self.row()['planning']['name']/'unrelated'
        self.assertEqual(p.read_bytes(),b'preserve')
        self.assertEqual(self.query()['receipt'],receipt)
        p.unlink()  # Deliberate test-owned sentinel, no product cleanup/adoption.
        def failure(point):
            if point=='scratch_unlinked_planning':raise PermissionError('synthetic cleanup failure')
        with self.assertRaises(PermissionError):self.publish(fault=failure)
        self.assertEqual(self.query()['receipt'],receipt)
        self.assertEqual(self.publish(),receipt)

    def test_foreign_replacement_during_cleanup_admission_is_not_unlinked(self):
        opened=False;changed=False;foreign_inode=None;selected=None
        real_open=os.open
        def observe(*args,**kwargs):
            nonlocal opened,selected
            fd=real_open(*args,**kwargs)
            if isinstance(args[0],str) and args[0].endswith('.jsonl'):
                row=self.row()
                spec=row['planning']
                if row['receipt'] is not None and spec is not None and spec['phase']=='clearing':
                    e=next((e for e in spec['entries'] if e['kind']=='file' and e['phase']=='deleting' and Path(e['path']).name==args[0]),None)
                    if e is not None:opened=True;selected=self.journal/spec['name']/e['path']
            return fd
        def admission():
            nonlocal changed,foreign_inode
            self.guard()()
            if opened and not changed:
                changed=True
                other=selected.with_name('deliberate-foreign')
                other.write_bytes(b'preserve foreign');other.chmod(0o600)
                foreign_inode=other.stat().st_ino;os.replace(other,selected)
        self.set_package(graph=True)
        with patch.object(publisher.os,'open',side_effect=observe):
            with self.assertRaises((publisher.Pending,durable.DurableError)):
                self.publish(check=admission)
        self.assertTrue(changed)
        self.assertEqual(selected.stat().st_ino,foreign_inode)
        self.assertEqual(selected.read_bytes(),b'preserve foreign')
        self.assertIsNotNone(self.query()['receipt'])

    def test_prereceipt_lookup_reconciles_actual_writer_without_writes(self):
        def pause(point):
            if point=='prepared':raise RuntimeError('pause')
        with self.assertRaises(RuntimeError):self.publish(fault=pause)
        read,write=os.pipe();done,send=os.pipe();pid=os.fork()
        if pid==0:
            os.close(write);os.close(done)
            try:os.read(read,1);self.publish();os.write(send,b'x')
            except BaseException:os._exit(99)
            finally:os.close(read);os.close(send)
            os._exit(0)
        os.close(read);os.close(send)
        real=publisher._current;calls=0
        def race(fd,req,row,check):
            nonlocal calls
            calls+=1
            if calls==1:
                os.write(write,b'x');self.assertEqual(os.read(done,1),b'x')
                self.assertEqual(os.waitpid(pid,0)[1],0)
            return real(fd,req,row,check)
        try:
            with patch.object(publisher,'_current',side_effect=race),patch.object(publisher,'_save',side_effect=AssertionError('readonly')):
                result=self.query()
            self.assertEqual(result['state'],'complete');self.assertIsNotNone(result['receipt'])
            self.assertEqual(calls,2)
        finally:os.close(write);os.close(done)


if __name__=='__main__':unittest.main()

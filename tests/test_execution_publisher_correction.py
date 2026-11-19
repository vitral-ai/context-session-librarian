"""Author regressions for externally reported publisher corrections; not review."""
import hashlib
import os
from pathlib import Path
import signal
import time
import unittest
from unittest.mock import patch

import csl_durable as durable
import csl_execution as execution
import csl_import_execution as publisher
from tests import test_execution_publisher as fixture


class CorrectionTests(unittest.TestCase):
    setUp=fixture.PublisherTests.setUp
    set_package=fixture.PublisherTests.set_package
    guard=fixture.PublisherTests.guard
    publish=fixture.PublisherTests.publish
    row=fixture.PublisherTests.row
    query=fixture.PublisherTests.query
    installed=fixture.PublisherTests.installed
    kill_at=fixture.PublisherTests.kill_at

    def wait(self,pid):
        end=time.monotonic()+15
        while time.monotonic()<end:
            got,status=os.waitpid(pid,os.WNOHANG)
            if got:return status
            time.sleep(.01)
        os.kill(pid,signal.SIGKILL);os.waitpid(pid,0)
        self.fail('owned synthetic child did not finish')

    def test_lock_replacement_during_admission_cannot_truncate_second_receipt(self):
        opened=False;replaced=False;completed=None
        real_open=os.open
        def observe(*args,**kwargs):
            nonlocal opened
            fd=real_open(*args,**kwargs)
            if isinstance(args[0],str) and args[0].startswith('.csl-ri_') and args[1]&os.O_WRONLY and not args[1]&os.O_CREAT:
                opened=True
            return fd
        def admission():
            nonlocal replaced,completed
            self.guard()()
            if opened and not replaced:
                replaced=True
                (self.journal/'operation.lock').rename(self.journal/'original-owned-lock')
                pid=os.fork()
                if pid==0:
                    try:self.publish()
                    except BaseException:os._exit(99)
                    os._exit(0)
                self.assertEqual(self.wait(pid),0)
                completed=self.installed()[0].read_bytes()
        with patch.object(publisher.os,'open',side_effect=observe):
            with self.assertRaisesRegex(durable.DurableError,'lock_replaced'):
                self.publish(check=admission)
        self.assertTrue(replaced)
        self.assertIsNotNone(completed)
        self.assertTrue(completed)
        self.assertEqual(self.installed()[0].read_bytes(),completed)
        self.assertEqual(self.query()['state'],'complete')

    def test_allocating_absence_recovers_for_each_member_and_writing_occurrence(self):
        self.set_package(graph=True)
        for i,occurrence in enumerate((2,3,5,6,8,9)):
            with self.subTest(occurrence=occurrence):
                self.value['execution_id']='re_'+format(i+1000,'064x')
                self.value['import_key']='ri_'+format(i+1000,'064x')
                from csl_backup_core import canonical
                self.req=execution.request(canonical(self.value))
                self.kill_at('metadata_preparing_renamed',occurrence)
                prior=self.row()
                allocating=[m for m in prior['members'] if m['phase']=='allocating']
                for m in allocating:
                    self.assertFalse(((self.home/m['path']).parent/m['keeper']).exists())
                receipt=self.publish()
                self.assertEqual(receipt['imported_native_id'],prior['plan']['mapping']['owners'][self.sid])
                self.assertEqual(self.row()['state'],'complete')
                self.assertEqual(len(list(self.journal.glob('.publisher-'+self.value['import_key']))),0)
                self.assertEqual(self.row()['workspace']['phase'],'retired')

    def test_existing_unacknowledged_keeper_remains_pending_and_preserved(self):
        self.kill_at('keeper_created_0')
        row=self.row();m=row['members'][0]
        keeper=(self.home/m['path']).parent/m['keeper'];inode=keeper.stat().st_ino
        with self.assertRaisesRegex(publisher.Pending,'uncommitted'):self.publish()
        self.assertEqual(keeper.stat().st_ino,inode)
        self.assertEqual(self.installed(),[])

    def test_owned_rewrite_kill_recovers_and_cleans_copies(self):
        self.set_package(graph=True)
        self.kill_at('workspace_rewritten')
        prior=self.row()
        result=self.publish()
        self.assertEqual(result['imported_native_id'],prior['plan']['mapping']['owners'][self.sid])
        for _ in range(3):self.assertEqual(self.publish(),result)
        self.assertEqual(len(list(self.journal.glob('.publisher-*'))),0)
        self.assertEqual(len(list(self.journal.glob('.planning-*'))),0)
        self.assertEqual(list(self.journal.glob('.graph-*')),[])
        self.assertEqual(len(self.installed()),3)

    def test_owned_planning_kill_recovers(self):
        self.set_package(graph=True)
        self.kill_at('planning_fsynced')
        self.assertEqual(self.query(),{'state':'planning','receipt':None})
        receipt=self.publish()
        self.assertEqual(self.query()['receipt'],receipt)
        self.assertEqual(len(list(self.journal.glob('.planning-*'))),0)

    def test_move_held_destination_parent_during_member_advance_fences_link(self):
        seen=0;moved=False
        def move(point):
            nonlocal seen,moved
            if point=='metadata_publishing_renamed':
                seen+=1
                if seen==2:
                    moved=True
                    (self.home/'sessions').rename(self.home/'old-sessions')
                    (self.home/'sessions').mkdir(mode=0o700)
        with self.assertRaisesRegex(publisher.Pending,'destination_parent_changed'):
            self.publish(fault=move)
        self.assertTrue(moved)
        self.assertEqual(self.installed(),[])
        self.assertEqual(self.row()['members'][0]['phase'],'publishing')
        self.assertIsNone(self.row()['receipt'])

    def test_same_content_journal_replacement_keeps_foreign_inode_unmodified(self):
        replacement=None
        def replace(point):
            nonlocal replacement
            if point=='prepared':
                path=self.journal/(self.value['import_key']+'.json')
                other=self.journal/'replacement-fixture'
                other.write_bytes(path.read_bytes());other.chmod(0o600)
                replacement=other.stat().st_ino
                os.replace(other,path)
        with self.assertRaisesRegex(publisher.Pending,'journal_inode_changed'):
            self.publish(fault=replace)
        path=self.journal/(self.value['import_key']+'.json')
        self.assertEqual(path.stat().st_ino,replacement)
        self.assertEqual(self.row()['state'],'prepared')
        self.assertEqual(self.installed(),[])

    def test_repeated_own_replacement_retains_only_bounded_owned_row_fds(self):
        counts=[]
        def count(point):
            if point.startswith('metadata_') and point.endswith('_renamed'):
                row_fds=[]
                for name in os.listdir('/proc/self/fd'):
                    try:target=os.readlink('/proc/self/fd/'+name)
                    except FileNotFoundError:continue
                    if target.startswith(str(self.journal)) and (target.endswith('.json') or '.tmp' in target or '.json (deleted)' in target):row_fds.append(name)
                counts.append(len(row_fds))
        self.set_package(graph=True)
        self.publish(fault=count)
        self.assertTrue(counts)
        self.assertLessEqual(max(counts),2)

    def test_lookup_real_process_retirement_race_retries_without_any_writes(self):
        def pause(point):
            if point=='receipted':raise RuntimeError('pause')
        with self.assertRaises(RuntimeError):self.publish(fault=pause)
        receipt=self.row()['receipt']
        start_r,start_w=os.pipe();done_r,done_w=os.pipe()
        pid=os.fork()
        if pid==0:
            os.close(start_w);os.close(done_r)
            try:
                os.read(start_r,1)
                self.publish()
                os.write(done_w,b'x')
            except BaseException:os._exit(99)
            finally:os.close(start_r);os.close(done_w)
            os._exit(0)
        os.close(start_r);os.close(done_w)
        real_verify=publisher._verify_installed;calls=0;settled=None;stale_failure=None
        def race(home,row,check):
            nonlocal calls,settled,stale_failure
            calls+=1
            if calls==1:
                os.write(start_w,b'x')
                self.assertEqual(os.read(done_r,1),b'x')
                self.assertEqual(self.wait(pid),0)
                path=self.journal/(self.value['import_key']+'.json')
                settled=(path.stat().st_ino,path.read_bytes())
            try:return real_verify(home,row,check)
            except publisher.Pending as failure:
                if calls==1:stale_failure=str(failure)
                raise
        try:
            with patch.object(publisher,'_verify_installed',side_effect=race),patch.object(publisher,'_save',side_effect=AssertionError('lookup write')),patch.object(publisher.os,'mkdir',side_effect=AssertionError('lookup mkdir')),patch.object(publisher.os,'link',side_effect=AssertionError('lookup link')),patch.object(publisher.os,'unlink',side_effect=AssertionError('lookup unlink')),patch.object(publisher.os,'replace',side_effect=AssertionError('lookup replace')):
                self.assertEqual(self.query(),{'state':'complete','receipt':receipt})
            self.assertEqual(calls,2)
            self.assertEqual(stale_failure,'execution_keeper_missing')
            path=self.journal/(self.value['import_key']+'.json')
            self.assertEqual((path.stat().st_ino,path.read_bytes()),settled)
        finally:os.close(start_w);os.close(done_r)

    def test_lookup_does_not_hide_unchanged_missing_keeper_corruption(self):
        def pause(point):
            if point=='receipted':raise RuntimeError('pause')
        with self.assertRaises(RuntimeError):self.publish(fault=pause)
        m=self.row()['members'][0]
        ((self.home/m['path']).parent/m['keeper']).unlink()
        with self.assertRaisesRegex(publisher.Pending,'keeper_missing'):self.query()


if __name__=='__main__':unittest.main()

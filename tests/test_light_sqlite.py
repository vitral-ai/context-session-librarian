"""Disk lock recovery and actual retry-loop maintenance; no external services."""
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

import csl_light as m
from csl_bridge_store import Store, encode

class Survival(unittest.TestCase):
 def setUp(self):
  m.STOP.clear();self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.j=m.Journal(Path(self.temp.name)/'journal/light.sqlite');self.block=None;self.thread=None;self.channel=None
  self.addCleanup(self.cleanup)
 def cleanup(self):
  if self.block:self.block.rollback();self.block.close();self.block=None
  m.STOP.set()
  if self.channel:self.channel.close()
  if self.thread:self.thread.join(2)
  m.STOP.clear()
 def start_locked(self,target):
  real_connect=sqlite3.connect;self.block=real_connect(self.j.path);self.block.execute('BEGIN EXCLUSIVE');self.busy=threading.Event()
  def short_timeout(*a,**k):k['timeout']=0.02;return real_connect(*a,**k)
  self.context=patch.object(m.sqlite3,'connect',side_effect=short_timeout);self.context.start();self.addCleanup(self.context.stop)
  self.logging=patch.object(m,'log',side_effect=lambda event,**fields:self.busy.set()if event=='journal_busy'else None);self.logging.start();self.addCleanup(self.logging.stop)
  self.thread=threading.Thread(target=target,daemon=True);self.thread.start();self.assertTrue(self.busy.wait(2),'busy failure did not stay in loop');self.assertTrue(self.thread.is_alive())
  self.block.rollback();self.block.close();self.block=None
 def test_pump_survives_real_disk_lock_and_sends_same_queued_id(self):
  id=str(uuid.uuid4());self.j.put('out','hub',id,{'type':'request','id':id});writer=io.BytesIO();node=SimpleNamespace(journal=self.j,config={'node':'guest'},advertise=lambda:{})
  self.channel=m.Channel(node,'hub',io.BytesIO(),writer,lambda:None)
  self.start_locked(self.channel.pump)
  end=time.monotonic()+3
  while id.encode()not in writer.getvalue()and time.monotonic()<end:time.sleep(.01)
  self.assertIn(id.encode(),writer.getvalue());self.assertTrue(self.thread.is_alive());self.assertEqual(len(self.j.pending('out','hub')),1)
 def test_pump_hello_database_read_survives_real_disk_lock(self):
  def advertise():
   with self.j.db()as d:d.execute('SELECT count(*) FROM rpc').fetchone()
   return {}
  writer=io.BytesIO();self.channel=m.Channel(SimpleNamespace(journal=self.j,config={'node':'guest'},advertise=advertise),'hub',io.BytesIO(),writer,lambda:None)
  self.start_locked(self.channel.pump)
  end=time.monotonic()+3
  while b'hello'not in writer.getvalue()and time.monotonic()<end:time.sleep(.01)
  self.assertIn(b'hello',writer.getvalue());self.assertTrue(self.thread.is_alive())
 def node(self, journal):
  node=object.__new__(m.Node);node.journal=journal;node.agents=lambda:{};node.config={'node':'guest','retention_days':0}
  for name in ('sync_links','pump_local','finish_pending','queue_events'):setattr(node,name,lambda:None)
  return node
 def test_retry_loop_survives_real_disk_lock_and_schedules_original(self):
  id=str(uuid.uuid4());self.j.put('in','hub',id,{'id':id});node=self.node(self.j);scheduled=[];node.schedule=lambda p,i:scheduled.append((p,i))
  self.start_locked(node.retry_loop)
  end=time.monotonic()+3
  while not scheduled and time.monotonic()<end:time.sleep(.01)
  self.assertEqual(scheduled[0],('hub',id));self.assertTrue(self.thread.is_alive());self.assertEqual(len(self.j.pending('in','hub')),1)
 def test_non_lock_operational_errors_are_not_silenced(self):
  journal=SimpleNamespace(pending=lambda *a:(_ for _ in ()).throw(sqlite3.OperationalError('no such table: rpc')))
  node=self.node(journal)
  with self.assertRaisesRegex(sqlite3.OperationalError,'no such table'):node.retry_loop()
  journal=SimpleNamespace(ready_out=lambda *a:(_ for _ in ()).throw(sqlite3.OperationalError('no such table: rpc')))
  self.channel=m.Channel(SimpleNamespace(journal=journal,config={'node':'guest'},advertise=lambda:{}),'hub',io.BytesIO(),io.BytesIO(),lambda:None)
  with self.assertRaisesRegex(sqlite3.OperationalError,'no such table'):self.channel.pump()

class UpstreamPreservation(unittest.TestCase):
    def test_actual_retry_loop_preserves_local_receipt_retention_and_link_reload(self):
        m.STOP.clear()
        with tempfile.TemporaryDirectory(prefix='g2-upstream-',dir='/var/tmp') as directory:
            root=Path(directory)
            cfg={'node':'hub','home':str(root/'light'),'bridges':{},'links':{},'retention_days':1}
            node=m.Node(cfg);node.home.joinpath('config.json').write_text(encode(cfg))
            local_id=str(uuid.uuid4());result={'ok':True,'data':{'state':'persisted'}}
            node.journal.put('out','hub',local_id,{'id':local_id});node.journal.put('in','hub',local_id,{'id':local_id});node.journal.finish('in','hub',local_id,result)
            old_id=str(uuid.uuid4());node.journal.put('out','remote',old_id,{'id':old_id});node.journal.finish('out','remote',old_id,result)
            with node.journal.db() as db:db.execute('UPDATE rpc SET updated=? WHERE id=?',(time.time()-2*86400,old_id))
            changed={**cfg,'links':{'remote':{'agents':['remote-worker'],'agent_threads':{'remote-worker':'synthetic-thread'},'connect_command':['SYNTHETIC-NOT-EXECUTED']}}}
            node.home.joinpath('config.json').write_text(encode(changed))
            started=threading.Event();node.ssh_loop=lambda *args:started.set()
            errors=[]
            def run():
                try:node.retry_loop()
                except BaseException as error:errors.append(repr(error))
            real_monotonic = time.monotonic
            clock_start = real_monotonic()
            with patch.object(m.time, 'monotonic', side_effect=lambda: 4000 + real_monotonic() - clock_start):
                thread=threading.Thread(target=run,daemon=True);thread.start()
                time.sleep(1.25);m.STOP.set();thread.join(2)
            try:
                with node.journal.db() as db:
                    old_rows=db.execute('SELECT count(*) FROM rpc WHERE id=?',(old_id,)).fetchone()[0]
                    tombstones=db.execute('SELECT count(*) FROM tombstones WHERE id=?',(old_id,)).fetchone()[0]
                observed={'local_out_state':node.journal.get('out','hub',local_id)['state'],'old_payload_rows':old_rows,'old_tombstones':tombstones,'link_started':started.is_set(),'errors':errors}
                print('UPSTREAM_PRESERVATION',json.dumps(observed),flush=True)
                self.assertEqual(observed,{'local_out_state':'done','old_payload_rows':0,'old_tombstones':1,'link_started':True,'errors':[]})
            finally:node.pool.shutdown(wait=True);m.STOP.clear()

    def test_actual_retry_loop_at_low_uptime_waits_for_original_prune_threshold(self):
        with tempfile.TemporaryDirectory(prefix='csl-low-uptime-') as directory:
            root = Path(directory)
            cfg = {'node': 'hub', 'home': str(root / 'light'), 'bridges': {},
                   'links': {}, 'retention_days': 1}
            node = m.Node(cfg)
            node.home.joinpath('config.json').write_text(encode(cfg))
            self.addCleanup(node.pool.shutdown, True)
            identifier = str(uuid.uuid4())
            node.journal.put('out', 'remote', identifier, {'id': identifier})
            node.journal.finish('out', 'remote', identifier, {'ok': True})
            with node.journal.db() as db:
                db.execute('UPDATE rpc SET updated=? WHERE id=?',
                           (time.time() - 2 * 86400, identifier))
            ticks = [100, 3600, 3601, 3602]
            observations = []
            def wait(seconds):
                self.assertEqual(seconds, 1)
                with node.journal.db() as db:
                    rows = db.execute('SELECT count(*) FROM rpc WHERE id=?', (identifier,)).fetchone()[0]
                    tombstones = db.execute('SELECT count(*) FROM tombstones WHERE id=?',
                                            (identifier,)).fetchone()[0]
                observations.append((ticks[len(observations)], rows, tombstones))
            stop = SimpleNamespace(is_set=lambda: len(observations) == len(ticks), wait=wait)
            with patch.object(m, 'STOP', stop), \
                 patch.object(m.time, 'monotonic', side_effect=lambda: ticks[len(observations)]), \
                 patch.object(node.journal, 'prune', wraps=node.journal.prune) as prune:
                node.retry_loop()
            self.assertEqual(observations, [(100, 1, 0), (3600, 1, 0),
                                            (3601, 0, 1), (3602, 0, 1)])
            prune.assert_called_once_with(1)

    def test_actual_retry_loop_collects_and_queues_synthetic_completion(self):
        m.STOP.clear()
        with tempfile.TemporaryDirectory(prefix='g2-events-',dir='/var/tmp') as directory:
            root=Path(directory);home=root/'bridge';thread_id=str(uuid.uuid4());turn_id=str(uuid.uuid4());task_id=str(uuid.uuid4())
            store=Store(home,create=True)
            with store.transaction() as db:
                db.execute('INSERT INTO agents VALUES(?,?)',('worker',thread_id))
                db.execute('INSERT INTO config VALUES(?,?)',('identity',encode('guest')))
                db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?)',(task_id,'hub','coordinator','worker',turn_id,'running'))
            request = {'version':1,'op':'followup','id':task_id,'source':'coordinator','target':'worker','text':'synthetic original'}
            receipt = {'state':'delivered','request_id':task_id,'turn_id':turn_id,'thread_id':thread_id}
            store.reserve('hub', request)
            store.operation(task_id, 'delivered', receipt)
            store.close()
            cfg={'node':'guest','home':str(root/'light'),'bridges':{'guest':{'home':str(home),'agents':['worker']}},'links':{}}
            node=m.Node(cfg);node.home.joinpath('config.json').write_text(encode(cfg))
            node.journal.put('in','hub',task_id,{'type':'request','id':task_id,'request':request,'origin':'hub'});node.journal.finish('in','hub',task_id,{'ok':True,'data':receipt})
            calls=[]
            class SyntheticEndpoint:
                def call(self,method,params):
                    calls.append(method)
                    assert method=='thread/turns/list'
                    return {'data':[{'id':turn_id,'status':'completed','items':[{'type':'agentMessage','text':'synthetic final'}]}]}
            node.native.client=lambda *args:SyntheticEndpoint()
            thread=threading.Thread(target=node.retry_loop,daemon=True);thread.start();time.sleep(1.25);m.STOP.set();thread.join(2)
            try:
                store=Store(home)
                with store.transaction() as db:status=db.execute('SELECT status FROM tasks WHERE id=?',(task_id,)).fetchone()[0];outbound=db.execute('SELECT count(*) FROM outbound').fetchone()[0]
                store.close()
                observed={'task_status':status,'outbound_events':outbound,'journal_events':len(node.journal.pending('out')),'endpoint_calls':calls}
                print('UPSTREAM_EVENTS',json.dumps(observed),flush=True)
                self.assertEqual(status,'completed');self.assertEqual(outbound,1);self.assertEqual(len(node.journal.pending('out')),1);self.assertEqual(calls,['thread/turns/list'])
            finally:node.pool.shutdown(wait=True);m.STOP.clear()


class MaintenanceOrder(unittest.TestCase):
    def test_retry_preserves_order_prune_cadence_and_existing_error_boundaries(self):
        calls = []
        node = object.__new__(m.Node)
        node.config = {'node': 'guest', 'retention_days': 1}
        node.journal = SimpleNamespace(
            prune=lambda days: calls.append(('prune', days)),
            pending=lambda direction: calls.append(('pending', direction)) or [{'peer': 'hub', 'id': 'fixture'}],
            route=lambda *args: calls.append(('route', args)))
        sync_attempts = 0
        def sync_links():
            nonlocal sync_attempts
            sync_attempts += 1
            calls.append(('sync_links',))
            if sync_attempts == 1:
                raise ValueError('synthetic malformed config')
        def finish_pending():
            calls.append(('finish_pending',))
            raise RuntimeError('synthetic endpoint unavailable')
        node.sync_links = sync_links
        node.pump_local = lambda: calls.append(('pump_local',))
        node.schedule = lambda *args: calls.append(('schedule', args))
        node.finish_pending = finish_pending
        node.queue_events = lambda: calls.append(('queue_events',))
        node.agents = lambda: calls.append(('agents',)) or {'worker': 'thread'}
        stopped = threading.Event()
        def wait(seconds):
            calls.append(('wait', seconds))
            if sum(item[0] == 'wait' for item in calls) == 2:
                stopped.set()
        with patch.object(m, 'STOP', SimpleNamespace(is_set=stopped.is_set, wait=wait)), \
             patch.object(m.time, 'monotonic', return_value=4000), patch.object(m, 'log') as log:
            node.retry_loop()
        self.assertEqual([item[0] for item in calls], [
            'prune', 'sync_links', 'pending', 'schedule', 'finish_pending',
            'queue_events', 'agents', 'route', 'wait',
            'sync_links', 'pump_local', 'pending', 'schedule', 'finish_pending',
            'queue_events', 'agents', 'route', 'wait'])
        self.assertEqual([item for item in calls if item[0] == 'prune'], [('prune', 1)])
        self.assertEqual([item for item in calls if item[0] == 'wait'], [('wait', 1), ('wait', 1)])
        self.assertEqual([call.args[0] for call in log.call_args_list], [
            'config_reload_deferred', 'event_delivery_deferred', 'event_delivery_deferred'])

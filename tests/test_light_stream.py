"""Real duplex framing/SQLite, synthetic native endpoint, no SSH or model calls."""
import contextlib
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import csl_light as light
from csl_bridge_store import Store, encode
from csl_bridge_engine import event_id
from enroll_light import initialize, atomic, adopt_peers
ROOT = Path(__file__).resolve().parents[1]


class StreamTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.nodes = []; self.connections = []
        self.host = self.make('hub', 'coordinator', True)
        self.guest = self.make('env-a', 'worker', False)
        self.host.config['links'] = {'env-a': {'agents': ['worker']}}
        self.guest.config.update(upstream='hub', links={'hub': {'agents': ['coordinator'], 'trusted_router': True}})
        self.host.journal.route('worker','env-a',self.guest.agents()['worker'])
        self.guest.journal.route('coordinator','hub',self.host.agents()['coordinator'])
        self.native_calls=[]
        self.guest.native.client=lambda home:object()
        self.guest.native.inject=lambda agent,r,session:self.native_calls.append(r['id'])or{
            'state':'delivered','request_id':r['id'],'thread_id':self.guest.agents()['worker'],'turn_id':self.turn}
        self.host.native.inject=lambda *args:{'state':'delivered'}
        self.turn=str(uuid.uuid4())
        self.addCleanup(self.cleanup)

    def make(self,node,agent,router):
        home=self.root/(node+'-bridge');store=Store(home,create=True)
        with store.transaction()as db:
            db.executemany('INSERT INTO config VALUES(?,?)',[('identity',encode(node)),('runtime_socket','null')]);db.execute('INSERT INTO agents VALUES(?,?)',(agent,str(uuid.uuid4())))
        store.close();cfg=initialize(self.root/node,node,home,[agent],router);config=json.loads((self.root/node/'config.json').read_text());result=light.Node(config);self.nodes.append(result);return result

    def connect(self):
        a,b=socket.socketpair()
        channels=[]
        for node,peer,sock in[(self.host,'env-a',a),(self.guest,'hub',b)]:
            reader=sock.makefile('rb');writer=sock.makefile('wb',buffering=0)
            channel=light.Channel(node,peer,reader,writer,lambda sock=sock:sock.shutdown(socket.SHUT_RDWR));thread=threading.Thread(target=channel.run,daemon=True);thread.start();channels.append((channel,thread,sock,reader,writer))
        self.connections.extend(channels)

    def cleanup(self):
        for c,_,_,_,_ in self.connections:c.close()
        for c,t,s,r,w in self.connections:
            t.join(2);r.close();w.close();s.close()
        for node in self.nodes:node.pool.shutdown(wait=True)

    def test_followup_and_final_push_survive_ack_loss_and_use_existing_event_envelope(self):
        self.connect();identifier=str(uuid.uuid4());request={'version':1,'op':'followup','id':identifier,'source':'coordinator','target':'worker','text':'synthetic work'}
        result=self.host.exchange('env-a',request,'hub',timeout=3);self.assertEqual(result['data']['state'],'delivered');self.assertEqual(self.native_calls,[identifier])
        guest_home=self.guest.config['bridges']['env-a']['home'];store=Store(guest_home)
        final={'task_id':identifier,'source':'worker','target':'coordinator','kind':'final','text':'synthetic complete','turn_id':self.turn,'status':'completed'}
        eid=event_id(identifier+'/'+self.turn+'/final')
        store.publish('hub',final,eid);store.close();self.guest.queue_events()
        until=time.monotonic()+3
        while time.monotonic()<until:
            row=self.host.journal.get('in','env-a',eid)
            if row and row['state']=='done':break
            time.sleep(.02)
        self.assertEqual(json.loads(row['result'])['data']['state'],'delivered')
        home=self.host.config['bridges']['hub']['home'];store=Store(home)
        try:self.assertEqual(len(store.rows('SELECT * FROM inbound WHERE id=?',(eid,))),1)
        finally:store.close()
        # Simulate lost response and restart transport: committed receiver result
        # returns without re-injecting the original native work.
        with self.host.journal.db()as db:db.execute("UPDATE rpc SET state='queued',result=NULL WHERE direction='out' AND id=?",(identifier,))
        for c,*_ in self.connections:c.close()
        self.connect()
        result=self.host.exchange('env-a',request,'hub',timeout=3)
        self.assertEqual(result['data']['state'],'delivered');self.assertEqual(self.native_calls,[identifier])

    def test_event_with_wrong_turn_is_refused_without_stalling_next_event(self):
        self.connect();i=str(uuid.uuid4());r={'version':1,'op':'followup','id':i,'source':'coordinator','target':'worker','text':'synthetic'};self.host.exchange('env-a',r,'hub',timeout=3)
        event={'id':str(uuid.uuid4()),'task_id':i,'source':'worker','target':'coordinator','kind':'final','text':'wrong turn','turn_id':str(uuid.uuid4()),'status':'completed'}
        with self.assertRaisesRegex(ValueError,'turn differs'):self.host.dispatch_event('env-a',{'type':'event','id':event['id'],'event':event})
        event.update(id=str(uuid.uuid4()),turn_id=self.turn,text='right turn')
        self.assertEqual(self.host.dispatch_event('env-a',{'type':'event','id':event['id'],'event':event})['state'],'delivered')

    def test_native_completion_is_recovered_after_receiver_restart(self):
        self.connect()
        identifier = str(uuid.uuid4())
        request = {'version': 1, 'op': 'followup', 'id': identifier,
                   'source': 'coordinator', 'target': 'worker', 'text': 'synthetic work'}
        self.host.exchange('env-a', request, 'hub', timeout=3)
        class CompletedNative:
            def call(inner, method, params):
                self.assertEqual(method, 'thread/turns/list')
                return {'data': [{'id': self.turn, 'status': 'completed',
                                  'items': [{'type': 'agentMessage', 'text': 'recovered reply'}]}]}
        self.guest.native.client = lambda home: CompletedNative()
        self.guest.finish_pending()
        self.guest.finish_pending()
        home = self.guest.config['bridges']['env-a']['home']
        store = Store(home)
        try:
            self.assertEqual(store.rows('SELECT status FROM tasks WHERE id=?', (identifier,))[0]['status'], 'completed')
            self.assertEqual(len(store.rows('SELECT * FROM outbound')), 1)
        finally:
            store.close()
        self.guest.queue_events()
        eid = event_id(identifier + '/' + self.turn + '/final')
        self.assertIsNotNone(self.guest.journal.get('out', 'hub', eid))

    def test_second_broker_cannot_remove_live_first_broker_socket(self):
        config=self.host.config;config['links']={};atomic(self.host.home/'config.json',encode(config).encode());argv=[sys.executable,'-I','-B',str(ROOT/'csl_light.py'),'--home',str(self.host.home),'run']
        first=subprocess.Popen(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            until=time.monotonic()+3
            while not(self.host.home/'light.sock').exists()and time.monotonic()<until:time.sleep(.02)
            second=subprocess.run(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=3)
            self.assertNotEqual(second.returncode,0);self.assertTrue((self.host.home/'light.sock').exists())
            self.assertTrue(light.local(self.host.home,{'action':'agents'})['ok'])
        finally:
            first.terminate();first.communicate(timeout=5)


if __name__=='__main__':unittest.main()

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
from unittest.mock import patch
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
        with self.assertRaisesRegex(ValueError,'turn does not match'):self.host.dispatch_event('env-a',{'type':'event','id':event['id'],'event':event})
        event.update(id=str(uuid.uuid4()),turn_id=self.turn,text='right turn')
        self.assertEqual(self.host.dispatch_event('env-a',{'type':'event','id':event['id'],'event':event})['state'],'delivered')

    def test_event_waits_without_turn_then_uses_original_journal_receipt(self):
        identifier = str(uuid.uuid4())
        request = {'version': 1, 'op': 'followup', 'id': identifier,
                   'source': 'coordinator', 'target': 'worker', 'text': 'Synthetic original task'}
        home = self.host.config['bridges']['hub']['home']
        store = Store(home)
        self.addCleanup(store.close)
        store.reserve('outbound:env-a', request)
        store.operation(identifier, 'uncertain', {'state': 'uncertain'})
        injections = []
        self.host.native.inject = lambda *args: injections.append(args) or {'state': 'delivered'}
        event = {'id': str(uuid.uuid4()), 'task_id': identifier, 'source': 'worker',
                 'target': 'coordinator', 'kind': 'final', 'text': 'Synthetic final',
                 'turn_id': self.turn, 'status': 'completed'}
        frame = {'type': 'event', 'id': event['id'], 'event': event}
        with self.assertRaisesRegex(RuntimeError, 'native acceptance receipt'):
            self.host.dispatch_event('env-a', frame)
        self.assertEqual(injections, [])
        self.assertEqual(store.rows('SELECT * FROM inbound'), [])
        self.host.journal.put('out', 'env-a', identifier,
                              {'id': identifier, 'type': 'request', 'request': request})
        self.host.journal.finish('out', 'env-a', identifier, {'ok': True, 'data': {
            'state': 'delivered', 'request_id': identifier, 'turn_id': self.turn}})
        result = self.host.dispatch_event('env-a', frame)
        self.assertEqual(result['state'], 'delivered')
        self.assertEqual(len(injections), 1)
        self.assertEqual(store.rows('SELECT delivery FROM inbound')[0]['delivery'], 'delivered')

    def event_fixture(self, node=None, peer='env-a', source='coordinator', target='worker'):
        node = node or self.host
        home, _ = node.agent(source)
        store = Store(home)
        self.addCleanup(store.close)
        request = {'version': 1, 'op': 'followup', 'id': str(uuid.uuid4()),
                   'source': source, 'target': target, 'text': 'Synthetic original'}
        store.reserve('outbound:' + peer, request)
        event = {'id': str(uuid.uuid4()), 'task_id': request['id'], 'source': target,
                 'target': source, 'kind': 'final', 'text': 'Synthetic final',
                 'turn_id': self.turn, 'status': 'completed'}
        payload = {'id': request['id'], 'type': 'request', 'request': request,
                   'destination': peer, 'origin': node.config['node']}
        return store, request, event, payload

    def test_exact_rpc_recovery_refuses_wrong_peer_body_and_unaccepted_results(self):
        for fault in ('peer', 'body', 'failed', 'client-only', 'request-id', 'hash', 'destination'):
            with self.subTest(fault=fault):
                store, request, event, payload = self.event_fixture()
                rpc_peer = 'env-b' if fault == 'peer' else 'env-a'
                if fault == 'body':
                    payload['request'] = dict(request, text='different original')
                if fault == 'destination':
                    payload['destination'] = 'env-b'
                self.host.journal.put('out', rpc_peer, request['id'], payload)
                data = {'state': 'delivered', 'request_id': request['id'], 'turn_id': self.turn}
                if fault == 'client-only':
                    data['state'] = 'mail_ready_for_client'
                if fault == 'request-id':
                    data['request_id'] = str(uuid.uuid4())
                self.host.journal.finish('out', rpc_peer, request['id'], {'ok': fault != 'failed', 'data': data})
                if fault == 'hash':
                    with self.host.journal.db() as db:
                        db.execute("UPDATE rpc SET hash='bad' WHERE id=?", (request['id'],))
                with patch.object(self.host.native, 'inject') as inject:
                    with self.assertRaises((ValueError, RuntimeError)):
                        self.host.dispatch_event('env-a', {'type': 'event', 'id': event['id'], 'event': event})
                    inject.assert_not_called()
                self.assertEqual(store.rows('SELECT * FROM inbound WHERE id=?', (event['id'],)), [])

    def test_authenticated_peer_cannot_answer_other_peer_operation(self):
        store, request, event, _ = self.event_fixture(peer='env-b')
        store.operation(request['id'], 'delivered', {'turn_id': self.turn})
        with patch.object(self.host.native, 'inject') as inject:
            with self.assertRaisesRegex(ValueError, 'peer differs'):
                self.host.dispatch_event('env-a', {'type': 'event', 'event': event})
            inject.assert_not_called()

    def test_trusted_hub_path_preserves_logical_bridge_alias_and_recovers_for_wait(self):
        store, request, event, payload = self.event_fixture(
            node=self.guest, peer='host-alias', source='worker', target='coordinator')
        with store.transaction() as db:
            db.execute('INSERT INTO peers VALUES(?,?)', ('host-alias', encode({
                'transport': 'light', 'peer': 'hub', 'home': str(self.guest.home)})))
        payload['destination'] = 'hub'
        self.guest.journal.put('out', 'hub', request['id'], payload)
        self.guest.journal.finish('out', 'hub', request['id'], {'ok': True, 'data': {
            'state': 'delivered', 'request_id': request['id'], 'turn_id': self.turn}})
        from csl_bridge_engine import Connector
        connector = Connector(store.home)
        self.addCleanup(connector.close)
        with store.transaction() as db:
            db.execute('INSERT INTO inbound(id,peer,body)VALUES(?,?,?)',
                       (event['id'], 'host-alias', encode(event)))
        result = []
        waiter = threading.Thread(target=lambda: result.append(connector.local({
            'local_op': 'wait', 'agent': 'worker', 'after': 0, 'timeout': 3})))
        waiter.start()
        deadline = time.monotonic() + 1
        while not connector.waiters['worker'] and time.monotonic() < deadline:
            time.sleep(.005)
        self.assertTrue(connector.waiters['worker'])
        def notify(*args, **kwargs):
            return connector.local({'local_op': '_light_events_changed'})
        with patch('csl_bridge_transport.local_call', side_effect=notify):
            self.guest.dispatch_event('hub', {'type': 'event', 'event': event, 'origin': 'hub'})
        waiter.join(1)
        self.assertFalse(waiter.is_alive())
        self.assertEqual(result[0]['events'][0]['event']['id'], event['id'])
        self.assertEqual(json.loads(store.rows('SELECT result FROM operations')[0]['result'])['turn_id'], self.turn)

    def test_routed_origin_is_distinct_from_authenticated_hub(self):
        store, request, event, payload = self.event_fixture(
            node=self.guest, peer='hub', source='worker', target='coordinator')
        payload['destination'] = 'env-b'
        self.guest.journal.put('out', 'hub', request['id'], payload)
        self.guest.journal.finish('out', 'hub', request['id'], {'ok': True, 'data': {
            'state': 'delivered', 'request_id': request['id'], 'turn_id': self.turn}})
        with patch('csl_bridge_transport.local_call', return_value={}):
            self.assertEqual(self.guest.dispatch_event('hub', {
                'type': 'event', 'event': event, 'origin': 'env-b'})['state'], 'delivered')
        event = dict(event, id=str(uuid.uuid4()))
        with self.assertRaisesRegex(ValueError, 'RPC binding'):
            self.guest.dispatch_event('hub', {'type': 'event', 'event': event, 'origin': 'env-c'})
        self.guest.config['links']['hub']['trusted_router'] = False
        with self.assertRaisesRegex(ValueError, 'Untrusted event origin'):
            self.guest.dispatch_event('hub', {'type': 'event', 'event': event, 'origin': 'env-b'})

    def test_publication_keeps_cursor_before_unconfirmed_original_then_recovers(self):
        home, thread = self.guest.agent('worker')
        store = Store(home)
        self.addCleanup(store.close)
        request = {'version': 1, 'op': 'followup', 'id': str(uuid.uuid4()),
                   'source': 'coordinator', 'target': 'worker', 'text': 'Synthetic original'}
        store.reserve('hub', request)
        final = {'task_id': request['id'], 'source': 'worker', 'target': 'coordinator',
                 'kind': 'final', 'text': 'Synthetic final', 'turn_id': self.turn, 'status': 'completed'}
        eventid = str(uuid.uuid4())
        store.publish('hub', final, eventid)
        self.guest.journal.put('in', 'hub', request['id'], {'type': 'request',
            'id': request['id'], 'request': request, 'origin': 'hub'})
        self.guest.journal.finish('in', 'hub', request['id'], {'ok': True, 'data': {'state': 'queued'}})
        self.guest.queue_events()
        self.assertIsNone(self.guest.journal.get('out', 'hub', eventid))
        with self.guest.journal.db() as db:
            self.assertEqual(db.execute('SELECT * FROM event_cursors').fetchall(), [])
        receipt = {'state': 'delivered', 'request_id': request['id'], 'turn_id': self.turn, 'thread_id': thread}
        store.operation(request['id'], 'delivered', receipt)
        self.guest.journal.finish('in', 'hub', request['id'], {'ok': True, 'data': receipt})
        self.guest.queue_events()
        self.assertIsNotNone(self.guest.journal.get('out', 'hub', eventid))
        self.assertEqual(json.loads(self.guest.journal.get('out', 'hub', eventid)['payload'])['origin'], 'env-a')

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

    def mail(self, source, target):
        return {'version':1,'op':'mail','message_version':1,'action':'send',
                'id':str(uuid.uuid4()),'source':source,'target':target,
                'envelope':{'kind':'message','body':'😀'*12000,'in_reply_to':None,
                            'grant_ref':None,'created_at':'2026-11-17T00:00:00Z','expires_at':None}}

    def test_twenty_large_frames_each_way_drain_without_deadlock(self):
        for node, peer, source, target in ((self.host,'env-a','coordinator','worker'),
                                            (self.guest,'hub','worker','coordinator')):
            node.dispatch = lambda *args: {'state':'delivered'}
            for _ in range(20):
                request = self.mail(source,target)
                node.journal.put('out',peer,request['id'],{'type':'request','id':request['id'],
                    'request':request,'origin':'ignored','destination':peer})
        self.connect()
        until = time.monotonic()+10
        while time.monotonic()<until and (self.host.journal.pending('out') or self.guest.journal.pending('out')):
            # Same bounded dispatcher used by a running broker's retry loop.
            for node in (self.host,self.guest):
                for row in node.journal.pending('in'):
                    node.schedule(row['peer'],row['id'])
            time.sleep(.02)
        self.assertEqual(len(self.host.journal.pending('out')),0)
        self.assertEqual(len(self.guest.journal.pending('out')),0)
        self.assertFalse(any(c.closed for c,*_ in self.connections))

    def test_bad_source_and_id_collision_nack_only_poison_message(self):
        self.connect()
        poison = self.mail('intruder','coordinator')
        self.guest.journal.put('out','hub',poison['id'],{'type':'request','id':poison['id'],
            'request':poison,'origin':'hub'})
        valid = self.mail('worker','coordinator')
        self.host.dispatch = lambda *args: {'state':'delivered'}
        self.guest.journal.put('out','hub',valid['id'],{'type':'request','id':valid['id'],
            'request':valid,'origin':'hub'})
        until=time.monotonic()+3
        while time.monotonic()<until and self.guest.journal.pending('out'):
            time.sleep(.02)
        self.assertFalse(json.loads(self.guest.journal.get('out','hub',poison['id'])['result'])['ok'])
        self.assertTrue(json.loads(self.guest.journal.get('out','hub',valid['id'])['result'])['ok'])
        # Same ID with a different payload gets a NACK; the next request still flows.
        collision = dict(valid,id=str(uuid.uuid4()))
        canonical = {'type':'request','id':collision['id'],'request':collision,'origin':'env-a',
                     'source_session':self.guest.agents()['worker']}
        self.host.journal.put('in','env-a',collision['id'],canonical)
        changed = {**canonical,'request':{**collision,'envelope':{**collision['envelope'],'body':'changed'}}}
        self.guest.journal.put('out','hub',collision['id'],changed)
        until=time.monotonic()+3
        while time.monotonic()<until and self.guest.journal.pending('out'):
            time.sleep(.02)
        self.assertFalse(json.loads(self.guest.journal.get('out','hub',collision['id'])['result'])['ok'])
        self.assertFalse(any(c.closed for c,*_ in self.connections))

    def test_router_rewrites_spoofed_session_and_origin_and_rejects_request_session(self):
        thread=self.guest.agents()['worker']
        self.host.config['links']['env-a']['agent_threads']={'worker':thread}
        request={'version':1,'op':'followup','id':str(uuid.uuid4()),'source':'worker','target':'remote','text':'synthetic'}
        frame={'type':'request','id':request['id'],'request':request,'origin':'hub',
               'source_session':self.host.agents()['coordinator']}
        self.host.authenticate('env-a',frame)
        self.assertEqual(frame['origin'],'env-a');self.assertEqual(frame['source_session'],thread)
        self.host.journal.route('remote','env-b',str(uuid.uuid4()))
        with patch.object(self.host,'exchange',return_value={'ok':True,'data':{'state':'delivered'}})as exchange:
            self.host.dispatch('env-a',frame)
            self.assertEqual(exchange.call_args.args[2],'env-a')
            self.assertEqual(exchange.call_args.kwargs['session'],thread)
            self.assertEqual(exchange.call_args.kwargs['timeout'],0)
        request['session']=self.host.agents()['coordinator']
        with self.assertRaisesRegex(ValueError,'enrolled thread'):
            self.host.authenticate('env-a',frame)

    def test_queued_forward_returns_native_receipt_on_next_nonblocking_attempt(self):
        self.connect()
        request={'version':1,'op':'followup','id':str(uuid.uuid4()),
                 'source':'other-worker','target':'worker','text':'synthetic relay'}
        frame={'type':'request','id':request['id'],'request':request,'origin':'env-b',
               'source_session':str(uuid.uuid4())}
        with self.assertRaisesRegex(RuntimeError,'Destination queued'):
            self.host.dispatch('env-b',frame)
        until=time.monotonic()+3
        while time.monotonic()<until:
            row=self.host.journal.get('out','env-a',request['id'])
            if row['state']=='done':break
            time.sleep(.02)
        result=self.host.dispatch('env-b',frame)
        self.assertEqual(result['turn_id'],self.turn)
        self.assertEqual(self.native_calls,[request['id']])

    def test_event_spoof_is_rewritten_and_hello_cannot_rebind_thread(self):
        thread=self.guest.agents()['worker']
        self.host.config['links']['env-a']['agent_threads']={'worker':thread}
        event={'id':str(uuid.uuid4()),'task_id':str(uuid.uuid4()),'source':'worker','target':'remote',
               'kind':'final','text':'synthetic','turn_id':self.turn,'status':'completed','session':thread}
        frame={'type':'event','id':event['id'],'event':event,'origin':'hub','source_session':str(uuid.uuid4())}
        self.host.authenticate('env-a',frame)
        self.assertEqual(frame['origin'],'env-a');self.assertEqual(frame['source_session'],thread)
        channel=light.Channel(self.host,'env-a',None,None,lambda:None)
        try:
            with self.assertRaisesRegex(ValueError,'Enrolled thread'):
                channel.hello({'node':'env-a','agents':{'worker':str(uuid.uuid4())}})
        finally:channel.close()
        self.assertEqual(self.host.journal.resolve('worker')['thread'],thread)

    def test_silent_link_times_out_instead_of_remaining_connected(self):
        a,b=socket.socketpair();r=a.makefile('rb');w=a.makefile('wb',buffering=0)
        channel=light.Channel(self.host,'env-a',r,w,lambda:a.shutdown(socket.SHUT_RDWR))
        channel.LIVENESS=.2
        t=threading.Thread(target=channel.run,daemon=True);t.start();t.join(2)
        self.assertFalse(t.is_alive());self.assertTrue(channel.closed)
        r.close();w.close();a.close();b.close()

    def test_initial_auth_grace_ends_when_first_application_frame_arrives(self):
        a,b=socket.socketpair();r=a.makefile('rb');w=a.makefile('wb',buffering=0)
        channel=light.Channel(self.host,'env-a',r,w,lambda:a.shutdown(socket.SHUT_RDWR))
        channel.LIVENESS=.1;channel.first_frame_deadline=time.monotonic()+1
        t=threading.Thread(target=channel.run,daemon=True);t.start()
        try:
            time.sleep(.25);self.assertFalse(channel.closed)
            b.sendall((light.wire({'type':'pong'})+'\n').encode())
            t.join(2);self.assertTrue(channel.closed);self.assertIsNone(channel.first_frame_deadline)
        finally:
            channel.close();t.join(1);r.close();w.close();a.close();b.close()

    def test_relay_drains_both_directions_with_small_socket_buffers(self):
        left,relay_left=socket.socketpair();relay_right,right=socket.socketpair()
        sockets=(left,relay_left,relay_right,right)
        for sock in sockets:sock.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,4096);sock.settimeout(5)
        alive=threading.Event();alive.set()
        relay=threading.Thread(target=light.relay_duplex,args=(relay_left.recv,relay_left.send,
            relay_right.recv,relay_right.send,alive.is_set),daemon=True);relay.start()
        body=b'x'*(20*48000);received=[b'',b'']
        def read(sock,index):
            while len(received[index])<len(body):received[index]+=sock.recv(65536)
        workers=[threading.Thread(target=left.sendall,args=(body,)),threading.Thread(target=right.sendall,args=(body,)),
                 threading.Thread(target=read,args=(left,0)),threading.Thread(target=read,args=(right,1))]
        try:
            for t in workers:t.start()
            for t in workers:t.join(6)
            self.assertEqual(received,[body,body]);self.assertFalse(any(t.is_alive()for t in workers))
        finally:
            alive.clear()
            for sock in sockets:sock.close()
            relay.join(1)

    def test_three_hundred_unresolved_frames_do_not_hide_new_message(self):
        self.host.schedule=lambda *args:None  # Persist+ACK, destination remains offline.
        for _ in range(300):
            request={'version':1,'op':'followup','id':str(uuid.uuid4()),
                     'source':'worker','target':'offline','text':'queued forward'}
            self.guest.journal.put('out','hub',request['id'],{'type':'request',
                'id':request['id'],'request':request,'origin':'env-a'})
        newest={'version':1,'op':'followup','id':str(uuid.uuid4()),
                'source':'worker','target':'coordinator','text':'new online recipient'}
        self.guest.journal.put('out','hub',newest['id'],{'type':'request',
            'id':newest['id'],'request':newest,'origin':'env-a'})
        self.connect()
        until=time.monotonic()+8
        while time.monotonic()<until and self.host.journal.get('in','env-a',newest['id'])is None:
            time.sleep(.02)
        self.assertIsNotNone(self.host.journal.get('in','env-a',newest['id']))
        with self.host.journal.db()as db:
            self.assertEqual(db.execute("SELECT count(*) FROM rpc WHERE direction='in'").fetchone()[0],301)
        self.assertFalse(any(c.closed for c,*_ in self.connections))

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

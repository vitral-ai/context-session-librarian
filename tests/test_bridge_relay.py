"""Real nested TLS over a loopback relay. No public endpoint, model, auth or SSH."""
import hashlib
import os
from pathlib import Path
import secrets
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from csl_bridge_store import BridgeError, Store, encode
from csl_bridge_lan import save, private_json, code_bytes
from csl_bridge_relay import Broker, Limits, RelayServer, rpc, dial, send_line, read_line, InnerTLS
from csl_bridge_relay_client import RelaySession, peer_request, exchange


class RelayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.root=Path(cls.temp.name);cls.root.chmod(0o700)
        cert=cls.root/'cert.pem';key=cls.root/'key.pem'
        subprocess.run(['/usr/bin/openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(key),'-out',str(cert),'-days','1','-subj','/CN=synthetic-relay'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True)
        token=secrets.token_hex(32);cls.enrollment=cls.root/'enrollment';cls.enrollment.write_text(token);cls.enrollment.chmod(0o600)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(cert,key)
        cls.broker=Broker(Limits(rate=1000,wait=.2,idle=2,lifetime=8),enrollment=token)
        cls.server=RelayServer(('127.0.0.1',0),context,cls.broker)
        cls.thread=threading.Thread(target=cls.server.serve_forever,daemon=True);cls.thread.start()
        cls.profile={'host':'127.0.0.1','port':cls.server.server_address[1],'fingerprint':hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert.read_text())).hexdigest(),'enrollment_file':str(cls.enrollment)}
        cls.profile_path=cls.root/'profile.json';save(cls.profile_path,cls.profile)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown();cls.server.server_close();cls.thread.join(2);cls.temp.cleanup()

    def make_session(self,label):
        root=self.root/(label+uuid.uuid4().hex[:6]);home=root/'bridge'
        store=Store(home,create=True)
        with store.transaction() as db:
            db.executemany('INSERT INTO config VALUES(?,?)',[('identity',encode(label)),('runtime_socket',encode(None))])
            db.execute('INSERT INTO agents VALUES(?,?)',('worker',str(uuid.uuid4())))
        store.close()
        session=RelaySession(root/'relay',home,self.profile_path)
        thread=threading.Thread(target=session.run,daemon=True);thread.start()
        deadline=time.monotonic()+3
        while session.route['route'] not in self.broker.routes and time.monotonic()<deadline:time.sleep(.01)
        return session,thread

    def test_unauthenticated_and_wrong_pin_do_not_create_routes(self):
        before=len(self.broker.routes)
        with dial(self.profile) as stream:
            send_line(stream,{'enrollment':'0'*64,'request':{'op':'register','route':'a'*64,'lease':'b'*64}})
            self.assertFalse(read_line(stream)['ok'])
        with self.assertRaisesRegex(BridgeError,'provisioned pin'):dial({**self.profile,'fingerprint':'0'*64})
        self.assertEqual(len(self.broker.routes),before)

    def test_bounded_control_rate_expiry_identity_and_revocation(self):
        now=[0.];broker=Broker(Limits(routes=1,connections=2,rate=1,lease=2,invite=1),clock=lambda:now[0],enrollment='a'*64)
        broker.admit('peer');broker.release()
        with self.assertRaises(BridgeError):broker.admit('peer')
        register={'op':'register','route':'1'*64,'lease':'2'*64};broker.control(register)
        with self.assertRaises(BridgeError):broker.control({**register,'lease':'3'*64})
        with self.assertRaises(BridgeError):broker.control({**register,'route':'4'*64})
        with self.assertRaises(BridgeError):broker.control({'op':'publish',**{k:v for k,v in register.items() if k!='op'},'locator':'a'*64,'descriptor':'x'*3000})
        now[0]=3;broker.control({**register,'route':'4'*64});self.assertNotIn('1'*64,broker.routes)
        broker.control({'op':'revoke','route':'4'*64,'lease':'2'*64});self.assertFalse(broker.routes)
        global_broker=Broker(Limits(rate=1))
        for index in range(4):global_broker.admit(str(index));global_broker.release()
        with self.assertRaisesRegex(BridgeError,'rate_global'):global_broker.admit('other')
        for malformed in ({'op':'shell'},{'op':'lookup','locator':'invalid'},[],{'op':'register','route':2,'lease':None}):
            with self.assertRaises(BridgeError):broker.control(malformed)

    def test_nested_tls_pair_and_private_payload_never_reaches_relay_plaintext(self):
        alice,at=self.make_session('alice');bob,bt=self.make_session('bob')
        observed=[];original=self.broker.forward
        def capture(left,right):
            class Tap:
                def __init__(self,raw):self.raw=raw
                def recv(self,size):
                    data=self.raw.recv(size);observed.append(data);return data
                def __getattr__(self,name):return getattr(self.raw,name)
            return original(Tap(left),Tap(right))
        self.broker.forward=capture
        try:
            invitation=alice.publish();secret=invitation['code']
            result=bob.connect_code(secret);self.assertEqual(result['state'],'paired');self.assertEqual(result['paired'],'alice')
            store=Store(bob.home)
            try:config=store.peers()['alice']
            finally:store.close()
            frame={'version':1,'op':'send_message','id':str(uuid.uuid4()),'source':'worker','target':'worker','text':'PRIVATE-PAYLOAD-NEVER-ON-RELAY'}
            with patch('csl_bridge_transport.local_call',return_value={'state':'delivered','request_id':frame['id']}) as call:
                reply=exchange(config,bob.identity,frame)
            self.assertEqual(reply['request_id'],frame['id']);self.assertEqual(call.call_args.args[1]['trusted_peer'],'bob')
            raw=b''.join(observed)
            self.assertNotIn(frame['text'].encode(),raw);self.assertNotIn(secret.encode(),raw)
            self.assertNotIn(private_json(config['token_file'])['token'].encode(),raw)
            with self.assertRaises(BridgeError):peer_request(self.profile,{**alice.remote,'fingerprint':'0'*64},{'op':'call','token':'never-send','request':frame})
            # Rejoin the same opaque route using its lease, with no replacement grant.
            rpc(self.profile,{'op':'register',**alice.route})
            self.assertEqual(len([p for p in alice.registry.values() if p=='bob']),1)
        finally:
            self.broker.forward=original
            alice.close();bob.close();at.join(2);bt.join(2)

    def test_pair_ack_loss_reconciles_same_grant_and_wrong_code_never_pairs(self):
        import csl_bridge_relay_client as client
        alice,at=self.make_session('alice');bob,bt=self.make_session('bob')
        try:
            invite=alice.publish()
            with self.assertRaises(BridgeError):bob.connect_code('A'*26)
            original=client.peer_request
            def lose(profile,remote,payload,**kwargs):
                result=original(profile,remote,payload,**kwargs)
                if payload['op']=='pair':raise OSError('Synthetic ACK loss after peer committed')
                return result
            with patch('csl_bridge_relay_client.peer_request',side_effect=lose):result=bob.connect_code(invite['code'])
            self.assertEqual(result['state'],'uncertain');before=dict(alice.registry)
            with self.assertRaisesRegex(BridgeError,'Reconcile'):bob.connect_code(invite['code'])
            result=bob.reconcile_pair();self.assertEqual(result['state'],'paired');self.assertEqual(alice.registry,before)
            self.assertEqual(len(alice.registry),1)
            # Relay loss does not erase peer credentials or issue another invitation.
            self.broker.control({'op':'revoke',**alice.route})
            end=time.monotonic()+3
            while alice.route['route'] not in self.broker.routes and time.monotonic()<end:time.sleep(.01)
            rpc(self.profile,{'op':'register',**alice.route})
            self.assertEqual(alice.registry,before)
        finally:alice.close();bob.close();at.join(2);bt.join(2)

    def test_real_relay_ipc_lost_task_ack_receipt_same_id_and_final(self):
        from csl_bridge_engine import Connector
        from test_bridge import FakeRuntime
        import csl_bridge_relay_client as client
        alice,at=self.make_session('owner');bob,bt=self.make_session('workerbox')
        connectors=[]
        try:
            owner=Connector(alice.home);owner.start();connectors.append(owner)
            worker=Connector(bob.home);worker.start();worker.client_only=False
            worker.runtime=FakeRuntime(worker.agents['worker']);connectors.append(worker)
            self.assertEqual(bob.connect_code(alice.publish()['code'])['state'],'paired')
            frame={'version':1,'op':'followup','id':str(uuid.uuid4()),'source':'worker','target':'worker','text':'Synthetic relay task; no inference.'}
            operation={'local_op':'call','peer':'workerbox','request':frame}
            original=client.peer_request;lost=[False]
            def lose(profile,remote,payload,**kwargs):
                result=original(profile,remote,payload,**kwargs)
                if payload.get('request',{}).get('id')==frame['id'] and not lost[0]:lost[0]=True;raise OSError('Synthetic lost task receipt')
                return result
            with patch('csl_bridge_relay_client.peer_request',side_effect=lose):
                self.assertEqual(owner.local(operation)['state'],'uncertain')
            self.assertEqual(owner.local(operation)['state'],'uncertain')
            receipt={'version':1,'op':'receipt','id':str(uuid.uuid4()),'request_id':frame['id']}
            result=owner.local({'local_op':'call','peer':'workerbox','request':receipt})
            self.assertEqual(result['state'],'delivered');turn=result['result']['turn_id']
            self.assertEqual(owner.local(operation)['turn_id'],turn)
            worker.local({'local_op':'progress','source':'worker','task_id':frame['id'],'id':str(uuid.uuid4()),'text':'Synthetic question?'})
            event=owner.local({'local_op':'wait','agent':'worker','after':0,'timeout':2})['events'][0]
            answer={**frame,'id':str(uuid.uuid4()),'op':'send_message','text':'Synthetic real caller choice.'}
            self.assertEqual(owner.local({'local_op':'call','peer':'workerbox','request':answer})['turn_id'],turn)
            worker.answers[(worker.agents['worker'],turn)]='Synthetic final.';worker.completed('worker',turn,'completed')
            final=owner.local({'local_op':'wait','agent':'worker','after':event['cursor'],'timeout':2})['events'][0]
            self.assertEqual(final['event']['kind'],'final');self.assertEqual(final['event']['turn_id'],turn)
            self.assertEqual(len([x for x in worker.runtime.calls if x[0]=='turn/start']),1)
        finally:
            for connector in connectors:connector.close()
            alice.close();bob.close();at.join(2);bt.join(2)
            for connector in connectors:
                for thread in connector.threads:thread.join(1)
                if all(not t.is_alive() for t in connector.threads):connector.store.close()

    def test_short_lease_expires_during_old_accept_wait_but_independent_heartbeat_survives(self):
        from csl_bridge_relay import open_tunnel, RelayFailure
        old_lease=self.broker.limits.lease;old_wait=self.broker.limits.wait
        self.broker.limits.lease=.12;self.broker.limits.wait=.45
        route={'route':secrets.token_hex(32),'lease':secrets.token_hex(32)};errors=[]
        def accept_old():
            try:open_tunnel(self.profile,{'op':'accept',**route},timeout=2)
            except BridgeError as exc:errors.append(getattr(exc,'reason','unknown'))
        session=thread=None
        try:
            rpc(self.profile,{'op':'register',**route})
            waiter=threading.Thread(target=accept_old);waiter.start();time.sleep(.2)
            with self.assertRaises(RelayFailure) as rejected:open_tunnel(self.profile,{'op':'connect','route':route['route']},timeout=1)
            self.assertEqual(rejected.exception.reason,'route_missing');waiter.join(1)
            self.assertGreaterEqual(self.broker.diagnostics()['reason_counts']['lease_expired'],1)
            session,thread=self.make_session('leaseguard')
            time.sleep(.3)
            self.assertIn(session.route['route'],self.broker.routes)
            self.assertGreater(session.relay_status['heartbeat_last_success'],session.relay_status['accept_last_attempt'])
            self.assertEqual(peer_request(self.profile,session.remote,{'op':'health'},timeout=2)['identity'],'leaseguard')
            # A slow application/native IPC handler does not block renewal either.
            def slow(value):time.sleep(.25);return {'synthetic_ipc':'completed'}
            with patch.object(session,'incoming',side_effect=slow):
                self.assertEqual(peer_request(self.profile,session.remote,{'op':'health'},timeout=2)['synthetic_ipc'],'completed')
            self.assertIn(session.route['route'],self.broker.routes)
            observed=encode(self.broker.diagnostics())
            self.assertNotIn(session.route['route'],observed);self.assertNotIn(session.route['lease'],observed);self.assertNotIn('127.0.0.1',observed)
        finally:
            if session:session.close();thread.join(2)
            self.broker.limits.lease=old_lease;self.broker.limits.wait=old_wait

    def test_fragmented_inner_tls_read_and_write_large_data(self):
        # Actual MemoryBIO TLS over a fragmenting socketpair; no relay plaintext.
        root=self.root/'fragment';root.mkdir(exist_ok=True)
        session,thread=self.make_session('fragment')
        left,right=socket.socketpair();left.settimeout(3);right.settimeout(3)
        class Fragment:
            def __init__(self,raw):self.raw=raw
            def recv(self,size):return self.raw.recv(min(size,17))
            def sendall(self,data):
                for offset in range(0,len(data),23):self.raw.sendall(data[offset:offset+23])
            def close(self):self.raw.close()
        payload=os.urandom(50000);errors=[]
        def serve():
            try:
                tls=InnerTLS(Fragment(right),session.server_context,server_side=True);data=bytearray()
                while len(data)<len(payload):data.extend(tls.recv(4096))
                tls.sendall(bytes(data));tls.close()
            except Exception as exc:errors.append(type(exc).__name__)
        worker=threading.Thread(target=serve);worker.start()
        from csl_bridge_relay_client import client_context
        tls=InnerTLS(Fragment(left),client_context(),pin=session.fingerprint);tls.sendall(payload);answer=bytearray()
        while len(answer)<len(payload):answer.extend(tls.recv(8192))
        self.assertEqual(bytes(answer),payload);tls.close();worker.join(3);self.assertFalse(errors)
        session.close();thread.join(2)

if __name__=='__main__':unittest.main()

"""Actual TLS/UDP loopback only; no model, SSH, OS permission or LAN claims."""
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from csl_bridge_store import Store, BridgeError, encode
from csl_bridge_lan import Session, code_bytes, mac, discover, link_endpoint, pair, request, exchange, private_json, control, COMMUNICATION_OPS, endpoint, apply_pair_result, reconcile_pair, select_interface, network_choices


class LANTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.root=Path(cls.temp.name);cls.sessions=[]
        for identity in ('alice','bob'):
            home=cls.root/identity
            store=Store(home,create=True)
            with store.transaction() as db:
                db.executemany('INSERT INTO config VALUES(?,?)',[('identity',encode(identity)),('runtime_socket',encode(None))])
                db.execute('INSERT INTO agents VALUES(?,?)',('worker',str(uuid.uuid4())))
            store.close()
            session=Session(cls.root/(identity+'-lan'),home,discovery_port=0)
            session.start(discovery=identity=='alice');cls.sessions.append(session)
        cls.alice,cls.bob=cls.sessions

    @classmethod
    def tearDownClass(cls):
        for session in cls.sessions:
            session.close()
            assert private_json(session.state/'registry.json')=={}
        cls.temp.cleanup()

    def test_discovery_authenticates_pin_and_nonce_without_code_on_wire(self):
        invite=self.alice.invite();secret=code_bytes(invite['code'])
        self.assertEqual(len(secret),16)
        query={'nonce':'a'*32,'proof':mac(secret,{'nonce':'a'*32})}
        response=self.alice.discovery_reply(query)
        self.assertEqual(response['endpoint'],self.alice.remote)
        self.assertNotIn(invite['code'],encode(query)+encode(response))
        self.assertIsNone(self.alice.discovery_reply({**query,'nonce':'b'*32}))
        self.assertEqual(discover(invite['code'],'127.0.0.1',self.alice.discovery_port),self.alice.remote)
        self.assertEqual(link_endpoint(invite['link'],invite['code']),self.alice.remote)
        with self.assertRaises(BridgeError):link_endpoint(invite['link'],'A'*26)

    def test_expiry_wrong_proof_and_attempt_bound(self):
        invite=self.alice.invite();offer={'identity':'bob','agent':'worker','endpoint':self.bob.remote,'token':'a'*64,'pair_id':str(uuid.uuid4())}
        for _ in range(6):
            with self.assertRaises(BridgeError):self.alice.incoming({'op':'pair','offer':offer,'proof':'0'*64})
        proof=mac(code_bytes(invite['code']),{'offer':offer,'endpoint':self.alice.remote})
        with self.assertRaises(BridgeError):self.alice.incoming({'op':'pair','offer':offer,'proof':proof})
        self.alice.invite();self.alice.invitation['expires']=0
        self.assertIsNone(self.alice.discovery_reply({'nonce':'a'*32,'proof':'0'*64}))
        with self.assertRaises(BridgeError):self.alice.incoming({'op':'pair','offer':offer,'proof':proof})

    def test_malformed_tokens_never_reach_credential_files(self):
        invitation=self.alice.invite();offer={'identity':'invalidpeer','agent':'worker','endpoint':self.bob.remote,'token':'bad','pair_id':str(uuid.uuid4())}
        before=set(self.alice.state.iterdir())
        with self.assertRaises(BridgeError):
            self.alice.incoming({'op':'pair','offer':offer,'proof':mac(code_bytes(invitation['code']),{'offer':offer,'endpoint':self.alice.remote})},'127.0.0.1')
        self.assertEqual(set(self.alice.state.iterdir()),before)
        attempt={'remote':self.alice.remote,'offer':{'token':'a'*64,'pair_id':str(uuid.uuid4())}}
        result={'identity':'invalidpeer','agent':'worker','endpoint':self.alice.remote,'token':'bad'}
        before=set(self.bob.state.iterdir())
        with self.assertRaises(BridgeError):apply_pair_result(self.bob,attempt,result)
        self.assertEqual(set(self.bob.state.iterdir()),before)

    def test_os_default_route_beats_bridges_and_explicit_interface_is_simple(self):
        choices={'wifi':['192.168.20.141'],'docker0':['172.17.0.1'],'wg0':['10.0.0.2']}
        with patch('csl_bridge_lan.network_choices',return_value=(choices,'wifi')):
            self.assertEqual(select_interface(),'192.168.20.141')
            self.assertEqual(select_interface('wg0'),'10.0.0.2')
        with patch('csl_bridge_lan.network_choices',return_value=(choices,None)):
            with self.assertRaisesRegex(BridgeError,'--interface NAME'):select_interface()

    def test_repair_receipt_reconciles_lost_ack_without_regrant_and_rotates_old_token(self):
        import csl_bridge_lan as lan
        invitation=self.alice.invite()
        old=private_json(self.bob.state/'to-alice.json')['token']
        original=lan.request
        def lose(remote,payload,**kwargs):
            result=original(remote,payload,**kwargs)
            if payload['op']=='pair':raise OSError('Synthetic reply lost after commit')
            return result
        with patch('csl_bridge_lan.request',side_effect=lose):
            result=pair(self.bob,invitation['code'],self.alice.remote)
        self.assertEqual(result['state'],'uncertain')
        registry=dict(self.alice.registry)
        with self.assertRaisesRegex(BridgeError,'reconcile'):pair(self.bob,invitation['code'],self.alice.remote)
        result=reconcile_pair(self.bob)
        self.assertEqual(result['state'],'paired');self.assertEqual(self.alice.registry,registry)
        self.assertEqual(sum(peer=='bob' for peer in registry.values()),1)
        frame={'version':1,'op':'status','id':str(uuid.uuid4()),'target':'worker'}
        with self.assertRaises(BridgeError):request(self.alice.remote,{'op':'call','token':old,'request':frame})

    def test_rate_and_malformed_public_endpoints_fail_closed(self):
        invitation=self.alice.invite();secret=code_bytes(invitation['code'])
        query={'nonce':'a'*32,'proof':mac(secret,{'nonce':'a'*32})}
        with patch('csl_bridge_lan.time.monotonic',return_value=10**15):
            replies=[self.alice.discovery_reply(query) for _ in range(9)]
        self.assertEqual(sum(x is not None for x in replies),8)
        for host in ('8.8.8.8','0.0.0.0','255.255.255.255','224.0.0.1','not-a-host'):
            with self.assertRaises(BridgeError):endpoint({**self.alice.remote,'host':host})
        offer={'identity':'bob','agent':'worker','endpoint':{**self.bob.remote,'host':'127.0.0.2'},'token':'a'*64,'pair_id':str(uuid.uuid4())}
        proof=mac(secret,{'offer':offer,'endpoint':self.alice.remote})
        with self.assertRaisesRegex(BridgeError,'connected LAN peer'):
            self.alice.incoming({'op':'pair','offer':offer,'proof':proof},'127.0.0.1')

    def test_tls_pin_is_checked_before_pair_secret_or_task(self):
        with patch.object(self.alice,'incoming',side_effect=AssertionError('No payload before pin')):
            with self.assertRaisesRegex(BridgeError,'identity changed'):
                request({**self.alice.remote,'fingerprint':'0'*64},{'op':'call','token':'never-send','request':{}})

    def test_pair_single_use_bilateral_authenticated_calls_and_no_model(self):
        invitation=self.alice.invite()
        result=pair(self.bob,invitation['code'],self.alice.remote)
        self.assertEqual(result['paired'],'alice');self.assertFalse(result['model_started'])
        self.assertEqual(pair(self.bob,invitation['code'],self.alice.remote)['state'],'rejected')
        for source,target in ((self.bob,self.alice),(self.alice,self.bob)):
            store=Store(source.home)
            try:config=store.peers()[target.identity]
            finally:store.close()
            self.assertEqual(set(config['allow_ops']),set(COMMUNICATION_OPS))
            self.assertEqual(Path(config['token_file']).stat().st_mode&0o777,0o600)
            frame={'version':1,'op':'status','id':str(uuid.uuid4()),'target':'worker'}
            with patch('csl_bridge_transport.local_call',return_value={'status':'idle'}) as call:
                self.assertEqual(exchange(config,source.identity,frame),{'status':'idle'})
            self.assertEqual(call.call_args.args[1]['trusted_peer'],source.identity)
            self.assertEqual(call.call_args.args[1]['request'],frame)
            with self.assertRaises(BridgeError):request(target.remote,{'op':'call','token':'f'*64,'request':frame})
        previous=self.bob;remote=dict(previous.remote);registry=dict(previous.registry)
        previous.close(revoke=False)
        replacement=Session(previous.state,previous.home,discovery_port=0);replacement.start(discovery=False)
        self.__class__.bob=replacement;self.__class__.sessions[1]=replacement
        self.assertEqual(replacement.remote,remote);self.assertEqual(replacement.registry,registry)
        self.assertTrue((replacement.state/'to-alice.json').exists())
        self.assertGreaterEqual(control(self.alice.state,'status')['paired_count'],1)
        with self.assertRaises(BridgeError):self.alice.incoming({'op':'pair_peer','config':{}})

class LANConnectorTests(unittest.TestCase):
    def test_actual_tls_ipc_task_clarification_progress_final_and_dedup(self):
        from csl_bridge_engine import Connector
        from test_bridge import FakeRuntime
        sessions=[];connectors=[]
        with tempfile.TemporaryDirectory() as tmp:
            try:
                for identity in ('owner','workerbox'):
                    root=Path(tmp);home=root/identity;thread=str(uuid.uuid4())
                    store=Store(home,create=True)
                    with store.transaction() as db:
                        db.executemany('INSERT INTO config VALUES(?,?)',[('identity',encode(identity)),('runtime_socket',encode(None))])
                        db.execute('INSERT INTO agents VALUES(?,?)',('worker',thread))
                    store.close()
                    connector=Connector(home);connector.start();connectors.append(connector)
                    if identity=='workerbox':connector.client_only=False;connector.runtime=FakeRuntime(thread)
                    session=Session(root/(identity+'-lan'),home,discovery_port=0);session.start(discovery=False);sessions.append(session)
                owner,worker=connectors;offer=sessions[0].invite();pair(sessions[1],offer['code'],sessions[0].remote)
                request_id=str(uuid.uuid4())
                frame={'version':1,'op':'followup','id':request_id,'source':'worker','target':'worker','text':'Synthetic task; no model.'}
                call={'local_op':'call','peer':'workerbox','request':frame}
                self.assertEqual(worker.peers['owner']['allow_sources'], ['worker'])
                spoofed={**frame,'id':str(uuid.uuid4()),'source':'uninvited'}
                with self.assertRaisesRegex(BridgeError, 'Source agent not admitted'):
                    worker.incoming('owner',spoofed)
                self.assertFalse([x for x in worker.runtime.calls if x[0]=='turn/start'])
                receipt=owner.local(call);self.assertEqual(receipt['state'],'delivered')
                again=owner.local(call);self.assertEqual(again,receipt)
                starts=[x for x in worker.runtime.calls if x[0]=='turn/start'];self.assertEqual(len(starts),1)
                worker.local({'local_op':'progress','source':'worker','task_id':request_id,'id':str(uuid.uuid4()),'text':'Synthetic clarification?'})
                event=owner.local({'local_op':'wait','agent':'worker','after':0,'timeout':2})['events'][0]
                self.assertEqual(event['event']['kind'],'progress')
                message={**frame,'id':str(uuid.uuid4()),'op':'send_message','text':'Synthetic owner answer.'}
                self.assertEqual(owner.local({'local_op':'call','peer':'workerbox','request':message})['turn_id'],receipt['turn_id'])
                worker.answers[(worker.agents['worker'],receipt['turn_id'])]='Synthetic final.'
                worker.completed('worker',receipt['turn_id'],'completed')
                final=owner.local({'local_op':'wait','agent':'worker','after':event['cursor'],'timeout':2})['events'][0]
                self.assertEqual(final['event']['kind'],'final');self.assertEqual(final['event']['task_id'],request_id)
                self.assertEqual(len([x for x in worker.runtime.calls if x[0]=='turn/start']),1)
            finally:
                for connector in connectors:connector.close()
                for session in sessions:session.close()
                for connector in connectors:
                    for thread in connector.threads:thread.join(1)
                    if all(not t.is_alive() for t in connector.threads):connector.store.close()

if __name__=='__main__':unittest.main()

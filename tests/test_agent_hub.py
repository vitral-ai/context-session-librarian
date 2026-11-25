import hashlib
import tempfile
from pathlib import Path
import unittest
import uuid

from csl_agent_hub import Hub
from csl_bridge_store import BridgeError, encode


class HubTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'hub.sqlite'
        self.tokens = {'laptop': 'a' * 64, 'remote': 'b' * 64, 'third': 'c' * 64}
        self.machines = {m: {'sha256': hashlib.sha256(t.encode()).hexdigest(), 'admin': m == 'laptop'}
                         for m, t in self.tokens.items()}
        self.hub = Hub(self.path, self.machines)
        for agent, cli, machine in [('claude', 'claude', 'laptop'), ('builder', 'codex', 'laptop'),
                                    ('client', 'codex', 'remote'), ('other', 'codex', 'third')]:
            self.do('laptop', action='register', name=agent, session=str(uuid.uuid4()), cli=cli, machine=machine)

    def tearDown(self):
        self.temp.cleanup()

    def do(self, auth_machine, **request):
        return self.hub.call(self.tokens[auth_machine], request)

    def allow(self, a='claude', b='client'):
        self.do('laptop', action='allow', source=a, target=b, both=True)

    def send(self, source='claude', target='client', machine='laptop', **extra):
        return self.do(machine, action='send', source=source, target=target,
                       id=extra.pop('id', str(uuid.uuid4())), text=extra.pop('text', 'hello'), **extra)

    def test_refused_pair_never_queues(self):
        with self.assertRaisesRegex(BridgeError, 'Unapproved pair'):
            self.send()
        self.assertEqual(self.do('remote', action='pull'), {'envelopes': []})

    def test_token_cannot_spoof_another_machine_or_self_approve(self):
        self.allow()
        with self.assertRaisesRegex(BridgeError, 'Source agent'):
            self.send(machine='remote')
        with self.assertRaisesRegex(BridgeError, 'owner machine'):
            self.do('remote', action='allow', source='client', target='builder')
        with self.assertRaisesRegex(BridgeError, 'another machine'):
            self.do('remote', action='register', name='claude', machine='remote', cli='codex', session=str(uuid.uuid4()))
        with self.assertRaisesRegex(BridgeError, 'Invalid machine token'):
            self.hub.call('x' * 64, {'action': 'list'})

    def test_bidirectional_and_same_machine_routes_and_actual_receipt(self):
        for a, b, sender, receiver in [('claude', 'client', 'laptop', 'remote'),
                                      ('client', 'claude', 'remote', 'laptop'),
                                      ('builder', 'client', 'laptop', 'remote'),
                                      ('client', 'builder', 'remote', 'laptop'),
                                      ('builder', 'claude', 'laptop', 'laptop')]:
            self.allow(a, b)
            result = self.send(a, b, sender)
            packet = self.do(receiver, action='pull')['envelopes'][0]
            self.assertEqual((packet['id'], packet['source']['name'], packet['target']['name']), (result['id'], a, b))
            self.do(receiver, action='ack', id=result['id'], receipt={'state': 'delivered', 'turn_id': 'native'})
            receipt = self.do(sender, action='receipt', id=result['id'])
            self.assertEqual(receipt['receipt']['turn_id'], 'native')

    def test_restart_replay_same_id_and_collision(self):
        self.allow()
        sent = self.send()
        self.hub = Hub(self.path, self.machines)
        self.assertEqual(self.send(id=sent['id']), {'id': sent['id'], 'state': 'queued', 'receipt': None})
        self.assertEqual(len(self.do('remote', action='pull')['envelopes']), 1)
        with self.assertRaisesRegex(BridgeError, 'collision'):
            self.send(id=sent['id'], text='different')

    def test_revocation_and_rebinding_hold_old_envelopes(self):
        self.allow(); self.send()
        self.do('laptop', action='deny', source='claude', target='client')
        self.assertEqual(self.do('remote', action='pull')['envelopes'], [])
        self.allow()
        self.do('laptop', action='register', name='client', machine='remote', cli='codex', session=str(uuid.uuid4()))
        self.allow()
        self.assertEqual(self.do('remote', action='pull')['envelopes'], [])

    def test_busy_target_backlog_cannot_hide_another_recipient(self):
        self.allow(); self.allow('claude', 'builder')
        for _ in range(20):
            self.send()
        self.send(target='builder')
        self.assertEqual(len(self.do('laptop', action='pull')['envelopes']), 1)

    def test_reply_must_reverse_endpoints_and_foreign_machine_cannot_ack_or_read(self):
        self.allow()
        sent = self.send()
        reply = self.send('client', 'claude', 'remote', in_reply_to=sent['id'])
        self.assertEqual(reply['state'], 'queued')
        with self.assertRaisesRegex(BridgeError, 'reverse'):
            self.send(in_reply_to=sent['id'])
        with self.assertRaisesRegex(BridgeError, 'recipient machine'):
            self.do('laptop', action='ack', id=sent['id'], receipt={'state': 'delivered'})
        with self.assertRaisesRegex(BridgeError, 'endpoint'):
            self.do('third', action='receipt', id=sent['id'])

    def test_large_backlog_is_bounded_by_wire_bytes_and_survives_until_ack(self):
        self.allow()
        for _ in range(20):
            self.send(text='x' * 9999)
        packets = self.do('remote', action='pull')
        self.assertLess(len(encode(packets).encode()), 65536)
        self.assertEqual(len(packets['envelopes']), 1)
        self.assertEqual(packets, self.do('remote', action='pull'))


if __name__ == '__main__':
    unittest.main()

class NativeDeliveryTests(unittest.TestCase):
    def connector(self, state='idle'):
        from types import SimpleNamespace
        import threading
        from unittest.mock import Mock
        source={'name':'claude','session':str(uuid.uuid4()),'cli':'claude','machine':'laptop'}
        target={'name':'client','session':str(uuid.uuid4()),'cli':'codex','machine':'remote'}
        connector=SimpleNamespace(agents={'client':target['session']},peers={},
            locks={'client':threading.RLock()},client_only=False,
            store=SimpleNamespace(agents=lambda: {'client': target['session']},
                                  native_operation=Mock(return_value=None), rows=Mock(return_value=[])),
            current=Mock(return_value={'status':state}),
            incoming=Mock(return_value={'state':'delivered','turn_id':'actual'}))
        return connector, {'id':str(uuid.uuid4()),'source':source,'target':target,'text':'hello'}

    def test_closed_and_busy_stay_queued_without_invoking_native(self):
        from unittest.mock import patch
        from csl_agents import deliver
        for state in ('agent_not_loaded','busy'):
            connector, packet=self.connector(state)
            with patch('csl_agents.config',return_value={'machine':'remote'}):
                self.assertEqual(deliver(connector,packet),{'state':'queued','reason':state})
            connector.incoming.assert_not_called()

    def test_idle_delivers_pinned_identity_and_replay_never_starts_another_turn(self):
        from unittest.mock import patch
        from csl_agents import deliver
        connector,packet=self.connector()
        with patch('csl_agents.config',return_value={'machine':'remote'}):
            result=deliver(connector,packet)
            self.assertEqual(result['turn_id'],'actual')
            request=connector.incoming.call_args.args[1]
            self.assertEqual(request['session'],packet['source']['session'])
            connector.store.native_operation.return_value = {
                'peer': 'agent-hub', 'request': request, 'state': 'delivered', 'result': result,
            }
            self.assertEqual(deliver(connector,packet),result)
            self.assertEqual(connector.incoming.call_count,1)

class ActiveNativeDeliveryTests(unittest.TestCase):
    def setUp(self):
        from test_bridge import BridgeTests
        self.bridge = BridgeTests()
        self.bridge.setUp()
        self.addCleanup(self.bridge.tearDown)
        from unittest.mock import patch
        setting = patch('csl_agents.config', return_value={'machine': 'remote'})
        setting.start()
        self.addCleanup(setting.stop)

    def packet(self):
        return {'id': str(uuid.uuid4()), 'text': 'Synthetic boundary message',
                'source': {'name': 'claude', 'session': str(uuid.uuid4()),
                           'cli': 'claude', 'machine': 'laptop'},
                'target': {'name': 'worker', 'session': self.bridge.thread,
                           'cli': 'codex', 'machine': 'remote'}}

    def test_continuously_active_turn_accepts_ordered_messages_once(self):
        from csl_agents import deliver
        runtime = self.bridge.runtime
        original = self.bridge.connector.incoming('alice', self.bridge.request())
        packets = [self.packet(), self.packet()]
        for packet in packets:
            receipt = deliver(self.bridge.connector, packet)
            self.assertEqual(receipt['state'], 'delivered')
            self.assertEqual(receipt['turn_id'], original['turn_id'])
            self.assertEqual(deliver(self.bridge.connector, packet), receipt)
        steers = [p for m, p in runtime.calls if m == 'turn/steer']
        self.assertEqual([p['clientUserMessageId'] for p in steers],
                         [p['id'] for p in packets])
        self.assertTrue(all(p['expectedTurnId'] == original['turn_id'] for p in steers))
        self.assertEqual(sum(m == 'turn/start' for m, _ in runtime.calls), 1)

    def test_active_delivery_lost_ack_is_not_replayed(self):
        from csl_agents import deliver
        self.bridge.connector.incoming('alice', self.bridge.request())
        self.bridge.runtime.lose_ack = True
        packet = self.packet()
        self.assertEqual(deliver(self.bridge.connector, packet)['state'], 'uncertain')
        self.assertEqual(deliver(self.bridge.connector, packet)['state'], 'uncertain')
        self.assertEqual(sum(m == 'turn/steer' for m, _ in self.bridge.runtime.calls), 1)

    def test_turn_finishes_before_steer_uses_existing_idle_race_recovery(self):
        from csl_agents import deliver
        from csl_bridge_rpc import RuntimeRejected
        runtime = self.bridge.runtime
        self.bridge.connector.incoming('alice', self.bridge.request())
        original_call = runtime.call
        def finish_before_steer(method, params, **kwargs):
            if method == 'turn/steer':
                runtime.history[-1]['status'] = 'completed'
                runtime.turn = None
                runtime.call = original_call
                raise RuntimeRejected('Turn finished before steer')
            return original_call(method, params, **kwargs)
        runtime.call = finish_before_steer
        packet = self.packet()
        receipt = deliver(self.bridge.connector, packet)
        self.assertEqual(receipt['state'], 'delivered')
        self.assertEqual(len(runtime.history), 2)
        self.assertEqual(runtime.history[-1]['items'][0]['clientId'], packet['id'])


class HTTPSAndMuxTests(unittest.TestCase):
    def test_tls_authenticated_request_and_ssh_share_one_listener(self):
        import json, shutil, socket, subprocess, threading, urllib.request, ssl
        if not shutil.which('openssl'):
            self.skipTest('openssl needed for isolated certificate fixture')
        from csl_agent_hub import server
        from csl_agents import call
        with tempfile.TemporaryDirectory() as directory:
            base=Path(directory);key=base/'tls.key';cert=base/'tls.pem'
            subprocess.run(['openssl','req','-x509','-newkey','ed25519','-nodes','-days','1',
                            '-keyout',str(key),'-out',str(cert),'-subj','/CN=localhost',
                            '-addext','subjectAltName=DNS:localhost'],check=True,capture_output=True)
            token='a'*64;tokenfile=base/'token';tokenfile.write_text(token);tokenfile.chmod(0o600)
            hub=Hub(base/'hub.sqlite',{'local':{'sha256':hashlib.sha256(token.encode()).hexdigest(),'admin':True}})
            ssh=socket.socket();ssh.bind(('127.0.0.1',0));ssh.listen()
            def ssh_banner():
                peer,_=ssh.accept()
                with peer:
                    self.assertTrue(peer.recv(128).startswith(b'SSH-2.0-'))
                    peer.sendall(b'SSH-2.0-test\r\n')
            threading.Thread(target=ssh_banner,daemon=True).start()
            http=server(hub,('127.0.0.1',0),cert,key,ssh.getsockname()[1])
            threading.Thread(target=http.serve_forever,daemon=True).start()
            settings={'url':'https://localhost:'+str(http.server_port),'cafile':str(cert),'token_file':str(tokenfile)}
            try:
                self.assertEqual(call(settings,{'action':'list'}),{'agents':[]})
                with socket.create_connection(http.server_address,timeout=3) as client:
                    client.sendall(b'SSH-2.0-testclient\r\n')
                    self.assertEqual(client.recv(128),b'SSH-2.0-test\r\n')
                tokenfile.write_text('b'*64)
                with self.assertRaisesRegex(BridgeError,'Invalid machine token'):
                    call(settings,{'action':'list'})
            finally:
                http.shutdown();http.server_close();ssh.close()

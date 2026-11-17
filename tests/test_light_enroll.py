"""Synthetic enrollment: actual installer/SQLite/CLI, owner SSH boundary stubbed."""
import base64
from concurrent.futures import ThreadPoolExecutor
import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import codex_sessions
from csl_bridge import configure
from csl_bridge_engine import Connector
from csl_bridge_store import Store, encode
import csl_light as light
import enroll_light as enroll
ROOT = Path(__file__).resolve().parents[1]


def bridge(path, identity, agent):
    store = Store(path, create=True)
    with store.transaction() as db:
        db.executemany('INSERT INTO config VALUES(?,?)', [('identity', encode(identity)), ('runtime_socket', 'null')])
        db.execute('INSERT INTO agents VALUES(?,?)', (agent, str(uuid.uuid4())))
    thread = store.agents()[agent]
    store.close()
    return thread


class EnrollmentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.host = self.root / 'host-bridge'; self.guest = self.root / 'guest' / 'bridge'
        bridge(self.host, 'host-core', 'coordinator')
        self.thread = bridge(self.guest, 'guest-core', 'worker')
        self.home = self.root / 'light'
        self.config = enroll.initialize(self.home, 'hub', self.host, ['coordinator'], True)
        self.config = json.loads((self.home / 'config.json').read_text())
        self.marker = self.root / 'guest' / 'first-brief.json'
        self.marker.write_text(encode({'agent': 'worker', 'thread': self.thread}))
        self.key = self.home / 'keys' / 'env-a'; self.key.parent.mkdir()
        self.key.write_text('SYNTHETIC LOCAL REVOKE FILE, NOT A CREDENTIAL')
        self.key.with_suffix('.pub').write_text('ssh-ed25519 AAAA synthetic')

    def test_full_enrollment_from_fresh_home_uses_real_installer_and_no_lifecycle(self):
        run = subprocess.run
        seen = []
        guest_home = self.root / 'guest'
        env = dict(os.environ, HOME=str(guest_home), CODEX_HOME=str(guest_home / 'codex'),
                   CLAUDE_CONFIG_DIR=str(guest_home / 'claude'))
        def owner_hook(argv, **kwargs):
            self.assertEqual(argv, ['synthetic-owner-installer'])
            request = json.loads(kwargs['input']); seen.append(request)
            self.assertFalse(request['payload']['authorize_key'])
            return run([sys.executable, '-I', '-B', '-c', request['script']],
                       input=encode(request['payload']).encode(), stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, check=True, env=env)
        args = ['--home', str(self.home), 'env-a', '--agent', 'worker',
                '--bootstrap-command', '["synthetic-owner-installer"]',
                '--connect-command', '["owner-stream", "{attach}"]', '--guest-bridge-home', str(self.guest),
                '--guest-light-home', str(guest_home / 'light'), '--first-brief-marker', str(self.marker),
                '--key', str(self.key), '--no-services']
        with patch('enroll_light.subprocess.run', side_effect=owner_hook), contextlib.redirect_stdout(io.StringIO()):
            enroll.main(args)
        self.assertEqual(len(seen), 1)
        snapshots = list((self.home / 'enrollments').glob('*/host-core-peer-before.json'))
        self.assertEqual(len(snapshots), 1)
        self.assertIsNone(json.loads(snapshots[0].read_text())['prior'])
        cfg = json.loads((self.home / 'config.json').read_text())
        self.assertEqual(cfg['links']['env-a']['agent_threads'], {'worker': self.thread})
        self.assertIn('attach', cfg['links']['env-a']['connect_command'][1])
        store = Store(self.host)
        try: self.assertEqual(store.peers()['env-a']['transport'], 'light')
        finally: store.close()
        store = Store(self.guest)
        try: self.assertEqual(store.peers()['host-core']['transport'], 'light')
        finally: store.close()
        self.assertTrue((guest_home / '.local/bin/csl-light').is_file())
        self.assertFalse((guest_home / '.ssh/authorized_keys').exists())
        self.assertFalse((guest_home / '.config/systemd/user').exists())

    def test_unbriefed_guest_refuses_before_peer_or_key_admission(self):
        source = list(ROOT.glob('csl_bridge*.py')) + [ROOT / n for n in
            ('enroll_light.py', 'csl_light.py', 'csl_light_cli.py', 'csl_secrets.py')]
        payload = {'files': {p.name: base64.b64encode(p.read_bytes()).decode() for p in source},
                   'light_home': str(self.root / 'guest/light'), 'bridge_home': str(self.guest),
                   'first_brief_marker': str(self.root / 'missing.json'), 'agent': 'worker'}
        result = subprocess.run([sys.executable, '-I', '-B', '-c', enroll.REMOTE_INSTALL],
            input=encode(payload).encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / 'guest/light/config.json').exists())
        store = Store(self.guest)
        try: self.assertEqual(store.peers(), {})
        finally: store.close()

    def test_peer_rollback_restores_exact_prior_and_refuses_concurrent_change(self):
        before = enroll.adopt_peers(self.config, 'env-a', 'worker')
        enroll.restore_peers(self.config, 'env-a', before)
        store = Store(self.host)
        try: self.assertNotIn('env-a', store.peers())
        finally: store.close()
        before = enroll.adopt_peers(self.config, 'env-a', 'worker')
        store = Store(self.host)
        try:
            with store.transaction() as db: db.execute('UPDATE peers SET config=? WHERE name=?', ('{"owner_changed":true}', 'env-a'))
        finally: store.close()
        with self.assertRaisesRegex(ValueError, 'changed concurrently'):
            enroll.restore_peers(self.config, 'env-a', before)

    def test_bridge_peer_defaults_light_and_keeps_explicit_ssh_fallback(self):
        parser = codex_sessions.build_parser()
        common = ['bridge', '--home', str(self.host), 'peer']
        for peer, extra, expected in [('env-a', [], 'light'), ('old-env', ['--ssh-alias', 'legacy-owner'], 'ssh')]:
            args = parser.parse_args(common + [peer, '--allow-agent', 'coordinator', '--allow-op', 'mail'] + extra)
            configure(args)
            store = Store(self.host)
            try: self.assertEqual(store.peers()[peer]['transport'], expected)
            finally: store.close()

    def test_existing_connector_discovers_light_without_restart(self):
        connector = Connector(self.host)
        try:
            enroll.adopt_peers(self.config, 'env-a', 'worker')
            self.assertNotIn('env-a', connector.peers)
            connector.reload_light_peers()
            self.assertEqual(connector.peers['env-a']['transport'], 'light')
        finally: connector.close()

    def test_connector_applies_deleted_and_rolled_back_light_rows(self):
        connector=Connector(self.host)
        try:
            before=enroll.adopt_peers(self.config,'env-a','worker')
            connector.reload_light_peers();self.assertIn('env-a',connector.peers)
            enroll.restore_peers(self.config,'env-a',before)
            connector.reload_light_peers();self.assertNotIn('env-a',connector.peers)
            store=Store(self.host)
            try:
                with store.transaction()as db:db.execute('INSERT INTO peers VALUES(?,?)',('env-a',encode({'transport':'ssh','alias':'original'})))
            finally:store.close()
            before=enroll.adopt_peers(self.config,'env-a','worker')
            connector.reload_light_peers()
            enroll.restore_peers(self.config,'env-a',before)
            connector.reload_light_peers();self.assertEqual(connector.peers['env-a']['transport'],'ssh')
        finally:connector.close()

    def test_retention_compacts_payload_but_keeps_duplicate_and_collision_guard(self):
        journal=light.Journal(self.home/'light.sqlite');identifier=str(uuid.uuid4());payload={'synthetic':'x'*48000}
        journal.put('in','env-a',identifier,payload);journal.finish('in','env-a',identifier,{'ok':True,'data':{'state':'delivered'}})
        with journal.db()as db:db.execute('UPDATE rpc SET updated=?',(time.time()-31*86400,))
        journal.prune(30)
        with journal.db()as db:self.assertEqual(db.execute('SELECT count(*) FROM rpc').fetchone()[0],0)
        self.assertEqual(journal.put('in','env-a',identifier,payload)['state'],'done')
        with self.assertRaisesRegex(ValueError,'collision'):journal.put('in','env-a',identifier,{'changed':True})
        self.assertEqual(json.loads(journal.get('in','env-a',identifier)['result'])['ok'],True)

    def test_retry_backoff_is_per_message_capped_and_does_not_block_other_peers(self):
        node=light.Node(self.config);self.addCleanup(node.pool.shutdown,False)
        bad,good=str(uuid.uuid4()),str(uuid.uuid4())
        node.journal.put('in','offline',bad,{'synthetic':True})
        node.journal.put('in','active',good,{'synthetic':True})
        for _ in range(10):node.journal.error('in','offline',bad,RuntimeError())
        self.assertFalse(node.journal.retry_due('offline',bad))
        self.assertEqual([r['id']for r in node.journal.pending('in')],[good])
        with node.journal.db()as db:
            when=db.execute('SELECT next FROM delivery_retries WHERE id=?',(bad,)).fetchone()[0]
        self.assertLessEqual(when-time.time(),60)
        node.executing={('offline',str(i))for i in range(4)}
        with patch.object(node.pool,'submit')as submit:
            node.schedule('offline',bad);submit.assert_not_called()
            node.schedule('active',good);submit.assert_called_once()

    def test_config_reload_seeds_new_routes_without_closing_existing_channel(self):
        node = light.Node(self.config); self.addCleanup(node.pool.shutdown, False)
        class Channel:
            closed = False
            def close(self): self.closed = True
        old = Channel(); node.channels['old-env'] = old
        node.config['links']['old-env'] = {'agents': ['old-worker']}
        changed = json.loads(json.dumps(node.config))
        changed['links']['env-a'] = {'agents': ['worker'], 'agent_threads': {'worker': self.thread}}
        enroll.atomic(self.home / 'config.json', encode(changed).encode())
        node.sync_links()
        self.assertFalse(old.closed)
        self.assertEqual(node.journal.resolve(self.thread)['peer'], 'env-a')

    def test_concurrent_duplicate_ids_make_one_record(self):
        journal = light.Journal(self.home / 'light.sqlite'); identifier = str(uuid.uuid4())
        with ThreadPoolExecutor(8) as pool:
            rows = list(pool.map(lambda _: journal.put('out', 'env-a', identifier, {'synthetic': True}), range(24)))
        self.assertEqual(len({row['hash'] for row in rows}), 1)
        with journal.db() as db: self.assertEqual(db.execute('SELECT count(*) FROM rpc').fetchone()[0], 1)

    def test_reenrollment_replaces_managed_authorized_key(self):
        source=list(ROOT.glob('csl_bridge*.py'))+[ROOT/n for n in ('enroll_light.py','csl_light.py','csl_light_cli.py','csl_secrets.py')]
        guest_home=self.root/'guest';authorized=guest_home/'.ssh/authorized_keys'
        authorized.parent.mkdir();authorized.write_text('ssh-ed25519 AAAA unrelated-owner-key\n')
        payload={'files':{p.name:base64.b64encode(p.read_bytes()).decode()for p in source},
                 'light_home':str(guest_home/'light'),'bridge_home':str(self.guest),
                 'first_brief_marker':str(self.marker),'agent':'worker','peer':'env-a',
                 'host_node':'hub','host_agents':['coordinator'],'host_bridges':{'host-core':['coordinator']},
                 'authorize_key':True,'service':False,'public_key':'ssh-ed25519 AAAAold synthetic'}
        env=dict(os.environ,HOME=str(guest_home))
        for key in ('AAAAold','AAAAnew'):
            payload['public_key']='ssh-ed25519 '+key+' synthetic'
            result=subprocess.run([sys.executable,'-I','-B','-c',enroll.REMOTE_INSTALL],
                input=encode(payload).encode(),env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=10)
            self.assertEqual(result.returncode,0,result.stderr.decode())
        text=authorized.read_text();self.assertIn('unrelated-owner-key',text)
        self.assertNotIn('AAAAold',text);self.assertEqual(text.count(' csl-light-env-a'),1)
        self.assertIn('AAAAnew',text)

    def test_remote_revocation_parks_reconnect_until_reenrollment(self):
        node=light.Node(self.config);self.addCleanup(node.pool.shutdown,False)
        link={'key':str(self.key),'agents':['worker'],'connect_command':[
            sys.executable,'-c','print(\'{"type":"revoked"}\',flush=True)']}
        node.config['links']['env-a']=link
        node.ssh_loop('env-a',link)
        self.assertTrue((self.home/'revoked-env-a').exists())
        with patch('csl_light.subprocess.Popen')as popen:
            node.ssh_loop('env-a',link);popen.assert_not_called()

    def test_deleted_guest_marker_emits_revocation_on_next_attach(self):
        import socket,threading
        listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        listener.bind(str(self.home/'light.sock'));listener.listen(1)
        def accept():
            connection,_=listener.accept()
            with connection:connection.recv(1)
        t=threading.Thread(target=accept,daemon=True);t.start()
        try:
            result=subprocess.run([sys.executable,'-I','-B',str(ROOT/'csl_light.py'),
                '--home',str(self.home),'attach','--peer','hub','--key-file',str(self.root/'deleted.pub')],
                stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=3)
            self.assertEqual(result.returncode,0);self.assertEqual(json.loads(result.stdout),{'type':'revoked'})
        finally:listener.close();t.join(1)

    def test_large_inbox_uses_sqlite_without_wire_size_limit(self):
        from csl_light_cli import inbox
        journal=light.Journal(self.home/'light.sqlite')
        for _ in range(3):
            identifier=str(uuid.uuid4())
            request={'op':'mail','action':'send','id':identifier,'source':'worker','target':'coordinator',
                     'envelope':{'body':'😀'*12000}}
            journal.put('in','env-a',identifier,{'type':'request','id':identifier,'request':request})
        messages=inbox(self.home)['messages'];self.assertEqual(len(messages),3)
        self.assertTrue(all(len(m['body'])==12000 for m in messages))

    def test_stop_terminates_a_stubborn_owned_process_group(self):
        child = subprocess.Popen([sys.executable, '-c',
            'import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);print("ready",flush=True);time.sleep(30)'],
            stdout=subprocess.PIPE, start_new_session=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), b'ready')
            light.stop_process(child)
            self.assertIsNotNone(child.poll())
        finally:
            if child.poll() is None: os.killpg(child.pid, signal.SIGKILL); child.wait()
            child.stdout.close()


if __name__ == '__main__': unittest.main()

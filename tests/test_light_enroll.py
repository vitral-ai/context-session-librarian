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

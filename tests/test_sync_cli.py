import argparse
from contextlib import redirect_stdout, redirect_stderr
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from csl_sync import command, configure_choice
from csl_sync_state import State, SyncError, private_json
from csl_sync_state import exclusive
from csl_sync_watch import process_birth, watch_owner


class CLITests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='csl-sync-cli-');self.root=Path(self.tmp.name);self.home=self.root/'sync'
    def tearDown(self):self.tmp.cleanup()
    def cli(self,*args):
        env=dict(os.environ,HOME=str(self.root/'home'),CODEX_HOME=str(self.root/'codex'),CLAUDE_CONFIG_DIR=str(self.root/'claude'))
        return subprocess.run([sys.executable,str(Path(__file__).resolve().parents[1]/'codex_sessions.py'),'sync','--home',str(self.home),*args],env=env,capture_output=True,text=True,timeout=5)

    def test_unsigned_cli_cannot_sync_and_status_does_not_discover_private_sources(self):
        status=self.cli('status','--json');self.assertEqual(status.returncode,0)
        self.assertFalse(json.loads(status.stdout)['authentication']['signed_in'])
        run=self.cli('run');self.assertEqual(run.returncode,2);self.assertIn('sign_in_required',run.stderr)
        self.assertNotIn(str(self.root),run.stderr);self.assertFalse((self.root/'codex').exists())

    def test_policy_status_contract_is_local_and_default_is_short(self):
        result = self.cli('configure', '--selection', 'new')
        self.assertEqual(result.returncode, 0, result.stderr)
        policy = json.loads(result.stdout)['policy']
        status = json.loads(self.cli('status', '--json').stdout)
        self.assertEqual(status['schema_version'], 1)
        self.assertEqual(status['policy'], policy)
        self.assertFalse(status['paired'])
        self.assertFalse(status['collector']['running'])
        self.assertEqual(status['totals']['pending'], 0)
        self.assertIn('Collector stopped; not paired; policy new.', self.cli('status').stdout)

    def test_interactive_choices_and_cancel_preserve_local_optouts(self):
        state = State(self.home)
        self.addCleanup(state.close)
        state.select('claude', 'private-old', False)
        choices = [('new', ['']), ('all', ['3']),
                   ('folders', ['4', str(self.root / 'project'), '']),
                   ('last', ['2', '1'])]
        for expected, answers in choices:
            with self.subTest(expected=expected), patch('csl_sync.sys.stdin.isatty', return_value=True), patch('builtins.input', side_effect=answers), patch('csl_sync.Providers') as providers, redirect_stderr(io.StringIO()):
                providers.return_value.list_sessions.return_value = [dict(native_id='recent', updated_at='2026-11-26T10:00:00Z')]
                policy = configure_choice(state)
                self.assertEqual(policy['selection'], expected)
                self.assertFalse(state.selection('claude', 'private-old'))
                if expected == 'last': self.assertEqual(policy['limit'], 1)
                else: providers.assert_not_called()
        before = state.get('selection_policy')
        with patch('csl_sync.sys.stdin.isatty', return_value=True), patch('builtins.input', side_effect=['4', EOFError]), redirect_stderr(io.StringIO()), self.assertRaises(SyncError) as error:
            configure_choice(state)
        self.assertEqual(error.exception.code, 'selection_cancelled')
        self.assertEqual(state.get('selection_policy'), before)
        result = self.cli('configure')
        self.assertEqual(result.returncode, 2)
        self.assertIn('selection_required', result.stderr)
        self.assertEqual(state.get('selection_policy'), before)

    def test_status_recovery_is_actionable_without_changing_credentials(self):
        state = State(self.home)
        self.addCleanup(state.close)
        state.set('settings', dict(enabled=True, endpoint='https://registry.example.test/v1',
                  discovery_url='https://identity.example.test/discovery', client_id='collector',
                  scope='session_sync offline_access'))
        state.set('registered', True)
        credentials = self.home / 'credentials.json'
        private_json(credentials, dict(access_token='PRIVATE_ACCESS', refresh_token='PRIVATE_REFRESH',
                     refresh_state='inflight', expires_at=time.time()+600))
        before = credentials.read_bytes()
        result = self.cli('status', '--json')
        self.assertEqual(result.returncode, 0, result.stderr)
        status = json.loads(result.stdout)
        self.assertFalse(status['paired'])
        recovery = status['recovery']
        self.assertEqual(recovery['reason'], 'refresh_outcome_unknown')
        self.assertEqual(recovery['argv'][:5], ['csl', 'sync', '--home', str(self.home), 'sign-in'])
        self.assertIn('https://registry.example.test/v1', recovery['argv'])
        human = self.cli('status')
        self.assertIn('Sign in again: csl sync --home', human.stdout)
        self.assertEqual(credentials.read_bytes(), before)
        self.assertNotIn('PRIVATE_', result.stdout + result.stderr + human.stdout + human.stderr)

    def test_follow_refreshes_cached_state_without_releasing_watch_owner(self):
        state = State(self.home)
        self.addCleanup(state.close)
        args = argparse.Namespace(sync_home=self.home, sync_action='status', json=True, verbose=False, follow=True, interval=1)
        out, err = io.StringIO(), io.StringIO()
        updates = iter([True, False])
        def update(_):
            if next(updates): state.set('last_error', 'registry_unavailable')
            else: raise KeyboardInterrupt
        with exclusive(state.root), watch_owner(state), patch('csl_sync.time.sleep', side_effect=update):
            with redirect_stdout(out), redirect_stderr(err), self.assertRaises(SystemExit) as stopped:
                command(args, None)
            self.assertEqual(stopped.exception.code, 130)
            rows = [json.loads(line) for line in out.getvalue().splitlines()]
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[1]['last_error'], 'registry_unavailable')
            self.assertTrue(rows[0]['collector']['busy'])
            with self.assertRaises(SyncError):
                with exclusive(state.root): pass
            self.assertEqual(state.get('watch_owner')['pid'], os.getpid())

    def test_status_reports_actual_watch_supervisor_not_busy_operation(self):
        if process_birth(os.getpid()) is None:
            self.skipTest('Exact native process identity unavailable')
        state = State(self.home)
        self.addCleanup(state.close)
        with exclusive(state.root), watch_owner(state):
            state.set('lock_action', 'run')
            status = json.loads(self.cli('status', '--json').stdout)
            self.assertTrue(status['collector']['running'])
            self.assertEqual(status['collector']['watch_pid'], os.getpid())
            record = state.get('watch_owner')
            state.set('watch_owner', {**record, 'birth': ['stale-pid-incarnation']})
            self.assertNotIn('watch_pid', json.loads(self.cli('status', '--json').stdout)['collector'])
        self.assertNotIn('watch_pid', json.loads(self.cli('status', '--json').stdout)['collector'])

    def test_local_selection_and_sign_out_do_not_delete_remote_data(self):
        for action in ['include','exclude']:
            r=self.cli(action,'claude','fixture-session');self.assertEqual(r.returncode,0)
            self.assertFalse(json.loads(r.stdout)['remote_deleted'])
        s=State(self.home);s.set('settings',{'enabled':True});s.close()
        private_json(self.home/'credentials.json',{'access_token':'fixture-access','refresh_token':'fixture-refresh','refresh_state':'ready','expires_at':time.time()+600})
        r=self.cli('sign-out');self.assertEqual(r.returncode,0);self.assertFalse(json.loads(r.stdout)['remote_revocation'])
        self.assertNotIn('fixture-access',r.stdout+r.stderr);self.assertFalse(json.loads(self.cli('status','--json').stdout)['enabled'])

    def test_foreground_watch_survives_transient_outage_with_bounded_backoff(self):
        s=State(self.home);s.set('settings',{'enabled':True,'endpoint':'http://127.0.0.1:1','token_endpoint':'http://127.0.0.1:1/token','client_id':'fake'});s.close()
        private_json(self.home/'credentials.json',{'access_token':'fixture-access','refresh_token':'fixture-refresh','refresh_state':'ready','expires_at':time.time()+600})
        args=argparse.Namespace(sync_home=self.home,sync_action='watch',all=False,days=30,providers=None,interval=2)
        out,err=io.StringIO(),io.StringIO()
        with patch('csl_sync_cache.CachedProviders'),patch('csl_sync_watch.run_pass') as worker,patch('csl_sync_watch.time.sleep',side_effect=[None,KeyboardInterrupt]):
            worker.side_effect=[(2,'','{"error":"registry_unavailable"}',123),
                                (0,'{"pending":0,"synced":0,"sessions":[]}','',124)]
            with redirect_stdout(out),redirect_stderr(err),self.assertRaises(SystemExit) as stopped:command(args,None)
            self.assertEqual(stopped.exception.code,130);self.assertEqual(worker.call_count,2)
        self.assertIn('pass_failed',err.getvalue());self.assertNotIn('fixture-access',out.getvalue()+err.getvalue())

    def test_sign_in_discloses_raw_privacy_before_enabling_without_starting_sync(self):
        args=argparse.Namespace(sync_home=self.home,sync_action='sign-in',endpoint='http://127.0.0.1:1/v1',
            client_id='public-client',discovery_url='http://127.0.0.1:1/discovery',scope='session_sync offline_access',
            audience=None,refresh_replay_grace_seconds=0,label='Fixture',kind='local',environment_name=None)
        out,err=io.StringIO(),io.StringIO()
        with patch('csl_sync.discover',return_value={'authorization_endpoint':'http://127.0.0.1:1/authorize','token_endpoint':'http://127.0.0.1:1/token'}),patch('csl_sync.Tokens.sign_in',return_value={'signed_in':True}),patch('csl_sync.Providers') as providers:
            with redirect_stdout(out),redirect_stderr(err):command(args,None)
            providers.assert_not_called()
        self.assertIn('including any secrets',err.getvalue());self.assertFalse(json.loads(out.getvalue())['sync_started'])

    def test_manual_browser_url_is_private_not_logged_and_removed_after_flow(self):
        args=argparse.Namespace(sync_home=self.home,sync_action='sign-in',endpoint='http://127.0.0.1:1/v1',
            client_id='public-client',discovery_url='http://127.0.0.1:1/discovery',scope='session_sync offline_access',
            audience=None,refresh_replay_grace_seconds=30,label='Fixture',kind='environment',environment_name='fixture',
            manual_browser=True,callback_port=8765)
        out,err=io.StringIO(),io.StringIO()
        def flow(**options):
            self.assertEqual(options['callback_port'],8765)
            options['browser']('http://127.0.0.1/authorize?state=PRIVATE_STATE_SENTINEL')
            file=self.home/'authorization.json';self.assertEqual(file.stat().st_mode & 0o777,0o600)
            self.assertIn('PRIVATE_STATE_SENTINEL',file.read_text());return {'signed_in':True}
        with patch('csl_sync.discover',return_value={'authorization_endpoint':'http://127.0.0.1:1/authorize','token_endpoint':'http://127.0.0.1:1/token'}),patch('csl_sync.Tokens.sign_in',side_effect=flow):
            with redirect_stdout(out),redirect_stderr(err):command(args,None)
        self.assertFalse((self.home/'authorization.json').exists())
        self.assertNotIn('PRIVATE_STATE_SENTINEL',out.getvalue()+err.getvalue());self.assertNotIn(str(self.home),out.getvalue()+err.getvalue())


if __name__=='__main__':unittest.main()

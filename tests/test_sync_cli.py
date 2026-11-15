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
from csl_sync import command
from csl_sync_state import State, SyncError, private_json


class CLITests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='csl-sync-cli-');self.root=Path(self.tmp.name);self.home=self.root/'sync'
    def tearDown(self):self.tmp.cleanup()
    def cli(self,*args):
        env=dict(os.environ,HOME=str(self.root/'home'),CODEX_HOME=str(self.root/'codex'),CLAUDE_CONFIG_DIR=str(self.root/'claude'))
        return subprocess.run([sys.executable,str(Path(__file__).resolve().parents[1]/'codex_sessions.py'),'sync','--home',str(self.home),*args],env=env,capture_output=True,text=True,timeout=5)

    def test_unsigned_cli_cannot_sync_and_status_does_not_discover_private_sources(self):
        status=self.cli('status');self.assertEqual(status.returncode,0)
        self.assertFalse(json.loads(status.stdout)['authentication']['signed_in'])
        run=self.cli('run');self.assertEqual(run.returncode,2);self.assertIn('sign_in_required',run.stderr)
        self.assertNotIn(str(self.root),run.stderr);self.assertFalse((self.root/'codex').exists())

    def test_local_selection_and_sign_out_do_not_delete_remote_data(self):
        for action in ['include','exclude']:
            r=self.cli(action,'claude','fixture-session');self.assertEqual(r.returncode,0)
            self.assertFalse(json.loads(r.stdout)['remote_deleted'])
        s=State(self.home);s.set('settings',{'enabled':True});s.close()
        private_json(self.home/'credentials.json',{'access_token':'fixture-access','refresh_token':'fixture-refresh','refresh_state':'ready','expires_at':time.time()+600})
        r=self.cli('sign-out');self.assertEqual(r.returncode,0);self.assertFalse(json.loads(r.stdout)['remote_revocation'])
        self.assertNotIn('fixture-access',r.stdout+r.stderr);self.assertFalse(json.loads(self.cli('status').stdout)['enabled'])

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

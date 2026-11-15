"""Pass supervision, including an actual synthetic signal crash without a core."""
import argparse
from contextlib import redirect_stdout, redirect_stderr
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
from unittest.mock import Mock, patch

from csl_sync_state import State, SyncError, exclusive, private_json
from csl_sync_watch import LOCK_ENV, pass_lock, run_pass, watch
from sync_support import Peer


class WatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = State(self.root/'state'); self.addCleanup(self.state.close)
        self.peer = Peer(); self.addCleanup(self.peer.close)
        self.state.set('settings', {'enabled':True,'endpoint':self.peer.url,'client_id':'fixture'})
        private_json(self.state.root/'credentials.json', {'access_token':'fixture-access','refresh_token':'fixture-refresh',
                     'refresh_state':'ready','expires_at':time.time()+600})
        self.args = argparse.Namespace(sync_home=self.state.root,days=30,all=True,providers=['codex'],interval=60)
        self.hints = Mock(); self.hints.changed.return_value = False

    def test_real_signal_crash_restarts_in_fresh_child_and_keeps_installation_lock(self):
        original = subprocess.Popen
        pids = []
        def spawn(command, **kwargs):
            with self.assertRaises(SyncError):
                with exclusive(self.state.root): pass
            if not pids:
                command = [sys.executable,'-c','import resource,os,signal;resource.setrlimit(resource.RLIMIT_CORE,(0,0));os.kill(os.getpid(),signal.SIGSEGV)']
            process = original(command, **kwargs); pids.append(process.pid)
            return process
        out, err = io.StringIO(), io.StringIO()
        with patch.dict(os.environ,CODEX_HOME=str(self.root/'empty-codex')), \
             patch('csl_sync_watch.subprocess.Popen',side_effect=spawn), \
             patch('csl_sync_watch.time.sleep',side_effect=[None,KeyboardInterrupt]), \
             redirect_stdout(out),redirect_stderr(err),self.assertRaises(KeyboardInterrupt):
            watch(self.args,self.state,self.hints)
        self.assertEqual(len(set(pids)),2)
        event = json.loads(err.getvalue().splitlines()[0])
        self.assertEqual(event['state'],'pass_crashed');self.assertEqual(event['exit_code'],139)
        self.assertEqual(event['signal'],'SIGSEGV')
        self.assertIn('"synced":0',out.getvalue())
        with exclusive(self.state.root): pass  # Released after supervisor shutdown.

    def test_crash_backoff_is_bounded_and_stderr_text_is_not_reflected(self):
        delays = []
        def pause(value):
            delays.append(value)
            if len(delays)==8: raise KeyboardInterrupt()
        out,err=io.StringIO(),io.StringIO()
        with patch('csl_sync_watch.run_pass',return_value=(-11,'','private transcript must not be echoed',123)), \
             patch('csl_sync_watch.time.sleep',side_effect=pause),redirect_stdout(out),redirect_stderr(err), \
             self.assertRaises(KeyboardInterrupt):
            watch(self.args,self.state,self.hints)
        self.assertEqual(delays,[1,2,4,8,16,32,60,60])
        self.assertNotIn('private transcript',out.getvalue()+err.getvalue())

    def test_revoked_credentials_stop_without_replaying_old_token_forever(self):
        with patch('csl_sync_watch.run_pass',return_value=(2,'','{"error":"credential_revoked"}',123)), \
             patch('csl_sync_watch.time.sleep') as pause,redirect_stderr(io.StringIO()),self.assertRaises(SyncError) as failed:
            watch(self.args,self.state,self.hints)
        self.assertEqual(failed.exception.code,'credential_revoked');pause.assert_not_called()

    def test_invalid_inherited_lock_descriptor_is_rejected(self):
        with patch.dict(os.environ,{LOCK_ENV:'999999'}),self.assertRaises(SyncError) as failed:
            with pass_lock(self.state.root): pass
        self.assertEqual(failed.exception.code,'invalid_pass_lock')

    def test_interrupted_supervisor_terminates_running_worker(self):
        original = subprocess.Popen
        child = original([sys.executable,'-c','import time;time.sleep(60)'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        communicate = child.communicate
        first = [True]
        def interrupted(*args,**kwargs):
            if first[0]: first[0]=False; raise KeyboardInterrupt()
            return communicate(*args,**kwargs)
        with exclusive(self.state.root) as fd,patch('csl_sync_watch.subprocess.Popen',return_value=child), \
             patch.object(child,'communicate',side_effect=interrupted),self.assertRaises(KeyboardInterrupt):
            run_pass(self.args,fd)
        self.assertIsNotNone(child.poll())

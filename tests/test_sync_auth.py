import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from csl_sync_auth import Tokens, discover
from csl_sync_state import State, SyncError, private_json, read_private_json


class AuthTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='csl-oauth-test-');self.state=State(Path(self.tmp.name)/'state')
        self.expected_challenge=None;self.forms=[];owner=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def send(self,status,value):
                raw=json.dumps(value).encode();self.send_response(status);self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
            def do_GET(self):
                origin='http://127.0.0.1:'+str(self.server.server_port)
                self.send(200,{'issuer':origin,'authorization_endpoint':origin+'/authorize','token_endpoint':origin+'/token',
                               'code_challenge_methods_supported':['S256'],'token_endpoint_auth_methods_supported':['none']})
            def do_POST(self):
                data=urllib.parse.parse_qs(self.rfile.read(int(self.headers['Content-Length'])).decode());owner.forms.append(data)
                verifier=data.get('code_verifier',[''])[0]
                challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
                if data.get('code')!=['fixture-code'] or challenge!=owner.expected_challenge:
                    return self.send(400,{'error':'invalid_grant'})
                self.send(200,{'token_type':'Bearer','access_token':'fixture-access','refresh_token':'fixture-refresh','expires_in':600,'session_id':'fixture-session'})
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.thread=threading.Thread(target=lambda:self.server.serve_forever(poll_interval=.01),daemon=True);self.thread.start()
        self.origin='http://127.0.0.1:'+str(self.server.server_port)
        self.config={'client_id':'fixture-public-client',**discover(self.origin+'/discovery')}
    def tearDown(self):self.server.shutdown();self.server.server_close();self.thread.join(2);self.state.close();self.tmp.cleanup()

    def test_browser_pkce_loopback_state_code_exchange_and_private_token_storage(self):
        seen={};threads=[]
        def browser(url):
            params=urllib.parse.parse_qs(urllib.parse.urlsplit(url).query);seen.update(params)
            self.expected_challenge=params['code_challenge'][0]
            def callback():
                # An unsolicited wrong-state callback must not complete the login.
                try:urllib.request.urlopen(params['redirect_uri'][0]+'?code=bad&state=wrong',timeout=2)
                except urllib.error.HTTPError as e:e.close()
                urllib.request.urlopen(params['redirect_uri'][0]+'?'+urllib.parse.urlencode({'code':'fixture-code','state':params['state'][0]}),timeout=2).close()
            t=threading.Thread(target=callback);threads.append(t);t.start();return True
        tokens=Tokens(self.state.root,self.config)
        result=tokens.sign_in(browser=browser,timeout=3,device_id='fixture-device')
        for t in threads:t.join(3)
        self.assertTrue(result['signed_in']);self.assertNotIn('access_token',result)
        self.assertEqual(seen['code_challenge_method'],['S256']);self.assertTrue(seen['redirect_uri'][0].startswith('http://127.0.0.1:'))
        self.assertEqual(os.stat(tokens.path).st_mode & 0o777,0o600)
        self.assertEqual(self.forms[0]['installation_id'],['fixture-device'])
        self.assertNotIn('client_secret',self.forms[0]);self.assertNotEqual(self.forms[0]['code_verifier'][0],self.expected_challenge)

    def rotating(self, grace):
        owner=self
        class FakeHTTP:
            calls=0
            def request(self,*args,**kwargs):
                self.calls+=1
                body=urllib.parse.parse_qs(kwargs['data'].decode());owner.assertEqual(body['refresh_token'],['old-generation'])
                # The identity committed a successor, but its first response was lost.
                if self.calls==1:raise SyncError('registry_unavailable',retryable=True)
                return {'token_type':'Bearer','access_token':'new-access','refresh_token':'successor-generation','expires_in':600}
        http=FakeHTTP();config={**self.config,'refresh_replay_grace_seconds':grace};tokens=Tokens(self.state.root,config,http=http)
        private_json(tokens.path,{'access_token':'expired-access','refresh_token':'old-generation','expires_at':0,'refresh_state':'ready'})
        return tokens,http

    def test_lost_refresh_response_replays_only_under_explicit_successor_grace_policy(self):
        tokens,http=self.rotating(30)
        self.assertEqual(tokens.access_token(),'new-access');self.assertEqual(http.calls,2)
        self.assertEqual(read_private_json(tokens.path)['refresh_token'],'successor-generation')
        self.assertEqual(read_private_json(tokens.path)['refresh_state'],'ready')

    def test_generic_issuer_never_blindly_retries_an_old_refresh_token(self):
        tokens,http=self.rotating(0)
        for _ in range(2):
            with self.assertRaises(SyncError) as error:tokens.access_token()
            self.assertEqual(error.exception.code,'refresh_outcome_unknown')
        self.assertEqual(http.calls,1)

    def test_discovered_successor_replay_policy_recovers_lost_response(self):
        class DiscoveryHTTP:
            def request(inner, *args, **kwargs):
                return {'code_challenge_methods_supported':['S256'],
                        'authorization_endpoint':self.origin+'/authorize',
                        'token_endpoint':self.origin+'/token',
                        'refresh_replay_grace_seconds':30}
        tokens,http=self.rotating(0)
        tokens.config.update(discover(self.origin+'/discovery',http=DiscoveryHTTP()))
        self.assertEqual(tokens.access_token(),'new-access')
        self.assertEqual(http.calls,2)

    def test_invalid_discovered_replay_policy_is_rejected(self):
        for grace in (True, -1, 31, '30', None):
            with self.subTest(grace=grace):
                class DiscoveryHTTP:
                    def request(inner, *args, **kwargs):
                        return {'code_challenge_methods_supported':['S256'],
                                'authorization_endpoint':self.origin+'/authorize',
                                'token_endpoint':self.origin+'/token',
                                'refresh_replay_grace_seconds':grace}
                with self.assertRaises(SyncError) as error:
                    discover(self.origin+'/discovery',http=DiscoveryHTTP())
                self.assertEqual(error.exception.code,'invalid_discovery')

    def test_restart_after_rotating_refresh_recovers_within_grace_but_not_after(self):
        tokens,http=self.rotating(30)
        value=read_private_json(tokens.path);value.update(refresh_state='inflight',refresh_started_at=time.time(),replay_attempted=False)
        private_json(tokens.path,value);http.calls=1
        self.assertEqual(tokens.access_token(),'new-access');self.assertEqual(http.calls,2)
        value.update(refresh_started_at=time.time()-31);private_json(tokens.path,value)
        with self.assertRaises(SyncError) as error:tokens.access_token()
        self.assertEqual(error.exception.code,'refresh_outcome_unknown');self.assertEqual(http.calls,2)

    def test_refresh_revocation_erases_credentials_and_does_not_loop(self):
        tokens,http=self.rotating(30)
        def revoked(*args,**kwargs):raise SyncError('invalid_grant',status=400)
        http.request=revoked
        with self.assertRaises(SyncError) as error:tokens.access_token()
        self.assertEqual(error.exception.code,'credential_revoked');self.assertFalse(tokens.status()['signed_in'])
        self.assertNotIn('refresh_token',read_private_json(tokens.path))

    def test_unsafe_token_permissions_are_rejected(self):
        tokens,http=self.rotating(0);tokens.path.chmod(0o644)
        with self.assertRaises(SyncError) as error:tokens.access_token()
        self.assertEqual(error.exception.code,'unsafe_credentials_file');self.assertEqual(http.calls,0)

    def test_new_authorization_resets_old_account_cursors_but_keeps_opt_outs(self):
        self.state.save_session('codex','old',{'files':{'main':{'revision':'old-account'}},'view':{'last_seq':20},'status':'synced'})
        self.state.select('codex','old',False);self.state.reset_checkpoints()
        self.assertEqual(self.state.session('codex','old')['files'],{});self.assertEqual(self.state.session('codex','old')['view'],{})
        self.assertFalse(self.state.selection('codex','old'))


if __name__=='__main__':unittest.main()

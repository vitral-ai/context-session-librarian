"""Typed owner-local boundary tests; no provider or real identity used."""
import copy
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch, Mock
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import csl_resume_action as action
from csl_sync_state import State, SyncError, private_json

NATIVE='11111111-1111-4111-8111-111111111111'
REQUEST='22222222-2222-4222-8222-222222222222'
SNAP='33333333-3333-4333-8333-333333333333'
DEVICE='44444444-4444-4444-8444-444444444444'
ACCOUNT='a'*24
REGISTRY='ss_'+hashlib.sha256(json.dumps([ACCOUNT,'codex',NATIVE],separators=(',',':')).encode()).hexdigest()

def request(**changes):
    value=dict(schema_version=1,action='check',intent='destination',request_id=REQUEST,provider='codex',
        native_id=NATIVE,registry_session_id=REGISTRY,expected_snapshot_id=SNAP,expected_generation=0,
        expected_host_id='host-'+'a'*16,target_id='project')
    value.update(changes)
    return value

class RequestTests(unittest.TestCase):
    def test_closed_input(self):
        self.assertEqual(action.validate(request()),request())
        for changed in (dict(extra='bad'),dict(schema_version=True),dict(expected_generation=True),
                        dict(native_id='X'),dict(target_id='../escape'),dict(action='release'),
                        dict(expected_host_id='other'),dict(request_id=REQUEST.upper().replace('2','A'))):
            with self.subTest(changed=changed),self.assertRaises(SyncError): action.validate(request(**changed))
        q=request(action='run',expected_device_id=DEVICE);q.pop('intent')
        action.validate(q)
        missing=request(); missing.pop('target_id')
        with self.assertRaises(SyncError): action.validate(missing)
        q.pop('expected_device_id')
        with self.assertRaises(SyncError):action.validate(q)

class LocalTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.host=self.root/'host';self.host.mkdir(mode=0o700)
        self.profile=self.root/'profile';s=State(self.profile);s.close()
        self.cwd=self.root/'project';self.cwd.mkdir()
        self.provider=self.root/'codex';self.provider.mkdir()
        self.binary=self.host/'vitral-host';self.binary.touch()
        self.target=self.host/'managed-resume-targets';self.target.mkdir(mode=0o700)
        p=self.host/'api.HOST_ID';p.write_text('host-'+'a'*16);p.chmod(0o600)
        private_json(self.host/'collector_setup.json',dict(home=str(self.profile),runtime=sys.executable,
            source=str(Path(action.__file__).parent)))
        private_json(self.target/'project.json',dict(schema_version=1,provider='codex',cwd=str(self.cwd),
            provider_home=str(self.provider),executable=str(self.binary)))
        self.env=patch.dict(os.environ,VITRAL_TUNNEL_HOME=str(self.host),CODEX_HOME=str(self.provider))
        self.env.start();self.addCleanup(self.env.stop)
    def test_local_binding_and_target(self):
        local=action.Local(request())
        self.assertEqual(local.profile,self.profile)
        self.assertEqual(local.target['cwd'],str(self.cwd))
        with self.assertRaises(SyncError):action.Local(request(expected_host_id='host-'+'b'*16))
        (self.host/'collector_setup.json').unlink()
        with self.assertRaises(SyncError):action.Local(request())
    def test_target_captures_canonical_paths_before_host_handoff(self):
        alias=self.root/'project-link';alias.symlink_to(self.cwd,target_is_directory=True)
        executable=self.root/'host-link';executable.symlink_to(self.binary)
        private_json(self.target/'project.json',dict(schema_version=1,provider='codex',cwd=str(alias),
            provider_home=str(self.provider),executable=str(executable)))
        local=action.Local(request())
        self.assertEqual(local.target['cwd'],str(self.cwd.resolve()))
        self.assertEqual(local.target['executable'],str(self.binary.resolve()))
        other=self.root/'other-project';other.mkdir()
        alias.unlink();alias.symlink_to(other,target_is_directory=True)
        self.assertEqual(local.target['cwd'],str(self.cwd.resolve()))
        self.assertNotEqual(action.Local(request()).target,local.target)

    def test_cyclic_target_has_fixed_diagnostic(self):
        loop=self.root/'private-project-loop';loop.symlink_to(loop)
        for field in ('cwd','executable'):
            target=dict(schema_version=1,provider='codex',cwd=str(self.cwd),
                provider_home=str(self.provider),executable=str(self.binary))
            target[field]=str(loop)
            private_json(self.target/'project.json',target)
            with self.subTest(field=field),self.assertRaisesRegex(SyncError,'^invalid_resume_target$'):
                action.Local(request())

    def test_target_must_match_collector_home(self):
        with patch.dict(os.environ,CODEX_HOME=str(self.root/'other')):
            with self.assertRaises(SyncError):action.Local(request())
    def test_host_uses_fixed_argv_and_validates_live_proof(self):
        q=request(action='run',expected_device_id=DEVICE);q.pop('intent')
        local=action.Local(q)
        result=dict(schema_version=1,request_id=REQUEST,provider='codex',native_session_id=NATIVE,state='launched',
            keeper_session_id='live-'+REQUEST,process=dict(pid=123,start_identity='incarnation'))
        def host(argv,**kw):
            self.assertEqual(argv,[str(self.binary),'keeper-managed-resume','--json-stdin'])
            payload=json.loads(kw['input'])
            self.assertEqual(payload['target_id'],'project')
            self.assertTrue(payload['readback_only'])
            self.assertEqual(payload['expected_target'],local.target)
            self.assertEqual(payload['expected_writer']['generation'],1)
            kw['stdout'].write(json.dumps(result).encode());return Mock(returncode=0)
        with patch('csl_resume_action.subprocess.run',side_effect=host):
            self.assertEqual(local.run_host(dict(expected_target=local.target,expected_writer=dict(generation=1),expected_binding={},readback_only=True)),result)
            result['native_session_id']=DEVICE
            with self.assertRaises(SyncError):local.run_host(dict(expected_target=local.target,expected_writer=dict(generation=1),expected_binding={},readback_only=True))
    def test_owner_bound_destination_preflight(self):
        registry=Mock();registry.call.return_value=dict(id=REGISTRY,provider='codex',native_id=NATIVE,snapshot_id=SNAP,
            writer=dict(state='released',generation=0),sync=dict(complete=True),package=dict(available=True))
        from csl_sync_providers import ProviderError
        with patch('csl_sync.attest_installation',return_value=dict(account_id=ACCOUNT,device_id=DEVICE)),\
             patch('csl_sync_http.Registry',return_value=registry),\
             patch('csl_sync_providers._resolve',side_effect=ProviderError('session_not_found')):
            s=State(self.profile);s.set('settings',dict(endpoint='https://registry.example'));s.close()
            result=action.execute(request())
            self.assertEqual(result['state'],'eligible')
            self.assertEqual(result['device_id'],DEVICE)
            for status in ('legacy', 'active', 'released'):
                registry.call.return_value['writer']['state'] = status
                self.assertEqual(action.execute(request())['state'], 'eligible')
            registry.call.return_value['writer']['generation'] = 1
            with self.assertRaises(SyncError): action.execute(request())
            registry.call.return_value['writer']['generation'] = 0
            registry.call.assert_called_with('GET','/sessions/'+REGISTRY)
            with self.assertRaises(SyncError):action.execute(request(registry_session_id='ss_'+'0'*64))
            with self.assertRaises(SyncError):action.execute(request(expected_device_id=NATIVE))
    def test_run_releases_native_lock_before_host(self):
        registry=Mock();registry.call.return_value=dict(id=REGISTRY,provider='codex',native_id=NATIVE,snapshot_id=SNAP,
            writer=dict(state='active',generation=1,device_id=DEVICE))
        s=State(self.profile);s.set('settings',dict(endpoint='https://registry.example'))
        s.save_session('codex',NATIVE,dict(writer=dict(state='active',generation=1,
            registry_id=REGISTRY,account_id=ACCOUNT,device_id=DEVICE)));s.close()
        held=[]
        @contextmanager
        def lock(*args):
            held.append(True)
            try:yield
            finally:held.pop()
        fence=dict(state='active',binding=dict(profile=str(self.profile),device_id=DEVICE,endpoint='https://registry.example'))
        def launch(admission):
            self.assertFalse(held)
            self.assertEqual(admission['expected_writer'],dict(registry_session_id=REGISTRY,account_id=ACCOUNT,device_id=DEVICE,generation=1))
            self.assertEqual(admission['expected_target']['cwd'],str(self.cwd))
            # Host must compare these captured expectations after taking its lock.
            from csl_sync_writer import read_fence
            self.assertEqual(read_fence('codex',NATIVE)['writer'],admission['expected_writer'])
            self.assertEqual(admission['expected_binding'],fence['binding'])
            return dict(state='launched',process=dict(pid=1,start_identity='start'),keeper_session_id='live-'+REQUEST)
        q=request(action='run',expected_device_id=DEVICE,expected_generation=1);q.pop('intent')
        with patch('csl_sync.attest_installation',return_value=dict(account_id=ACCOUNT,device_id=DEVICE)),\
             patch('csl_sync_http.Registry',return_value=registry),patch('csl_resume_action.check_fence',return_value=fence),\
             patch('csl_sync_providers._resolve'),patch('csl_resume_action.native_lock',lock),\
             patch.object(action.Local,'run_host',side_effect=launch):
            self.assertEqual(action.execute(q)['state'],'launched')
            fence['writer']=dict(registry_session_id=REGISTRY,account_id=ACCOUNT,device_id=DEVICE,generation=2)
            with self.assertRaisesRegex(SyncError,'writer_fenced'): action.execute(q)

if __name__=='__main__':unittest.main()

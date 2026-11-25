"""Stdio adapter boundaries; no native runtime, network, auth or inference."""
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from csl_bridge_mcp import Adapter, serve, configuration
from csl_bridge_store import BridgeError, Store, encode


class MCPTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = Path(self.temp.name) / 'bridge'
        store = Store(self.home, create=True)
        with store.transaction() as db:
            db.execute('INSERT INTO agents VALUES(?,?)', ('worker', str(uuid.uuid4())))
        store.close()
        self.adapter = Adapter(self.home, 'worker')

    def tearDown(self):
        self.temp.cleanup()

    def test_reviewable_configuration_scopes_explicit_consent_without_applying(self):
        import tomllib
        before = set(self.home.iterdir())
        with patch('csl_bridge_mcp.local_call') as call:
            profile = tomllib.loads(configuration(self.home, 'worker', ['send_progress']))
        call.assert_not_called()
        self.assertEqual(before, set(self.home.iterdir()))
        config = profile['mcp_servers']['csl_bridge']
        self.assertEqual(config['tools'], {'send_progress': {'approval_mode': 'approve'}})
        self.assertEqual(config['default_tools_approval_mode'], 'writes')
        self.assertEqual(config['args'], ['--home', str(self.home), '--agent', 'worker'])
        self.assertFalse(set(config['enabled_tools']) & {'followup_task', 'interrupt_agent', 'send_message'})
        self.assertNotIn('approval_policy', profile)
        self.assertNotIn('sandbox_mode', profile)
        unapproved = tomllib.loads(configuration(self.home, 'worker'))['mcp_servers']['csl_bridge']
        self.assertNotIn('tools', unapproved)
        self.assertNotIn('send_progress', unapproved['enabled_tools'])
        with self.assertRaises(BridgeError):
            configuration(self.home, 'worker', ['followup_task'])

    def test_history_is_read_only_bound_and_calls_hub_without_runtime(self):
        self.assertTrue(next(t for t in self.adapter.catalog() if t['name'] == 'history')['annotations']['readOnlyHint'])
        with patch('csl_agents.config', return_value={}), patch('csl_agents.call', return_value={'messages': []}) as hub, patch('csl_bridge_mcp.local_call') as runtime:
            result = self.adapter.call('history', {'with': 'other', 'last': 3})
        hub.assert_called_once_with({}, {'action': 'history', 'source': 'worker', 'with': 'other', 'last': 3})
        runtime.assert_not_called()
        self.assertEqual(result['data'], {'messages': []})
        with self.assertRaises(BridgeError):
            self.adapter.call('history', {})
        with self.assertRaises(BridgeError):
            self.adapter.call('history', {'with': 'other', 'source': 'spoof'})

    def nats_adapter(self, transfer=None):
        store = Store(self.home)
        try:
            thread = store.agents()['worker']
        finally:
            store.close()
        config = Path(self.temp.name) / 'nats.json'
        settings = {'tenant': 'vi', 'connector_home': self.home, 'agents': {'worker': {'session': thread}}}
        with patch('csl_nats.config', return_value=settings):
            adapter = Adapter(self.home, 'worker', nats_config=config, transfer_dir=transfer)
        return adapter, settings

    def test_nats_config_replacement_cannot_rebind_running_mcp(self):
        adapter, settings = self.nats_adapter()
        altered = [
            dict(settings, tenant='other'),
            dict(settings, connector_home=self.home.parent / 'other'),
            dict(settings, agents={'worker': {'session': str(uuid.uuid4())}}),
        ]
        with patch('csl_nats_tools.Client.connect', new_callable=AsyncMock) as connect:
            for replacement in altered:
                with patch('csl_nats_tools.config', return_value=replacement):
                    with self.assertRaisesRegex(BridgeError, 'bound MCP'):
                        adapter.call('versions', {})
            connect.assert_not_awaited()

    def test_nats_file_tools_reject_a_substituted_directory(self):
        directory = Path(self.temp.name) / 'transfer'
        directory.mkdir(mode=0o700)
        adapter, settings = self.nats_adapter(directory)
        directory.rename(directory.with_name('original'))
        directory.mkdir(mode=0o700)
        (directory / 'substitute.txt').write_text('must not upload')
        client = SimpleNamespace(request=AsyncMock(), connection=SimpleNamespace(close=AsyncMock()))
        with patch('csl_nats_tools.config', return_value=settings), patch('csl_nats_tools.Client.connect', return_value=client):
            for tool, args in [('put_file', {'name': 'substitute.txt'}), ('fetch_file', {'id': 'a' * 64})]:
                with self.assertRaisesRegex(BridgeError, 'directory changed'):
                    adapter.call(tool, args)
        self.assertEqual([call.args[0] for call in client.request.await_args_list], ['ctl.hello'] * 2)

    def test_nats_fetch_refuses_to_report_a_stale_path(self):
        directory = Path(self.temp.name) / 'transfer'
        directory.mkdir(mode=0o700)
        adapter, settings = self.nats_adapter(directory)
        original = directory.with_name('original')
        digest = hashlib.sha256(b'a').hexdigest()

        async def request(verb, _):
            if verb == 'ctl.hello':
                return {}
            self.assertEqual(verb, 'ctl.fetch')
            directory.rename(original)
            directory.mkdir(mode=0o700)
            return {'id': digest, 'name': 'report.txt', 'size': 1, 'data': 'YQ==', 'next_offset': None}

        client = SimpleNamespace(request=request, connection=SimpleNamespace(close=AsyncMock()))
        with patch('csl_nats_tools.config', return_value=settings), patch('csl_nats_tools.Client.connect', return_value=client):
            with self.assertRaisesRegex(BridgeError, 'directory changed during download'):
                adapter.call('fetch_file', {'id': digest})
        self.assertEqual(list(directory.iterdir()), [])
        self.assertEqual((original / 'report.txt').read_bytes(), b'a')

    def test_nats_fetch_returns_a_path_to_the_verified_file(self):
        directory = Path(self.temp.name) / 'transfer'
        directory.mkdir(mode=0o700)
        adapter, settings = self.nats_adapter(directory)
        digest = hashlib.sha256(b'a').hexdigest()
        request = AsyncMock(side_effect=[{}, {'id': digest, 'name': 'report.txt', 'size': 1,
                                              'data': 'YQ==', 'next_offset': None}])
        client = SimpleNamespace(request=request, connection=SimpleNamespace(close=AsyncMock()))
        with patch('csl_nats_tools.config', return_value=settings), patch('csl_nats_tools.Client.connect', return_value=client):
            result = adapter.call('fetch_file', {'id': digest})['data']
        path = Path(result['path'])
        self.assertEqual(path, directory / 'report.txt')
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), result['id'])

    def test_fixed_identity_and_no_sensitive_surfaces(self):
        with patch('csl_bridge_mcp.local_call', return_value={'state': 'delivered'}) as call:
            self.adapter.call('followup_task', {'peer': 'alice', 'target': 'coordinator', 'message': 'Synthetic review'})
        body = call.call_args.args[1]['request']
        self.assertEqual(body['source'], 'worker')
        self.assertEqual(body['op'], 'followup')
        for tool in self.adapter.catalog():
            self.assertFalse(tool['inputSchema']['additionalProperties'])
            self.assertFalse(set(tool['inputSchema']['properties']) & {'command', 'home', 'source', 'auth', 'token'})
        with self.assertRaises(BridgeError):
            self.adapter.call('followup_task', {'peer': 'alice', 'target': 'coordinator',
                                               'message': 'Synthetic', 'source': 'owner'})

    def test_unknown_actor_tool_bounds_and_secrets_rejected_before_io(self):
        invalid = [('run_shell', {}), ('send_message', {'peer': '../bad', 'target': 'worker', 'message': 'x'}),
                   ('wait_agent', {'timeout': 21}), ('wait_agent', {'after': -1}),
                   ('send_message', {'peer': 'alice', 'target': 'coordinator', 'message': 'password="synthetic-secret-value"'})]
        with patch('csl_bridge_mcp.local_call') as call:
            for tool, arguments in invalid:
                with self.subTest(tool=tool), self.assertRaises(BridgeError):
                    self.adapter.call(tool, arguments)
        call.assert_not_called()
        with self.assertRaises(BridgeError):
            Adapter(self.home, 'not-registered')

    def test_uncertain_send_keeps_id_without_resend(self):
        identifier = str(uuid.uuid4())
        with patch('csl_bridge_mcp.local_call', side_effect=BridgeError('Synthetic lost reply')) as call:
            result = self.adapter.call('followup_task', {'peer': 'alice', 'target': 'coordinator',
                                                        'message': 'Synthetic', 'id': identifier})
        self.assertEqual(call.call_count, 1)
        self.assertEqual(result['data']['state'], 'uncertain')
        self.assertEqual(result['data']['request_id'], identifier)

    def test_progress_and_wait_are_bound_to_local_agent(self):
        with patch('csl_bridge_mcp.local_call', return_value={}) as call:
            self.adapter.call('send_progress', {'task_id': str(uuid.uuid4()), 'message': 'Synthetic progress'})
            self.assertEqual(call.call_args.args[1]['source'], 'worker')
            self.adapter.call('wait_agent', {'timeout': 1, 'after': 4})
            self.assertEqual(call.call_args.args[1], {'local_op': 'wait', 'agent': 'worker', 'after': 4, 'timeout': 1})

    def test_stdio_initialize_catalog_and_tool_result(self):
        messages = [{'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {'protocolVersion': '2024-11-05'}},
                    {'jsonrpc': '2.0', 'method': 'notifications/initialized'},
                    {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/list'},
                    {'jsonrpc': '2.0', 'id': 3, 'method': 'tools/call', 'params': {'name': 'list_agents', 'arguments': {}}}]
        stdin = io.BytesIO(('\n'.join(encode(m) for m in messages) + '\n').encode())
        stdout = io.StringIO()
        with patch('csl_bridge_mcp.local_call', return_value={'agents': []}):
            serve(self.adapter, stdin, stdout)
        responses = {row['id']: row for row in map(json.loads, stdout.getvalue().splitlines())}
        self.assertEqual(responses[1]['result']['protocolVersion'], '2024-11-05')
        self.assertEqual(len(responses[2]['result']['tools']), 16)
        text = json.loads(responses[3]['result']['content'][0]['text'])
        self.assertEqual(text['bound_agent'], 'worker')
        self.assertEqual(text['data'], {'agents': []})

    def test_stdio_rejects_calls_before_initialization(self):
        raw=encode({'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':'list_agents'}})+'\n'
        out=io.StringIO()
        with patch('csl_bridge_mcp.local_call') as call:
            serve(self.adapter,io.BytesIO(raw.encode()),out)
        call.assert_not_called()
        self.assertEqual(json.loads(out.getvalue())['error']['code'],-32002)

    def test_stdio_wait_does_not_block_other_tool_calls(self):
        import threading
        release=threading.Event()
        def local(home,command):
            if command['local_op']=='wait':
                self.assertTrue(release.wait(2))
                return {'events':[]}
            release.set()
            return {'agents':[]}
        rows=[{'jsonrpc':'2.0','id':1,'method':'initialize','params':{'protocolVersion':'2024-11-05'}},
              {'jsonrpc':'2.0','id':2,'method':'tools/call','params':{'name':'wait_agent','arguments':{}}},
              {'jsonrpc':'2.0','id':3,'method':'tools/call','params':{'name':'list_agents','arguments':{}}}]
        out=io.StringIO()
        with patch('csl_bridge_mcp.local_call',side_effect=local):
            serve(self.adapter,io.BytesIO(('\n'.join(encode(r) for r in rows)+'\n').encode()),out)
        replies=[json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual({r['id'] for r in replies},{1,2,3})
        self.assertTrue(all('result' in r for r in replies))


if __name__ == '__main__':
    unittest.main()

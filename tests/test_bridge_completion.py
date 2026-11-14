"""Communication completion contracts, using only fresh synthetic bridge homes."""
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch

from csl_bridge_mailbox import grant
from csl_bridge_mcp import Adapter, configuration, serve
from csl_bridge_store import BridgeError, Store, encode
import test_bridge_mailbox as helpers

ROOT = Path(__file__).resolve().parents[1]


class CompletionTests(unittest.TestCase):
    setUp = helpers.MailboxTests.setUp
    tearDown = helpers.MailboxTests.tearDown
    exchange = helpers.MailboxTests.exchange
    send = helpers.MailboxTests.send
    flush = helpers.MailboxTests.flush
    receive = helpers.MailboxTests.receive

    def cli(self, home, *arguments):
        env = {'PATH': os.environ['PATH'], 'HOME': str(self.root/'process-home'),
               'CODEX_HOME': str(self.root/'process-codex'), 'CLAUDE_CONFIG_DIR': str(self.root/'process-claude')}
        result = subprocess.run([sys.executable, str(ROOT/'csl'), 'bridge', '--home', str(home), *arguments],
                                env=env, capture_output=True, text=True, timeout=5)
        return result, json.loads(result.stdout)

    def mcp(self, adapter, tool, arguments):
        inputs = [{'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {'protocolVersion': '2024-11-05'}},
                  {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call', 'params': {'name': tool, 'arguments': arguments}}]
        output = io.StringIO()
        serve(adapter, io.BytesIO(('\n'.join(encode(item) for item in inputs)+'\n').encode()), output)
        return next(json.loads(line)['result'] for line in output.getvalue().splitlines() if json.loads(line)['id'] == 2)

    def test_cli_logical_rejection_has_nonzero_exit_and_structured_json(self):
        self.a.start()
        selected = self.root/'selected.txt'; selected.write_text('First synthetic value')
        message_id = helpers.uid()
        command = ['send', '--from', 'alice', '--peer', 'bob', '--to', 'bob', '--message-id', message_id, '--text-file', str(selected)]
        first, data = self.cli(self.a.store.home, *command)
        self.assertEqual(first.returncode, 0); self.assertTrue(data['facts']['queued'])
        selected.write_text('Different synthetic value')
        rejected, data = self.cli(self.a.store.home, *command)
        self.assertEqual(rejected.returncode, 1)
        self.assertEqual(data, {'state': 'rejected', 'reason': 'id_collision'})
        unknown, data = self.cli(self.a.store.home, 'ack', '--agent', 'alice', '--message-id', helpers.uid())
        self.assertEqual(unknown.returncode, 1); self.assertEqual(data['state'], 'rejected')

    def test_new_mail_mcp_iserror_distinguishes_rejection_from_queued_success(self):
        self.a.start()
        adapter = Adapter(self.a.store.home, 'alice')
        arguments = {'peer': 'bob', 'target': 'bob', 'id': helpers.uid(), 'body': 'One'}
        self.assertFalse(self.mcp(adapter, 'send', arguments)['isError'])
        rejected = self.mcp(adapter, 'send', {**arguments, 'body': 'Two'})
        self.assertTrue(rejected['isError'])
        self.assertEqual(json.loads(rejected['content'][0]['text'])['data']['reason'], 'id_collision')

    def test_inbox_status_contains_only_bounded_metadata_and_does_not_ack(self):
        grant(self.b.store, 'review', 'alice', 'alice', 'bob', ['work'], 'PRIVATE DESCRIPTIVE SCOPE')
        first = self.send(body='UNTRUSTED BODY INSTRUCTIONS', grant_ref='review')
        second = self.send(body='Another untrusted body')
        self.flush(self.a, 'bob'); self.b.start()
        result, status = self.cli(self.b.store.home, 'inbox-status', '--agent', 'bob', '--limit', '1')
        self.assertEqual(result.returncode, 0)
        self.assertEqual(status['pending_count'], 2); self.assertEqual(status['total_count'], 2)
        self.assertEqual(set(status['headers'][0]), {'id', 'from', 'kind'})
        self.assertEqual(status['headers'][0]['id'], first['id'])
        self.assertNotIn('UNTRUSTED', encode(status)); self.assertNotIn('SCOPE', encode(status)); self.assertNotIn('review', encode(status))
        next_status = Adapter(self.b.store.home, 'bob').call('inbox_status', {'after': status['cursor']})['data']
        self.assertEqual(next_status['pending_count'], 2)
        self.assertEqual(next_status['headers'][0]['id'], second['id'])
        self.assertFalse(self.receive()['messages'][0]['facts']['acknowledged'])
        self.b.local({'local_op': 'mail_ack', 'agent': 'bob', 'message_id': first['id']})
        self.assertEqual(Adapter(self.b.store.home, 'bob').call('inbox_status', {})['data']['pending_count'], 1)

    def test_cli_grant_lifecycle_scope_expiry_and_unknown_reference(self):
        self.b.start()  # local owner registry changes work without restarting connector
        past = (datetime.now(timezone.utc)-timedelta(days=1)).isoformat()
        created, _ = self.cli(self.b.store.home, 'grant', 'example', '--peer', 'alice', '--from', 'alice', '--to', 'bob',
                              '--permission', 'work', '--scope', 'Descriptive review scope', '--expires-at', past)
        self.assertEqual(created.returncode, 1)
        future = (datetime.now(timezone.utc)+timedelta(days=1)).isoformat()
        created, active = self.cli(self.b.store.home, 'grant', 'example', '--peer', 'alice', '--from', 'alice', '--to', 'bob',
                                   '--permission', 'work', '--scope', 'Descriptive review scope', '--expires-at', future)
        self.assertEqual(created.returncode, 0); self.assertTrue(active['active']); self.assertFalse(active['expired'])
        self.b.store.db.execute('UPDATE mail_grants SET expires_at=? WHERE ref=?', (past, 'example'))
        listed, data = self.cli(self.b.store.home, 'grant-list', '--agent', 'bob')
        self.assertEqual(listed.returncode, 0); self.assertEqual(data['grants'][0]['scope'], 'Descriptive review scope')
        self.assertTrue(data['grants'][0]['expired']); self.assertFalse(data['grants'][0]['active'])
        shown, data = self.cli(self.b.store.home, 'grant-show', 'example')
        self.assertEqual(shown.returncode, 0); self.assertEqual(data['grant']['permissions'], ['work'])
        self.assertEqual(self.cli(self.b.store.home, 'grant-revoke', 'example')[0].returncode, 0)
        self.assertTrue(self.cli(self.b.store.home, 'grant-show', 'example')[1]['grant']['revoked'])
        self.assertEqual(self.cli(self.b.store.home, 'grant-show', 'missing')[0].returncode, 1)

    def test_mcp_grant_admin_is_explicit_and_bound_to_own_endpoint(self):
        default = Adapter(self.b.store.home, 'bob')
        admin = Adapter(self.b.store.home, 'bob', grant_admin=True)
        tools = {item['name'] for item in default.catalog()}
        self.assertTrue({'grant_list', 'grant_show', 'inbox_status'} <= tools)
        self.assertFalse({'grant_create', 'grant_revoke'} & tools)
        args = {'reference': 'example', 'peer': 'alice', 'source': 'alice', 'permissions': ['communicate', 'work'], 'scope': 'Descriptive scope'}
        denied = self.mcp(default, 'grant_create', args)
        self.assertTrue(denied['isError'])
        self.assertEqual(json.loads(denied['content'][0]['text'])['data']['reason'], 'owner_admin_consent_required')
        self.assertTrue(admin.call('grant_create', args)['data']['configured'])
        self.assertEqual(default.call('grant_show', {'reference': 'example'})['data']['grant']['target'], 'bob')
        self.assertTrue(admin.call('grant_revoke', {'reference': 'example'})['data']['revoked'])
        with self.b.store.transaction() as db:
            db.execute('INSERT INTO agents VALUES(?,?)', ('another', helpers.uid()))
        grant(self.b.store, 'foreign', 'alice', 'alice', 'another', ['work'], 'Hidden other scope')
        self.assertNotIn('foreign', encode(default.call('grant_list', {})))
        self.assertEqual(default.call('grant_show', {'reference': 'foreign'})['data']['state'], 'rejected')
        self.assertEqual(admin.call('grant_create', {**args, 'reference': 'foreign'})['data']['state'], 'rejected')
        self.assertEqual(admin.call('grant_revoke', {'reference': 'foreign'})['data']['state'], 'rejected')
        with self.assertRaises(BridgeError):
            default.call('grant_create', {**args, 'grant_admin': True})
        profile = configuration(self.b.store.home, 'bob', grant_admin=True)
        self.assertIn('--grant-admin', profile); self.assertNotIn('approval_mode = "approve"', profile)

    def test_concurrent_bound_grant_admins_cannot_overwrite_another_endpoint_reference(self):
        self.b.mailbox
        with self.b.store.transaction() as db:
            db.execute('INSERT INTO agents VALUES(?,?)', ('another', helpers.uid()))
        def create(target):
            store = Store(self.b.store.home)
            try:
                try:
                    return grant(store, 'contested', 'alice', 'alice', target, ['work'], 'Synthetic scope', bound_target=target)
                except BridgeError:
                    return {'rejected': True}
            finally:
                store.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(create, ['bob', 'another']))
        self.assertEqual(sum(bool(result.get('configured')) for result in results), 1)
        self.assertEqual(sum(bool(result.get('rejected')) for result in results), 1)

    def test_duplicate_grants_require_explicit_owner_replace_and_do_not_widen_old_mail(self):
        admin = Adapter(self.b.store.home, 'bob', grant_admin=True)
        args = {'reference': 'stable', 'peer': 'alice', 'source': 'alice', 'permissions': ['communicate'], 'scope': 'Information only'}
        first = admin.call('grant_create', args)['data']
        self.assertFalse(first['replaced']); self.assertTrue(first['active'])
        message = self.send(grant_ref='stable'); self.flush(self.a, 'bob')
        widened = {**args, 'permissions': ['communicate', 'work'], 'scope': 'Work after deliberate replacement'}
        self.assertEqual(admin.call('grant_create', widened)['data']['reason'], 'grant_exists')
        self.assertEqual(self.receive()['messages'][0]['authority']['permissions'], ['communicate'])
        replaced = admin.call('grant_create', {**widened, 'replace': True})['data']
        self.assertTrue(replaced['replaced'])
        self.assertEqual(self.receive()['messages'][0]['authority']['permissions'], ['communicate', 'work'])
        admin.call('grant_revoke', {'reference': 'stable'})
        self.assertEqual(admin.call('grant_create', widened)['data']['reason'], 'grant_exists')
        self.assertTrue(admin.call('grant_create', {**widened, 'replace': True})['data']['replaced'])
        self.assertEqual(admin.call('grant_create', {**args, 'reference': 'past', 'expires_at': '2000-01-01T00:00:00+00:00'})['data']['reason'], 'grant_expiry_not_future')
        result, data = self.cli(self.b.store.home, 'grant', 'stable', '--peer', 'alice', '--from', 'alice', '--to', 'bob', '--permission', 'work', '--scope', 'Explicit CLI scope')
        self.assertEqual(result.returncode, 1); self.assertEqual(data['reason'], 'grant_exists')
        result, data = self.cli(self.b.store.home, 'grant', 'stable', '--peer', 'alice', '--from', 'alice', '--to', 'bob', '--permission', 'work', '--scope', 'Explicit CLI scope', '--replace')
        self.assertEqual(result.returncode, 0); self.assertTrue(data['replaced'])

    def test_unknown_recipient_is_terminal_but_unadmitted_existing_recipient_is_unauthorized(self):
        self.send(target='missing'); self.flush(self.a, 'bob')
        row = self.receive(self.a, agent='alice', outbox=True)['messages'][0]
        self.assertEqual(row['delivery']['state'], 'undeliverable')
        self.assertEqual(row['delivery']['result']['reason'], 'unknown_recipient')
        self.b.agents['private'] = helpers.uid()
        self.send(target='private'); self.flush(self.a, 'bob')
        row = self.receive(self.a, agent='alice', outbox=True)['messages'][-1]
        self.assertEqual(row['delivery']['state'], 'rejected')
        self.assertEqual(row['delivery']['result']['reason'], 'unauthorized_sender')

    def test_mixed_connector_inbox_status_diagnostic_retains_observed_reason(self):
        old = {'state': 'rejected', 'reason': 'invalid_mail_fields'}
        with patch('csl_bridge_mcp.local_call', return_value=old):
            value = Adapter(self.b.store.home, 'bob').call('inbox_status', {})['data']
        self.assertEqual(value, {'state': 'rejected', 'reason': 'connector_does_not_accept_inbox_status', 'observed_reason': 'invalid_mail_fields'})
        from csl_bridge import run
        args = SimpleNamespace(action='inbox-status', home=self.b.store.home, agent='bob', after=0, limit=20)
        output = io.StringIO()
        with patch('csl_bridge.local_call', return_value=old), redirect_stdout(output), self.assertRaises(SystemExit) as exit:
            run(args)
        self.assertEqual(exit.exception.code, 1); self.assertEqual(json.loads(output.getvalue()), value)
        args.limit = 0
        with patch('csl_bridge.local_call') as call, redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
            run(args)
        call.assert_not_called()

    def test_mcp_schema_diagnostic_contains_field_names_but_never_values(self):
        adapter = Adapter(self.b.store.home, 'bob')
        inputs = [{'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {'protocolVersion': '2024-11-05'}},
                  {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call', 'params': {'name': 'send', 'arguments': {'peer': 'alice', 'target': 'alice', 'message': 'NEVER_ECHO_THIS_VALUE'}}}]
        output = io.StringIO()
        serve(adapter, io.BytesIO(('\n'.join(encode(item) for item in inputs)+'\n').encode()), output)
        response = next(json.loads(line) for line in output.getvalue().splitlines() if json.loads(line)['id'] == 2)
        self.assertEqual(response['error']['code'], -32602)
        self.assertEqual(response['error']['data']['missing'], ['body'])
        self.assertEqual(response['error']['data']['unexpected'], ['message'])
        self.assertNotIn('NEVER_ECHO_THIS_VALUE', output.getvalue())


if __name__ == '__main__':
    unittest.main()

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
        self.assertEqual(created.returncode, 0)
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


if __name__ == '__main__':
    unittest.main()

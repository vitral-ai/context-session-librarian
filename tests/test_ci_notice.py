import hashlib
import io
import urllib.error
from contextlib import redirect_stdout
from unittest.mock import patch
import json
from pathlib import Path
import tempfile
import unittest
import uuid

from csl_agent_hub import Hub
from csl_bridge_store import BridgeError
from csl_ci_notice import notice_request, workflow_notice, main


class CompletionNoticeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.policy = {'sha256': hashlib.sha256(b'notice-secret').hexdigest(), 'scope': 'notices',
                       'repository': 'vitral-ai/example', 'source': 'ci-notices', 'source_machine': 'laptop',
                       'routes': {'feat/existing': 'client'}}
        self.hub = Hub(Path(self.temp.name) / 'hub.sqlite', {
            'laptop': {'sha256': hashlib.sha256(b'owner').hexdigest(), 'admin': True},
            'remote': {'sha256': hashlib.sha256(b'remote').hexdigest()}, 'ci-example': self.policy})
        for agent, machine in [('ci-notices', 'laptop'), ('client', 'remote')]:
            self.hub.call('owner', dict(action='register', name=agent, machine=machine,
                                       cli='codex', session=str(uuid.uuid4())))
        self.hub.call('owner', dict(action='allow', source='ci-notices', target='client'))
        self.notice = dict(action='notice', repository='vitral-ai/example', number=799,
                           branch='codex/client/fix', workflow='Codex PR Review', run_id=123,
                           attempt=1, sha='a' * 40, results={'review': 'success'})

    def post(self, **changes):
        return self.hub.call('notice-secret', dict(self.notice, **changes))

    def test_completion_is_durable_idempotent_and_deliverable_after_restart(self):
        sent = self.post()
        self.assertEqual(sent['state'], 'queued')
        self.hub = Hub(self.hub.database, self.hub.machines)
        self.assertEqual(self.post()['id'], sent['id'])
        packets = self.hub.call('remote', {'action': 'pull'})['envelopes']
        self.assertEqual(len(packets), 1)
        self.assertIn('#799: Codex PR Review passed', packets[0]['text'])
        self.assertIn('/actions/runs/123/attempts/1', packets[0]['text'])
        self.hub.call('remote', dict(action='ack', id=sent['id'], receipt={'state': 'delivered'}))
        self.assertEqual(self.post()['state'], 'delivered')
        self.assertNotEqual(self.post(attempt=2)['id'], sent['id'])

    def test_notice_token_cannot_read_send_approve_register_or_ack(self):
        for action in ('send', 'pull', 'list', 'show', 'allow', 'register', 'ack', 'receipt', 'deny', 'remove'):
            with self.subTest(action=action), self.assertRaises(BridgeError):
                self.hub.call('notice-secret', dict(action=action))
        with self.assertRaises(BridgeError):
            self.hub.call('owner', self.notice)

    def test_repo_scope_unknown_branch_agent_pair_and_free_text_refused(self):
        for changes in ({'repository': 'vitral-ai/other'}, {'branch': 'feat/no-mapping'},
                        {'branch': 'codex/unknown/fix'}, {'text': 'arbitrary instructions'},
                        {'sha': 'not-a-sha'}, {'results': {'review': 'green'}}, {'number': True}):
            with self.subTest(changes=changes), self.assertRaises(BridgeError):
                self.post(**changes)
        self.hub.call('owner', dict(action='deny', source='ci-notices', target='client'))
        with self.assertRaisesRegex(BridgeError, 'Unapproved pair'):
            self.post()

    def test_failure_cancelled_skipped_and_explicit_legacy_branch(self):
        for results, outcome in [({'review': 'failure'}, 'failed'),
                                 ({'review': 'cancelled'}, 'cancelled'),
                                 ({'review': 'skipped'}, 'skipped'),
                                 ({'review': 'success', 'fixture': 'failure'}, 'failed')]:
            request = notice_request(self.policy, dict(self.notice, results=results))
            self.assertIn('Codex PR Review ' + outcome, request['text'])
        self.assertEqual(notice_request(self.policy, dict(self.notice, branch='feat/existing'))['target'], 'client')

    def test_same_completion_cannot_change_payload(self):
        self.post()
        with self.assertRaisesRegex(BridgeError, 'collision'):
            self.post(results={'review': 'failure'})

    def run_environment(self):
        run = dict(id=123, run_attempt=1, name='Codex PR Review', status='completed',
                   event='pull_request_target', conclusion='failure',
                   head_branch='codex/client/$(touch /tmp/untrusted)', head_sha='a' * 40,
                   head_repository={'full_name': 'vitral-ai/example'},
                   pull_requests=[dict(number=799, head={'repo': {'id': 42},
                       'ref': 'codex/client/newer-branch', 'sha': 'b' * 40})])
        return run, dict(CSL_EVENT=json.dumps({'workflow_run': run}),
                         CSL_REPOSITORY='vitral-ai/example', CSL_REPOSITORY_ID='42')

    def test_completed_run_metadata_is_data_and_requires_one_same_repo_pr(self):
        run, env = self.run_environment()
        payload = workflow_notice(env)
        self.assertEqual(payload['results'], {'completion': 'failure'})
        self.assertEqual(payload['sha'], 'a' * 40)
        self.assertEqual(payload['branch'], run['head_branch'])
        self.assertNotEqual(payload['sha'], run['pull_requests'][0]['head']['sha'])
        self.assertEqual(notice_request(self.policy, payload)['target'], 'client')
        for changes in ({'head_repository': {'full_name': 'fork/example'}},
                        {'event': 'push'}, {'status': 'in_progress'}, {'pull_requests': []},
                        {'pull_requests': [run['pull_requests'][0]] * 2},
                        {'pull_requests': [dict(number=799, head={'repo': {'id': 43}})]}):
            with self.subTest(changes=changes), self.assertRaises(BridgeError):
                workflow_notice(dict(env, CSL_EVENT=json.dumps({'workflow_run': dict(run, **changes)})))

    def test_completion_outcomes_include_timeout_and_neutral(self):
        run, env = self.run_environment()
        for conclusion, result in [('timed_out', 'failure'), ('neutral', 'skipped'),
                                   ('success', 'success'), ('cancelled', 'cancelled')]:
            payload = workflow_notice(dict(env, CSL_EVENT=json.dumps(
                {'workflow_run': dict(run, conclusion=conclusion)})))
            self.assertEqual(payload['results'], {'completion': result})

    def test_current_head_lookup_skips_superseded_before_hub_post(self):
        _, env = self.run_environment()
        env.update(CSL_GITHUB_TOKEN='github-read-token')
        output = io.StringIO()
        with patch.dict('os.environ', env, clear=True), redirect_stdout(output), patch(
                'urllib.request.urlopen', return_value=io.StringIO(
                    json.dumps({'head': {'sha': 'b' * 40}}))) as opener:
            main()
        self.assertEqual(opener.call_count, 1)
        request = opener.call_args.args[0]
        self.assertEqual(request.full_url, 'https://api.github.com/repos/vitral-ai/example/pulls/799')
        self.assertIsNone(request.data)
        self.assertIn('superseded by ' + 'b' * 40, output.getvalue())
        self.assertNotIn('github-read-token', output.getvalue())

    def test_matching_head_and_lookup_failures_post_exact_run(self):
        _, env = self.run_environment()
        env.update(CSL_GITHUB_TOKEN='github-read-token', CSL_HUB_NOTICE_TOKEN='hub-notice-token',
                   CSL_HUB_CA_PEM='test-ca', CSL_HUB_URL='https://hub.example/v1')
        cases = [json.dumps({'head': {'sha': 'a' * 40}}),
                 urllib.error.HTTPError('https://api.github.com', 403, 'forbidden', {}, None),
                 TimeoutError('timeout'), 'not JSON', '{}', json.dumps({'head': {'sha': 'invalid'}})]
        for first in cases:
            with self.subTest(first=type(first).__name__):
                response = first if isinstance(first, Exception) else io.StringIO(first)
                output = io.StringIO()
                with patch.dict('os.environ', env, clear=True), redirect_stdout(output), patch(
                        'ssl.create_default_context'), patch('urllib.request.urlopen', side_effect=[
                            response, io.StringIO(json.dumps({'ok': True, 'data': {
                                'id': 'notice-id', 'state': 'queued'}}))]) as opener:
                    main()
                self.assertEqual(opener.call_count, 2)
                post = opener.call_args.args[0]
                self.assertEqual(post.full_url, 'https://hub.example/v1')
                self.assertEqual(json.loads(post.data)['sha'], 'a' * 40)
                self.assertNotIn('github-read-token', output.getvalue())
                self.assertNotIn('hub-notice-token', output.getvalue())


if __name__ == '__main__':
    unittest.main()

from pathlib import Path
import re
import tempfile
import textwrap
import unittest
from unittest.mock import patch


class NoticeWorkflowTests(unittest.TestCase):
    def test_reusable_job_has_no_candidate_checkout_or_shell_interpolation(self):
        workflow = Path('.github/workflows/pr-completion-notice.yml').read_text()
        self.assertIn('repository: vitral-ai/context-session-librarian', workflow)
        self.assertRegex(workflow, r'ref: [0-9a-f]{40}\n')
        self.assertNotIn('pull_request.head.sha', workflow)
        self.assertIn('persist-credentials: false', workflow)
        self.assertIn('runs-on: ubuntu-latest', workflow)
        self.assertEqual(re.findall(r'^        run: (.*)$', workflow, re.M), [
            '|',
            'timeout --signal=KILL 90s go version',
            'timeout --signal=KILL 90s go build -mod=readonly -trimpath -o "$RUNNER_TEMP/csl-ci-notice" ./cmd/csl-ci-notice',
            '|',
        ])
        self.assertIn('environment: csl-ci-notices', workflow)
        self.assertIn('pull-requests: read', workflow)
        self.assertIn('CSL_GITHUB_TOKEN: ${{ github.token }}', workflow)
        self.assertIn('CSL_NATS_CI_CREDS:\n        required: true', workflow)
        self.assertNotIn('CSL_HUB_', workflow)
        self.assertIn("github.event_name == 'workflow_run'", workflow)
        self.assertIn('github.event.repository.default_branch', workflow)
        self.assertNotIn('secrets: inherit', workflow)
        self.assertNotIn('inputs:', workflow)
        self.assertNotIn('download-artifact', workflow)

    def test_nats_publish_has_a_separate_protected_credential_and_deadline(self):
        workflow = Path('.github/workflows/pr-completion-notice.yml').read_text()
        _, selection = workflow.split('      - name: Select current PR completion', 1)
        selection, nats = selection.split('      - name: Set up the pinned Go toolchain', 1)
        build, publish = nats.split('      - name: Post NATS completion', 1)
        self.assertIn('current_head_allows_notice(notice, os.environ)', selection)
        self.assertNotIn('post_hub', workflow)
        self.assertNotIn('CSL_NATS_CI_CREDS', selection + build)
        self.assertIn('working-directory: nats-control', build)
        self.assertEqual(build.count('GOTOOLCHAIN: go1.27.4'), 2)
        self.assertNotIn('continue-on-error: true', nats)
        self.assertIn('CSL_NATS_CI_CREDS: ${{ secrets.CSL_NATS_CI_CREDS }}', publish)
        self.assertIn('CSL_NATS_CI_PUBLISHER: ${{ runner.temp }}/csl-ci-notice', publish)
        self.assertIn('timeout --signal=KILL 30s python3', publish)
        self.assertIn("steps.current-completion.outputs.eligible == 'true'", publish)
        self.assertNotIn('current_head_allows_notice', publish)
        self.assertNotIn('CSL_GITHUB_TOKEN', publish)
        self.assertIn('post_nats(workflow_notice(os.environ), os.environ)', publish)
        self.assertNotIn('github.event', publish.split('        run:')[1])

    def test_current_pr_success_or_failure_is_the_only_admission_witness(self):
        workflow = Path('.github/workflows/pr-completion-notice.yml').read_text()
        selection = textwrap.dedent(re.search(r"python3 - <<'PY2'\n(.*?)^          PY2$", workflow,
                                              re.M | re.S).group(1))
        with tempfile.TemporaryDirectory() as scratch:
            output = Path(scratch) / 'outputs'
            self.assertEqual(workflow.count("steps.current-completion.outputs.eligible == 'true'"), 2)
            notices = [{'results': {'completion': outcome}} for outcome in ('skipped', 'success', 'failure')]
            with patch.dict('os.environ', {'GITHUB_OUTPUT': str(output)}, clear=True), patch(
                    'csl_ci_notice.workflow_notice', side_effect=notices), patch(
                    'csl_ci_notice.current_head_allows_notice', side_effect=[False, True]) as head:
                exec(selection, {})
                self.assertFalse(output.exists())
                head.assert_not_called()
                exec(selection, {})
                self.assertFalse(output.exists())
                exec(selection, {})
                self.assertEqual(output.read_text(), 'eligible=true\n')
                self.assertEqual(head.call_count, 2)

    def test_unprivileged_producers_have_no_notice_credentials(self):
        for filename in ('test.yml', 'workload-image.yml'):
            workflow = Path('.github/workflows', filename).read_text()
            self.assertNotIn('CSL_HUB_NOTICE_TOKEN', workflow)
            self.assertNotIn('CSL_NATS_CI_CREDS', workflow)
            self.assertNotIn('csl-completion-notice:', workflow)

    def test_default_branch_completed_handler_pins_reviewed_workflow(self):
        workflow = Path('.github/workflows/ci-completion-notice.yml').read_text()
        self.assertIn('workflow_run:', workflow)
        self.assertIn('types: [completed]', workflow)
        self.assertRegex(workflow, r'pr-completion-notice.yml@[0-9a-f]{40}')
        self.assertNotIn('pull_request:', workflow)
        self.assertNotIn('CSL_HUB_', workflow)
        self.assertIn('CSL_NATS_CI_CREDS: ${{ secrets.CSL_NATS_CI_CREDS }}', workflow)
        self.assertNotIn('secrets: inherit', workflow)
        self.assertNotIn('checkout', workflow)

    def test_handler_filters_non_pr_and_cancelled_completions(self):
        # Evaluate the actual caller expression for representative GitHub runs.
        workflow = Path('.github/workflows/ci-completion-notice.yml').read_text()
        expression = re.search(r"if: \$\{\{ (.*?) \}\}", workflow).group(1)
        for event in ('pull_request', 'pull_request_target', 'push', 'workflow_dispatch'):
            for conclusion in ('success', 'failure', 'cancelled', 'timed_out'):
                with self.subTest(event=event, conclusion=conclusion):
                    predicate = expression.replace('github.event.workflow_run.event', repr(event))
                    predicate = predicate.replace('github.event.workflow_run.conclusion', repr(conclusion))
                    predicate = predicate.replace('&&', ' and ').replace('||', ' or ')
                    actual = eval(predicate, {'__builtins__': {}}, {})
                    self.assertEqual(actual, event in ('pull_request', 'pull_request_target')
                                     and conclusion != 'cancelled')

from pathlib import Path
import re
import unittest


class NoticeWorkflowTests(unittest.TestCase):
    def test_reusable_job_has_no_candidate_checkout_or_shell_interpolation(self):
        workflow = Path('.github/workflows/pr-completion-notice.yml').read_text()
        self.assertIn('repository: vitral-ai/context-session-librarian', workflow)
        self.assertRegex(workflow, r'ref: [0-9a-f]{40}\n')
        self.assertNotIn('pull_request.head.sha', workflow)
        self.assertIn('persist-credentials: false', workflow)
        self.assertIn('runs-on: ubuntu-latest', workflow)
        self.assertEqual(re.findall(r'run: (.*)', workflow), [
            'go build -mod=readonly -trimpath -o "$RUNNER_TEMP/csl-ci-notice" ./cmd/csl-ci-notice',
            'python3 csl_ci_notice.py',
        ])
        self.assertIn('environment: csl-ci-notices', workflow)
        self.assertIn('pull-requests: read', workflow)
        self.assertIn('CSL_GITHUB_TOKEN: ${{ github.token }}', workflow)
        self.assertIn('CSL_HUB_NOTICE_TOKEN:\n        required: true', workflow)
        self.assertIn("github.event_name == 'workflow_run'", workflow)
        self.assertIn('github.event.repository.default_branch', workflow)
        self.assertNotIn('secrets: inherit', workflow)
        self.assertNotIn('inputs:', workflow)
        self.assertNotIn('download-artifact', workflow)

    def test_nats_build_has_no_credentials_and_uses_pinned_tools(self):
        workflow = Path('.github/workflows/pr-completion-notice.yml').read_text()
        self.assertIn('CSL_NATS_CI_CREDS:\n        required: false', workflow)
        self.assertRegex(workflow, r'actions/setup-go@[0-9a-f]{40}')
        self.assertIn('CSL_NATS_CI_PUBLISHER: ${{ runner.temp }}/csl-ci-notice', workflow)
        self.assertEqual(workflow.count("if: ${{ vars.CSL_NATS_CI_URL != '' }}"), 2)
        build, publish = workflow.split('      - name: Post completion once per configured transport')
        self.assertIn('working-directory: nats-control', build)
        self.assertNotIn('secrets.CSL_NATS_CI_CREDS', build)
        self.assertNotIn('github.event', publish.split('        run:')[1])
        self.assertIn("!cancelled() && steps.trusted-checkout.outcome == 'success'", publish)
        self.assertIn('CSL_NATS_CI_CREDS: ${{ secrets.CSL_NATS_CI_CREDS }}', publish)

    def test_stalled_optional_setup_leaves_time_for_hub(self):
        workflow = Path('.github/workflows/pr-completion-notice.yml').read_text()
        job_timeout = int(re.search(r'^    timeout-minutes: (\d+)$', workflow, re.M).group(1))
        step_timeouts = [int(value) for value in re.findall(r'^        timeout-minutes: (\d+)$', workflow, re.M)]
        self.assertEqual(step_timeouts, [2, 1, 1, 2])
        self.assertGreater(job_timeout, sum(step_timeouts))

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
        self.assertIn('CSL_HUB_NOTICE_TOKEN: ${{ secrets.CSL_HUB_NOTICE_TOKEN }}', workflow)
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

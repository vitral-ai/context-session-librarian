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
        self.assertEqual(re.findall(r'run: (.*)', workflow), ['python3 csl_ci_notice.py'])
        self.assertIn('environment: csl-ci-notices', workflow)
        self.assertIn("github.event_name == 'workflow_run'", workflow)
        self.assertIn('github.event.repository.default_branch', workflow)
        self.assertNotIn('secrets: inherit', workflow)
        self.assertNotIn('inputs:', workflow)
        self.assertNotIn('download-artifact', workflow)

    def test_unprivileged_producers_have_no_notice_credentials(self):
        for filename in ('test.yml', 'workload-image.yml'):
            workflow = Path('.github/workflows', filename).read_text()
            self.assertNotIn('CSL_HUB_NOTICE_TOKEN', workflow)
            self.assertNotIn('csl-completion-notice:', workflow)

    def test_default_branch_completed_handler_pins_reviewed_workflow(self):
        workflow = Path('.github/workflows/ci-completion-notice.yml').read_text()
        self.assertIn('workflow_run:', workflow)
        self.assertIn('types: [completed]', workflow)
        self.assertRegex(workflow, r'pr-completion-notice.yml@[0-9a-f]{40}')
        self.assertNotIn('pull_request:', workflow)
        self.assertNotIn('secrets:', workflow)
        self.assertNotIn('checkout', workflow)

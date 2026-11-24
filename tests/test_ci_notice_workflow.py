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
        self.assertNotIn('secrets: inherit', workflow)

    def test_existing_jobs_all_finish_before_notice_and_push_does_not_notify(self):
        for filename in ('test.yml', 'workload-image.yml'):
            workflow = Path('.github/workflows', filename).read_text()
            before, notice = workflow.split('  csl-completion-notice:')
            jobs = re.findall(r'^  ([A-Za-z0-9_-]+):\n', before.split('\njobs:\n')[1], re.M)
            self.assertIn('needs: [' + ', '.join(jobs) + ']', notice)
            self.assertIn('always() && github.event.pull_request &&', notice)
            self.assertIn('head.repo.full_name == github.repository', notice)
            self.assertIn('toJSON(needs)', notice)
            self.assertNotIn('secrets: inherit', notice)

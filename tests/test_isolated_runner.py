"""Public test runner prerequisite checks and environment fencing, no models."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('csl_isolated_runner', ROOT/'scripts/run_isolated_tests.py')
runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)


class RunnerTests(unittest.TestCase):
    def test_full_profile_rejects_observed_old_native_cli_before_running_suite(self):
        self.assertIn('PATH', runner.codex_validation_error('codex-cli 0.118.0'))
        self.assertIsNone(runner.codex_validation_error('codex-cli 0.169.0'))
        self.assertIsNone(runner.codex_validation_error('codex-cli 0.170.0'))
        self.assertIsNotNone(runner.codex_validation_error('unknown native build'))

    def test_allowlisted_environment_has_fresh_private_homes_and_no_provider_auth(self):
        with tempfile.TemporaryDirectory() as directory:
            inherited = {'PATH': os.defpath, 'HOME': '/never-real-home', 'CODEX_HOME': '/never-codex',
                         'CLAUDE_CONFIG_DIR': '/never-claude', 'OPENAI_API_KEY': 'synthetic-key',
                         'ANTHROPIC_API_KEY': 'synthetic-key', 'CLAUDE_CODE_USE_VERTEX': '1',
                         'GOOGLE_APPLICATION_CREDENTIALS': '/never-creds', 'GH_TOKEN': 'synthetic-token',
                         'PYTHONPATH': '/never-import', 'PIP_INDEX_URL': 'https://synthetic.invalid'}
            clean = runner.isolated_environment(directory, inherited)
            self.assertEqual(clean['PATH'], os.defpath)
            for key in ('OPENAI_API_KEY', 'ANTHROPIC_API_KEY', 'CLAUDE_CODE_USE_VERTEX', 'GOOGLE_APPLICATION_CREDENTIALS', 'GH_TOKEN', 'PYTHONPATH', 'PIP_INDEX_URL'):
                self.assertNotIn(key, clean)
            for key in ('HOME', 'CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'XDG_CONFIG_HOME', 'XDG_CACHE_HOME', 'XDG_DATA_HOME', 'XDG_STATE_HOME', 'XDG_RUNTIME_DIR'):
                path = Path(clean[key]); self.assertTrue(path.is_relative_to(Path(directory)))
                self.assertEqual(path.stat().st_mode & 0o777, 0o700)
            self.assertEqual(clean['PIP_CONFIG_FILE'], os.devnull)

    def test_missing_full_prerequisites_are_reported_without_installing(self):
        with patch.object(runner.shutil, 'which', return_value=None):
            versions, errors = runner.prerequisites({'PATH': '/synthetic-empty'}, True)
        self.assertFalse(versions)
        self.assertTrue(any('codex' in item for item in errors))
        self.assertTrue(any('claude' in item for item in errors))
        self.assertTrue(any('bwrap' in item for item in errors))

    def test_check_command_writes_report_without_executing_suite_or_inheriting_auth(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)/'results'
            env = {**os.environ, 'ANTHROPIC_API_KEY': 'synthetic-test-key', 'CLAUDE_CODE_USE_VERTEX': '1'}
            result = subprocess.run([sys.executable, str(ROOT/'scripts/run_isolated_tests.py'), '--check', '--output', str(output)],
                                    env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads((output/'report.json').read_text())
            self.assertFalse(report['tests_executed']); self.assertEqual(report['paid_model_calls'], 0)
            self.assertFalse((output/'tests.log').exists())
            again = subprocess.run([sys.executable, str(ROOT/'scripts/run_isolated_tests.py'), '--check', '--output', str(output)],
                                   capture_output=True, text=True, timeout=10)
            self.assertEqual(again.returncode, 2); self.assertNotIn('Traceback', again.stderr)


if __name__ == '__main__':
    unittest.main()

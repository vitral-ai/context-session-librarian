"""Retired providers fail before local installation, upload or native launch."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock
import uuid
import zipfile

import csl_capabilities as caps
import csl_provider_bundle as bundle
import csl_sync_providers as providers
from csl_sync_engine import Collector, PROVIDERS
from csl_sync_state import SyncError
from csl_transfer import resume_command, TransferError

ROOT = Path(__file__).resolve().parents[1]


class RetiredProviderTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.project = self.root / 'project'
        self.project.mkdir()
        self.env = {**os.environ, 'HOME': str(self.root), 'CODEX_HOME': str(self.root / 'codex'),
                    'CLAUDE_CONFIG_DIR': str(self.root / 'claude'),
                    'CSL_ANTIGRAVITY_HOME': str(self.root / 'antigravity')}
        # Release notes are the historical source of retired identifiers. The
        # rejection implementation is generic, with no retired-provider registry.
        self.retired = re.findall(r'^- Removed `([^`]+)` CLI support\.',
                                   (ROOT / 'CHANGELOG.md').read_text(), re.MULTILINE)
        self.assertTrue(self.retired, 'provider-removal release note is required')

    def cli(self, *args):
        return subprocess.run([sys.executable, str(ROOT / 'csl'), *args], env=self.env,
                              cwd=self.project, capture_output=True, text=True, timeout=15)

    def test_capabilities_and_local_interfaces_have_only_current_providers(self):
        expected = {'codex', 'claude', 'antigravity'}
        self.assertEqual(set(providers.PROVIDERS), expected)
        self.assertEqual(set(PROVIDERS), expected)
        report = caps.coverage()
        self.assertEqual(set(report['providers']), expected)
        for command in report['commands'].values():
            self.assertEqual(set(command), expected)
        for name in self.retired:
            for call in (lambda: providers.list_sessions(name, home=self.root),
                         lambda: providers.raw_files(name, 'fixture', home=self.root),
                         lambda: providers.view_records(name, 'fixture', home=self.root),
                         lambda: providers.package_descriptor(name, 'fixture', home=self.root)):
                with self.assertRaisesRegex(providers.ProviderError, 'unsupported_provider'):
                    call()
            with self.assertRaisesRegex(TransferError, 'Unsupported provider'):
                resume_command('fixture', self.project, provider=name)

    def test_removed_cli_choices_fail_clearly_without_traceback(self):
        for name in self.retired:
            cases = [
                ('sessions', '--provider', name),
                ('providers', 'list', '--provider', name),
                ('export', 'fixture', '--provider', name, '--raw', '--to', str(self.root / 'out.zip')),
                ('import', 'old.zip', '--provider', name),
                ('import', 'old.zip', '--target', name),
                ('convert', 'fixture', '--to', name, '--out', str(self.root / 'out.zip')),
                ('sync', 'include', name, 'fixture'),
                ('sync', 'run', '--provider', name),
                ('sync', 'watch', '--provider', name),
            ]
            for args in cases:
                with self.subTest(command=args[0:2]):
                    result = self.cli(*args)
                    self.assertEqual(result.returncode, 2)
                    self.assertIn('invalid choice', result.stderr)
                    self.assertIn(name, result.stderr)
                    self.assertNotIn('Traceback', result.stderr)
        self.assertFalse((self.root / 'out.zip').exists())
        self.assertFalse((self.root / 'codex').exists())

    def test_old_v3_package_refused_before_inspection_or_installation(self):
        for name in self.retired:
            sid = str(uuid.uuid4())
            body = json.dumps({'sessionId': sid, 'projectHash': 'fixture',
                'messages': [{'id': 'u', 'type': 'user', 'content': 'synthetic decision'}]}).encode()
            member = f'sessions/{sid}/main.json'
            manifest = {'format': 'codex-session-librarian', 'version': 3, 'mode': 'raw',
                'sessions': [{'id': sid, 'provider': name, 'primary': member,
                    'files': [{'file_key': 'main', 'file': member, 'relative_path': 'session.json',
                        'role': 'transcript', 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}]}]}
            package = self.root / 'old.csl.zip'
            with zipfile.ZipFile(package, 'w') as archive:
                archive.writestr('manifest.json', json.dumps(manifest))
                archive.writestr(member, body)
            original = package.read_bytes()
            with zipfile.ZipFile(package) as archive, self.assertRaisesRegex(ValueError, 'Unsupported native provider'):
                bundle.validate(archive)
            for flags in (['--list'], ['--cwd', str(self.project)], ['--cwd', str(self.project), '--open']):
                result = self.cli('import', str(package), *flags)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('Unsupported native provider', result.stderr)
                self.assertNotIn('Traceback', result.stderr)
            self.assertEqual(package.read_bytes(), original)
        self.assertFalse((self.root / 'codex').exists())
        self.assertFalse((self.root / 'claude').exists())
        self.assertFalse((self.root / 'antigravity').exists())

    def test_collector_refuses_removed_provider_before_network_or_state_changes(self):
        collector = Collector.__new__(Collector)
        collector.registry = Mock()
        collector.state = Mock()
        collector.providers = Mock()
        for name in self.retired:
            with self.assertRaisesRegex(SyncError, 'unsupported_provider'):
                collector.run(providers=[name])
        self.assertFalse(collector.registry.mock_calls)
        self.assertFalse(collector.state.mock_calls)
        self.assertFalse(collector.providers.mock_calls)

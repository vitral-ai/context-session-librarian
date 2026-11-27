"""CLI startup is portable even when terminal/POSIX extras are absent."""
import ast
import builtins
import contextlib
import importlib
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))


class CLIStartupTests(unittest.TestCase):
    def test_every_root_cli_entry_point_imports_without_optional_os_modules(self):
        entries = []
        for path in sorted(REPO.glob('*.py')):
            tree = ast.parse(path.read_text(encoding='utf-8'))
            if any(isinstance(node, ast.If) and '__name__' in ast.unparse(node.test)
                   and '__main__' in ast.unparse(node.test) for node in tree.body):
                entries.append(path.stem)
        self.assertIn('codex_sessions', entries)
        self.assertIn('csl_sync', entries)
        with tempfile.TemporaryDirectory() as temporary:
            env = {**os.environ, 'CODEX_HOME': temporary + '/codex',
                   'CLAUDE_CONFIG_DIR': temporary + '/claude', 'PYTHONUTF8': '1'}
            script = '''import builtins, importlib, sys
sys.path.insert(0, sys.argv[1])
original = builtins.__import__
def restricted(name, *args, **kwargs):
    if name in ('curses', '_curses'):
        raise ModuleNotFoundError(name)
    return original(name, *args, **kwargs)
builtins.__import__ = restricted
for name in sys.argv[2:]:
    importlib.import_module(name)
'''
            result = subprocess.run([sys.executable, '-c', script, str(REPO), *entries],
                                    env=env, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            for args in (['--help'], ['sync', '--help'], ['sync', 'pull', '--help'],
                         ['import', '--help']):
                with self.subTest(args=args):
                    command = script.split('for name in sys.argv[2:]:')[0] + '''
import runpy
sys.argv = [str(__import__('pathlib').Path(sys.argv[1]) / 'csl'), *sys.argv[2:]]
runpy.run_path(sys.argv[0], run_name='__main__')
'''
                    result = subprocess.run([sys.executable, '-c', command, str(REPO), *args],
                                            env=env, capture_output=True, text=True, timeout=30)
                    self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_curses_uses_plain_cockpit(self):
        import codex_sessions as csl
        original = builtins.__import__
        def unavailable(name, *args, **kwargs):
            if name == 'curses':
                raise ModuleNotFoundError('curses')
            return original(name, *args, **kwargs)
        args = SimpleNamespace(print=False)
        with patch('builtins.__import__', side_effect=unavailable), \
             patch.object(csl, 'cockpit_print') as plain, \
             contextlib.redirect_stderr(io.StringIO()) as error:
            csl.cockpit(args)
        plain.assert_called_once_with(args)
        self.assertIn('plain output', error.getvalue())


if __name__ == '__main__':
    unittest.main()

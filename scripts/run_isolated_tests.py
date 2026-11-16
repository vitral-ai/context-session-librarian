#!/usr/bin/env python3
"""Run CSL's synthetic suite in fresh homes, without account or model credentials.

Default: standard-library/base tests (optional checks may be skipped).
--full: native/media/migration opt-ins, Linux bubblewrap and isolated media deps.
No paid generation or login is performed. This is isolation, not an OS sandbox.
"""
import argparse
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
FULL_FLAGS = {'CSL_RUN_MEDIA_TESTS': '1', 'CSL_RUN_CODEX_NATIVE_TESTS': '1',
              'CSL_RUN_CLAUDE_NATIVE_TESTS': '1', 'CSL_RUN_MIGRATION_E2E_TESTS': '1',
              'CSL_MEDIA_SANDBOX': 'required'}
CODEX_VALIDATION_FLOOR = (0, 169, 0)


def codex_validation_error(version):
    match = re.search(r'codex-cli (\d+)\.(\d+)\.(\d+)', version)
    if not match or tuple(map(int, match.groups())) < CODEX_VALIDATION_FLOOR:
        return 'Full native validation requires Codex CLI 0.169.0 or newer; check PATH order. This is a test-profile prerequisite, not a minimum for all CSL operations.'
    return None


def isolated_environment(root, source=None):
    """Allowlist, not a blacklist: provider flags, tokens and private config drop."""
    source = os.environ if source is None else source
    env = {'PATH': source.get('PATH', os.defpath), 'LANG': 'C.UTF-8',
           'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONNOUSERSITE': '1',
           'PIP_CONFIG_FILE': os.devnull, 'PIP_DISABLE_PIP_VERSION_CHECK': '1'}
    for key, folder in {'HOME': 'home', 'CODEX_HOME': 'codex', 'CLAUDE_CONFIG_DIR': 'claude',
                        'CSL_ANTIGRAVITY_HOME': 'antigravity',
                        'XDG_CONFIG_HOME': 'xdg-config', 'XDG_DATA_HOME': 'xdg-data',
                        'XDG_CACHE_HOME': 'xdg-cache', 'XDG_STATE_HOME': 'xdg-state',
                        'XDG_RUNTIME_DIR': 'xdg-runtime', 'TMPDIR': 'tmp'}.items():
        path = Path(root)/folder; path.mkdir(mode=0o700)
        env[key] = str(path)
    return env


def prerequisites(env, full):
    result, errors = {}, []
    if not full:
        return result, errors
    if not sys.platform.startswith('linux'):
        errors.append('--full requires Linux: native migration/media isolation uses bubblewrap')
    for name in ('codex', 'claude', 'bwrap'):
        binary = shutil.which(name, path=env['PATH'])
        if not binary:
            errors.append('Missing required executable: '+name)
            continue
        try:
            run = subprocess.run([binary, '--version'], env=env, text=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=15)
            result[name] = {'version': run.stdout.strip()[:300], 'version_exit': run.returncode}
            if run.returncode:
                errors.append('Version check failed: '+name)
            elif name == 'codex':
                result[name]['minimum_tested_version'] = '0.169.0'
                problem = codex_validation_error(run.stdout)
                if problem:
                    errors.append(problem)
        except (OSError, subprocess.TimeoutExpired):
            errors.append('Version check unavailable: '+name)
    if 'bwrap' in result and sys.platform.startswith('linux'):
        try:
            run = subprocess.run(['bwrap', '--unshare-net', '--ro-bind', '/', '/', '--', '/usr/bin/true'],
                                 env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
            if run.returncode:
                errors.append('bubblewrap network/user namespace check failed; no host policy was changed')
        except (OSError, subprocess.TimeoutExpired):
            errors.append('bubblewrap namespace check unavailable')
    return result, errors


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--full', action='store_true', help='Require and enable native/media/migration checks; never runs paid live verification.')
    parser.add_argument('--check', action='store_true', help='Check prerequisites only; no tests, venv creation or dependency installation.')
    parser.add_argument('--media-python', type=Path, help='Existing dedicated test interpreter with the pinned requirements, instead of a temporary venv.')
    parser.add_argument('--wheelhouse', type=Path, help='Install temporary-venv dependencies offline from this wheel directory.')
    parser.add_argument('--output', type=Path, help='New private result directory; defaults to a new directory under the OS temporary directory.')
    args = parser.parse_args(argv)
    if sys.version_info < (3, 11):
        parser.error('Python 3.11+ is required')
    if (args.media_python or args.wheelhouse) and not args.full:
        parser.error('--media-python/--wheelhouse require --full')
    output = args.output.absolute() if args.output else Path(tempfile.mkdtemp(prefix='csl-test-results-'))
    if args.output:
        try:
            output.mkdir(mode=0o700, parents=True, exist_ok=False)
        except OSError:
            parser.error('--output must be a new writable directory; existing results are never overwritten')
    report = {'mode': 'full' if args.full else 'base', 'paid_model_calls': 0,
              'fresh_homes': True, 'inherited_provider_auth_environment': False,
              'os_sandbox': False, 'source': str(ROOT), 'tests_executed': False}
    def finish(code):
        report['exit'] = code
        (output/'report.json').write_text(json.dumps(report, indent=2)+'\n')
        print(json.dumps({'exit': code, 'report': str(output/'report.json'), 'tests_executed': report['tests_executed'],
                          'errors': report.get('errors', [])}), flush=True)
        return code
    with tempfile.TemporaryDirectory(prefix='csl-test-homes-') as temporary:
        env = isolated_environment(Path(temporary))
        report['prerequisites'], errors = prerequisites(env, args.full)
        if errors:
            report['errors'] = errors
            return finish(2)
        if args.check:
            report['dependency_policy'] = 'existing test interpreter or temporary venv with scripts/isolated-test-requirements.txt'
            return finish(0)
        python = sys.executable
        try:
            if args.full:
                python = str(args.media_python.resolve(strict=True)) if args.media_python else str(Path(temporary)/'venv/bin/python')
                if not args.media_python:
                    subprocess.run([sys.executable, '-m', 'venv', str(Path(temporary)/'venv')],
                                   env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120)
                    command = [python, '-m', 'pip', 'install', '-r', str(ROOT/'scripts/isolated-test-requirements.txt')]
                    if args.wheelhouse:
                        command += ['--no-index', '--find-links', str(args.wheelhouse.resolve(strict=True))]
                    with (output/'dependencies.log').open('w') as log:
                        dependency = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=300)
                    if dependency.returncode:
                        report['errors'] = ['Pinned test dependency installation failed; see dependencies.log']
                        return finish(2)
                dependency_probe = subprocess.run([python, '-c',
                    'import json; from importlib.metadata import version; expected={"Pillow":"12.3.0","pypdfium2":"5.14.0","reportlab":"4.5.1","jsonschema":"4.26.0"}; actual={k:version(k) for k in expected}; print(json.dumps(actual)); assert actual==expected'],
                    env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
                if dependency_probe.returncode:
                    report['errors'] = ['Test interpreter is missing the pinned requirements; use a dedicated venv or omit --media-python']
                    return finish(2)
                report['direct_dependencies'] = json.loads(dependency_probe.stdout)
                env.update(FULL_FLAGS, CSL_MEDIA_PYTHON=python)
            report['python'] = subprocess.check_output([python, '--version'], env=env, text=True).strip()
            report['command'] = [python, '-m', 'unittest', 'discover', '-s', 'tests', '-v']
            start = time.monotonic()
            with (output/'tests.log').open('w') as log:
                run = subprocess.run(report['command'], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
            report.update(tests_executed=True, seconds=time.monotonic()-start)
            return finish(run.returncode)
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            report['errors'] = ['Test preparation failed: '+type(exc).__name__]
            return finish(2)


if __name__ == '__main__':
    raise SystemExit(main())

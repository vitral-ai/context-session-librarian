"""Foreground pass supervisor. Native crashes never run inside the supervisor."""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

from csl_sync_state import SyncError, exclusive

LOCK_ENV = 'CSL_SYNC_PASS_LOCK_FD'
AUTH_ERRORS = {'sign_in_required', 'credential_revoked', 'session_revoked', 'refresh_outcome_unknown'}


@contextmanager
def pass_lock(root):
    """Reuse only the exact open-file lock inherited from our supervisor."""
    inherited = os.environ.pop(LOCK_ENV, None)
    if inherited is None:
        with exclusive(root) as fd: yield fd
        return
    try:
        fd = int(inherited)
        actual, expected = os.fstat(fd), (root / 'collector.lock').stat()
        if (actual.st_dev, actual.st_ino, actual.st_uid) != (expected.st_dev, expected.st_ino, os.getuid()):
            raise ValueError()
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, ValueError):
        raise SyncError('invalid_pass_lock') from None
    try: yield fd
    finally: os.close(fd)


def run_pass(args, fd):
    command = [sys.executable, str(Path(__file__).with_name('codex_sessions.py')), 'sync', '--home',
               str(args.sync_home), 'run', '--days', str(args.days)]
    if args.all: command.append('--all')
    for provider in args.providers or []: command.extend(['--provider', provider])
    env = dict(os.environ)
    env[LOCK_ENV] = str(fd)
    process = subprocess.Popen(command, env=env, pass_fds=(fd,), stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, errors='replace',
                               start_new_session=True)
    try:
        stdout, stderr = process.communicate()
        return process.returncode, stdout, stderr, process.pid
    finally:
        if process.poll() is None:
            process.terminate()
            try: process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill(); process.communicate()


def error_code(stderr):
    for line in reversed(stderr.splitlines()):
        try:
            value = json.loads(line)
            code = value.get('error')
            if isinstance(code, str) and re.fullmatch(r'[a-z0-9_]{1,64}', code): return code
        except (ValueError, AttributeError): pass
    return None


def watch(args, state, hints):
    """Keep one installation lock while each independent interpreter runs a pass."""
    backoff = 1
    previous = signal.getsignal(signal.SIGTERM)
    def terminate(*_): raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, terminate)
    try:
        with exclusive(state.root) as fd:
            while True:
                started = time.monotonic()
                try:
                    code, stdout, stderr, pid = run_pass(args, fd)
                except OSError:
                    code, stdout, stderr, pid = 127, '', '', None
                detail = error_code(stderr)
                # Child diagnostics are intentionally not echoed: native crashes
                # may emit paths. Only validated CLI summaries and safe codes pass.
                for line in stdout.splitlines():
                    try:
                        value = json.loads(line)
                        if isinstance(value, dict) and 'synced' in value and 'pending' in value:
                            print(json.dumps(value, separators=(',', ':')), flush=True)
                    except ValueError: pass
                if code:
                    number = -code if code < 0 else None
                    event = {'state': 'pass_crashed' if number else 'pass_failed', 'pid': pid,
                             'exit_code': 128 + number if number else code,
                             'signal_number': number, 'signal': signal.Signals(number).name if number else None,
                             'error': detail, 'retry_in_seconds': backoff}
                    state.set('last_error', detail or ('pass_signal_' + str(number) if number else 'pass_failed'))
                    print(json.dumps(event), file=sys.stderr, flush=True)
                    if detail in AUTH_ERRORS: raise SyncError(detail)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue
                backoff = 1
                hints.load_hints(args.providers or ('codex', 'claude'))
                deadline = started + args.interval
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0: break
                    time.sleep(min(1, remaining))
                    if hints.changed() is True: break
    finally:
        signal.signal(signal.SIGTERM, previous)

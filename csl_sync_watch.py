"""Foreground pass supervisor. Native crashes never run inside the supervisor."""
from contextlib import contextmanager, nullcontext
import json
import os
from pathlib import Path, PureWindowsPath
import re
import signal
import subprocess
import sys
import time

from csl_sync_fs import WINDOWS, try_lock
from csl_sync_state import SyncError, exclusive

LOCK_ENV = 'CSL_SYNC_PASS_LOCK_FD'
AUTH_ERRORS = {'sign_in_required', 'credential_revoked', 'session_revoked', 'refresh_outcome_unknown'}


def history_notification(provider, filename):
    parts = PureWindowsPath(filename.casefold()).parts
    if not parts:
        return False
    if provider == 'codex':
        return parts[0] in ('sessions', 'archived_sessions') and parts[-1].endswith(('.jsonl', '.jsonl.zst'))
    if provider == 'claude':
        return len(parts) == 3 and parts[0] == 'projects' and parts[-1].endswith('.jsonl')
    if provider == 'antigravity':
        return len(parts) == 2 and parts[0] == 'conversations' and parts[-1].endswith(('.db', '.db-wal'))
    return False


@contextmanager
def pass_lock(root):
    """Reuse only the exact open-file lock inherited from our supervisor."""
    inherited = os.environ.pop(LOCK_ENV, None)
    if inherited is None:
        with exclusive(root) as fd: yield fd
        return
    fd = None
    try:
        fd = int(inherited)
        if WINDOWS:
            import msvcrt
            fd = msvcrt.open_osfhandle(fd, os.O_RDWR | os.O_BINARY)
        actual, expected = os.fstat(fd), (root / 'collector.lock').stat()
        if not os.path.samestat(actual, expected) or actual.st_uid != expected.st_uid:
            raise ValueError()
        if not WINDOWS:
            try_lock(fd)
    except (OSError, ValueError):
        # On Windows a failed conversion leaves no CRT descriptor to close.
        raise SyncError('invalid_pass_lock') from None
    try:
        yield fd
    finally:
        os.close(fd)


def run_pass(args, fd):
    command = [sys.executable, str(Path(__file__).with_name('codex_sessions.py')), 'sync', '--home',
               str(args.sync_home), 'run', '--days', str(args.days)]
    if args.all: command.append('--all')
    for provider in args.providers or []: command.extend(['--provider', provider])
    env = dict(os.environ)
    options = {}
    if WINDOWS:
        import msvcrt
        command[1] = str(Path(__file__).resolve().with_name('csl_sync.py'))
        handle = msvcrt.get_osfhandle(fd)
        env[LOCK_ENV] = str(handle)
        options['startupinfo'] = subprocess.STARTUPINFO(lpAttributeList={'handle_list': [handle]})
        os.set_handle_inheritable(handle, True)
    else:
        env[LOCK_ENV] = str(fd)
        options.update(pass_fds=(fd,), start_new_session=True)
    try:
        process = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                   errors='replace', **options)
    finally:
        if WINDOWS:
            os.set_handle_inheritable(handle, False)
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
    notifications = nullcontext()
    if WINDOWS:
        from csl_sync_windows import DirectoryChanges
        providers = args.providers or ('codex', 'claude', 'antigravity')
        roots = {}
        for name in providers:
            roots.setdefault(hints.module.provider_home(name), []).append(name)
        notifications = DirectoryChanges(roots, lambda root, filename:
                                         any(history_notification(p, filename) for p in roots[root]))
    try:
        with exclusive(state.root) as fd, notifications as changes:
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
                    if changes is not None:
                        if changes.wait(min(1, remaining)):
                            break
                    else:
                        time.sleep(min(1, remaining))
                    if hints.changed() is True: break
    finally:
        signal.signal(signal.SIGTERM, previous)

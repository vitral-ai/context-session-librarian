"""One-shot PID1 preparation for a Claude Code sole producer; unchanged Runner owns the exec-only writer.

Derived from the Codex gate. The Codex fresh-birth launch binding is not used:
the owner pins the session with a literal --session-id argument instead."""
import os
from pathlib import Path
import signal
import stat
import sys
import time

PYTHON = '/usr/local/bin/python3'
SCRIPT = '/opt/vitral/claude/prepare.py'
CLAUDE = '/opt/vitral/claude/bin/claude'
HOME = Path('/agent-private')
NATIVE = HOME/'.claude'
TRANSCRIPTS = Path('/transcripts')
STATE = Path('/collector-state')
SIGNALS = {signal.SIGUSR1, signal.SIGTERM, signal.SIGINT}
STOPS = {signal.SIGTERM, signal.SIGINT}


class Refused(Exception): pass


class Cancelled(BaseException):
    def __init__(self, signum): self.signum = signum


def clock(): return time.clock_gettime(time.CLOCK_BOOTTIME)


def check_deadline(deadline, now=clock):
    remaining = deadline-now()
    if not 0 < remaining <= 1800: raise Refused('preparation deadline')


def arguments(argv):
    seconds = 900
    if argv[:1] == ['--prepare-seconds']:
        if (len(argv) < 3 or not argv[1].isascii() or not argv[1].isdigit()
                or len(argv[1]) > 4 or str(int(argv[1])) != argv[1]): raise Refused('arguments')
        seconds, argv = int(argv[1]), argv[2:]
    if not 1 <= seconds <= 1800 or argv[:1] != ['--'] or len(argv) < 2:
        raise Refused('arguments')
    values = argv[1:]
    if (len(values) > 128 or any(not isinstance(value, str) or '\x00' in value for value in values)
            or sum(len(value.encode('utf-8')) for value in values) > 65536): raise Refused('arguments')
    return seconds, values


RESUME = {'--resume', '-r', '--continue', '-c', '--fork-session', '--from-pr'}


def session_binding(values, env):
    """The literal argv must pin exactly the owner-expected fresh session."""
    expected = env.get('VITRAL_COLLECTOR_EXPECT_SESSION')
    if (not isinstance(expected, str) or values.count('--session-id') != 1
            or any(value in RESUME or value.split('=', 1)[0] in RESUME | {'--session-id'} and '=' in value
                   for value in values)):
        raise Refused('session binding')
    index = values.index('--session-id')
    if index+1 >= len(values) or values[index+1] != expected: raise Refused('session binding')
    return expected


def readonly(path): return bool(os.statvfs(path).f_flag & os.ST_RDONLY)


def validate(env):
    from csl_workload_config import Configuration
    config = Configuration.from_environment(env)
    if (env.get('HOME') != str(HOME) or env.get('CLAUDE_CONFIG_DIR') != str(NATIVE)
            or config.provider != 'claude' or config.profile != 'claude-2.1.308-text-v1'
            or config.expect_session is None
            or config.transcript_root != TRANSCRIPTS or config.state_root != STATE): raise Refused('layout')
    for path in (HOME, NATIVE):
        info = path.lstat()
        if (path.resolve() != path or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o700 or os.path.ismount(path) or readonly(path)):
            raise Refused('private home')
    native = NATIVE/'projects'
    if native.resolve() != native or not native.is_dir(): raise Refused('projects')
    def identity(path):
        info = path.stat()
        return info.st_dev, info.st_ino
    native_id, transcript_id = identity(native), identity(TRANSCRIPTS)
    if (native_id != transcript_id
            or identity(STATE) in {identity(TRANSCRIPTS), identity(HOME), identity(NATIVE)}
            or readonly(STATE) or not readonly(TRANSCRIPTS) or readonly(native)):
        raise Refused('mount aliases')
    with os.scandir(TRANSCRIPTS) as entries:
        if next(entries, None) is not None: raise Refused('source not fresh')
    if (identity(native), identity(TRANSCRIPTS)) != (native_id, transcript_id):
        raise Refused('mount changed')
    return native_id, transcript_id


def encode_views(views):
    return ':'.join(str(number) for identity in views for number in identity)


def cancel(signum, _frame): raise Cancelled(signum)


def stop_pending():
    pending = signal.sigpending()
    return signal.SIGTERM if signal.SIGTERM in pending else signal.SIGINT if signal.SIGINT in pending else None


def prepare(argv, env, *, now=clock, validator=validate, run=None, wait=signal.sigtimedwait):
    # The process owns this signal mask until exit. Tests use isolated children.
    signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
    seconds, values = arguments(argv)
    session_binding(values, env)
    deadline = now()+seconds
    stop = stop_pending()
    if stop: raise Cancelled(stop)
    check_deadline(deadline, now)
    prepared_views = validator(env)
    check_deadline(deadline, now)
    while True:
        check_deadline(deadline, now)
        notification = wait(SIGNALS, min(.25, deadline-now()))
        stop = stop_pending()
        if stop: raise Cancelled(stop)
        if notification is None: continue
        if notification.si_signo in STOPS: raise Cancelled(notification.si_signo)
        if notification.si_signo != signal.SIGUSR1: raise Refused('signal')
        if validator(env) != prepared_views: raise Refused('mount changed')
        check_deadline(deadline, now)
        stop = stop_pending()
        if stop: raise Cancelled(stop)
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)
        for signum in STOPS: signal.signal(signum, cancel)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, SIGNALS)
        check_deadline(deadline, now)
        if run is None:
            from csl_workload_runner import main as run
        # No interpreter exec gap: cancel remains installed until Runner installs
        # its own handlers. The exec-only child checks this same deadline later.
        check_deadline(deadline, now)
        command = [PYTHON, '-E', '-s', '-S', SCRIPT, '--native-exec',
                   repr(deadline), encode_views(prepared_views), '--', *values]
        return run(['--', *command])


def native_exec(argv, env, *, now=clock, validator=validate, execute=os.execve):
    if len(argv) < 4 or argv[2] != '--' or len(argv[0]) > 32 or len(argv[1]) > 84:
        raise Refused('handoff')
    deadline = float(argv[0])
    # Reuse the literal-argument bounds without opening an alternate command.
    _, values = arguments(['--', *argv[3:]])
    session_binding(values, env)
    check_deadline(deadline, now)
    if encode_views(validator(env)) != argv[1]: raise Refused('mount changed')
    check_deadline(deadline, now)
    execute(CLAUDE, [CLAUDE, *values], env)
    raise Refused('exec returned')


def main():
    try:
        argv = sys.argv[1:]
        if argv[:1] == ['--native-exec']:
            if os.getpid() == 1: raise Refused('writer is not PID1')
            sys.path.insert(0, '/opt/vitral/collector')
            native_exec(argv[1:], dict(os.environ))
        else:
            if os.getpid() != 1 or os.getuid() != 10001: raise Refused('PID1 user')
            signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
            import resource
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
            sys.path.insert(0, '/opt/vitral/collector')
            return prepare(argv, dict(os.environ))
    except Cancelled as error:
        return 128+error.signum
    except Exception:
        return 2
    return 2


if __name__ == '__main__': raise SystemExit(main())

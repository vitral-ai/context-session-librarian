"""One-shot PID1 preparation; unchanged Runner owns the real exec-only writer."""
import os
from pathlib import Path
import signal
import stat
import sys
import time

PYTHON = '/usr/local/bin/python3'
SCRIPT = '/opt/vitral/codex/prepare.py'
CODEX = '/opt/vitral/codex/bin/codex'
HOME = Path('/agent-private')
NATIVE = HOME/'.codex'
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


def readonly(path): return bool(os.statvfs(path).f_flag & os.ST_RDONLY)


def validate(env):
    from csl_workload_config import Configuration
    config = Configuration.from_environment(env)
    if (env.get('HOME') != str(HOME) or env.get('CODEX_HOME') != str(NATIVE)
            or config.provider != 'codex' or config.profile != 'codex-0.169.0-text-v1'
            or config.transcript_root != TRANSCRIPTS or config.state_root != STATE): raise Refused('layout')
    for path in (HOME, NATIVE):
        info = path.lstat()
        if (path.resolve() != path or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o700 or os.path.ismount(path) or readonly(path)):
            raise Refused('private home')
    native = NATIVE/'sessions'
    if native.resolve() != native or not native.is_dir(): raise Refused('sessions')
    def identity(path):
        info = path.stat()
        return info.st_dev, info.st_ino
    if (identity(native) != identity(TRANSCRIPTS)
            or identity(STATE) in {identity(TRANSCRIPTS), identity(HOME), identity(NATIVE)}
            or readonly(STATE) or not readonly(TRANSCRIPTS) or readonly(native)):
        raise Refused('mount aliases')
    with os.scandir(TRANSCRIPTS) as entries:
        if next(entries, None) is not None: raise Refused('source not fresh')


def cancel(signum, _frame): raise Cancelled(signum)


def stop_pending():
    pending = signal.sigpending()
    return signal.SIGTERM if signal.SIGTERM in pending else signal.SIGINT if signal.SIGINT in pending else None


def prepare(argv, env, *, now=clock, validator=validate, run=None, wait=signal.sigtimedwait):
    # The process owns this signal mask until exit. Tests use isolated children.
    signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
    started = now()
    seconds, values = arguments(argv)
    deadline = started+seconds
    validator(env); check_deadline(deadline, now)
    while True:
        check_deadline(deadline, now)
        notification = wait(SIGNALS, min(.25, deadline-now()))
        stop = stop_pending()
        if stop: raise Cancelled(stop)
        if notification is None: continue
        if notification.si_signo in STOPS: raise Cancelled(notification.si_signo)
        if notification.si_signo != signal.SIGUSR1: raise Refused('signal')
        validator(env); check_deadline(deadline, now)
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
        return run(['--', PYTHON, '-E', '-s', '-S', SCRIPT, '--native-exec',
                    repr(deadline), '--', *values])


def native_exec(argv, env, *, now=clock, validator=validate, execute=os.execve):
    if len(argv) < 3 or argv[1] != '--' or len(argv[0]) > 32: raise Refused('handoff')
    deadline = float(argv[0])
    # Reuse the literal-argument bounds without opening an alternate command.
    _, values = arguments(['--', *argv[2:]])
    check_deadline(deadline, now)
    validator(env); check_deadline(deadline, now)
    execute(CODEX, [CODEX, 'exec', *values], env)
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

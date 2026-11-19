"""One-shot PID1 preparation; unchanged Runner owns the real exec-only writer."""
import json
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
OWNER_LAUNCH = Path('/run/vitral-a8')
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


def validate(env, *, prepared=None):
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
    native_id, transcript_id = identity(native), identity(TRANSCRIPTS)
    if (native_id != transcript_id
            or identity(STATE) in {identity(TRANSCRIPTS), identity(HOME), identity(NATIVE)}
            or readonly(STATE) or not readonly(TRANSCRIPTS) or readonly(native)):
        raise Refused('mount aliases')
    directory = None
    try:
        if prepared is not None:
            from csl_workload_launch_binding import PreparedLaunch
            if not isinstance(prepared, PreparedLaunch): raise Refused('root lease')
            try:
                directory = os.open(TRANSCRIPTS, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                info = os.fstat(directory)
                if (info.st_dev, info.st_ino) != transcript_id: prepared.failed = True
            except OSError: prepared.failed = True
            if prepared.failed and directory is not None:
                os.close(directory); directory = None
        with os.scandir(TRANSCRIPTS if directory is None else directory) as entries:
            if next(entries, None) is not None: raise Refused('source not fresh')
        if (identity(native), identity(TRANSCRIPTS)) != (native_id, transcript_id):
            raise Refused('mount changed')
        if prepared is not None and not prepared.failed: prepared.capture(directory, (native_id, transcript_id))
    finally:
        if directory is not None: os.close(directory)
    return native_id, transcript_id


def encode_views(views):
    return ':'.join(str(number) for identity in views for number in identity)


def cancel(signum, _frame): raise Cancelled(signum)


def stop_pending():
    pending = signal.sigpending()
    return signal.SIGTERM if signal.SIGTERM in pending else signal.SIGINT if signal.SIGINT in pending else None


def prepare(argv, env, *, now=clock, validator=validate, run=None, wait=signal.sigtimedwait, launch=None):
    if launch is not None:
        from csl_workload_launch_binding import LaunchAcceptanceDriver
        if not isinstance(launch, LaunchAcceptanceDriver): raise Refused('launch binding')
    try:
        return _prepare(argv, env, now=now, validator=validator, run=run, wait=wait, launch=launch)
    finally:
        if launch is not None: launch.close()


def _prepare(argv, env, *, now, validator, run, wait, launch):
    # The process owns this signal mask until exit. Tests use isolated children.
    signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
    started = now()
    seconds, values = arguments(argv)
    deadline = started+seconds
    if launch is not None:
        from csl_workload_launch_binding import LaunchAcceptanceDriver
        if not isinstance(launch, LaunchAcceptanceDriver) or validator is not validate or run is not None:
            raise Refused('launch binding')
    def validate_views():
        return validator(env) if launch is None else validator(env, prepared=launch.prepared)
    prepared_views = validate_views()
    check_deadline(deadline, now)
    while True:
        check_deadline(deadline, now)
        notification = wait(SIGNALS, min(.25, deadline-now()))
        stop = stop_pending()
        if stop: raise Cancelled(stop)
        if notification is None: continue
        if notification.si_signo in STOPS: raise Cancelled(notification.si_signo)
        if notification.si_signo != signal.SIGUSR1: raise Refused('signal')
        if validate_views() != prepared_views: raise Refused('mount changed')
        check_deadline(deadline, now)
        stop = stop_pending()
        if stop: raise Cancelled(stop)
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)
        for signum in STOPS: signal.signal(signum, cancel)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, SIGNALS)
        check_deadline(deadline, now)
        if run is None and launch is None:
            from csl_workload_runner import main as run
        # No interpreter exec gap: cancel remains installed until Runner installs
        # its own handlers. The exec-only child checks this same deadline later.
        check_deadline(deadline, now)
        command = [PYTHON, '-E', '-s', '-S', SCRIPT, '--native-exec',
                   repr(deadline), encode_views(prepared_views), '--', *values]
        if launch is not None:
            from csl_workload_config import Configuration
            from csl_workload_runner import Runner
            launch.prepared.handoff(prepared_views, command)
            return Runner(Configuration.from_environment(env), observation_driver=launch).run(command)
        return run(['--', *command])


def native_exec(argv, env, *, now=clock, validator=validate, execute=os.execve):
    if len(argv) < 4 or argv[2] != '--' or len(argv[0]) > 32 or len(argv[1]) > 84:
        raise Refused('handoff')
    deadline = float(argv[0])
    # Reuse the literal-argument bounds without opening an alternate command.
    _, values = arguments(['--', *argv[3:]])
    check_deadline(deadline, now)
    if encode_views(validator(env)) != argv[1]: raise Refused('mount changed')
    check_deadline(deadline, now)
    execute(CODEX, [CODEX, 'exec', *values], env)
    raise Refused('exec returned')


def owner_expected(directory, uid):
    from csl_canonical import strict_json

    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with os.fdopen(os.open('expected.json', flags, dir_fd=directory), 'rb') as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != uid or info.st_nlink != 1 or info.st_size > 2048):
            raise Refused('owner input')
        raw = stream.read(2049)
        if len(raw) != info.st_size:
            raise Refused('owner input changed')
    expected = strict_json(raw)
    if type(expected) is not dict:
        raise Refused('owner input schema')
    version = expected.pop('version', None)
    if (expected.keys() != {'source', 'scope', 'account_id', 'device_id'}
            or type(version) is not int or version != 1
            or any(type(value) is not str for value in expected.values())):
        raise Refused('owner input schema')
    return expected


def owner_result(descriptor, report, driver):
    if descriptor is None:
        return
    try:
        payload = (json.dumps({'report': report, 'observation': driver.result},
                              separators=(',', ':')) + '\n').encode('utf-8')
        if len(payload) <= os.fpathconf(descriptor, 'PC_PIPE_BUF'):
            os.write(descriptor, payload)
    except Exception:
        # The private reader sees no frame if the atomic write cannot proceed.
        pass


def owner_result_pipe():
    descriptor = 3
    try:
        if not stat.S_ISFIFO(os.fstat(descriptor).st_mode):
            return None
        os.set_inheritable(descriptor, False)
        os.set_blocking(descriptor, False)
        return descriptor
    except OSError:
        return None


def owner_launch(argv, env, result_pipe):
    from csl_workload_config import Configuration
    from csl_workload_launch_binding import PROFILE, LaunchAcceptanceDriver, PreparedLaunch
    from csl_workload_observation_driver import runtime_fingerprint
    from csl_workload_runner import scope

    if 'VITRAL_COLLECTOR_OBSERVE_SESSION' in env:
        raise Refused('session selector')
    config = Configuration.from_environment(env)
    if config.provider != 'codex' or config.profile != PROFILE:
        raise Refused('owner provider or profile')
    uid = os.getuid()
    directory = os.open(OWNER_LAUNCH, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(directory)
        if info.st_uid != uid or stat.S_IMODE(info.st_mode) != 0o700:
            raise Refused('owner directory')
        expected = owner_expected(directory, uid)
        if expected['source'] != runtime_fingerprint() or expected['scope'] != scope(config):
            raise Refused('owner runtime or scope')
        with os.scandir(directory) as entries:
            if next(entries).name != 'expected.json' or next(entries, None) is not None:
                raise Refused('fresh owner directory')
        driver = LaunchAcceptanceDriver(**expected, prepared=PreparedLaunch())
        report = prepare(argv, env, launch=driver)
        owner_result(result_pipe, report, driver)
        if not report['complete']:
            return 2
        code = report['writer_exit'] or 0
        return 128 - code if code < 0 else code
    finally:
        os.close(directory)


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
            if argv[:1] == ['--observe-owned-launch']:
                return owner_launch(argv[1:], dict(os.environ), owner_result_pipe())
            return prepare(argv, dict(os.environ))
    except Cancelled as error:
        return 128+error.signum
    except Exception:
        return 2
    return 2


if __name__ == '__main__': raise SystemExit(main())

"""Fixture-only, replica-local W7 enrollment. No server or product CLI.

Run via the operator's private exec pipe, never as a pod logging entry point.
Only the public consent projection crosses that pipe; keys/tokens stay in STATE.
"""
import argparse
import fcntl
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import select
import signal
import stat
import sys
import time
from urllib.parse import urlsplit


class FixtureRefused(Exception):
    pass


def boottime():
    return time.clock_gettime(time.CLOCK_BOOTTIME)


def boundary(deadline, clock):
    now = clock()
    if not math.isfinite(now) or now < 0 or not math.isfinite(deadline) or now >= deadline:
        raise FixtureRefused()
    return deadline-now


def pipe_writer(descriptor):
    info = os.fstat(descriptor)
    if (descriptor < 1 or not stat.S_ISFIFO(info.st_mode) or os.isatty(descriptor)
            or fcntl.fcntl(descriptor, fcntl.F_GETFL) & os.O_ACCMODE == os.O_RDONLY):
        raise FixtureRefused()
    duplicate = os.dup(descriptor)
    os.set_blocking(duplicate, False)
    return duplicate


def write_public(descriptor, event, deadline, clock=boottime):
    data = json.dumps(event, allow_nan=False, separators=(',', ':')).encode()+b'\n'
    if len(data) > min(2048, os.fpathconf(descriptor, 'PC_PIPE_BUF')):
        raise FixtureRefused()
    while True:
        remaining = boundary(deadline, clock)
        _, ready, _ = select.select([], [descriptor], [], min(0.1, remaining))
        if not ready: continue
        boundary(deadline, clock)
        try:
            if os.write(descriptor, data) != len(data): raise FixtureRefused()
            return
        except BlockingIOError:
            continue


def consent_record(config, public):
    from csl_workload_identity import metadata
    if (not isinstance(public, dict) or set(public) != {'user_code', 'verification_uri', 'expires_in', 'interval'}
            or not isinstance(public['user_code'], str)
            or not re.fullmatch(r'[23456789ABCDEFGHJKLMNPQRSTUVWXYZ]{4}-[23456789ABCDEFGHJKLMNPQRSTUVWXYZ]{4}', public['user_code'])
            or public['verification_uri'] != config.origin+'/native-auth/v1/device'):
        raise FixtureRefused()
    for field, maximum in (('expires_in', 600), ('interval', 60)):
        value = public[field]
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= maximum:
            raise FixtureRefused()
    return {'v': 1, 'kind': 'consent', **public,
            'metadata_sha256': metadata(config.location)[1],
            'provider': config.provider, 'profile': config.profile}


def enroll(environment, emit, *, deadline, clock=boottime, sleep=time.sleep):
    from csl_workload_auth import WorkloadTokens
    from csl_workload_config import Configuration
    from csl_workload_identity import WorkloadKey
    from csl_workload_runner import DeadlineHTTP
    from csl_workload_state import child

    if boundary(deadline, clock) > 120: raise FixtureRefused()
    config = Configuration.from_environment(environment)
    host = urlsplit(config.origin).hostname
    try: fixture_host = ipaddress.ip_address(host).is_loopback
    except ValueError: fixture_host = host == 'localhost' or host.endswith('.test')
    if not fixture_host or config.ca_file is None: raise FixtureRefused()
    # No standalone trust/credential mechanism: the image's existing client,
    # exact-origin transport/write deadline, journal and private children only.
    http = DeadlineHTTP(config.origin, deadline, ca_file=config.ca_file, attempts=1, timeout=5)
    key = WorkloadKey(child(config.state_root, 'identity'), executable='/usr/bin/openssl')
    tokens = WorkloadTokens(child(config.state_root, 'auth'), config.origin, config.location,
        http=http, key=key, clock=clock, stopped=lambda: clock() >= deadline)
    public = tokens.begin(timeout=boundary(deadline, clock))
    boundary(deadline, clock)
    emit(consent_record(config, public), deadline)
    while True:
        boundary(deadline, clock)
        result = tokens.poll()
        boundary(deadline, clock)
        if result == {'state': 'ready'}:
            emit({'v': 1, 'kind': 'result', 'state': 'ready'}, deadline)
            return
        if result != {'state': 'pending'}: raise FixtureRefused()
        sleep(min(0.1, boundary(deadline, clock)))


def failure_record(error):
    code = getattr(error, 'code', None)
    states = {'workload_enrollment_refused': 'denied', 'workload_approval_expired': 'expired',
              'workload_auth_already_started': 'existing_state'}
    state = states.get(code, 'pending') if isinstance(code, str) else 'pending'
    return {'v': 1, 'kind': 'result', 'state': state}


class Arguments(argparse.ArgumentParser):
    def error(self, message):
        raise FixtureRefused()


def main(argv=None):
    writer = deadline = None
    previous = {}
    try:
        parser = Arguments(description=__doc__)
        parser.add_argument('--runtime', required=True, type=Path)
        parser.add_argument('--consent-fd', required=True, type=int)
        parser.add_argument('--budget-seconds', type=int, default=90)
        args = parser.parse_args(argv)
        if not 1 <= args.budget_seconds <= 120: raise FixtureRefused()
        deadline = boottime()+args.budget_seconds
        writer = pipe_writer(args.consent_fd)
        runtime = args.runtime
        if not runtime.is_absolute() or runtime.resolve() != runtime or not runtime.is_dir():
            raise FixtureRefused()
        sys.path.insert(0, str(runtime))
        def stopped(*unused): raise FixtureRefused()
        for name in (signal.SIGTERM, signal.SIGINT):
            previous[name] = signal.signal(name, stopped)
        enroll(os.environ, lambda event, end: write_public(writer, event, end), deadline=deadline)
        return 0
    except (Exception, KeyboardInterrupt) as error:
        if writer is not None:
            try: write_public(writer, failure_record(error), deadline)
            except (Exception, KeyboardInterrupt): pass
        return 2
    finally:
        for name, handler in previous.items(): signal.signal(name, handler)
        if writer is not None: os.close(writer)


if __name__ == '__main__': raise SystemExit(main())

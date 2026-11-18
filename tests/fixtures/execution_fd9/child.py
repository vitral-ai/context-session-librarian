"""Owned synthetic child, launched only by parent.go; never installs a session."""
import base64
import fcntl
import json
import os
from pathlib import Path
import stat
import sys
import time

sys.path.insert(0, sys.argv[1])
import csl_execution as e
from csl_execution_transport import Channel

root = Path(sys.argv[2])
scenario = sys.argv[3]
req = e.request(base64.b64decode(sys.argv[4], validate=True))


def report(value):
    with (root/'report.json').open('x') as stream:
        json.dump(value, stream)


try:
    if scenario == 'setup-stall':
        time.sleep(3)  # before any Python clock/checker exists
    # Inspect actual inherited FDs, not an expected-table mock. listdir's own
    # transient directory fd has closed before the fstat loop below.
    fds = []
    for name in os.listdir('/proc/self/fd'):
        fd = int(name)
        if fd < 3:
            continue
        try:
            os.fstat(fd)
        except OSError:
            continue
        fds.append(fd)
    assert sorted(fds) == list(range(3, 10)), fds
    assert stat.S_ISFIFO(os.fstat(3).st_mode)
    assert stat.S_ISFIFO(os.fstat(4).st_mode)
    assert stat.S_ISREG(os.fstat(5).st_mode)
    for fd in (6, 7):
        assert stat.S_ISDIR(os.fstat(fd).st_mode)
    assert stat.S_ISCHR(os.fstat(8).st_mode)
    assert stat.S_ISSOCK(os.fstat(9).st_mode)
    if scenario == 'death-prehello':
        (root/'ready').write_text(str(os.getpid()))
    with Channel(9, req, parent_pid=os.getppid(), parent_uid=os.getuid()) as channel:
        channel.admit()
        if scenario == 'death-effect':
            (root/'ready').write_text(str(os.getpid()))
            channel.check('effect')  # parent killed before acknowledging
            (root/'effect').write_text('must not exist')
        channel.read_request(3)
        if scenario == 'result-pressure':
            channel.write_result(4, b'x'*e.MAX_MESSAGE)
        else:
            captured = e.capture(5, req, channel.check, 1024)
            try:
                channel.check('captured')  # parent rewrites FD5's original inode
                assert captured.rewind().read() == b'package'
                try:
                    os.pwrite(captured.stream.fileno(), b'x', 0)
                except PermissionError:
                    pass
                else:
                    raise AssertionError('capture mutable')
                result = dict(status='captured', fds=sorted(fds), sealed=True,
                              input_offset_zero=os.lseek(5, 0, os.SEEK_CUR) == 0)
                channel.write_result(4, json.dumps(result).encode())
                report(result)
            finally:
                captured.close()
except e.ExecutionError as exc:
    report(dict(status='refused', error=str(exc)))
finally:
    for fd in range(3, 9):
        os.close(fd)

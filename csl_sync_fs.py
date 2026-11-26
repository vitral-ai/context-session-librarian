"""Private file and snapshot operations shared by the native collector."""
import os
import stat
import time
from contextlib import contextmanager
from contextvars import ContextVar

_work = ContextVar('collector_work', default=None)


class PassYield(Exception):
    """Normal cooperative yield, never a provider or credential failure."""


class WorkBudget:
    def __init__(self, seconds, size, *, clock=time.monotonic):
        self.clock, self.deadline = clock, clock() + seconds
        self.remaining, self.used = size, 0

    def check(self, size=0):
        if self.clock() >= self.deadline or size > self.remaining:
            raise PassYield()
        self.remaining -= size
        self.used += size


def check_work(size=0, *, available=0):
    budget = _work.get()
    if budget is not None:
        if available > budget.remaining: raise PassYield()
        budget.check(size)


@contextmanager
def work_budget(budget):
    token = _work.set(budget)
    try: yield
    finally: _work.reset(token)


class _BudgetStream:
    def __init__(self, stream): self.stream = stream
    def __getattr__(self, name): return getattr(self.stream, name)
    def __enter__(self): return self
    def __exit__(self, *args): return self.stream.__exit__(*args)
    def _limit(self, size):
        maximum = _work.get().remaining + 1
        return maximum if size < 0 else min(size, maximum)
    def read(self, size=-1):
        check_work()
        data = self.stream.read(self._limit(size))
        check_work(len(data))
        return data
    def readline(self, size=-1):
        check_work()
        data = self.stream.readline(self._limit(size))
        check_work(len(data))
        return data

WINDOWS = os.name == 'nt'

if WINDOWS:
    from csl_sync_windows import change_time, durable_replace, extended_path, is_private, open_lock, open_read, replace
else:
    import fcntl
    from os import replace
    durable_replace = replace

    def extended_path(path):
        return path

    def open_read(path):
        return os.open(path, os.O_RDONLY | os.O_NOFOLLOW)

    def change_time(_path, info):
        return info.st_ctime_ns

    def open_lock(path):
        return os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)


def open_stream(path):
    stream = os.fdopen(open_read(path), 'rb')
    return _BudgetStream(stream) if _work.get() is not None else stream


def reparse(path):
    try:
        return bool(path.lstat().st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT) if WINDOWS else path.is_symlink()
    except FileNotFoundError:
        return False


def make_private_directory(path):
    if not reparse(path):
        # CPython applies a restricted DACL to mode 0700 on Windows (3.13+).
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
        info = path.stat()
        if not stat.S_ISDIR(info.st_mode):
            return False
        if WINDOWS:
            return is_private(path)
        if info.st_uid == os.getuid():
            path.chmod(0o700)
            return True
    return False


def private_file(path, info):
    if not stat.S_ISREG(info.st_mode):
        return False
    if WINDOWS:
        return info.st_nlink == 1 and not reparse(path) and is_private(path)
    return info.st_uid == os.getuid() and not info.st_mode & 0o077


def protect_fd(fd):
    if not WINDOWS:
        os.fchmod(fd, 0o600)


def protect_path(path):
    if WINDOWS:
        if not private_file(path, path.stat()):
            raise OSError('unsafe state file')
    else:
        path.chmod(0o600)


def flush_directory(path):
    if not WINDOWS:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def try_lock(fd):
    if not WINDOWS:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

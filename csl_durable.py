"""Private FD-relative execution metadata; no CLI or destination publisher."""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import secrets
import stat

from csl_backup_core import canonical


class DurableError(ValueError):
    pass


class MissingMetadata(FileNotFoundError):
    """Only a proven absence at initial open, never an observed record loss."""


def directory(fd):
    """Check the actual held directory AND every non-writable ancestor."""
    info = os.fstat(fd)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise DurableError('execution_directory_insecure')
    path = Path(os.readlink(f'/proc/self/fd/{fd}'))
    current = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            nxt = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
            os.close(current)
            current = nxt
            item = os.fstat(current)
            if item.st_uid not in (0, os.getuid()) or item.st_mode & 0o022:
                raise DurableError('execution_ancestor_insecure')
        if (os.fstat(current).st_dev, os.fstat(current).st_ino) != (info.st_dev, info.st_ino):
            raise DurableError('execution_directory_changed')
    finally:
        os.close(current)
    return info


def _name(name):
    if not isinstance(name, str) or not name or name in ('.', '..') or '/' in name or '\\' in name or '\0' in name:
        raise DurableError('execution_name_invalid')


def regular(fd):
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
        raise DurableError('execution_file_insecure')
    return info


@contextmanager
def locked(fd, check):
    directory(fd)
    check()
    lock = os.open('operation.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, 0o600, dir_fd=fd)
    try:
        regular(lock)
        check()
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        directory(fd)
        held = regular(lock)
        try:
            named = os.stat('operation.lock', dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            raise DurableError('execution_lock_uncertain') from None
        if (named.st_dev, named.st_ino) != (held.st_dev, held.st_ino):
            raise DurableError('execution_lock_replaced')
        check()
        yield
    finally:
        os.close(lock)


def read(fd, name, limit):
    _name(name)
    directory(fd)
    try:
        src = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=fd)
    except FileNotFoundError:
        raise MissingMetadata('execution_metadata_missing') from None
    try:
        before = regular(src)
        if before.st_size > limit:
            raise DurableError('execution_metadata_limit')
        with os.fdopen(os.dup(src), 'rb') as stream:
            raw = stream.read(limit + 1)
        after = regular(src)
        try:
            named = os.stat(name, dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            raise DurableError('execution_metadata_uncertain') from None
        if (named.st_dev, named.st_ino) != (after.st_dev, after.st_ino):
            raise DurableError('execution_metadata_replaced')
        if len(raw) > limit or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise DurableError('execution_metadata_changed')
        return raw
    finally:
        os.close(src)


def create(fd, name, value, check, limit):
    """Exclusive metadata only. An uncertain final link is reconciled by read."""
    _name(name)
    raw = canonical(value) + b'\n'
    if len(raw) > limit:
        raise DurableError('execution_metadata_limit')
    directory(fd)
    stage = '.execution-' + secrets.token_hex(16)
    check()
    out = os.open(stage, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=fd)
    owned = os.fstat(out)
    try:
        regular(out)
        offset = 0
        while offset < len(raw):
            check()
            n = os.write(out, raw[offset:])
            if n <= 0:
                raise DurableError('execution_short_write')
            offset += n
        regular(out)
        check()
        os.fsync(out)
        directory(fd)
        check()
        named = os.stat(stage, dir_fd=fd, follow_symlinks=False)
        if (named.st_dev, named.st_ino) != (owned.st_dev, owned.st_ino):
            raise DurableError('execution_staging_replaced')
        check()
        os.link(stage, name, src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
        # Durability failure keeps the known final reference; never roll it back.
        os.fsync(fd)
    finally:
        os.close(out)
        try:
            named = os.stat(stage, dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            if (named.st_dev, named.st_ino) == (owned.st_dev, owned.st_ino):
                os.unlink(stage, dir_fd=fd)

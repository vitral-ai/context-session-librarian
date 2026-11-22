"""Private file and snapshot operations shared by the native collector."""
import os
import stat

WINDOWS = os.name == 'nt'

if WINDOWS:
    from csl_sync_windows import change_time, extended_path, is_private, open_lock, open_read, replace
else:
    import fcntl
    from os import replace

    def extended_path(path):
        return path

    def open_read(path):
        return os.open(path, os.O_RDONLY | os.O_NOFOLLOW)

    def change_time(_path, info):
        return info.st_ctime_ns

    def open_lock(path):
        return os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)


def open_stream(path):
    return os.fdopen(open_read(path), 'rb')


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

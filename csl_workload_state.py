"""Private, bounded workload files shared by supervisor and owned guardian."""
import os
from pathlib import Path
import re
import stat

from csl_canonical import strict_json
from csl_sync_state import SyncError, private_directory

HOST_ID = re.compile(r'host-[a-f0-9]{16}')


def adjacent_host_id(root):
    """Read the optional public host ID handoff, never the tunnel state."""
    try:
        fd = os.open(root/'host-id', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 22
                    or info.st_uid not in (0, os.getuid()) or info.st_mode & 0o022):
                return None
            value = stream.read(23).decode('ascii').removesuffix('\n')
    except (OSError, UnicodeError):
        return None
    return value if HOST_ID.fullmatch(value) else None


def child(root, name):
    if (not isinstance(name, str) or not re.fullmatch(r'[a-z][a-z0-9-]{0,63}', name)
            or root.resolve() != root):
        raise SyncError('workload_runtime_storage_refused')
    path = root/name
    if path.exists() or path.is_symlink():
        info = path.lstat()
        if (not stat.S_ISDIR(info.st_mode) or path.resolve() != path
                or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700):
            raise SyncError('workload_runtime_storage_refused')
    return private_directory(path)


def read_record(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    except OSError:
        raise SyncError('workload_runtime_storage_refused') from None
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > 65536):
            raise SyncError('workload_runtime_storage_refused')
        data = stream.read(65537)
    try:
        value = strict_json(data)
        if isinstance(value, dict) and len(data) <= 65536: return value
    except (ValueError, UnicodeError, RecursionError):
        pass
    raise SyncError('workload_runtime_record_refused')

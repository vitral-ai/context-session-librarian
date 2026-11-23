"""Windows filesystem primitives for the native session collector."""
import ctypes as c
from ctypes import wintypes as w
from functools import lru_cache
import os
from pathlib import Path
import stat
import struct
import time

if os.name == 'nt':
    import msvcrt

    kernel = c.WinDLL('kernel32', use_last_error=True)
    security = c.WinDLL('advapi32', use_last_error=True)

INVALID_HANDLE = c.c_void_p(-1).value
REPARSE = stat.FILE_ATTRIBUTE_REPARSE_POINT



def extended_path(path):
    """Use Win32 extended paths independently of the machine's long-path policy."""
    absolute = os.path.abspath(path)
    prefix = '\\\\?\\'
    if absolute.startswith(prefix):
        return Path(absolute)
    if absolute.startswith('\\\\'):
        return Path(prefix + 'UNC\\' + absolute[2:])
    return Path(prefix + absolute)


def api(library, name, result, *args):
    function = getattr(library, name)
    function.restype, function.argtypes = result, args
    return function


pointer = c.c_void_p
out = c.POINTER(pointer)
if os.name == 'nt':
    create_file = api(kernel, 'CreateFileW', w.HANDLE, w.LPCWSTR, w.DWORD, w.DWORD, pointer, w.DWORD, w.DWORD, w.HANDLE)
    close_handle = api(kernel, 'CloseHandle', w.BOOL, w.HANDLE)
    local_free = api(kernel, 'LocalFree', w.HLOCAL, w.HLOCAL)
    file_info = api(kernel, 'GetFileInformationByHandleEx', w.BOOL, w.HANDLE, c.c_int, pointer, w.DWORD)
    move_file = api(kernel, 'MoveFileExW', w.BOOL, w.LPCWSTR, w.LPCWSTR, w.DWORD)
    named_security = api(security, 'GetNamedSecurityInfoW', w.DWORD, w.LPWSTR, c.c_int, w.DWORD, out, out, out, out, out)
    get_ace = api(security, 'GetAce', w.BOOL, pointer, w.DWORD, out)
    to_string_sid = api(security, 'ConvertSidToStringSidW', w.BOOL, pointer, out)
    current_process = api(kernel, 'GetCurrentProcess', w.HANDLE)
    open_token = api(security, 'OpenProcessToken', w.BOOL, w.HANDLE, w.DWORD, out)
    token_info = api(security, 'GetTokenInformation', w.BOOL, w.HANDLE, c.c_int, pointer, w.DWORD, c.POINTER(w.DWORD))


def sid_string(sid):
    value = pointer()
    if not to_string_sid(sid, c.byref(value)):
        raise c.WinError(c.get_last_error())
    try:
        return c.wstring_at(value)
    finally:
        local_free(value)


@lru_cache(maxsize=1)
def user_sid():
    token = pointer()
    if not open_token(current_process(), 0x0008, c.byref(token)):  # TOKEN_QUERY
        raise c.WinError(c.get_last_error())
    try:
        size = w.DWORD()
        token_info(token, 1, None, 0, c.byref(size))  # TokenUser
        buffer = c.create_string_buffer(size.value)
        if not token_info(token, 1, buffer, size, c.byref(size)):
            raise c.WinError(c.get_last_error())
        return sid_string(pointer.from_buffer(buffer))
    finally:
        close_handle(token)


def is_private(path):
    """Accept ordinary allow/deny ACLs confined to this user and local admins."""
    owner, dacl, descriptor = pointer(), pointer(), pointer()
    code = named_security(str(path), 1, 0x5, c.byref(owner), None, c.byref(dacl), None, c.byref(descriptor))
    if code:
        raise c.WinError(code)
    try:
        admin, system, owner_rights = 'S-1-5-32-544', 'S-1-5-18', 'S-1-3-4'
        user = user_sid()
        if not owner or not dacl or sid_string(owner) not in (user, admin):
            return False
        count = c.c_ushort.from_address(dacl.value + 4).value  # ACL.AceCount
        for index in range(count):
            ace = pointer()
            if not get_ace(dacl, index, c.byref(ace)):
                raise c.WinError(c.get_last_error())
            kind = c.c_ubyte.from_address(ace.value).value
            if kind == 1:  # ACCESS_DENIED_ACE
                continue
            if kind != 0 or sid_string(ace.value + 8) not in (user, admin, system, owner_rights):
                return False
        return True
    finally:
        local_free(descriptor)


def open_read(path):
    """Open a real file in binary mode while allowing producers to append or rotate."""
    handle = create_file(str(path), 0x80000000, 0x7, None, 3, 0x00200000, None)
    return file_descriptor(handle, os.O_RDONLY)


def open_lock(path):
    # One writable file object, shared only by handle inheritance. If the
    # supervisor dies while a pass is running, the child still owns the lock.
    handle = create_file(str(path), 0xc0000000, 1, None, 4, 0x00200000, None)
    if handle == INVALID_HANDLE and c.get_last_error() == 32:  # ERROR_SHARING_VIOLATION
        raise BlockingIOError()
    fd = file_descriptor(handle, os.O_RDWR)
    try:
        if not is_private(path) or os.fstat(fd).st_nlink != 1:
            raise OSError('unsafe lock file')
    except BaseException:
        os.close(fd)
        raise
    return fd


def file_descriptor(handle, access):
    if handle == INVALID_HANDLE:
        raise c.WinError(c.get_last_error())
    try:
        info = (w.DWORD * 2)()
        if not file_info(handle, 9, c.byref(info), c.sizeof(info)):  # FileAttributeTagInfo
            raise c.WinError(c.get_last_error())
        if info[0] & (REPARSE | stat.FILE_ATTRIBUTE_DIRECTORY):
            raise OSError('unsafe source')
        return msvcrt.open_osfhandle(handle, access | os.O_BINARY)
    except BaseException:
        close_handle(handle)
        raise


class Overlapped(c.Structure):
    _fields_ = [('internal', c.c_size_t), ('internal_high', c.c_size_t),
                ('offset', w.DWORD), ('offset_high', w.DWORD), ('event', w.HANDLE)]


if os.name == 'nt':
    create_event = api(kernel, 'CreateEventW', w.HANDLE, pointer, w.BOOL, w.BOOL, w.LPCWSTR)
    reset_event = api(kernel, 'ResetEvent', w.BOOL, w.HANDLE)
    read_changes = api(kernel, 'ReadDirectoryChangesW', w.BOOL, w.HANDLE, pointer, w.DWORD, w.BOOL,
                       w.DWORD, c.POINTER(w.DWORD), c.POINTER(Overlapped), pointer)
    wait_handles = api(kernel, 'WaitForMultipleObjects', w.DWORD, w.DWORD, c.POINTER(w.HANDLE), w.BOOL, w.DWORD)
    cancel_io = api(kernel, 'CancelIoEx', w.BOOL, w.HANDLE, c.POINTER(Overlapped))
    io_result = api(kernel, 'GetOverlappedResult', w.BOOL, w.HANDLE, c.POINTER(Overlapped),
                    c.POINTER(w.DWORD), w.BOOL)


class DirectoryWatch:
    def __init__(self, path):
        # FILE_LIST_DIRECTORY, shared access, OPEN_EXISTING, BACKUP_SEMANTICS | OVERLAPPED.
        self.handle = create_file(str(path), 1, 7, None, 3, 0x42000000, None)
        if self.handle == INVALID_HANDLE:
            raise c.WinError(c.get_last_error())
        self.event = create_event(None, True, False, None)
        if not self.event:
            close_handle(self.handle)
            raise c.WinError(c.get_last_error())
        self.buffer = c.create_string_buffer(32768)
        self.overlapped = Overlapped(event=self.event)
        self.pending = False
        try:
            self.arm()
        except OSError:
            self.close()
            raise

    def arm(self):
        reset_event(self.event)
        # File and directory names, size, and last-write changes in the full subtree.
        if not read_changes(self.handle, self.buffer, len(self.buffer), True, 0x1b, None,
                            c.byref(self.overlapped), None):
            raise c.WinError(c.get_last_error())
        self.pending = True

    def consume(self):
        transferred = w.DWORD()
        ok = io_result(self.handle, c.byref(self.overlapped), c.byref(transferred), False)
        self.pending = False
        if not ok and c.get_last_error() != 1022:  # ERROR_NOTIFY_ENUM_DIR (overflow)
            raise c.WinError(c.get_last_error())
        data = self.buffer.raw[:transferred.value] if ok else b''
        self.arm()
        return notification_paths(data)

    def close(self):
        if self.pending:
            cancel_io(self.handle, c.byref(self.overlapped))
            transferred = w.DWORD()
            io_result(self.handle, c.byref(self.overlapped), c.byref(transferred), True)
        close_handle(self.handle)
        close_handle(self.event)


def notification_paths(data):
    offset = 0
    while offset + 12 <= len(data):
        following, _, length = struct.unpack_from('<III', data, offset)
        start = offset + 12
        yield data[start:start + length].decode('utf-16-le', errors='surrogatepass')
        if not following:
            break
        offset += following


class DirectoryChanges:
    """Use native notifications only as hints. Timed verified scans still run."""
    def __init__(self, paths, accept=lambda _root, _name: True):
        self.paths = tuple(dict.fromkeys(paths))
        self.accept = accept
        self.watches = {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        for watch in self.watches.values():
            watch.close()

    def wait(self, seconds):
        for path in self.paths:
            if path not in self.watches:
                try:
                    self.watches[path] = DirectoryWatch(path)
                except OSError:
                    pass  # Missing or unsupported roots remain on the timed scan.
        deadline = time.monotonic() + seconds
        while (remaining := deadline - time.monotonic()) > 0:
            rows = list(self.watches.items())
            if not rows:
                time.sleep(remaining)
                break
            events = (w.HANDLE * len(rows))(*(watch.event for _, watch in rows))
            index = wait_handles(len(rows), events, False, max(1, int(remaining * 1000)))
            if index >= len(rows):
                break
            path, watch = rows[index]
            try:
                if any(self.accept(path, name) for name in watch.consume()):
                    return True
            except OSError:
                watch.close()
                del self.watches[path]
        return False


if os.name == 'nt':
    set_file_info = api(kernel, 'SetFileInformationByHandle', w.BOOL, w.HANDLE, c.c_int, pointer, w.DWORD)


class RenameInfo(c.Structure):
    _fields_ = [('flags', w.DWORD), ('root', w.HANDLE), ('length', w.DWORD), ('name', w.WCHAR * 1)]


def replace(source, target):
    """Atomically replace the name even when a reader holds the old file open."""
    filename = os.fsdecode(target).encode('utf-16-le')
    offset = RenameInfo.name.offset
    size = max(c.sizeof(RenameInfo), offset + len(filename) + c.sizeof(w.WCHAR))
    buffer = c.create_string_buffer(size)
    info = RenameInfo.from_buffer(buffer)
    # ReplaceIfExists | PosixSemantics leaves old handles on the old contents.
    info.flags, info.length = 3, len(filename)
    c.memmove(c.addressof(buffer) + offset, filename, len(filename))
    handle = create_file(os.fsdecode(source), 0x00010000, 0x7, None, 3, 0x00200000, None)
    if handle == INVALID_HANDLE:
        raise c.WinError(c.get_last_error())
    try:
        if set_file_info(handle, 22, buffer, size):  # FileRenameInfoEx
            return
        error = c.get_last_error()
    finally:
        close_handle(handle)
    if error not in (1, 50, 87, 120):  # FileRenameInfoEx unsupported by the OS or volume.
        raise c.WinError(error)
    os.replace(source, target)


def durable_replace(source, target):
    # REPLACE_EXISTING | WRITE_THROUGH. The temporary file is already flushed.
    source, target = os.fsdecode(source), os.fsdecode(target)
    for delay in (0, 0.01, 0.025, 0.05, 0.1, 0.2):
        if delay:
            time.sleep(delay)
        if move_file(source, target, 0x9):
            return
        error = c.get_last_error()
        # A concurrent status read may briefly hold the old destination.
        if error not in (5, 32):
            break
    raise c.WinError(error)


def change_time(path, _stat):
    handle = create_file(str(path), 0x80, 7, None, 3, 0x00200000, None)  # FILE_READ_ATTRIBUTES
    if handle == INVALID_HANDLE:
        raise c.WinError(c.get_last_error())
    try:
        # FILE_BASIC_INFO reports NTFS ChangeTime independently of the birth time.
        times = (c.c_longlong * 5)()
        if not file_info(handle, 0, c.byref(times), c.sizeof(times)):
            raise c.WinError(c.get_last_error())
        return times[3] * 100
    finally:
        close_handle(handle)

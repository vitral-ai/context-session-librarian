"""Private Linux FD9 transport. Unwired: metadata never grants native authority.

The native launcher must own the original pre-hello deadline, kill/reap policy,
and every endpoint copy. This module supplies no launcher or default checker.
"""
import fcntl
import os
import re
import selectors
import socket
import stat
import struct
import threading

from csl_backup_core import canonical
from csl_canonical import strict_json
from csl_execution import ExecutionError, MAX_MESSAGE, linux_clock, request

CONTROL_LIMIT = 1024
QUANTUM = .05


class TransportError(ExecutionError):
    pass


class Denied(TransportError):
    """Exact bounded parent denial; sticky for this channel."""
    def __init__(self, reason):
        self.reason = reason
        super().__init__('execution_parent_denied:' + reason)


class Channel:
    """Consumes the sole child control endpoint, closes it on exit.

    req is the separately supplied launch commitment, NOT proof of authority.
    Parent must answer each check using its real, original, live authority.
    The req's canonical bytes bind this attempt; immutable import identity stays
    in csl_execution's journal, independently of this channel and its sequence.
    """
    def __init__(self, fd, req, *, parent_pid, parent_uid, clock=linux_clock):
        self.sock = None
        self.failure = None
        self._lock = threading.Lock()
        self.req = request(req.encoded)
        self.bound = self.req.value['authority']
        self.clock = clock
        self.last = int(self.bound['start_boottime_ns'])
        self.end = int(self.bound['end_boottime_ns'])
        self.seq = 0
        self.admitted = False
        try:
            self.sock = socket.socket(fileno=fd)
            if (self.sock.family != socket.AF_UNIX or
                    self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != socket.SOCK_STREAM):
                raise TransportError('execution_control_type')
            pid, uid, _ = struct.unpack('3i', self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
            if (type(parent_pid) is not int or type(parent_uid) is not int or
                    pid != parent_pid or pid != os.getppid() or uid != parent_uid):
                raise TransportError('execution_control_peer')
            self.sock.set_inheritable(False)
            self.sock.setblocking(False)
            self._local()
        except BaseException:
            if self.sock is None:
                os.close(fd)
            else:
                self.close()
            raise

    def close(self):
        if self.sock is not None:
            self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.close()

    def _local(self):
        boot, now = self.clock()
        if (type(now) is not int or boot != self.bound['boot_id'] or
                now < self.last or now >= self.end):
            raise TransportError('execution_original_bound_lost')
        self.last = now
        return (self.end - now) / 1_000_000_000

    @staticmethod
    def _decode(raw):
        try:
            value = strict_json(raw)
            if not isinstance(value, dict):
                raise ValueError()
            return value
        except (ValueError, UnicodeError):
            raise TransportError('execution_control_invalid') from None

    @staticmethod
    def _denial(value):
        if value.get('type') == 'deny':
            reason = value.get('reason')
            if set(value) != {'type', 'reason'} or not isinstance(reason, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', reason):
                raise TransportError('execution_control_invalid')
            raise Denied(reason)

    def _idle(self):
        """Any queued byte, including a partial denial, fences dispatch now."""
        try:
            raw = self.sock.recv(CONTROL_LIMIT + 5)
        except BlockingIOError:
            return
        if not raw:
            raise TransportError('execution_parent_eof')
        if len(raw) >= 4:
            n = struct.unpack('!I', raw[:4])[0]
            if 0 < n <= CONTROL_LIMIT and len(raw) == n + 4:
                self._denial(self._decode(raw[4:]))
        raise TransportError('execution_control_unsolicited_or_partial')

    def _wait(self, fd, event, *, control=False):
        if not control:
            self._idle()
        self._local()
        with selectors.DefaultSelector() as selector:
            selector.register(fd, event | selectors.EVENT_READ if fd == self.sock.fileno() else event)
            if fd != self.sock.fileno():
                selector.register(self.sock, selectors.EVENT_READ)
            ready = selector.select(min(self._local(), QUANTUM))
        # Pending authoritative denial is diagnosed before clock expiry.
        if not control:
            self._idle()
        available = any(key.fd == fd and mask & event for key, mask in ready)
        if not control or not available:
            self._local()
        return available

    def _receive(self):
        raw = bytearray()
        need = 4
        while len(raw) < need:
            try:
                part = self.sock.recv(need - len(raw))
            except BlockingIOError:
                self._wait(self.sock.fileno(), selectors.EVENT_READ, control=True)
                continue
            if not part:
                raise TransportError('execution_parent_eof')
            raw.extend(part)
            if len(raw) == 4 and need == 4:
                n = struct.unpack('!I', raw)[0]
                if not 0 < n <= CONTROL_LIMIT:
                    raise TransportError('execution_control_limit')
                need += n
            if len(raw) < need:
                self._local()
        value = self._decode(bytes(raw[4:]))
        self._denial(value)
        self._idle()
        self._local()
        return value

    def _send(self, value):
        raw = canonical(value)
        if not 0 < len(raw) <= CONTROL_LIMIT:
            raise TransportError('execution_control_limit')
        frame = struct.pack('!I', len(raw)) + raw
        offset = 0
        while offset < len(frame):
            self._idle()
            self._local()
            try:
                n = self.sock.send(frame[offset:])
            except BlockingIOError:
                self._wait(self.sock.fileno(), selectors.EVENT_WRITE)
                continue
            if not n:
                raise TransportError('execution_parent_eof')
            offset += n

    def _run(self, action):
        if self.failure is not None:
            raise self.failure
        if not self._lock.acquire(blocking=False):
            raise TransportError('execution_control_concurrent')
        try:
            if self.failure is not None:
                raise self.failure
            return action()
        except BaseException as exc:
            self.failure = exc
            raise
        finally:
            self._lock.release()

    def admit(self):
        def action():
            if self.admitted:
                raise TransportError('execution_hello_replayed')
            value = self._receive()
            if value != {'type': 'hello', 'request_sha256': self.req.commitment, 'authority': self.bound}:
                raise TransportError('execution_hello_mismatch')
            self.admitted = True
        return self._run(action)

    def _check(self, phase):
        if not self.admitted or not isinstance(phase, str) or not re.fullmatch(r'[a-z][a-z0-9_-]{0,63}', phase):
            raise TransportError('execution_check_invalid')
        self._idle()
        self._local()
        self.seq += 1
        if self.seq > (1 << 64) - 1:
            raise TransportError('execution_sequence_exhausted')
        self._send({'type': 'check', 'seq': str(self.seq), 'phase': phase})
        value = self._receive()
        if value != {'type': 'ok', 'seq': str(self.seq), 'authority': self.bound}:
            raise TransportError('execution_check_mismatch')
        self._idle()
        self._local()

    def check(self, phase='capture'):
        return self._run(lambda: self._check(phase))

    @staticmethod
    def _pipe(fd, mode):
        if not stat.S_ISFIFO(os.fstat(fd).st_mode) or fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE != mode:
            raise TransportError('execution_pipe_invalid')
        os.set_inheritable(fd, False)
        os.set_blocking(fd, False)

    def read_request(self, fd):
        """Borrow sole FD3 endpoint; require one bounded frame AND writer EOF."""
        def action():
            self._pipe(fd, os.O_RDONLY)
            raw = bytearray()
            need = 4
            while True:
                self._check('request')
                try:
                    part = os.read(fd, min(need - len(raw) + 1, MAX_MESSAGE + 5))
                except BlockingIOError:
                    self._wait(fd, selectors.EVENT_READ)
                    continue
                self._check('request')
                if not part:
                    if len(raw) != need or need == 4:
                        raise TransportError('execution_request_truncated')
                    break
                raw.extend(part)
                if len(raw) >= 4 and need == 4:
                    n = struct.unpack('!I', raw[:4])[0]
                    if not 0 < n <= MAX_MESSAGE:
                        raise TransportError('execution_request_limit')
                    need += n
                if len(raw) > need:
                    raise TransportError('execution_request_trailing')
            parsed = request(bytes(raw[4:]))
            if parsed.commitment != self.req.commitment:
                raise TransportError('execution_request_mismatch')
            return parsed
        return self._run(action)

    def write_result(self, fd, raw):
        """Borrow sole FD4 endpoint; original bound covers full pipe pressure."""
        def action():
            self._pipe(fd, os.O_WRONLY)
            if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_MESSAGE:
                raise TransportError('execution_result_limit')
            frame = struct.pack('!I', len(raw)) + raw
            offset = 0
            while offset < len(frame):
                self._check('result')
                try:
                    n = os.write(fd, frame[offset:])
                except BlockingIOError:
                    self._wait(fd, selectors.EVENT_WRITE)
                    continue
                if not n:
                    raise TransportError('execution_result_closed')
                offset += n
                self._check('result')
        return self._run(action)

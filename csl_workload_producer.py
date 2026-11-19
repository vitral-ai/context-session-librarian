"""Owned producer guardian: lifetime pipe, gated exec and durable stop proof."""
import ctypes
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time
import uuid

from csl_sync_state import SyncError, private_json
from csl_workload_auth import boot_clock, boot_identity
from csl_workload_state import read_record
from csl_canonical import strict_json

STOP_SECONDS = 5
REAP_SECONDS = 0.5


def completed(value, expected_scope):
    if (not isinstance(value, dict) or value.get('scope') != expected_scope or value.get('phase') != 'stopped'
            or type(value.get('version')) is not int or value['version'] != 1
            or type(value.get('writer_exit')) is not int or not -127 <= value['writer_exit'] <= 255): return False
    try:
        return value['run_id'] == str(uuid.UUID(value['run_id'])) and value['boot'] == str(uuid.UUID(value['boot']))
    except (ValueError, TypeError, KeyError, AttributeError): return False


def recover(runtime, expected_scope):
    value = read_record(runtime/'producer.json')
    if value is not None and not completed(value, expected_scope):
        raise SyncError('workload_producer_outcome_unknown')


def process_start(pid, parent):
    """Read a Linux process identity without exposing argv or environment."""
    try:
        data = Path('/proc') / str(pid) / 'stat'
        fields = data.read_text().rsplit(')', 1)[1].split()
        if int(fields[1]) == parent:
            start = int(fields[19])
            if start > 0:
                return start
    except (OSError, ValueError, IndexError):
        pass
    return None


class Producer:
    def __init__(self, runtime, expected_scope, command, *, capture_launch=False):
        recover(runtime, expected_scope)
        self.runtime, self.expected_scope = runtime, expected_scope
        self.run_id = str(uuid.uuid4())
        self.returncode = None
        # A missing guardian/launch Ack is never permission to retry the command.
        self.boot = boot_identity()
        private_json(runtime/'producer.json', {'version': 1, 'scope': expected_scope,
            'run_id': self.run_id, 'boot': self.boot, 'phase': 'launching'})
        self.launch_fd, launch_write = None, None
        if capture_launch:
            try: self.launch_fd, launch_write = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
            except OSError: pass
        self.cached_launch = None
        read_fd, self.control = os.pipe()
        try:
            carrier = [] if launch_write is None else ['--launch-carrier', str(launch_write)]
            self.process = subprocess.Popen([sys.executable, '-S', str(Path(__file__).absolute()),
                str(runtime), self.run_id, str(os.getpid()), str(read_fd), *carrier, '--', *command],
                pass_fds=(read_fd,) if launch_write is None else (read_fd, launch_write), start_new_session=True, stdin=None,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except BaseException:
            os.close(self.control); self.control = None
            self.close_launch()
            raise
        finally:
            os.close(read_fd)
            if launch_write is not None:
                try: os.close(launch_write)
                except OSError: pass

    def close_launch(self):
        if self.launch_fd is not None:
            try: os.close(self.launch_fd)
            except OSError: pass
            self.launch_fd = None

    def poll_launch_identity(self):
        """Optional bounded anonymous-pipe read, never producer.json or /proc."""
        if self.launch_fd is None: return self.cached_launch
        try:
            raw = os.read(self.launch_fd, 4097)
        except BlockingIOError: return None
        except OSError:
            self.close_launch(); return None
        self.close_launch()
        try:
            value = strict_json(raw)
            keys = {'run_id', 'boot', 'guardian_pid', 'guardian_start', 'producer_pid', 'producer_start'}
            if (len(raw) > 4096 or type(value) is not dict or set(value) != keys
                    or value['run_id'] != self.run_id or value['boot'] != self.boot
                    or value['guardian_pid'] != self.process.pid
                    or any(type(value[k]) is not int or value[k] <= 0 for k in keys-{'run_id', 'boot'})
                    or value['guardian_pid'] == value['producer_pid']): return None
            self.cached_launch = value
        except (ValueError, TypeError, RecursionError): pass
        return self.cached_launch

    def launch_identity(self):
        """Return optional pre-gate facts from this guardian's original run."""
        value = read_record(self.runtime/'producer.json')
        if (not value or value.get('run_id') != self.run_id
                or value.get('scope') != self.expected_scope
                or value.get('boot') != boot_identity()
                or value.get('guardian_pid') != self.process.pid):
            return None
        keys = ('guardian_pid', 'guardian_start', 'producer_pid', 'producer_start')
        if any(type(value.get(key)) is not int or value[key] <= 0 for key in keys):
            return None
        return {key: value[key] for key in ('run_id', 'boot', *keys)}

    def _proof(self):
        value = read_record(self.runtime/'producer.json')
        return value if completed(value, self.expected_scope) and value['run_id'] == self.run_id else None

    def poll(self):
        code = self.process.poll()
        if code is None: return None
        value = self._proof()
        self.returncode = value.get('writer_exit') if value else None
        return self.returncode if self.returncode is not None else 2

    def stop(self):
        if self.control is not None:
            os.close(self.control); self.control = None

    def wait(self, timeout):
        self.process.wait(timeout=timeout)
        self.poll()
        return self.returncode

    def quiesced(self): return self.process.poll() == 0 and self._proof() is not None


def stopped(control):
    return bool(select.select([control], [], [], 0)[0])


def group_signal(pid, value):
    try: os.killpg(pid, value)
    except ProcessLookupError: pass


def group_present(pid):
    try: os.killpg(pid, 0)
    except ProcessLookupError: return False
    return True


def adopted_gone():
    while True:
        try: pid, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError: return True
        if pid == 0: return False


def producer_exec(parent, gate, command):
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != parent:
        raise SyncError('workload_parent_unavailable')
    try: allowed = os.read(gate, 1) == b'G'
    finally: os.close(gate)
    if not allowed or os.getppid() != parent: raise SyncError('workload_parent_unavailable')
    os.execvp(command[0], command)


def guardian(runtime, run_id, parent, control, command, launch_fd=None):
    record = read_record(runtime/'producer.json')
    if (not record or record.get('phase') != 'launching' or record.get('run_id') != run_id
            or record.get('boot') != boot_identity()): raise SyncError('workload_producer_outcome_unknown')
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0: raise SyncError('workload_parent_unavailable')
    writer = None
    stop_requested = [False]
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop_requested.__setitem__(0, True))
    try:
        if not stopped(control) and os.getppid() == parent:
            read_gate, write_gate = os.pipe()
            try:
                writer = subprocess.Popen([sys.executable, '-S', str(Path(__file__).absolute()),
                    '--exec', str(os.getpid()), str(read_gate), '--', *command],
                    pass_fds=(read_gate,), start_new_session=True, stdin=None,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                # The child cannot execute user argv until this identity is durable.
                record.update(phase='running', guardian_pid=os.getpid(), producer_pid=writer.pid)
                guardian_start = process_start(os.getpid(), parent)
                producer_start = process_start(writer.pid, os.getpid())
                if guardian_start is not None and producer_start is not None:
                    record.update(guardian_start=guardian_start, producer_start=producer_start)
                private_json(runtime/'producer.json', record)
                if launch_fd is not None:
                    try:
                        keys = ('run_id', 'boot', 'guardian_pid', 'guardian_start', 'producer_pid', 'producer_start')
                        data = json.dumps({k: record[k] for k in keys}, separators=(',', ':')).encode()
                        if len(data) <= 4096: os.write(launch_fd, data)
                    except (OSError, KeyError): pass
                    finally: os.close(launch_fd); launch_fd = None
                if not stopped(control) and not stop_requested[0] and os.getppid() == parent:
                    os.write(write_gate, b'G')
            finally:
                os.close(read_gate); os.close(write_gate)
            while not stopped(control) and not stop_requested[0]:
                # Keep the root PID unreaped until group signals are finished.
                if os.waitid(os.P_PID, writer.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None: break
                time.sleep(.02)
            group_signal(writer.pid, signal.SIGTERM)
            deadline = boot_clock()+STOP_SECONDS
            while boot_clock() < deadline:
                if os.waitid(os.P_PID, writer.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None: break
                time.sleep(.02)
            group_signal(writer.pid, signal.SIGKILL)
            writer.wait(timeout=REAP_SECONDS)
            deadline = boot_clock()+REAP_SECONDS
            while not adopted_gone() and boot_clock() < deadline: time.sleep(.01)
            if not adopted_gone() or group_present(writer.pid): return 2
        record.update(phase='stopped', writer_exit=writer.returncode if writer else 143)
        private_json(runtime/'producer.json', record)
        return 0
    finally:
        if launch_fd is not None: os.close(launch_fd)
        if writer is not None and writer.returncode is None:
            group_signal(writer.pid, signal.SIGKILL)
            try: writer.wait(timeout=REAP_SECONDS)
            except subprocess.TimeoutExpired: pass
        os.close(control)


def main():
    args = sys.argv[1:]
    try:
        if len(args) >= 5 and args[0] == '--exec' and args[3] == '--':
            producer_exec(int(args[1]), int(args[2]), args[4:])
        elif len(args) >= 8 and args[4] == '--launch-carrier' and args[6] == '--':
            return guardian(Path(args[0]), args[1], int(args[2]), int(args[3]), args[7:], int(args[5]))
        elif len(args) >= 6 and args[4] == '--':
            return guardian(Path(args[0]), args[1], int(args[2]), int(args[3]), args[5:])
    except Exception: pass
    return 2


if __name__ == '__main__': raise SystemExit(main())

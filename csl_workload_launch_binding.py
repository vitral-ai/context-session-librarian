"""Private fresh-Codex launch binding. No delivery, consent or stop authority."""
import array
import hashlib
import math
import os
from pathlib import PurePosixPath
import socket
import stat
import threading
import time
import uuid

from csl_workload_auth import boot_clock
from csl_canonical import strict_json
from csl_workload_identity import canonical
from csl_workload_observation import identity
from csl_workload_observation_agent import (Channel, LAUNCH_HANDSHAKE_SECONDS, RemoteObservation,
                                            wait_readable)
from csl_workload_observation_driver import AcceptanceDriver, StoredDriver
from csl_workload_observation_journal import sha

LIMIT = 4096
PROFILE = 'codex-0.169.0-text-v1'
KERNEL = {'run_id', 'boot', 'guardian_pid', 'guardian_start', 'producer_pid', 'producer_start'}
ASSIGNMENT_FACTS = ('worker', 'seal', 'launch', 'run', 'source', 'scope', 'binding')


def kernel(value):
    if type(value) is not dict or set(value) != KERNEL: raise ValueError()
    identity(value['run_id']); identity(value['boot'])
    if (any(type(value[k]) is not int or not 0 < value[k] < 2**63 for k in KERNEL-{'run_id', 'boot'})
            or value['guardian_pid'] == value['producer_pid']): raise ValueError()
    return dict(value)


def send(sock, value, fds=()):
    data = canonical(value)
    if len(data) > LIMIT or len(fds) > 2: raise ValueError()
    ancillary = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', fds))] if fds else []
    if sock.sendmsg([data], ancillary, socket.MSG_DONTWAIT) != len(data): raise ValueError()


def receive(sock):
    try:
        raw, ancillary, flags, _ = sock.recvmsg(LIMIT, socket.CMSG_SPACE(8),
            socket.MSG_DONTWAIT | socket.MSG_CMSG_CLOEXEC)
    except BlockingIOError: return None
    descriptors = []
    try:
        for level, kind, data in ancillary:
            if (level, kind) != (socket.SOL_SOCKET, socket.SCM_RIGHTS): raise ValueError()
            values = array.array('i'); values.frombytes(data)
            descriptors.extend(values)
        if not raw or flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC) or len(descriptors) > 2: raise ValueError()
        value = strict_json(raw)
        if type(value) is not dict: raise ValueError()
        return value, descriptors
    except Exception:
        for fd in descriptors: os.close(fd)
        raise


class PreparedLaunch:
    """Root lease supplied ONLY by actual empty dual-view preparation validation."""
    def __init__(self):
        self.fd, self.views, self.armed, self.closed, self.failed = None, None, False, False, False
        self.command = None

    def capture(self, fd, views):
        if (self.closed or type(views) is not tuple or len(views) != 2
                or any(type(v) is not tuple or len(v) != 2
                       or any(type(n) is not int or not 0 <= n < 2**64 for n in v)
                       or v[1] == 0 for v in views) or views[0] != views[1]): raise ValueError()
        if self.views is not None and self.views != views: raise ValueError()
        if self.fd is None:
            try: self.fd, self.views = os.dup(fd), views
            except OSError: self.failed = True

    def handoff(self, views, command):
        if self.failed or self.fd is None: return False
        if self.closed or views != self.views or self.armed: raise ValueError()
        if type(command) not in (tuple, list) or not command or any(type(v) is not str for v in command): raise ValueError()
        self.command = tuple(command)
        self.armed = True
        return True

    def close(self):
        self.closed = True
        if self.fd is not None:
            try: os.close(self.fd)
            except OSError: pass
            self.fd = None


class LaunchObservation(Channel):
    """Real worker admission and deadline-bounded grant, never a native-row fallback."""
    def __init__(self, config, assignment, binding, expected_worker):
        if (set(assignment) != {'mode', 'worker', 'seal', 'deadline', 'launch', 'run', 'source', 'scope', 'binding', 'observer_fd'}
                or assignment['mode'] != 'fresh-codex' or assignment['worker'] != expected_worker
                or config.observe_session is not None or config.provider != 'codex' or config.profile != PROFILE
                or assignment['scope'] != binding['scope']
                or assignment['binding'] != hashlib.sha256(canonical({k: binding[k] for k in ('account_id', 'device_id')})).hexdigest()):
            raise ValueError()
        for name in ('worker', 'seal', 'launch', 'run'): identity(assignment[name])
        sha(assignment['source'])
        if type(assignment['deadline']) not in (int, float) or not math.isfinite(assignment['deadline']): raise ValueError()
        super().__init__(socket.socket(fileno=os.dup(assignment['observer_fd'])))
        self.config, self.assignment, self.binding = config, assignment, binding
        self.worker, self.deadline = expected_worker, assignment['deadline']
        self.roots, self.failed, self.birth, self.remote = [], False, None, None
        self.declared = False
        self.session = None

    def root(self, fd):
        # Duplication/IPC only; descriptor metadata checks run off product control.
        try:
            if self.failed: return
            if len(self.roots) == 2: raise ValueError()
            self.roots.append(os.dup(fd))
        except Exception: self.failed = True

    def close_roots(self):
        for fd in self.roots:
            try: os.close(fd)
            except OSError: pass
        self.roots.clear()

    def header(self, records):
        # Called only AFTER the actual full pinned-profile projection succeeds.
        try:
            value = records[0]['payload']
            if (self.declared or records[0]['type'] != 'session_meta'
                    or value.get('cli_version') != '0.169.0' or value.get('source') != 'exec'): raise ValueError()
            self.declared = True
        except Exception: self.failed = True

    def no_source(self):
        # Only the actual empty-source return, before any Collector/registry call.
        if (self.failed or self.birth is not None or self.remote is not None or self.roots
                or self.declared or boot_clock() >= self.deadline): return {}
        notice = {'event': 'no_source', 'session_puts': 0,
                  **{k: self.assignment[k] for k in ASSIGNMENT_FACTS}}
        send(self.sock, notice)
        return {'native_no_source': notice}

    def admitted(self, adapter, accepted):
        try:
            if (self.failed or not self.declared or self.birth is not None or boot_clock() >= self.deadline
                    or adapter.provider != 'codex' or len(adapter.sources) != 1
                    or len(adapter.prepared) != 1 or len(accepted) != 1 or len(self.roots) != 2): raise ValueError()
            metadata, = accepted
            session = identity(metadata['native_id'])
            entry = adapter.prepared[session]
            root, relative = adapter.sources[0]
            cursor = entry['source_cursor']
            if (root != self.config.transcript_root or entry['metadata'] != metadata
                    or type(relative) is not str or len(relative.encode()) > 1024
                    or cursor['partial'] is not False or type(cursor['complete_length']) is not int
                    or cursor['complete_length'] <= 0
                    or any(type(cursor[k]) is not int or cursor[k] < 0 for k in ('device', 'inode'))
                    or cursor['inode'] == 0): raise ValueError()
            sha(cursor['sha256']); sha(entry['policy_sha256'])
            self.birth = {'session': session, 'relative': relative, 'device': cursor['device'],
                          'inode': cursor['inode'], 'policy': entry['policy_sha256']}
            send(self.sock, {'event': 'admitted', **{k: self.assignment[k] for k in
                ('worker', 'seal', 'launch', 'run', 'source', 'scope', 'binding')}, 'birth': self.birth}, self.roots)
            if boot_clock() >= self.deadline: raise ValueError()
            self.session = session
        except Exception: self.failed = True
        finally: self.close_roots()

    def grant(self):
        answer = receive(self.sock)
        if answer is None:
            until = min(self.deadline, boot_clock() + LAUNCH_HANDSHAKE_SECONDS)
            if not wait_readable(self.sock, until): raise ValueError()
            answer = receive(self.sock)
        if answer is None: raise ValueError()
        message, fds = answer
        try:
            if (set(message) != {'event', 'launch', 'birth', 'assignment', 'kernel'} or message['event'] != 'grant'
                    or message['launch'] != self.assignment['launch'] or message['birth'] != self.birth
                    or len(fds) != 1): raise ValueError()
            assignment = message['assignment']
            self.kernel = kernel(message['kernel'])
            if (assignment['worker'] != self.worker or assignment['seal'] != self.assignment['seal']
                    or assignment['deadline'] != self.deadline
                    or assignment['context']['run'] != self.assignment['run']
                    or assignment['context']['source'] != self.assignment['source']): raise ValueError()
            self.remote = RemoteObservation(self.config, {**assignment, 'observer_fd': fds[0]},
                self.binding, self.worker, selected_session=self.session, wait_for_prepared=True)
            if boot_clock() >= self.deadline: raise ValueError()
        finally:
            for fd in fds: os.close(fd)

    def event(self, name, *args):
        if self.failed: return
        try:
            # Device registration/auth are outside this observer, before admission.
            if name == 'begin' and args[0] == 'PUT' and args[1].startswith('/sessions/'):
                if self.session is None: raise ValueError()
                if self.remote is None: self.grant()
            if self.remote is not None: self.remote.event(name, *args)
        except Exception: self.failed = True

    def finalize(self):
        try:
            if self.failed or self.remote is None: return {}
            return {**self.remote.finalize(), 'native_birth': self.birth, 'native_launch': self.assignment['launch'],
                    'native_kernel': self.kernel}
        finally: self.close()

    def close(self):
        self.close_roots()
        self.sock.close()
        if self.remote is not None and self.remote.sock.fileno() >= 0: self.remote.sock.close()


class LaunchAcceptanceDriver(AcceptanceDriver):
    """Private parent selection from actual worker admission, not helper receipts."""
    def __init__(self, *, source, scope, account_id, device_id, prepared):
        if not isinstance(prepared, PreparedLaunch): raise TypeError()
        StoredDriver._initialize(self, source, scope, account_id, device_id, None)
        self._facade({'source': source, 'scope': scope, 'account_id': account_id,
                      'device_id': device_id, 'session': None})
        self.prepared, self.launch = prepared, str(uuid.uuid4())
        self.writer = self.kernel = self.birth = self.binder = None
        self.closed, self.launch_used, self.launch_prepared, self.poisoned = False, False, False, False
        self.parent_sock = self.worker_sock = None
        self.empty_notice = self.empty_receipt = None

    def prepare(self, config, expected_scope):
        try: return self._prepare(config, expected_scope)
        except Exception: self.invalidate(); return False

    def _prepare(self, config, expected_scope):
        if (self.closed or self.poisoned or config.observe_session is not None or config.provider != 'codex'
                or config.profile != PROFILE or expected_scope != self.context['scope']
                or not self.prepared.armed or self.prepared.closed or self.prepared.failed):
            self.invalidate(); return False
        self.config = config
        if self.agent is None:
            from csl_workload_observation_agent import Agent
            from csl_workload_runner import environment
            self.agent = Agent({'configuration': environment(config), 'expected': dict(self.expected),
                                'run': self.run, 'native_launch': self.launch})
        self.valid = True
        return True

    def message(self, message):
        if message.get('event') == 'launch_prepared':
            if message.get('launch') != self.launch or self.launch_prepared: raise ValueError()
            self.launch_prepared = True
        else: super().message(message)

    def start(self, config, expected_scope):
        if self.launch_used: self.invalidate(); return
        prepared = self.prepare(config, expected_scope)
        self.launch_used = True
        self.valid = prepared and not self.closed

    def launched(self, writer):
        from csl_workload_producer import Producer
        if (self.writer is not None or not isinstance(writer, Producer)
                or writer.expected_scope != self.context['scope']): self.invalidate(); return
        self.writer = writer
        self.original_run, self.original_guardian, self.original_boot = writer.run_id, writer.process.pid, writer.boot

    def command_matches(self, command):
        if self.prepared.command is None or tuple(command or ()) != self.prepared.command:
            self.invalidate(); return False
        return self.valid

    def current(self, sequence, seal, deadline):
        return (self.valid and not self.closed and not self.prepared.closed
            and self.expected_worker == sequence and self.expected_seal == seal
            and boot_clock() < deadline and self.writer is not None
            and self.writer.run_id == self.original_run and self.writer.process.pid == self.original_guardian
            and self.writer.boot == self.original_boot)

    def assign(self, sequence, deadline):
        try: return self._assign(sequence, deadline)
        except Exception:
            self.spawned()
            if self.parent_sock is not None: self.parent_sock.close()
            self.invalidate(); return None

    def _assign(self, sequence, deadline):
        self.clear_result()
        if (not self.valid or self.writer is None or self.binder is not None and self.binder.is_alive()):
            self.invalidate(); return None
        if self.agent is not None:
            self.poll()
            if not self.valid or self.awaiting_retention: self.invalidate(); return None
        if self.expected_worker is not None and self.birth is None:
            previous = (self.expected_worker, self.expected_seal)
            if self.empty_notice != previous or self.empty_receipt != previous:
                self.invalidate(); return None
        self.expected_worker, self.expected_seal, self.deadline = identity(sequence), str(uuid.uuid4()), deadline
        self.empty_notice = self.empty_receipt = None
        self.finals.clear()
        self.parent_sock, self.worker_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.parent_sock.setblocking(False); self.worker_sock.setblocking(False)
        parent, worker = self.parent_sock, self.worker_sock
        seal = self.expected_seal
        self.binder = threading.Thread(target=self.bind, args=(parent, sequence, seal, deadline),
                                       name='csl-launch-binding', daemon=True)
        self.binder.start()
        return {'mode': 'fresh-codex', 'worker': sequence, 'seal': seal, 'deadline': deadline,
                'launch': self.launch, 'run': self.run, 'source': self.context['source'],
                'scope': self.context['scope'], 'binding': self.context['binding'],
                'observer_fd': worker.fileno()}

    def spawned(self):
        if self.worker_sock is not None: self.worker_sock.close(); self.worker_sock = None

    def bind(self, channel, sequence, seal, deadline):
        descriptors = []
        empty = False
        try:
            proposal = None
            while self.current(sequence, seal, deadline):
                if self.kernel is None:
                    value = self.writer.poll_launch_identity()
                    if value is not None:
                        value = kernel(value)
                        if (value['run_id'] != self.original_run or value['boot'] != self.original_boot
                                or value['guardian_pid'] != self.original_guardian): raise ValueError()
                        self.kernel = value
                if proposal is None:
                    proposal = receive(channel)
                    if proposal is not None: descriptors = proposal[1]
                if proposal is not None and (self.kernel is not None or proposal[0].get('event') == 'no_source'): break
                time.sleep(.001)  # Off-control binder only, never a product wait.
            if proposal is None or not self.current(sequence, seal, deadline): raise ValueError()
            message, descriptors = proposal
            expected = {'event': 'admitted', 'worker': sequence, 'seal': seal, 'launch': self.launch,
                        'run': self.run, 'source': self.context['source'],
                        'scope': self.context['scope'], 'binding': self.context['binding']}
            if message.get('event') == 'no_source':
                expected.update(event='no_source', session_puts=0)
                if (self.birth is not None or self.started or self.expected_plan is not None or descriptors
                        or canonical(message) != canonical(expected)): raise ValueError()
                self.empty_notice = (sequence, seal)
                empty = True
                return
            if set(message) != set(expected)|{'birth'} or any(message[k] != v for k, v in expected.items()): raise ValueError()
            birth = message['birth']
            if (type(birth) is not dict or set(birth) != {'session', 'relative', 'device', 'inode', 'policy'}
                    or type(birth['relative']) is not str or not 0 < len(birth['relative'].encode()) <= 1024
                    or any(type(birth[k]) is not int or birth[k] < 0 for k in ('device', 'inode'))
                    or birth['inode'] == 0 or len(descriptors) != 2): raise ValueError()
            identity(birth['session']); sha(birth['policy'])
            path = PurePosixPath(birth['relative'])
            if path.is_absolute() or '..' in path.parts or str(path) != birth['relative'] or '\\' in birth['relative']:
                raise ValueError()
            for fd in (self.prepared.fd, *descriptors):
                info = os.fstat(fd)
                if not stat.S_ISDIR(info.st_mode) or (info.st_dev, info.st_ino) != self.prepared.views[0]: raise ValueError()
            if self.birth is not None and self.birth != birth: raise ValueError()
            if not self.current(sequence, seal, deadline): raise ValueError()
            if self.birth is None:
                self.birth = dict(birth)
                self.context['session'] = birth['session']
                self.expected['session'] = birth['session']
                while self.current(sequence, seal, deadline) and not self.launch_prepared:
                    self.poll(); time.sleep(.001)
                if not self.current(sequence, seal, deadline): raise ValueError()
                self.agent.send({'op': 'bind_launch', 'launch': self.launch, 'session': birth['session'], 'deadline': deadline})
                while self.current(sequence, seal, deadline) and not self.started:
                    self.poll(); time.sleep(.001)
            if not self.current(sequence, seal, deadline) or not self.started: raise ValueError()
            assignment = {'context': dict(self.context), 'worker': sequence, 'seal': seal,
                          'deadline': deadline, 'predecessor': self.anchor}
            self.agent.send({'op': 'assign', 'assignment': assignment})
            send(channel, {'event': 'grant', 'launch': self.launch, 'birth': self.birth, 'kernel': self.kernel,
                           'assignment': assignment}, (self.agent.sock.fileno(),))
            if not self.current(sequence, seal, deadline): self.invalidate()
        except Exception: self.invalidate()
        finally:
            if not empty or self.kernel is not None: self.writer.close_launch()
            for fd in descriptors: os.close(fd)
            channel.close()
            if self.closed and self.agent is not None: self.agent.close()

    def retain(self, receipt, sequence, *, now):
        if type(receipt) is dict and 'native_no_source' in receipt:
            try:
                expected = {'event': 'no_source', 'session_puts': 0, 'worker': sequence,
                    'seal': self.expected_seal, 'launch': self.launch, 'run': self.run,
                    'source': self.context['source'], 'scope': self.context['scope'],
                    'binding': self.context['binding']}
                if (not self.current(sequence, self.expected_seal, self.deadline)
                        or max(now, boot_clock()) >= self.deadline or self.birth is not None
                        or self.started or self.expected_plan is not None
                        or set(receipt) != {'sequence', 'scope', 'ok', 'synced', 'pending', 'manifest', 'native_no_source'}
                        or receipt['sequence'] != sequence or receipt['scope'] != self.context['scope']
                        or receipt['ok'] is not False or type(receipt['synced']) is not int or receipt['synced'] != 0
                        or type(receipt['pending']) is not int or receipt['pending'] != 1
                        or receipt['manifest'] != []
                        or canonical(receipt['native_no_source']) != canonical(expected)): raise ValueError()
                self.clear_result()
                self.empty_receipt = (sequence, self.expected_seal)
            except Exception: self.invalidate()
            return
        super().retain(receipt, sequence, now=now)

    def combined(self, receipt, sequence, now):
        if (self.birth is None or type(receipt) is not dict or receipt.get('native_birth') != self.birth
                or receipt.get('native_launch') != self.launch or receipt.get('native_kernel') != self.kernel): return None
        return super().combined(receipt, sequence, now)

    def close(self):
        self.closed = True
        self.prepared.close()
        self.spawned()
        if self.parent_sock is not None: self.parent_sock.close()
        if self.writer is not None and (self.binder is None or not self.binder.is_alive()): self.writer.close_launch()
        super().close()

    def invalidate(self):
        self.poisoned = True
        super().invalidate()

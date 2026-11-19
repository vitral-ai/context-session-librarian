"""Private bounded evidence IPC. Storage lives only in a killable helper."""
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import uuid

from csl_workload_auth import boot_clock
from csl_workload_identity import canonical
from csl_workload_observation import identity

MAX_PACKET = 96*1024
MAX_BODY = 48*1024
MAX_EVENTS = 8192


def encode(value):
    if type(value) is bytes:
        if len(value) > MAX_BODY: raise ValueError()
        return {'bytes': base64.b64encode(value).decode('ascii')}
    return value


def decode(value):
    if type(value) is dict and set(value) == {'bytes'}:
        raw = base64.b64decode(value['bytes'], validate=True)
        if len(raw) > MAX_BODY: raise ValueError()
        return raw
    return value


class Channel:
    def __init__(self, sock):
        self.sock, self.events = sock, 0
        sock.setblocking(False)

    def send(self, value):
        self.events += 1
        if self.events > MAX_EVENTS: raise ValueError()
        raw = bytearray()
        for part in json.JSONEncoder(sort_keys=True, separators=(',', ':'), ensure_ascii=False).iterencode(value):
            data = part.encode()
            if len(raw)+len(data) > MAX_PACKET: raise ValueError()
            raw.extend(data)
        if self.sock.send(raw, socket.MSG_DONTWAIT) != len(raw): raise ValueError()

    def poll(self):
        result = []
        for _ in range(32):
            try: raw, _, flags, _ = self.sock.recvmsg(MAX_PACKET, 0, socket.MSG_DONTWAIT)
            except BlockingIOError: break
            if not raw or flags & socket.MSG_TRUNC: raise ValueError()
            value = json.loads(raw)
            if type(value) is not dict: raise ValueError()
            result.append(value)
        return result


class AgentProcess(Channel):
    def __init__(self, payload):
        client, server = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        super().__init__(client)
        try:
            self.process = subprocess.Popen(self.command(server.fileno()), pass_fds=(server.fileno(),),
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True, env={'PATH': os.defpath, 'LANG': 'C.UTF-8', 'PYTHONDONTWRITEBYTECODE': '1'})
            self.send({'op': 'start', **payload})
        except BaseException:
            if hasattr(self, 'process'): self.close()
            else: client.close()
            raise
        finally: server.close()

    def command(self, fd):
        return [sys.executable, '-B', '-S', str(Path(__file__).absolute()), str(fd), str(os.getpid())]

    def close(self):
        if self.sock.fileno() < 0: return
        self.sock.close()
        if self.process.poll() is None:
            try: self.process.kill()
            except ProcessLookupError: pass
        # Never join/reap-wait on the producer/request/receipt path. The sole
        # owned helper is killed first; a daemon reaps it independently.
        threading.Thread(target=self.process.wait, daemon=True).start()


class Agent:
    """Even helper exec/startup runs outside the parent's stop/control thread."""
    def __init__(self, payload):
        self.instance = None
        self.failed = False
        self.cancelled = threading.Event()
        def launch():
            try:
                instance = AgentProcess(payload)
                self.instance = instance
                if self.cancelled.is_set(): instance.close()
            except Exception: self.failed = True
        self.launcher = threading.Thread(target=launch, daemon=True)
        self.launcher.start()

    @property
    def sock(self): return self.instance.sock

    @property
    def process(self): return self.instance.process

    def poll(self):
        if self.failed: raise ValueError()
        return [] if self.instance is None else self.instance.poll()

    def send(self, value):
        if self.instance is None or self.cancelled.is_set(): raise ValueError()
        self.instance.send(value)

    def close(self):
        self.cancelled.set()
        if self.instance is not None: self.instance.close()


class RemoteObservation(Channel):
    def __init__(self, config, assignment, binding, expected_worker):
        from csl_workload_http import Origin
        context = assignment['context']
        if (assignment['worker'] != expected_worker or context['scope'] != binding['scope']
                or context['session'] != config.observe_session
                or context['binding'] != hashlib.sha256(canonical({k: binding[k] for k in ('account_id', 'device_id')})).hexdigest()):
            raise ValueError()
        super().__init__(socket.socket(fileno=os.dup(assignment['observer_fd'])))
        self.worker, self.seal = map(identity, (expected_worker, assignment['seal']))
        self.context, self.deadline = context, assignment['deadline']
        self.path = '/sessions/'+config.provider+'/'+config.observe_session
        self.url = Origin(config.origin).registry+self.path
        self.failed, self.call = False, None
        self.prepared = {}

    def readiness(self):
        for value in self.poll():
            if (value.get('event') == 'prepared' and value.get('worker') == self.worker
                    and value.get('seal') == self.seal): self.prepared[value['call']] = value

    def selected(self, value, prefix):
        return type(value) is str and (value == prefix or value.startswith((prefix+'/', prefix+'?')))

    def event(self, name, *args):
        if self.failed: return
        try:
            if name in ('begin', 'end'):
                if args[0] != 'PUT' or not self.selected(args[1], self.path): return
            elif name == 'request':
                if args[1] != 'PUT' or not self.selected(args[0], self.url): return
            elif not args or args[0] != self.path: return
            if boot_clock() >= self.deadline: raise ValueError()
            if name == 'begin': self.call = str(uuid.uuid4())
            if name == 'request':
                self.readiness()
                ready = self.prepared.pop(self.call, None)
                if (type(args[2]) is not bytes or len(args[2]) > MAX_BODY or ready is None
                        or ready.get('url') != args[0]
                        or ready.get('digest') != hashlib.sha256(args[2]).hexdigest()): raise ValueError()
            self.send({'op': 'event', 'name': name, 'args': [encode(a) for a in args],
                       'worker': self.worker, 'seal': self.seal, 'call': self.call})
            if name == 'end': self.call = None
        except Exception: self.failed = True

    def finalize(self):
        try:
            if self.failed or boot_clock() >= self.deadline: return {}
            ticket = str(uuid.uuid4())
            self.send({'op': 'finalize', 'ticket': ticket, 'worker': self.worker, 'seal': self.seal})
            return {'observation_pending': ticket, 'observation_run': self.context['run'],
                    'observation_seal': self.seal,
                    'observation_context': hashlib.sha256(canonical(self.context)).hexdigest()}
        except Exception: return {}
        finally: self.sock.close()


def serve(fd, parent):
    from csl_workload_runner import parent_death, scope
    from csl_workload_config import Configuration
    from csl_workload_observation_driver import StoredDriver, runtime_fingerprint
    from csl_workload_observation_durable import DurableObservation
    parent_death(parent)
    sock = socket.socket(fileno=fd)
    driver = observation = config = None
    finals = {}
    for _ in range(MAX_EVENTS):
        raw, _, flags, _ = sock.recvmsg(MAX_PACKET)
        if not raw or flags & socket.MSG_TRUNC: return
        message = json.loads(raw)
        op = message['op']
        if op == 'start':
            if driver is not None: return
            config = Configuration.from_environment(message['configuration'])
            driver = StoredDriver(**message['expected'])
            identity(message['run'])
            driver.run = message['run']; driver.context['run'] = message['run']
            driver.start(config, scope(config))
            if not driver.valid: return
            answer = {'event': 'started', 'anchor': driver.anchor}
        elif op == 'assign':
            assignment = message['assignment']
            driver.expected_worker, driver.expected_seal, driver.deadline = assignment['worker'], assignment['seal'], assignment['deadline']
            observation = DurableObservation(config, assignment,
                {'scope': scope(config), 'account_id': driver.account, 'device_id': driver.device},
                runtime_fingerprint(), driver.deadline, driver.expected_worker)
            finals.clear()
            answer = {'event': 'assigned', 'worker': driver.expected_worker, 'seal': driver.expected_seal}
        elif op in ('event', 'finalize', 'retain'):
            if (observation is None or message.get('worker') != driver.expected_worker
                    or message.get('seal') != driver.expected_seal): return
            answer = None
            if op == 'event':
                args = [decode(a) for a in message['args']]
                observation.event(message['name'], *args)
                if observation.failed: return
                if message['name'] == 'begin':
                    answer = {'event': 'prepared', 'worker': driver.expected_worker, 'seal': driver.expected_seal,
                              'call': message['call'], 'url': observation.call['url'], 'digest': observation.call['digest'].hex()}
            elif op == 'finalize':
                if finals: return
                ticket = identity(message['ticket'])
                result = observation.finalize()
                finals[ticket] = result
                answer = {'event': 'final', 'worker': driver.expected_worker, 'seal': driver.expected_seal,
                          'ticket': ticket, 'result': result}
            else:
                result = finals.get(message['ticket'])
                if not result: return
                receipt = {**message['receipt'], **result}
                driver.retain(receipt, driver.expected_worker, now=boot_clock())
                if not driver.valid: return
                answer = {'event': 'retained', 'worker': driver.expected_worker, 'seal': driver.expected_seal,
                          'anchor': driver.anchor, 'plan': driver.expected_plan}
        else: return
        if answer is not None:
            data = canonical(answer)
            if len(data) > MAX_PACKET: return
            sock.sendall(data)


if __name__ == '__main__':
    try: serve(int(sys.argv[1]), int(sys.argv[2]))
    except Exception: pass

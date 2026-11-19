"""Parent-owned private acceptance reader. Never network, delivery or stop authority."""
import hashlib
import json
from pathlib import Path
import re
import uuid

from csl_workload_http import WorkloadHTTP
from csl_workload_auth import WorkloadTokens, boot_clock
from csl_workload_identity import canonical, WorkloadKey
from csl_workload_observation import identity, receipt_valid
from csl_workload_observation_journal import Evidence, sha


def delivery_fact(receipt):
    """Closed product-proof fact, separate from the helper's observation result."""
    from csl_workload_observation_journal import check_plan, PLAN
    value = receipt.get('delivery_plan')
    if type(value) is not dict or set(value) != PLAN-{'body_sha256'}: raise ValueError()
    check_plan({**value, 'body_sha256': '0'*64})
    return dict(value)


def runtime_fingerprint():
    """Bound public runtime contents identically in checkout and assembled image."""
    root = Path(__file__).resolve().parent
    manifest = root/'runtime-manifest.json'
    if manifest.exists():
        with manifest.open('rb') as stream: raw = stream.read(65537)
        if len(raw) > 65536: raise ValueError()
        value = json.loads(raw)
        paths = list(value['sha256'])
        expected = value['sha256']
    else:
        with (root/'packaging/collector-image/runtime.json').open('rb') as stream: raw = stream.read(65537)
        if len(raw) > 65536: raise ValueError()
        value = json.loads(raw)
        paths, expected = value['modules']+value['data'], None
    if len(paths) > 64 or len(set(paths)) != len(paths): raise ValueError()
    actual, total = {}, 0
    for name in paths:
        path = root/name
        if (not isinstance(name, str) or Path(name).is_absolute() or '..' in Path(name).parts
                or path.is_symlink() or path.resolve() != path): raise ValueError()
        with path.open('rb') as source:
            data = source.read(1024*1024+1)
        total += len(data)
        if len(data) > 1024*1024 or total > 8*1024*1024: raise ValueError()
        actual[name] = hashlib.sha256(data).hexdigest()
    if expected is not None and actual != expected: raise ValueError()
    return hashlib.sha256(canonical(actual)).hexdigest()


class StoredDriver:
    """Create once per run using independent expected artifact/owner/session facts.

    result is None unless the current parent consumed a complete final worker
    receipt before cleanup. A non-null detection is evidence, NOT an A8 PASS.
    """
    def __init__(self, *, source, scope, account_id, device_id, session):
        sha(source); sha(scope); identity(session)
        if (type(account_id) is not str or not re.fullmatch('[0-9a-f]{24}', account_id)
                or type(device_id) is not str or not re.fullmatch('ses_[A-Za-z0-9_-]{1,124}', device_id)): raise ValueError()
        self.run = str(uuid.uuid4())
        self.context = {'run': self.run, 'scope': scope, 'source': source, 'session': session,
                        'binding': hashlib.sha256(canonical({'account_id': account_id, 'device_id': device_id})).hexdigest()}
        self.account, self.device = account_id, device_id
        self.valid = False
        self.used = False
        self.expected_worker = self.expected_seal = self.deadline = None
        self._result = None
        self.expected_plan = None

    @property
    def result(self):
        return json.loads(json.dumps(self._result)) if self._result is not None else None

    def clear_result(self): self._result = None

    def start(self, config, expected_scope):
        self._result = None
        if self.used: self.valid = False; return
        self.used = True
        try:
            if (config.observe_session != self.context['session'] or expected_scope != self.context['scope']
                    or runtime_fingerprint() != self.context['source']): raise ValueError()
            http = WorkloadHTTP(config.origin, ca_file=config.ca_file, attempts=1)
            tokens = WorkloadTokens(config.state_root/'auth', config.origin, config.location,
                                   http=http, key=WorkloadKey(config.state_root/'identity'))
            # Atomic validated read only: never take a product auth/sync lock
            # that optional stalled I/O could hold against the real worker.
            state = tokens._read()
            if (state is None or state['phase'] not in ('ready', 'refresh_inflight')
                    or state['account_id'] != self.account or state['device_id'] != self.device): raise ValueError()
            self.evidence = Evidence(config.state_root, self.context, create=True)
            self.anchor = hashlib.sha256(canonical(self.evidence.load())).hexdigest()
            self.config = config
            self.valid = True
        except Exception: self.valid = False

    def assign(self, sequence, deadline):
        # In-memory invalidation precedes ALL I/O or launch; old successful seals
        # cannot be adopted after a disk error or a new/current worker mismatch.
        self._result = None
        self.expected_worker, self.expected_seal, self.deadline = sequence, str(uuid.uuid4()), deadline
        if not self.valid: return None
        try:
            identity(sequence)
            return {'context': dict(self.context), 'worker': sequence,
                    'seal': self.expected_seal, 'deadline': deadline, 'predecessor': self.anchor}
        except Exception: self.valid = False

    def _candidate(self, receipt, sequence, *, complete, now):
        self._result = None
        allowed, self.valid = self.valid, False
        try:
            if (not allowed or not complete or now >= self.deadline or sequence != self.expected_worker
                    or type(receipt) is not dict or receipt.get('sequence') != self.expected_worker
                    or receipt.get('scope') != self.context['scope'] or receipt.get('ok') is not True
                    or type(receipt.get('pending')) is not int or receipt['pending'] != 0
                    or receipt.get('observation_run') != self.run
                    or receipt.get('observation_seal') != self.expected_seal
                    or receipt.get('observation_context') != hashlib.sha256(canonical(self.context)).hexdigest()
                    or self.expected_plan is None or receipt.get('observation_plan') != self.expected_plan
                    or type(receipt.get('synced')) is not int or receipt['synced'] != 1
                    or type(receipt.get('manifest')) is not list or len(receipt['manifest']) != 1
                    or not receipt_valid(receipt.get('observation'))): return
            if any(receipt['observation'][kind]['matched_identity'] != self.expected_plan[name]
                   for kind, name in zip(('snapshot', 'content', 'view'), ('snapshot', 'revision', 'generation'))): return
            return json.loads(json.dumps(receipt['observation']))
        except Exception: self.valid = False

    def consume(self, receipt, sequence, *, complete, now):
        self._result = self._candidate(receipt, sequence, complete=complete, now=now)

    def retain(self, receipt, sequence, *, now):
        """A missing/failed live-worker observation cannot be healed by a later pass."""
        if not self.valid: return
        try:
            if (now >= self.deadline or type(receipt) is not dict
                    or receipt.get('sequence') != sequence or sequence != self.expected_worker
                    or receipt.get('ok') is not True or receipt.get('pending') != 0
                    or receipt.get('observation_run') != self.run
                    or receipt.get('observation_seal') != self.expected_seal
                    or receipt.get('observation_context') != hashlib.sha256(canonical(self.context)).hexdigest()
                    or not receipt_valid(receipt.get('observation'))):
                self.invalidate()
                return
            self.expected_plan = delivery_fact(receipt)
            value = self.evidence.load()
            if (value['worker'] != sequence or value['poisoned'] or value['pending'] is not None
                    or value['final'] != {'worker': sequence, 'seal': self.expected_seal}):
                self.invalidate()
                return
            self.anchor = hashlib.sha256(canonical(value)).hexdigest()
        except Exception: self.invalidate()

    def invalidate(self):
        self.valid = False
        self._result = None


class AcceptanceDriver(StoredDriver):
    """Nonblocking parent facade; no optional filesystem reads/writes here."""
    def __init__(self, **expected):
        super().__init__(**expected)
        self.expected = dict(expected)
        self.agent = None
        self.started = self.awaiting_retention = False
        self.finals = {}

    def prepare(self, config, expected_scope):
        self._result = None
        if self.used: self.valid = False; return
        try:
            from csl_workload_observation_agent import Agent
            from csl_workload_runner import environment
            if config.observe_session != self.context['session'] or expected_scope != self.context['scope']: return
            configuration = environment(config)
            if self.agent is not None:
                if self.configuration != configuration: self.invalidate()
                return
            self.configuration = configuration
            self.agent = Agent({'configuration': configuration, 'expected': self.expected, 'run': self.run})
            self.valid = True
        except Exception: self.valid = False

    def start(self, config, expected_scope):
        self.prepare(config, expected_scope)
        self.used = True

    def poll(self):
        if not self.valid or self.agent is None: return
        try:
            for message in self.agent.poll():
                if message.get('event') == 'started' and not self.started:
                    self.anchor, self.started = sha(message['anchor']), True
                elif (message.get('worker') == self.expected_worker and message.get('seal') == self.expected_seal):
                    if boot_clock() >= self.deadline: raise ValueError()
                    if message.get('event') == 'final': self.finals[identity(message['ticket'])] = message['result']
                    elif message.get('event') == 'retained' and self.awaiting_retention:
                        if message['plan'] != self.expected_plan: raise ValueError()
                        self.anchor = sha(message['anchor'])
                        self.awaiting_retention = False
        except Exception: self.invalidate()

    def assign(self, sequence, deadline):
        self._result = None
        self.poll()
        if not self.valid or not self.started or self.awaiting_retention:
            self.invalidate(); return None
        assignment = super().assign(sequence, deadline)
        self.finals.clear()
        try:
            self.agent.send({'op': 'assign', 'assignment': assignment})
            return {**assignment, 'observer_fd': self.agent.sock.fileno()}
        except Exception:
            self.invalidate(); return None

    def combined(self, receipt, sequence, now):
        self.poll()
        now = max(now, boot_clock())
        if (not self.valid or now >= self.deadline or sequence != self.expected_worker
                or type(receipt) is not dict or receipt.get('sequence') != sequence
                or receipt.get('scope') != self.context['scope'] or receipt.get('ok') is not True
                or type(receipt.get('pending')) is not int or receipt['pending'] != 0
                or type(receipt.get('synced')) is not int or receipt['synced'] != 1
                or type(receipt.get('manifest')) is not list or len(receipt['manifest']) != 1
                or type(receipt['manifest'][0]) is not dict or receipt.get('observation_run') != self.run
                or receipt.get('observation_seal') != self.expected_seal
                or receipt.get('observation_context') != hashlib.sha256(canonical(self.context)).hexdigest()):
            return None
        result = self.finals.pop(receipt.get('observation_pending'), None)
        if type(result) is not dict or not receipt_valid(result.get('observation')): return None
        if (result.get('observation_run') != self.run or result.get('observation_seal') != self.expected_seal
                or result.get('observation_context') != receipt.get('observation_context')): return None
        # Helper output cannot replace the existing product status/manifest.
        return {**receipt, **{k: result[k] for k in ('observation', 'observation_plan')}}

    def retain(self, receipt, sequence, *, now):
        try:
            combined = self.combined(receipt, sequence, now)
            if combined is None: self.invalidate(); return
            self.expected_plan = delivery_fact(receipt)
            self.agent.send({'op': 'retain', 'worker': sequence, 'seal': self.expected_seal,
                             'ticket': receipt['observation_pending'], 'receipt': receipt})
            self.awaiting_retention = True
        except Exception: self.invalidate()

    def consume(self, receipt, sequence, *, complete, now):
        self.clear_result()
        try:
            combined = self.combined(receipt, sequence, now)
            if combined is not None and delivery_fact(receipt) != self.expected_plan: combined = None
            candidate = self._candidate(combined, sequence, complete=complete, now=max(now, boot_clock()))
            # Candidate remains private until the SAME worker cap is checked
            # again after all polling, validation and copying, not the drain cap.
            if candidate is not None and boot_clock() < self.deadline:
                self._result = candidate
        except Exception: self.invalidate()

    def close(self):
        if self.agent is not None:
            try: self.agent.close()
            except Exception: self.invalidate()

    def invalidate(self):
        super().invalidate()
        self.finals.clear()

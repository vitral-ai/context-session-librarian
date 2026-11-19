"""Parent-owned private acceptance reader. Never network, delivery or stop authority."""
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import urllib.parse
import uuid

from csl_workload_http import WorkloadHTTP
from csl_workload_auth import WorkloadTokens
from csl_workload_identity import canonical, WorkloadKey
from csl_workload_observation import identity, receipt_valid
from csl_workload_observation_journal import Evidence, sha


def product_plan(config):
    """Read existing product ACK state, never create/repair/advance it."""
    path = config.state_root/'sync/state.sqlite'
    if not path.exists(): return None
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > config.spool_bytes): raise ValueError()
    db = sqlite3.connect('file:'+urllib.parse.quote(str(path))+'?mode=ro', uri=True, timeout=0)
    try:
        db.set_progress_handler(lambda: 1, 10000)
        row = db.execute('SELECT substr(data,1,65537) FROM sessions WHERE provider=? AND native_id=?',
                         (config.provider, config.observe_session)).fetchone()
        if row is None: return None
        if len(row[0].encode()) > 65536: raise ValueError()
        value = json.loads(row[0])
        if value['status'] != 'synced' or len(value['files']) != 1: return None
        key, file = next(iter(value['files'].items()))
        plan = {'snapshot': identity(value['snapshot_id']), 'revision': identity(file['revision']),
                'generation': identity(value['view']['generation']), 'file_key': key,
                'length': file['length'], 'sha256': file['sha256'], 'last_seq': value['view']['last_seq']}
        if file['offset'] != plan['length'] or file['offset_sha256'] != plan['sha256']: return None
        return plan
    finally: db.close()


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


class AcceptanceDriver:
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
            state = tokens.status()  # Existing validated private record, never refresh/enroll.
            if (state['state'] not in ('ready', 'refresh_inflight')
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

    def consume(self, receipt, sequence, *, complete, now):
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
            self._result = json.loads(json.dumps(receipt['observation']))
        except Exception: self.valid = False

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
            # Read only after a reaped live worker, OUTSIDE the final-drain
            # budget. The final reader does no disk I/O and cannot adopt IDs
            # from the receipt it verifies. Changed final plans refuse evidence.
            self.expected_plan = product_plan(self.config)
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

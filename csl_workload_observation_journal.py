"""Bounded evidence store fed only by checked worker ACK/proof callbacks.

This store cannot authorize requests or establish delivery/EOF by itself.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import os
from pathlib import Path
import re
import stat
import uuid

from csl_sync_state import SyncError, private_json
from csl_workload_observation import CANARY, KINDS, MAX_BODY, MAX_TOTAL, MAX_ATTEMPTS, identity
from csl_workload_state import child, read_record

EMPTY = hashlib.sha256(b'').hexdigest()
CONTEXT = {'run', 'scope', 'binding', 'source', 'session'}
PLAN = {'snapshot', 'revision', 'generation', 'file_key', 'length', 'sha256', 'last_seq', 'body_sha256'}
FIELDS = {'version', 'context', 'plan', 'worker', 'worker_seal', 'offset', 'prefix', 'tail', 'seq', 'content_seen',
          'view_seen', 'snapshot_worker', 'attempts', 'total', 'present', 'pending', 'final', 'poisoned'}
PENDING = {'attempt', 'worker', 'kind', 'body_sha256', 'next_tail', 'start', 'end', 'first', 'last', 'observed'}


def refused(): raise SyncError('workload_observation_unqualified')


def sha(value, length=64):
    if type(value) is not str or not re.fullmatch('[0-9a-f]{'+str(length)+'}', value): refused()
    return value


def number(value, maximum=MAX_TOTAL):
    if type(value) is not int or not 0 <= value <= maximum: refused()
    return value


def check_plan(value):
    if type(value) is not dict or set(value) != PLAN: refused()
    for key in ('snapshot', 'revision', 'generation'): identity(value[key])
    if type(value['file_key']) is not str or not re.fullmatch('[A-Za-z0-9._-]{1,128}', value['file_key']): refused()
    number(value['length']); number(value['last_seq'], 1000000)
    sha(value['sha256']); sha(value['body_sha256'])


class Evidence:
    def __init__(self, parent, context, *, create=False):
        if type(context) is not dict or set(context) != CONTEXT: refused()
        identity(context['run']); identity(context['session'])
        sha(context['scope']); sha(context['binding']); sha(context['source'])
        self.context = dict(context)
        self.root = child(Path(parent), 'observation')
        self.path = self.root/(context['run']+'.json')
        if create:
            with os.scandir(self.root) as entries:
                for count, _ in enumerate(entries, 1):
                    if count > 126: refused()
            with self.locked():
                if read_record(self.path) is not None: refused()
                private_json(self.path, {'version': 1, 'context': self.context, 'plan': None, 'worker': None, 'worker_seal': None,
                    'offset': 0, 'prefix': EMPTY, 'tail': 0, 'seq': 0, 'content_seen': False, 'view_seen': False,
                    'snapshot_worker': None, 'attempts': 0, 'total': 0, 'present': dict.fromkeys(KINDS, False),
                    'pending': None, 'final': None, 'poisoned': False})
        with self.locked(): self.load()

    @contextmanager
    def locked(self):
        fd = None
        try:
            fd = os.open(self.root/(self.context['run']+'.lock'),
                         os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size): refused()
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        except OSError: refused()
        finally:
            if fd is not None: os.close(fd)

    def load(self):
        value = read_record(self.path)
        if (type(value) is not dict or set(value) != FIELDS or type(value['version']) is not int
                or value['version'] != 1 or value['context'] != self.context):
            refused()
        if value['plan'] is not None: check_plan(value['plan'])
        for key in ('worker', 'worker_seal', 'snapshot_worker'):
            if value[key] is not None: identity(value[key])
        for key in ('offset', 'total'): number(value[key])
        number(value['attempts'], MAX_ATTEMPTS); number(value['tail'], len(CANARY)-1); number(value['seq'], 1000000)
        sha(value['prefix'])
        if any(type(value[k]) is not bool for k in ('content_seen', 'view_seen', 'poisoned')): refused()
        if (type(value['present']) is not dict or set(value['present']) != set(KINDS)
                or any(type(v) is not bool for v in value['present'].values())): refused()
        pending = value['pending']
        if pending is not None:
            if type(pending) is not dict or set(pending) != PENDING or pending['kind'] not in KINDS: refused()
            identity(pending['attempt']); identity(pending['worker']); sha(pending['body_sha256'])
            if type(pending['observed']) is not bool: refused()
            number(pending['next_tail'], len(CANARY)-1)
            for key in ('start', 'end', 'first', 'last'):
                if pending[key] is not None: number(pending[key])
        final = value['final']
        if final is not None:
            if type(final) is not dict or set(final) != {'worker', 'seal'}: refused()
            identity(final['worker']); identity(final['seal'])
        return value

    def change(self, action):
        with self.locked():
            value = self.load()
            try: result = action(value)
            except Exception:
                value['poisoned'] = True; value['final'] = None
                private_json(self.path, value)
                raise SyncError('workload_observation_unqualified') from None
            private_json(self.path, value)
            return result

    def worker(self, worker, seal=None):
        identity(worker)
        seal = str(uuid.uuid4()) if seal is None else identity(seal)
        def update(v):
            if v['pending'] is not None: v['poisoned'] = True
            if v['worker'] == worker: refused()
            v.update(worker=worker, worker_seal=seal, final=None)
        self.change(update)

    def plan(self, plan):
        def update(v):
            check_plan(plan)
            if v['pending'] is not None or v['worker'] is None: refused()
            old = v['plan']
            if old is not None:
                if old['snapshot'] == plan['snapshot'] and old != plan: refused()
                if old['file_key'] != plan['file_key'] and old['revision'] == plan['revision']: refused()
                if old['revision'] != plan['revision']:
                    if old['generation'] == plan['generation']: refused()
                    v.update(offset=0, prefix=EMPTY, tail=0, content_seen=False)
                elif (plan['length'] < v['offset'] or plan['length'] == old['length'] and plan['sha256'] != old['sha256']):
                    refused()
                if old['generation'] != plan['generation']: v.update(seq=0, view_seen=False)
                elif plan['last_seq'] < v['seq']: refused()
            v.update(plan=dict(plan), snapshot_worker=None, final=None)
        self.change(update)

    def begin(self, worker, kind, body, *, start=None, end=None, prefix=None, first=None, last=None, observed=True):
        def update(v):
            identity(worker)
            if (worker != v['worker'] or v['pending'] is not None or v['plan'] is None or kind not in KINDS
                    or type(observed) is not bool or type(body) is not bytes or len(body) > MAX_BODY or v['attempts'] >= MAX_ATTEMPTS
                    or v['total']+len(body) > MAX_TOTAL): refused()
            plan = v['plan']; digest = hashlib.sha256(body).hexdigest()
            if kind == 'snapshot':
                if digest != plan['body_sha256'] or any(x is not None for x in (start, end, prefix, first, last)): refused()
            elif kind == 'content':
                if (type(start) is not int or start != v['offset'] or type(end) is not int
                        or end != start+len(body) or end > plan['length'] or prefix != v['prefix']
                        or first is not None or last is not None): refused()
            elif (type(first) is not int or first != v['seq']+1 or type(last) is not int
                  or not first <= last <= plan['last_seq'] or any(x is not None for x in (start, end, prefix))): refused()
            joined = CANARY[:v['tail']]+body if kind == 'content' else body
            v['present'][kind] |= CANARY in joined
            tail = next((n for n in range(len(CANARY)-1, 0, -1) if joined.endswith(CANARY[:n])), 0)
            attempt = str(uuid.uuid4())
            v.update(pending={'worker': worker, 'attempt': attempt, 'kind': kind, 'body_sha256': digest,
                              'next_tail': tail if kind == 'content' else 0, 'start': start, 'end': end,
                              'first': first, 'last': last, 'observed': observed}, final=None,
                     attempts=v['attempts']+1, total=v['total']+len(body))
            return attempt
        return self.change(update)

    def observed(self, attempt, worker, digest):
        def update(v):
            p = v['pending']
            if (p is None or p['attempt'] != attempt or p['worker'] != worker
                    or p['body_sha256'] != digest or p['observed']): refused()
            p['observed'] = True
        self.change(update)

    def acknowledged(self, attempt, matched, *, end=None, sha256=None, last=None):
        def update(v):
            identity(attempt); identity(matched)
            p, plan = v['pending'], v['plan']
            if p is None or p['attempt'] != attempt or p['worker'] != v['worker'] or not p['observed']: refused()
            kind = p['kind']
            if kind == 'snapshot':
                if matched != plan['snapshot'] or any(x is not None for x in (end, sha256, last)): refused()
                v['snapshot_worker'] = v['worker']
            elif kind == 'content':
                if (matched != plan['revision'] or type(end) is not int or end != p['end'] or last is not None
                        or p['start'] == 0 and sha256 != p['body_sha256']): refused()
                sha(sha256)
                v.update(offset=end, prefix=sha256, tail=p['next_tail'], content_seen=True)
            else:
                if matched != plan['generation'] or type(last) is not int or last != p['last'] or end is not None or sha256 is not None: refused()
                v.update(seq=last, view_seen=True)
            v['pending'] = None
        self.change(update)

    def finalized(self, worker, *, snapshot, revision, generation, length, sha256, last_seq, seal=None):
        def update(v):
            for value in (worker, snapshot, revision, generation): identity(value)
            p = v['plan']
            if (type(length) is not int or type(last_seq) is not int or v['poisoned']
                    or v['pending'] is not None or worker != v['worker'] or worker != v['snapshot_worker']
                    or not v['content_seen'] or not v['view_seen'] or p is None
                    or (snapshot, revision, generation, length, sha256, last_seq) !=
                       (p['snapshot'], p['revision'], p['generation'], p['length'], p['sha256'], p['last_seq'])
                    or v['offset'] != length or v['prefix'] != sha256 or v['seq'] != last_seq): refused()
            if seal is not None and seal != v['worker_seal']: refused()
            v['final'] = {'worker': worker, 'seal': v['worker_seal']}
            return v['final']['seal']
        return self.change(update)

    def consume(self, worker, seal, receipt_path):
        """Read only a private expected-worker receipt; not product close authority."""
        try:
            identity(worker); identity(seal)
            receipt_path = Path(receipt_path)
            if (receipt_path.parent != self.root.parent/'runtime'/'receipts'
                    or receipt_path.name != worker+'.json' or receipt_path.resolve() != receipt_path): return None
            for parent in (receipt_path.parent, receipt_path.parent.parent):
                info = parent.lstat()
                if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                        or stat.S_IMODE(info.st_mode) != 0o700): return None
            with self.locked():
                v = self.load()
                result = read_record(receipt_path)
                if (v['poisoned'] or v['pending'] is not None or v['final'] != {'worker': worker, 'seal': seal}
                        or v['plan'] is None or not v['content_seen'] or not v['view_seen']
                        or v['offset'] != v['plan']['length'] or v['prefix'] != v['plan']['sha256']
                        or v['seq'] != v['plan']['last_seq'] or v['snapshot_worker'] != worker
                        or v['worker'] != worker
                        or result is None or result.get('sequence') != worker or result.get('scope') != self.context['scope']
                        or result.get('observation_run') != self.context['run'] or result.get('observation_seal') != seal
                        or result.get('ok') is not True or type(result.get('pending')) is not int or result['pending'] != 0
                        or type(result.get('synced')) is not int or result['synced'] != 1
                        or type(result.get('manifest')) is not list or len(result['manifest']) != 1
                        or type(result['manifest'][0]) is not dict): return None
                return {kind: {'canary_present': v['present'][kind], 'matched_identity': v['plan'][name]}
                        for kind, name in zip(KINDS, ('snapshot', 'revision', 'generation'))}
        except (OSError, ValueError, TypeError, SyncError):
            return None

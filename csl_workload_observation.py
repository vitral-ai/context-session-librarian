"""Opt-in, one-pass serialized-body evidence; never upload or admission authority."""
import hashlib
import re
import urllib.parse
import uuid

from csl_workload_http import Origin

CANARY = b'sk-proj-' + b'Q' * 32
KINDS = ('snapshot', 'content', 'view')
MAX_BODY = 8 * 1024 * 1024
MAX_TOTAL = 128 * 1024 * 1024
MAX_ATTEMPTS = 1024


def identity(value):
    if type(value) is not str or len(value) != 36 or str(uuid.UUID(value)) != value:
        raise ValueError('invalid observation identity')
    return value


def receipt_valid(value):
    try:
        return (type(value) is dict and set(value) == set(KINDS) and all(
            type(value[k]) is dict and set(value[k]) == {'canary_present', 'matched_identity'}
            and type(value[k]['canary_present']) is bool
            and identity(value[k]['matched_identity']) == value[k]['matched_identity'] for k in KINDS))
    except (ValueError, TypeError, AttributeError):
        return False


class Observation:
    """Only complete, observed-from-zero coverage within this worker can qualify.

    All callbacks are best effort: failure poisons evidence, never the delivery
    result. Failed/retried attempts are scanned but cannot qualify this pass.
    """
    def __init__(self, origin, provider, session):
        if provider not in ('codex', 'claude', 'gemini'): raise ValueError('invalid observation provider')
        self.path = '/sessions/' + provider + '/' + identity(session)
        self.url = Origin(origin).registry + self.path
        self.failed = False
        self.plan = None
        self.snapshot_digest = None
        self.call = self.last = self.upload = None
        self.offset = self.sequence = self.attempts = self.total = self.snapshots = 0
        self.content_hash = hashlib.sha256()
        self.tail = b''
        self.present = dict.fromkeys(KINDS, False)
        self.seen = set()
        self.proved = False

    def event(self, name, *args):
        try: getattr(self, '_' + name)(*args)
        except Exception: self.failed = True

    def _upload_planned(self, path, file_key, revision, start, end):
        if path != self.path: return
        self.proved = False
        if self.upload is not None or self.last is not None: raise ValueError()
        self.upload = file_key, revision, start, end

    def _begin(self, method, path, body, binary, key):
        if method != 'PUT' or not (path == self.path or path.startswith((self.path + '/', self.path + '?'))): return
        self.proved = False
        if self.call is not None or self.last is not None: raise ValueError()
        if path == self.path:
            if type(body) is not dict or type(body.get('files')) is not list or len(body['files']) != 1:
                raise ValueError()
            file, view = body['files'][0], body['view']
            snapshot, revision, generation = map(identity,
                (body['snapshot_id'], file['revision'], view['generation']))
            length, last_seq, sha = file['length'], view['last_seq'], file['sha256']
            if (type(length) is not int or not 0 <= length <= MAX_TOTAL or type(last_seq) is not int
                    or not 0 <= last_seq <= 1000000 or type(sha) is not str
                    or not re.fullmatch('[0-9a-f]{64}', sha) or type(file['file_key']) is not str
                    or not 1 <= len(file['file_key']) <= 2048): raise ValueError()
            plan = snapshot, revision, generation, file['file_key'], length, sha, last_seq
            if self.plan is not None and self.plan != plan: raise ValueError()
            self.plan = plan
            kind, details = 'snapshot', None
            if type(key) is not str or not re.fullmatch('[0-9a-f]{64}', key): raise ValueError()
            snapshot_digest = bytes.fromhex(key)
            if self.snapshot_digest is not None and snapshot_digest != self.snapshot_digest: raise ValueError()
            self.snapshot_digest = snapshot_digest
        elif self.plan is None:
            raise ValueError()
        elif path == self.path + '/view':
            records = body['records']
            if (body['generation'] != self.plan[2] or type(records) is not list or not 1 <= len(records) <= 100
                    or type(body['first_seq']) is not int or body['first_seq'] != self.sequence + 1
                    or any(type(r.get('seq')) is not int or r['seq'] != self.sequence + n
                           for n, r in enumerate(records, 1))): raise ValueError()
            kind, details = 'view', records[-1]['seq']
        else:
            file_key, revision, start, end = self.upload
            suffix = '/files/' + urllib.parse.quote(file_key, safe='') + '/content?'
            query = urllib.parse.parse_qs(path.partition('?')[2], keep_blank_values=True, strict_parsing=True)
            if (file_key != self.plan[3] or revision != self.plan[1]
                    or path.partition('?')[0] != self.path + suffix[:-1]
                    or query not in ({'revision': [revision], 'mode': ['append']},
                                     {'revision': [revision], 'mode': ['replace']})
                    or type(start) is not int or start != self.offset or type(end) is not int
                    or type(binary) is not bytes or len(binary) > MAX_BODY
                    or end != start + len(binary) or end > self.plan[4]):
                raise ValueError()
            self.upload = None
            kind, details = 'content', (start, end)
        if kind == 'content': body_digest = hashlib.sha256(binary).digest()
        else:
            if type(key) is not str or not re.fullmatch('[0-9a-f]{64}', key): raise ValueError()
            body_digest = bytes.fromhex(key)
        self.call = {'url': self.url + path[len(self.path):], 'kind': kind,
                     'digest': body_digest, 'details': details, 'attempts': 0}

    def _request(self, url, method, data):
        if method != 'PUT' or not (url == self.url or url.startswith((self.url + '/', self.url + '?'))): return
        self.attempts += 1
        if (self.attempts > MAX_ATTEMPTS or type(data) is not bytes or len(data) > MAX_BODY
                or self.total + len(data) > MAX_TOTAL): raise ValueError()
        self.total += len(data)
        call = self.call
        if call is None or url != call['url'] or hashlib.sha256(data).digest() != call['digest']:
            raise ValueError()
        call['attempts'] += 1
        if call['attempts'] != 1: self.failed = True
        kind = call['kind']
        joined = self.tail + data if kind == 'content' else data
        self.present[kind] |= CANARY in joined
        self.seen.add(kind)
        if kind == 'snapshot': self.snapshots += 1
        if kind == 'content':
            candidate = self.content_hash.copy(); candidate.update(data)
            suffix = next((CANARY[:n] for n in range(len(CANARY)-1, 0, -1)
                           if joined.endswith(CANARY[:n])), b'')
            call['content'] = candidate, suffix

    def _end(self, method, path, success):
        if method != 'PUT' or not (path == self.path or path.startswith((self.path + '/', self.path + '?'))): return
        call, self.call = self.call, None
        if not success or call is None or call['attempts'] != 1: raise ValueError()
        if call['kind'] != 'snapshot': self.last = call

    def _content_acked(self, path, file_key, checkpoint):
        if path != self.path: return
        last, self.last = self.last, None
        if (last is None or last['kind'] != 'content' or file_key != self.plan[3]
                or checkpoint['revision'] != self.plan[1] or checkpoint['offset'] != last['details'][1]
                or type(checkpoint['offset']) is not int
                or checkpoint['offset_sha256'] != last['content'][0].hexdigest()): raise ValueError()
        self.offset = checkpoint['offset']
        self.content_hash, self.tail = last['content']

    def _view_acked(self, path, view):
        if path != self.path: return
        last, self.last = self.last, None
        if (last is None or last['kind'] != 'view' or view['generation'] != self.plan[2]
                or type(view['last_seq']) is not int or view['last_seq'] != last['details']): raise ValueError()
        self.sequence = view['last_seq']

    def _proven(self, path, plan):
        if path != self.path: return
        if (self.plan is None or self.call is not None or self.last is not None or self.upload is not None
                or self.seen != set(KINDS) or self.snapshots < 2
                or self.offset != self.plan[4] or self.content_hash.hexdigest() != self.plan[5]
                or self.sequence != self.plan[6] or plan['snapshot_id'] != self.plan[0]
                or plan['files'][0]['revision'] != self.plan[1] or plan['view']['generation'] != self.plan[2]):
            raise ValueError()
        self.proved = True

    def receipt(self):
        if (self.failed or not self.proved or self.call is not None or self.last is not None
                or self.upload is not None): return None
        return {kind: {'canary_present': self.present[kind], 'matched_identity': self.plan[n]}
                for n, kind in enumerate(KINDS)}

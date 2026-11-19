"""Private cross-worker evidence; all delivery authority stays with Collector."""
import hashlib
import json

from csl_workload_auth import boot_clock
from csl_workload_identity import canonical
from csl_workload_observation import Observation, CANARY, KINDS, identity
from csl_workload_observation_journal import Evidence


class DurableObservation(Observation):
    def __init__(self, config, assignment, binding, source, deadline, expected_worker):
        super().__init__(config.origin, config.provider, config.observe_session)
        if type(assignment) is not dict or set(assignment) != {'context', 'worker', 'seal', 'deadline', 'predecessor'}:
            raise ValueError()
        context = assignment['context']
        if (assignment['deadline'] != deadline or assignment['worker'] != expected_worker
                or context['session'] != config.observe_session
                or context['source'] != source or context['scope'] != binding['scope']
                or context['binding'] != hashlib.sha256(canonical({k: binding[k] for k in ('account_id', 'device_id')})).hexdigest()):
            raise ValueError()
        self.worker, self.seal = map(identity, (assignment['worker'], assignment['seal']))
        self.context, self.deadline = context, deadline
        self.evidence = Evidence(config.state_root, context)
        if hashlib.sha256(canonical(self.evidence.load())).hexdigest() != assignment['predecessor']: raise ValueError()
        self.evidence.worker(self.worker, self.seal)
        value = self.evidence.load()
        if (value['worker'] != self.worker or value['worker_seal'] != self.seal
                or value['pending'] is not None or value['poisoned']): raise ValueError()
        self.prefix = None

    def _upload_planned(self, path, key, revision, start, end, prefix=None):
        super()._upload_planned(path, key, revision, start, end, prefix)
        if path == self.path: self.prefix = prefix

    def _begin(self, method, path, body, binary, key):
        super()._begin(method, path, body, binary, key)
        if self.call is None: return
        if self.call['kind'] == 'snapshot':
            snapshot, revision, generation, file_key, length, sha, seq = self.plan
            self.evidence.plan({'snapshot': snapshot, 'revision': revision, 'generation': generation,
                'file_key': file_key, 'length': length, 'sha256': sha, 'last_seq': seq, 'body_sha256': key})
            value = self.evidence.load()
            self.offset, self.sequence, self.tail = value['offset'], value['seq'], CANARY[:value['tail']]
        call = self.call
        expected = binary if call['kind'] == 'content' else json.dumps(
            body, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
        if hashlib.sha256(expected).digest() != call['digest']: raise ValueError()
        options = {}
        if call['kind'] == 'content':
            options = dict(start=call['details'][0], end=call['details'][1], prefix=self.prefix)
        elif call['kind'] == 'view': options = dict(first=self.sequence+1, last=call['details'])
        # Helper-only preparation is NOT an observation or product ACK. The
        # real Request hook must see this durable readiness without waiting,
        # verify the exact immutable bytes, then mark the intent observed.
        call['intent'] = self.evidence.begin(self.worker, call['kind'], expected, observed=False, **options)

    def _request(self, url, method, data):
        super()._request(url, method, data)
        if self.call is None or url != self.call['url']: return
        if self.failed or boot_clock() >= self.deadline: raise ValueError()
        call = self.call
        self.evidence.observed(call['intent'], self.worker, hashlib.sha256(data).hexdigest())
        if boot_clock() >= self.deadline: raise ValueError()

    def _end(self, method, path, success, result=None):
        call = self.call
        super()._end(method, path, success, result)
        if call is not None and call['kind'] == 'snapshot':
            if type(result) is not dict or result.get('snapshot_id') != self.plan[0]: raise ValueError()
            self.evidence.acknowledged(call['intent'], self.plan[0])

    def _content_acked(self, path, key, checkpoint):
        if path != self.path: return
        call, self.last = self.last, None
        if (call is None or call['kind'] != 'content' or key != self.plan[3]
                or checkpoint['revision'] != self.plan[1] or type(checkpoint['offset']) is not int
                or checkpoint['offset'] != call['details'][1]): raise ValueError()
        # Collector already checked the cumulative digest and fsynced its ACK.
        # The evidence store independently requires the prior observed prefix.
        self.evidence.acknowledged(call['intent'], checkpoint['revision'],
            end=checkpoint['offset'], sha256=checkpoint['offset_sha256'])
        value = self.evidence.load()
        self.offset, self.tail = value['offset'], CANARY[:value['tail']]

    def _view_acked(self, path, view):
        if path != self.path: return
        call, self.last = self.last, None
        if (call is None or call['kind'] != 'view' or view['generation'] != self.plan[2]
                or type(view['last_seq']) is not int or view['last_seq'] != call['details']): raise ValueError()
        self.evidence.acknowledged(call['intent'], view['generation'], last=view['last_seq'])
        self.sequence = view['last_seq']

    def _proven(self, path, plan):
        if path != self.path: return
        if (self.plan is None or self.call is not None or self.last is not None or self.upload is not None
                or self.snapshots < 2 or plan['snapshot_id'] != self.plan[0]
                or plan['files'][0]['revision'] != self.plan[1]
                or plan['view']['generation'] != self.plan[2]): raise ValueError()
        self.proved = True

    def finalize(self):
        """Called only after the existing authoritative final proof AND source EOF."""
        if self.failed or not self.proved or boot_clock() >= self.deadline: return {}
        try:
            snapshot, revision, generation, key, length, sha, seq = self.plan
            self.evidence.finalized(self.worker, snapshot=snapshot, revision=revision,
                generation=generation, length=length, sha256=sha, last_seq=seq, seal=self.seal)
            value = self.evidence.load()
            if (boot_clock() >= self.deadline or value['final'] != {'worker': self.worker, 'seal': self.seal}):
                return {}
            return {'observation': {kind: {'canary_present': value['present'][kind], 'matched_identity': self.plan[n]}
                        for n, kind in enumerate(KINDS)},
                    'observation_run': self.context['run'], 'observation_seal': self.seal,
                    'observation_context': hashlib.sha256(canonical(self.context)).hexdigest(),
                    'observation_plan': {'snapshot': snapshot, 'revision': revision, 'generation': generation,
                        'file_key': key, 'length': length, 'sha256': sha, 'last_seq': seq}}
        except Exception:
            self.failed = True
            return {}

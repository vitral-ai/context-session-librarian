"""In-process source units with synthetic bodies; no network or worker execution."""
from dataclasses import replace
import copy
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch
import urllib.error
import urllib.parse
import uuid

from csl_sync_engine import Collector, digest
from csl_sync_state import State, SyncError, private_json
from csl_workload_config import Configuration, PREFIX, PROVIDER_PROFILES
from csl_workload_observation import CANARY, KINDS, MAX_BODY, Observation, receipt_valid
from csl_workload_runner import DeadlineHTTP, ProofRegistry, PassProcess, boot_clock, environment, scope

ORIGIN = 'https://identity.example.test'
SID, SNAPSHOT, REVISION, GENERATION = [str(uuid.UUID(int=n)) for n in range(1, 5)]
PATH = '/sessions/codex/' + SID


class Response(io.BytesIO):
    def __enter__(self): return self
    def __exit__(self, *args): self.close()


class ObservationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='body-observer-unit-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.data = b'public before ' + CANARY + b' public after'
        self.plan = {'snapshot_id': SNAPSHOT, 'title': 'private-metadata-unit',
                     'files': [{'file_key': 'primary', 'revision': REVISION, 'length': len(self.data),
                                'sha256': hashlib.sha256(self.data).hexdigest()}],
                     'view': {'generation': GENERATION, 'last_seq': 1}, 'package': {'available': False}}
        self.http = DeadlineHTTP(ORIGIN, boot_clock()+100, attempts=3, sleep=lambda _: None)
        self.observer = Observation(ORIGIN, 'codex', SID)
        self.http.observation = self.observer
        self.calls, self.remote = [], b''
        self.fail_content = 0
        self.proof = True
        self.snapshots = 0
        def opened(request, **kwargs):
            self.calls.append((request.get_method(), request.full_url, request.data))
            path = urllib.parse.urlsplit(request.full_url).path
            if request.get_method() == 'GET':
                result = {'items': [{**self.plan['files'][0], 'length': len(self.remote),
                                     'sha256': hashlib.sha256(self.remote).hexdigest()}]}
            elif path.endswith('/content'):
                start = int(request.get_header('X-start'))
                data = request.data
                if start == len(self.remote): self.remote += data
                else: self.assertEqual(self.remote[start:start+len(data)], data)
                if self.fail_content:
                    self.fail_content -= 1
                    raise urllib.error.URLError('synthetic lost response')
                end = start+len(data)
                result = {'length': end, 'revision': self.plan['files'][0]['revision'],
                          'sha256': hashlib.sha256(self.remote[:end]).hexdigest()}
            elif path.endswith('/view'): result = {}
            else:
                self.snapshots += 1
                result = {'snapshot_id': self.plan['snapshot_id'], 'view': dict(self.plan['view']),
                          'sync': {'complete': self.proof}}
            return Response(json.dumps(result).encode())
        self.http.opener = SimpleNamespace(open=opened)
        self.registry = ProofRegistry(self.http.origin.registry, SimpleNamespace(access_token=lambda: 'unit-token'), http=self.http)
        # Any accidental attempt to leave the in-memory transport is a test error.
        self.no_socket = patch('socket.socket', side_effect=AssertionError('network not allowed'))
        self.no_socket.start(); self.addCleanup(self.no_socket.stop)
        self.no_process = patch('subprocess.Popen', side_effect=AssertionError('process not allowed'))
        self.no_process.start(); self.addCleanup(self.no_process.stop)

    def snapshot(self): self.registry.call('PUT', PATH, body=self.plan, key=digest(self.plan))

    def upload(self, *, remote=None, chunk=23):
        spool = self.root/'synthetic-body'; spool.write_bytes(self.data)
        state = SimpleNamespace(root=self.root)
        uploader = Collector(state, self.registry, None, chunk_size=chunk)
        self.checkpoint = {'revision': REVISION}
        self.saved = []
        uploader.upload(PATH, {'spool': str(spool), 'file_key': 'primary', 'mode': 'append',
                               'complete_length': len(self.data)}, self.checkpoint, remote,
                        on_progress=lambda: self.saved.append(dict(self.checkpoint)))

    def view(self):
        body = {'generation': GENERATION, 'first_seq': 1,
                'records': [{'seq': 1, 'text': 'private-view-unit ' + CANARY.decode()}]}
        self.registry.call('PUT', PATH+'/view', body=body, key=digest(body))
        self.registry.view_acknowledged(PATH, {'generation': GENERATION, 'last_seq': 1})

    def prove(self):
        local = {'snapshot_id': SNAPSHOT, 'status': 'synced', 'view': dict(self.plan['view']),
                 'files': {'primary': {'revision': REVISION, 'offset': len(self.data),
                                       'offset_sha256': hashlib.sha256(self.data).hexdigest()}}}
        self.registry.prove(SimpleNamespace(session=lambda *_: local), 'codex')

    def complete(self):
        self.snapshot(); self.upload(); self.view(); self.prove()

    def test_actual_request_bytes_split_content_and_fresh_authoritative_proof(self):
        observed = []
        hook = self.http.request_created
        def before(request):
            if request.get_method() == 'PUT': observed.append(request.data)
            hook(request)
        self.http.request_created = before
        self.complete()
        value = self.observer.receipt()
        self.assertTrue(receipt_valid(value))
        self.assertEqual([value[k]['matched_identity'] for k in KINDS], [SNAPSHOT, REVISION, GENERATION])
        self.assertEqual([value[k]['canary_present'] for k in KINDS], [False, True, True])
        self.assertEqual(self.snapshots, 2)
        self.assertEqual(self.remote, self.data)
        self.assertEqual(self.saved[-1]['offset'], len(self.data))
        self.assertNotIn('private-', json.dumps(value)); self.assertNotIn(CANARY.decode(), json.dumps(value))
        self.assertNotIn(self.data, [v for v in vars(self.observer).values() if type(v) is bytes])
        handed = [data for method, _, data in self.calls if method == 'PUT']
        self.assertEqual(len(observed), len(handed))
        for before, after in zip(observed, handed): self.assertIs(before, after)

    def test_snapshot_package_metadata_bytes_are_observed(self):
        self.plan['package']['reason'] = CANARY.decode()
        self.complete()
        self.assertTrue(self.observer.receipt()['snapshot']['canary_present'])

    def test_in_process_sync_ack_hooks_follow_persistence_and_final_proof(self):
        # No Collector.run, PassProcess, network, token operation or native source.
        source = self.root/'public-projected-unit'; source.write_bytes(self.data)
        sha = hashlib.sha256(self.data).hexdigest()
        item = {'file_key': 'primary', 'mode': 'append', 'path': str(source),
                'complete_length': len(self.data), 'sha256': sha, 'role': 'primary'}
        anchor = {'file_key': 'primary', 'length': len(self.data), 'sha256': sha}
        def view_records(*args, position=None, **kwargs):
            return {'source_file': anchor, 'position': 'done', 'complete': True,
                    'records': [] if position else [{'seq': 1, 'text': CANARY.decode(), 'role': 'user',
                        'kind': 'text', 'raw_offset': {'file_key': 'primary', 'offset': 0}}]}
        adapter = SimpleNamespace(raw_files=lambda *_: [item], view_records=view_records,
                                  package_descriptor=lambda *_: {'available': False})
        state = State(self.root/'unit-journal'); self.addCleanup(state.close)
        state.set('device_id', SID)
        opened = self.http.opener.open
        def snapshot_plan(request, **kwargs):
            if request.get_method() == 'PUT' and request.full_url.endswith(PATH):
                self.plan = json.loads(request.data)
            return opened(request, **kwargs)
        self.http.opener.open = snapshot_plan
        saved = []
        def view_acked(path, view):
            self.assertEqual(state.session('codex', SID)['view']['last_seq'], view['last_seq'])
            saved.append('view')
            self.observer.event('view_acked', path, view)
        def content_acked(path, key, checkpoint):
            self.assertEqual(state.session('codex', SID)['files'][key]['offset'], checkpoint['offset'])
            saved.append('content')
            self.observer.event('content_acked', path, key, checkpoint)
        self.registry.view_acknowledged = view_acked
        self.registry.content_acknowledged = content_acked
        Collector(state, self.registry, adapter, chunk_size=23).sync_session('codex', SID, {'byte_count': len(self.data)})
        self.assertIsNone(self.observer.receipt())
        self.registry.prove(state, 'codex')
        self.assertTrue(receipt_valid(self.observer.receipt()))
        self.assertIn('content', saved); self.assertIn('view', saved)

    def test_bad_content_ack_or_persistence_failure_does_not_advance_observed_range(self):
        self.snapshot()
        opened = self.http.opener.open
        def wrong(request, **kwargs):
            response = opened(request, **kwargs)
            if '/content?' in request.full_url:
                data = json.loads(response.read()); data['sha256'] = '0'*64
                return Response(json.dumps(data).encode())
            return response
        self.http.opener.open = wrong
        with self.assertRaises(SyncError): self.upload()
        self.assertEqual(self.observer.offset, 0)
        self.assertIsNone(self.observer.receipt())

    def test_checkpoint_write_failure_prevents_evidence_ack_without_altering_error(self):
        self.snapshot()
        spool = self.root/'synthetic-body'; spool.write_bytes(self.data)
        uploader = Collector(SimpleNamespace(root=self.root), self.registry, None)
        checkpoint = {'revision': REVISION}
        saves = []
        def persist():
            saves.append(dict(checkpoint))
            if len(saves) == 2: raise OSError('synthetic persistence failure')
        with self.assertRaises(OSError):
            uploader.upload(PATH, {'spool': str(spool), 'file_key': 'primary', 'mode': 'append',
                                   'complete_length': len(self.data)}, checkpoint, None, on_progress=persist)
        self.assertEqual(self.observer.offset, 0)
        self.assertIsNone(self.observer.receipt())

    def test_expired_response_keeps_existing_deadline_and_no_observation_receipt(self):
        with patch('csl_workload_runner.boot_clock', return_value=1000) as clock:
            self.http.deadline = 1001
            opened = self.http.opener.open
            def expired(request, **kwargs):
                result = opened(request, **kwargs); clock.return_value = 1002
                return result
            self.http.opener.open = expired
            with self.assertRaises(SyncError): self.snapshot()
        self.assertTrue(self.observer.failed)
        self.assertIsNone(self.observer.receipt())

    def test_missing_final_proof_never_qualifies(self):
        self.snapshot(); self.upload(); self.view()
        self.assertIsNone(self.observer.receipt())
        self.proof = False
        with self.assertRaises(SyncError): self.prove()
        self.assertIsNone(self.observer.receipt())

    def test_each_authoritative_final_identity_and_completeness_is_required(self):
        for field in ('snapshot_id', 'generation', 'last_seq', 'complete'):
            with self.subTest(field=field):
                self.setUp()
                self.snapshot(); self.upload(); self.view()
                original = self.http.opener.open
                def corrupt(request, **kwargs):
                    response = original(request, **kwargs)
                    if request.get_method() == 'PUT' and request.full_url.endswith(PATH):
                        data = json.loads(response.read())
                        if field == 'snapshot_id': data[field] = SID
                        elif field == 'complete': data['sync'][field] = False
                        elif field == 'last_seq': data['view'][field] = 2
                        else: data['view'][field] = SID
                        return Response(json.dumps(data).encode())
                    return response
                self.http.opener.open = corrupt
                with self.assertRaises(SyncError): self.prove()
                self.assertIsNone(self.observer.receipt())

    def test_missing_content_observation_or_ack_is_not_a_clean_boolean(self):
        self.snapshot()
        with patch.object(self.http, 'request_created'):
            self.upload()
        self.view(); self.prove()
        self.assertIsNone(self.observer.receipt())

    def test_other_session_never_borrows_selected_ids_or_scans_its_body(self):
        other = '/sessions/codex/'+GENERATION
        self.registry.call('PUT', other, body={**self.plan, 'title': CANARY.decode()}, key=digest(self.plan))
        self.assertEqual(self.observer.attempts, 0)
        self.assertIsNone(self.observer.plan)
        self.assertIsNone(self.observer.receipt())

    def test_lost_ack_scans_attempt_and_does_not_change_retry_behavior(self):
        self.fail_content = 1
        self.snapshot(); self.upload(chunk=len(self.data)); self.view(); self.prove()
        content = [row for row in self.calls if '/content?' in row[1]]
        self.assertEqual(len(content), 2)
        self.assertIs(content[0][2], content[1][2])
        self.assertTrue(self.observer.present['content'])
        self.assertIsNone(self.observer.receipt())
        self.assertEqual(self.checkpoint['offset'], len(self.data))

    def test_resume_prior_prefix_and_unchanged_prior_upload_are_unqualified(self):
        for offset in (5, len(self.data)):
            with self.subTest(offset=offset):
                self.setUp()
                self.snapshot(); self.remote = self.data[:offset]
                self.upload(remote={'length': offset, 'sha256': hashlib.sha256(self.remote).hexdigest()})
                self.view(); self.prove()
                self.assertIsNone(self.observer.receipt())

    def test_missing_view_empty_view_and_other_session_never_qualify(self):
        self.snapshot(); self.upload()
        self.plan['view']['last_seq'] = 0
        self.registry.plans[PATH] = copy.deepcopy(self.plan)
        self.prove()
        self.assertIsNone(self.observer.receipt())
        self.assertFalse(self.observer.present['view'])

    def test_no_auth_get_devices_headers_or_foreign_origin_observation(self):
        for method, suffix in (('POST', '/native-auth/v1/token'), ('GET', '/session-registry/v1'+PATH),
                               ('PUT', '/session-registry/v1/devices/'+SID)):
            self.http.request(ORIGIN+suffix, method, data=CANARY, headers={'Authorization': 'unit-token'})
        self.assertEqual(self.observer.attempts, 0)
        self.assertFalse(any(self.observer.present.values()))
        before = len(self.calls)
        with self.assertRaises(SyncError): self.http.request('https://foreign.example.test/session-registry/v1'+PATH, 'PUT', data=CANARY)
        self.assertEqual(len(self.calls), before)
        with self.assertRaises(SyncError): self.http.request(ORIGIN+'/session-registry/v1'+PATH, 'PUT', headers={'Host': 'foreign'})
        self.assertEqual(len(self.calls), before)

    def test_wrong_body_route_bounds_and_pending_call_poison_only_evidence(self):
        cases = ('body', 'route', 'query', 'size', 'attempts', 'total', 'pending')
        for case in cases:
            with self.subTest(case=case):
                self.setUp(); self.snapshot()
                if case == 'body':
                    self.observer.event('begin', 'PUT', PATH, self.plan, None, '0'*64)
                    self.http.request(self.observer.url, 'PUT', data=b'{}')
                elif case == 'route': self.registry.call('PUT', PATH+'/unknown', binary=CANARY)
                elif case == 'query': self.registry.call('PUT', PATH+'?unknown=1', body=self.plan, key=digest(self.plan))
                elif case == 'size': self.http.request(self.observer.url, 'PUT', data=b'x'*(MAX_BODY+1))
                elif case == 'pending':
                    self.observer.event('begin', 'PUT', PATH, self.plan, None, digest(self.plan))
                    self.registry.call('PUT', PATH, body=self.plan, key=digest(self.plan))
                else:
                    setattr(self.observer, case, 2**32)
                    self.registry.call('PUT', PATH, body=self.plan, key=digest(self.plan))
                self.assertTrue(self.observer.failed)
                self.assertIsNone(self.observer.receipt())

    def test_new_attempt_invalidates_prior_proof_and_does_not_retain_arbitrary_body(self):
        self.complete()
        self.assertIsNotNone(self.observer.receipt())
        self.registry.call('PUT', PATH, body=self.plan, key=digest(self.plan))
        self.assertIsNone(self.observer.receipt())
        self.assertTrue(self.observer.tail in [CANARY[:n] for n in range(len(CANARY))])

    def test_changed_metadata_or_multiple_files_cannot_reuse_plan(self):
        self.snapshot()
        for update in ({'title': 'changed'}, {'files': self.plan['files']*2}):
            body = {**self.plan, **update}
            self.registry.call('PUT', PATH, body=body, key=digest(body))
            self.assertTrue(self.observer.failed)
            self.assertIsNone(self.observer.receipt())

    def test_observer_off_does_not_change_body_or_call_count(self):
        self.http.observation = None
        self.complete()
        off = [(m, u, d) for m, u, d in self.calls]
        self.setUp(); self.complete()
        self.assertEqual(off, self.calls)

    def test_receipt_schema_refuses_extra_fields_noncanonical_ids_and_missing_surfaces(self):
        self.complete(); value = self.observer.receipt()
        for bad in (None, {}, {**value, 'raw': 'private'}, {**value, 'snapshot': {'canary_present': 0, 'matched_identity': SNAPSHOT}},
                    {**value, 'view': {'canary_present': False, 'matched_identity': 'not-a-uuid'}},
                    {**value, 'content': {'canary_present': False, 'matched_identity': None}}):
            self.assertFalse(receipt_valid(bad))


class ObservationConfigurationTests(unittest.TestCase):
    def test_default_off_and_explicit_private_worker_config_no_spawn(self):
        from test_workload_config import ConfigurationTests
        fixture = ConfigurationTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        plain = Configuration.from_environment(fixture.env)
        self.assertIsNone(plain.observe_session)
        self.assertNotIn(PREFIX+'OBSERVE_SESSION', environment(plain))
        config = replace(plain, observe_session=SID)
        values = environment(config)
        self.assertEqual(Configuration.from_environment(values).observe_session, SID)
        self.assertEqual(scope(config), scope(plain))  # Evidence opt-in is not account authority.
        with patch('csl_workload_runner.subprocess.Popen', return_value=Mock(returncode=0)) as spawned:
            worker = PassProcess(config, str(uuid.uuid4()), boot_clock()+100)
        self.assertNotIn(SID, repr(spawned.call_args))
        self.assertEqual(set(spawned.call_args.kwargs['env']), {'PATH', 'LANG', 'HOME', 'PYTHONDONTWRITEBYTECODE'})
        value = {'sequence': worker.sequence, 'scope': scope(config), 'ok': False, 'synced': 0, 'pending': 1,
                 'manifest': [], 'observation': {'raw': 'private'}}
        private_json(worker.path, value)
        self.assertIsNone(worker.receipt()['observation'])
        for invalid in ('', True, 'auto', 'ABCDEFAB-ABCD-4ABC-8ABC-ABCDEFABCDEF', 'not-a-session'):
            with self.subTest(invalid=invalid), self.assertRaises(SyncError):
                Configuration.from_environment({**fixture.env, PREFIX+'OBSERVE_SESSION': invalid})

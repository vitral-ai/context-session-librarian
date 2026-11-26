"""Budget and memory witnesses use synthetic transcripts, never installed homes."""
import json
import os
from pathlib import Path
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch
import uuid

import csl_sync_providers as native
from csl_sync_engine import Collector, Providers
from csl_sync_fs import PassYield, WorkBudget, work_budget, open_stream
from csl_sync_http import HTTP, Registry
from csl_sync_policy import configure
from csl_sync_state import State
from sync_support import FakeTokens, Peer


class BudgetTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.home = self.root/'codex'; (self.home/'sessions').mkdir(parents=True)
        self.claude = self.root/'claude'
        env = patch.dict(os.environ, CODEX_HOME=str(self.home), CLAUDE_CONFIG_DIR=str(self.claude))
        env.start(); self.addCleanup(env.stop)
        self.state = State(self.root/'state'); self.addCleanup(self.state.close)
        self.peer = Peer(); self.addCleanup(self.peer.close)
        self.providers = Providers(self.state.root)
        self.collector = Collector(self.state, Registry(self.peer.url, FakeTokens(), http=HTTP(sleep=lambda _:None)), self.providers)

    def transcript(self, *, provider='codex', text='small prompt', created='2099-01-01T00:00:00Z'):
        sid = str(uuid.uuid4())
        if provider == 'codex':
            path = self.home/'sessions'/f'rollout-{sid}.jsonl'
            records = [{'type':'session_meta','timestamp':created,'payload':{'id':sid}},
                       {'type':'response_item','payload':{'type':'message','role':'user',
                        'content':[{'type':'input_text','text':text}]}}]
        else:
            path = self.claude/'projects'/'synthetic'/f'{sid}.jsonl'; path.parent.mkdir(parents=True,exist_ok=True)
            records = [{'type':'user','sessionId':sid,'timestamp':created,
                        'message':{'role':'user','content':text}}]
        path.write_text(''.join(json.dumps(row)+'\n' for row in records))
        return sid,path

    def test_discovery_does_not_open_parse_or_freeze_transcripts(self):
        self.transcript(); self.transcript(provider='claude')
        with patch.object(native,'_metadata',side_effect=AssertionError('parsed')), \
             patch.object(self.providers.cache,'prepare',side_effect=AssertionError('frozen')), \
             patch('csl_sync_cache.open_stream',side_effect=AssertionError('opened')):
            rows = self.providers.discover_sessions('codex') + self.providers.discover_sessions('claude')
        self.assertEqual(len(rows),2)
        self.assertTrue(all({'_path','byte_count','mtime','native_id'} <= row.keys() for row in rows))

    def test_huge_old_file_does_not_block_small_new_uploads_or_allocate_its_contents(self):
        configure(self.state, 'new')
        old,path = self.transcript(created='2001-01-01T00:00:00Z')
        with path.open('ab') as stream: stream.truncate(1024**3)
        os.utime(path,(1,1))
        small = [self.transcript(provider='claude' if i%2 else 'codex')[0] for i in range(16)]
        original = self.providers.prepare_session
        def prepare(provider,row):
            self.assertNotEqual(row['native_id'],old,'huge transcript must be deferred before preparation')
            return original(provider,row)
        tracemalloc.start()
        try:
            with patch.object(self.providers,'prepare_session',side_effect=prepare):
                result=self.collector.run(days=None,providers=('codex','claude'),budget_bytes=2*1024**2)
            _,peak=tracemalloc.get_traced_memory()
        finally: tracemalloc.stop()
        self.assertEqual(result['synced'],len(small),result)
        self.assertEqual(result['errors'],0,result)
        self.assertEqual(result['deferred'],1)
        self.assertLess(peak,16*1024**2)
        self.assertEqual({row['native_id'] for row in result['sessions'] if row['status']=='synced'},set(small))
        self.assertNotIn(('codex',old),self.state.tracked())

    def test_oversized_preparation_eventually_finishes_without_manual_budget_change(self):
        large,_ = self.transcript(text='large prompt ' * 10000)
        small = [self.transcript(provider='claude')[0] for _ in range(3)]
        def run():
            return self.collector.run(days=None, providers=('codex','claude'),
                                      budget_seconds=1, budget_bytes=32768)
        first = run()
        rows = {r['native_id']:r for r in first['sessions']}
        self.assertTrue(all(rows[sid]['status']=='synced' for sid in small), first)
        self.assertEqual(rows[large]['status'], 'deferred')
        second = run()
        self.assertEqual(second['errors'], 0, second)
        saved = dict(self.state.get('budget_deferrals'))
        self.assertGreater(next(iter(saved.values()))['seconds'], 1)
        # Fresh small work preempts the dedicated retry without resetting its
        # earned allowance. No source truncation or external budget change.
        fresh,_ = self.transcript(provider='claude')
        third = run()
        self.assertTrue(any(r['native_id']==fresh and r['status']=='synced'
                            for r in third['sessions']), third)
        self.assertEqual(saved, self.state.get('budget_deferrals'))
        for _ in range(12):
            result = run()
            self.assertEqual(result['errors'], 0, result)
            if self.state.session('codex',large).get('status') == 'synced':
                break
        self.assertEqual(self.state.session('codex',large).get('status'), 'synced', result)
        self.assertFalse(self.state.get('budget_deferrals'))
        self.assertFalse(list(self.collector.spool.iterdir()))

    def test_legacy_filename_uses_verified_identity_and_selection(self):
        sid,path = self.transcript(created='2001-01-01T00:00:00Z')
        old = path.with_name('older-native-name.jsonl'); path.rename(old)
        self.state.select('codex', old.stem, False)  # Hint is not authority.
        self.state.select('codex', sid, True)
        result = self.collector.run(providers=('codex',))
        self.assertEqual((result['synced'], result['errors']), (1,0), result)
        self.assertEqual(result['sessions'][0]['native_id'], sid)
        self.assertNotIn(('codex',old.stem), self.state.tracked())

    def test_legacy_verified_exclusion_does_not_upload(self):
        sid,path = self.transcript()
        path.rename(path.with_name('old.jsonl'))
        self.state.select('codex',sid,False)
        result = self.collector.run(days=None,providers=('codex',))
        self.assertEqual(result['synced'],0,result)
        self.assertNotIn(('codex',sid), self.state.tracked())

    def test_legacy_aliases_refuse_duplicate_native_ids_before_upload(self):
        for canonical in (False, True):
            with self.subTest(canonical=canonical):
                sid,path = self.transcript()
                first = path if canonical else path.with_name('old-first.jsonl')
                if not canonical: path.rename(first)
                second = path.with_name('old-second.jsonl')
                second.write_bytes(first.read_bytes())
                result = self.collector.run(days=None,providers=('codex',))
                self.assertEqual(result['synced'],0,result)
                self.assertTrue(all(r.get('error')=='ambiguous_native_id' for r in result['sessions']),result)
                self.assertNotIn(('codex',sid),self.state.tracked())
                first.unlink(); second.unlink()

    def test_large_legacy_metadata_does_not_spend_small_upload_budget(self):
        legacy, path = self.transcript(created='2001-01-01T00:00:00Z')
        old = path.with_name('old-format.jsonl'); path.rename(old)
        records = old.read_text().splitlines()
        header = json.loads(records[0]); header['payload']['padding'] = 'x' * 131072
        old.write_text(json.dumps(header) + '\n' + records[1] + '\n')
        small, _ = self.transcript()
        result = self.collector.run(days=None, providers=('codex',), budget_bytes=32768)
        rows = {row['native_id']: row for row in result['sessions']}
        self.assertEqual(rows[small]['status'], 'synced', result)
        self.assertEqual(result['errors'], 0, result)
        self.assertEqual(result['deferred'], 1, result)
        self.assertNotIn(('codex', legacy), self.state.tracked())

    def test_legacy_identity_cached_by_file_fingerprint_and_own_budget(self):
        sid, path = self.transcript()
        old = path.with_name('old-format.jsonl'); path.rename(old)
        records = old.read_text().splitlines()
        header = json.loads(records[0]); header['payload']['padding'] = 'x' * 131072
        old.write_text(json.dumps(header) + '\n' + records[1] + '\n')
        original = native._lines
        seen = []
        def lines(*args, **kwargs):
            for record in original(*args, **kwargs):
                seen.append(record)
                yield record
        budget = WorkBudget(10, 32768)
        with work_budget(budget), patch.object(native, '_lines', side_effect=lines):
            self.assertEqual(self.providers.cache.legacy_identity(old, self.home), sid)
        self.assertEqual(budget.used, 0)
        self.assertEqual(len(seen), 1, 'stop after metadata, not the transcript body')
        fresh = Providers(self.state.root)
        with patch.object(native, '_lines', side_effect=AssertionError('cached header re-read')):
            self.assertEqual(fresh.cache.legacy_identity(old, self.home), sid)
        # A changed source invalidates the persisted identity even across passes.
        replacement = str(uuid.uuid4())
        header['payload']['id'] = replacement
        old.write_text(json.dumps(header) + '\n' + records[1] + '\n')
        self.assertEqual(fresh.cache.legacy_identity(old, self.home), replacement)

    def test_malformed_legacy_id_cannot_poison_later_healthy_pass(self):
        _, path = self.transcript()
        old = path.with_name('malformed-old.jsonl'); path.rename(old)
        old.write_text(json.dumps({'type':'session_meta', 'payload':{'id':'x'*1024**2}}) + '\n')
        for _ in range(2):
            healthy, _ = self.transcript()
            result = self.collector.run(days=None, providers=('codex',))
            rows = {row['native_id']:row for row in result['sessions']}
            self.assertEqual(rows[healthy]['status'], 'synced', result)
            self.assertEqual(rows[old.stem]['error'], 'invalid_native_id', result)
        self.assertFalse(list(self.providers.cache.root.glob('*.identity.json')))

    def test_legacy_identity_uses_existing_native_id_bounds(self):
        _, path = self.transcript()
        old = path.with_name('old-bounds.jsonl'); path.rename(old)
        for sid in ('a', 'a'*128, 'a'*129, 'space id', 42):
            with self.subTest(native_id=str(sid)[:16]):
                old.write_text(json.dumps({'type':'session_meta', 'payload':{'id':sid}}) + '\n')
                if isinstance(sid, str) and sid in ('a', 'a'*128):
                    self.assertEqual(self.providers.cache.legacy_identity(old, self.home), sid)
                else:
                    with self.assertRaisesRegex(native.ProviderError, 'invalid_native_id'):
                        self.providers.cache.legacy_identity(old, self.home)

    def test_oversized_prior_identity_cache_recovers_as_miss(self):
        import hashlib
        from csl_sync_cache import encoded
        from csl_sync_state import private_json, read_private_json
        sid, path = self.transcript()
        old = path.with_name('old-format.jsonl'); path.rename(old)
        cache = self.providers.cache
        key = hashlib.sha256(encoded(['legacy-identity', str(self.home), str(old)])).hexdigest()
        cache_path = cache.root / (key + '.identity.json')
        private_json(cache_path, {'fingerprint':cache.fingerprint([old], self.home),
                                 'complete':True, 'native_id':'x'*1024**2})
        healthy, _ = self.transcript()
        result = self.collector.run(days=None, providers=('codex',))
        self.assertEqual(result['errors'], 0, result)
        self.assertEqual({row['native_id'] for row in result['sessions']
                          if row['status']=='synced'}, {sid, healthy}, result)
        self.assertEqual(read_private_json(cache_path)['native_id'], sid)
        self.assertLess(cache_path.stat().st_size, 1024)

    def test_unreadable_preflight_cache_is_not_a_credential_failure(self):
        import csl_sync_cache
        from csl_sync_state import SyncError
        sid, path = self.transcript()
        old = path.with_name('old-unreadable.jsonl'); path.rename(old)
        # Restrict the injected failure to preflight metadata, never auth files.
        original = csl_sync_cache.read_private_json
        def read(path):
            if str(path).endswith('.identity.json'):
                raise SyncError('unsafe_credentials_file')
            return original(path)
        with patch.object(csl_sync_cache, 'read_private_json', side_effect=read):
            result = self.collector.run(days=None, providers=('codex',))
        self.assertEqual((result['synced'], result['errors']), (1, 0), result)
        self.assertEqual(result['sessions'][0]['native_id'], sid)

    def test_legacy_identity_preamble_resumes_without_spending_peer_budget(self):
        sid, path = self.transcript()
        old = path.with_name('old-preamble.jsonl'); path.rename(old)
        old.write_text((json.dumps({'type': 'event_msg', 'payload': {'text': 'x'*100}})
                        + '\n') * 40 + old.read_text())
        peer_budget = WorkBudget(10, 32768)
        starts = []
        original = native._lines
        def lines(*args, **kwargs):
            starts.append(kwargs.get('start_offset', 0))
            return original(*args, **kwargs)
        # A small independent allowance exercises resumable identity preflight.
        with work_budget(peer_budget), \
             patch('csl_sync_cache.WorkBudget', side_effect=lambda *_: WorkBudget(10, 1024)), \
             patch.object(native, '_lines', side_effect=lines):
            for _ in range(20):
                cache = Providers(self.state.root).cache
                try:
                    actual = cache.legacy_identity(old, self.home)
                    break
                except PassYield:
                    continue
            else:
                self.fail('identity preflight did not progress across passes')
        self.assertEqual(actual, sid)
        self.assertGreater(starts[-1], starts[0])
        self.assertEqual(peer_budget.used, 0)

    def test_large_legacy_header_still_refuses_alias_before_small_upload(self):
        sid, path = self.transcript()
        old = path.with_name('old-alias.jsonl')
        header = json.loads(path.read_text().splitlines()[0])
        header['payload']['padding'] = 'x' * 131072
        old.write_text(json.dumps(header) + '\n')
        result = self.collector.run(days=None, providers=('codex',), budget_bytes=32768)
        self.assertEqual(result['synced'], 0, result)
        self.assertTrue(any(row.get('error') == 'ambiguous_native_id'
                            for row in result['sessions']), result)
        self.assertNotIn(('codex', sid), self.state.tracked())

    def test_budget_yield_does_not_grant_unknown_age_tracking(self):
        sid,_=self.transcript()
        result=self.collector.run(providers=('codex',),budget_bytes=1)
        self.assertEqual(result['deferred'],1)
        self.assertEqual(result['errors'],0)
        self.assertNotIn(('codex',sid),self.state.tracked())
        self.assertIsNone(self.state.get('last_error'))
        self.assertEqual(self.collector.run(days=None,providers=('codex',))['synced'],1)

    def test_single_oversized_record_isolated_from_healthy_peer(self):
        bad,path=self.transcript()
        with path.open('ab') as stream:
            stream.write(b'{"padding":"')
            for _ in range(9): stream.write(b'x'*1024**2)
            stream.write(b'"}\n')
        good,_=self.transcript(provider='claude')
        result=self.collector.run(days=None,providers=('codex','claude'))
        rows={row['native_id']:row for row in result['sessions']}
        self.assertEqual(rows[good]['status'],'synced',result)
        self.assertEqual(rows[bad]['error'],'record_size_limit')

    def test_time_budget_uses_monotonic_and_read_is_bounded_before_allocation(self):
        _,path=self.transcript()
        ticks=[10]
        budget=WorkBudget(2,10,clock=lambda:ticks[0])
        with work_budget(budget),open_stream(path) as stream:
            with self.assertRaises(PassYield): stream.read()
            self.assertLessEqual(stream.tell(),11)
        ticks[0]=12
        with self.assertRaises(PassYield): budget.check()

    def test_upload_ack_survives_cooperative_yield_and_next_pass(self):
        sid,_=self.transcript(text='x'*20000)
        self.collector.chunk_size=1024
        original=self.collector.upload
        def upload(*args,**kwargs):
            progress=kwargs['on_progress']; calls=[0]
            def checkpoint():
                progress();calls[0]+=1
                if calls[0]==2: raise PassYield()
            return original(*args,**{**kwargs,'on_progress':checkpoint})
        with patch.object(self.collector,'upload',side_effect=upload):
            result=self.collector.run(days=None,providers=('codex',))
        self.assertEqual(result['deferred'],1,result)
        saved=self.state.session('codex',sid)['files']['main']
        self.assertEqual(saved['offset'],1024)
        self.assertEqual(self.collector.run(days=None,providers=('codex',))['synced'],1)
        self.assertFalse(list(self.collector.spool.iterdir()))


if __name__=='__main__':unittest.main()

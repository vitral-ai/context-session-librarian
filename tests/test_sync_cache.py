"""Synthetic collector performance invariants, not wall-clock speed assertions."""
import json
from contextlib import closing
import os
import subprocess
import select
import sqlite3
import sys
import time
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from csl_sync_engine import Collector, Providers
from csl_sync_http import HTTP, Registry
from csl_sync_state import State, SyncError
from csl_sync_state import private_json
import csl_sync_providers as native
from sync_support import FakeTokens, Peer


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.home = self.root / 'native'
        self.sid = str(uuid.uuid4())
        self.path = self.home / 'sessions' / ('rollout-' + self.sid + '.jsonl')
        self.path.parent.mkdir(parents=True)
        self.rows = [
            {'type': 'session_meta', 'payload': {'id': self.sid, 'cwd': '/synthetic/project'}},
            {'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'choose blue'}]}},
            {'type': 'response_item', 'payload': {'type': 'function_call', 'name': 'read', 'call_id': 'fixture-call', 'arguments': '{}'}}]
        self.write()
        self.env = patch.dict(os.environ, CODEX_HOME=str(self.home))
        self.env.start(); self.addCleanup(self.env.stop)
        self.state = State(self.root / 'state'); self.addCleanup(self.state.close)
        self.peer = Peer(); self.addCleanup(self.peer.close)
        self.providers = Providers(self.state.root)
        self.clock = [1000.0]
        self.providers.cache.clock = lambda: self.clock[0]
        self.collector = Collector(self.state, Registry(self.peer.url, FakeTokens(), http=HTTP(sleep=lambda _: None)), self.providers)

    def write(self):
        self.path.write_text(''.join(json.dumps(row) + '\n' for row in self.rows))

    def run_pass(self): return self.collector.run(days=None, providers=['codex'])
    def local(self): return self.state.session('codex', self.sid)

    def test_unchanged_skips_parsing_hashing_views_and_per_session_http(self):
        self.assertEqual(self.run_pass()['synced'], 1)
        before = self.local()['last_synced_at']; calls = len(self.peer.calls)
        with patch.object(native, '_metadata', side_effect=AssertionError('metadata repeated')), \
             patch.object(native, '_files', side_effect=AssertionError('hashes repeated')), \
             patch.object(native, '_view', side_effect=AssertionError('view repeated')):
            result = self.run_pass()
        self.assertTrue(result['sessions'][0]['unchanged'])
        self.assertEqual(self.local()['last_synced_at'], before)
        self.assertTrue(all('/sessions/' not in path for _, path in self.peer.calls[calls:]))

    def test_append_renders_only_tail_and_preserves_historical_tool_correlation(self):
        self.run_pass(); old_length = self.path.stat().st_size
        position = self.local()['view']['position']
        row = {'type': 'response_item', 'payload': {'type': 'function_call_output', 'call_id': 'fixture-call', 'output': 'tail evidence'}}
        with self.path.open('a') as out: out.write(json.dumps(row) + '\n')
        with patch.object(native, '_view', wraps=native._view) as rendered:
            self.assertEqual(self.run_pass()['synced'], 1)
        indexed = [call.kwargs['start_offset'] for call in rendered.call_args_list if 'start_offset' in call.kwargs]
        self.assertEqual(indexed, [old_length])
        expected = native.view_records('codex', self.sid, home=self.home, position=position)
        observed = self.providers.view_records('codex', self.sid, position=position)
        self.assertEqual(observed, expected)
        self.assertEqual(observed['records'][0]['tool'], 'read')

    def test_same_size_and_mtime_rewrite_detected_by_ctime(self):
        self.run_pass(); old = self.local()['view']['generation']; stat = self.path.stat()
        self.path.write_bytes(self.path.read_bytes().replace(b'choose blue', b'choose gold'))
        os.utime(self.path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        self.assertEqual(self.run_pass()['synced'], 1)
        self.assertNotEqual(self.local()['view']['generation'], old)

    def test_periodic_verification_bounds_all_metadata_lies_without_new_completeness_claim(self):
        self.run_pass(); old = self.local()['last_synced_at']
        fingerprint = self.providers.cache.entries[('codex', self.sid)][1]['fingerprint']
        self.path.write_bytes(self.path.read_bytes().replace(b'choose blue', b'choose gold'))
        with patch.object(self.providers.cache, 'fingerprint', return_value=fingerprint):
            self.assertTrue(self.run_pass()['sessions'][0]['unchanged'])
            self.assertEqual(self.local()['last_synced_at'], old)
            self.clock[0] += 301
            self.assertEqual(self.run_pass()['synced'], 1)
        view = self.providers.view_records('codex', self.sid)
        self.assertEqual(view['records'][0]['text'], 'choose gold')

    def test_truncation_replacement_and_backwards_clock_force_verification(self):
        self.run_pass(); revision = self.local()['files']['main']['revision']
        self.rows = self.rows[:2]; self.write()
        self.assertEqual(self.run_pass()['synced'], 1)
        self.assertNotEqual(self.local()['files']['main']['revision'], revision)
        revision = self.local()['files']['main']['revision']
        other = self.path.with_suffix('.replacement'); other.write_bytes(self.path.read_bytes()); other.replace(self.path)
        self.run_pass(); self.assertNotEqual(self.local()['files']['main']['revision'], revision)
        self.clock[0] -= 10
        with patch.object(self.providers.cache, 'verify_entry', wraps=self.providers.cache.verify_entry) as verified:
            self.run_pass(); self.assertEqual(verified.call_count, 1)

    def test_continuously_growing_source_publishes_frozen_complete_prefix(self):
        package = native._package_mapping
        def growing(*args, **kwargs):
            result = package(*args, **kwargs)
            with self.path.open('a') as out:
                out.write(json.dumps({'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'next prefix'}]}}) + '\n')
            return result
        with patch.object(native, '_package_mapping', side_effect=growing):
            self.assertEqual(self.run_pass()['synced'], 1)
        self.assertEqual(self.run_pass()['synced'], 1)
        self.assertEqual(self.providers.view_records('codex', self.sid)['records'][-1]['text'], 'next prefix')

    def test_private_cache_restart_and_periodic_validation(self):
        self.run_pass()
        self.providers = Providers(self.state.root)
        self.providers.cache.clock = lambda: self.clock[0]
        self.collector.providers = self.providers
        self.assertTrue(self.run_pass()['sessions'][0]['unchanged'])
        self.clock[0] += 301
        self.assertEqual(self.run_pass()['synced'], 1)
        for path in self.providers.cache.root.rglob('*'):
            if path.is_file(): self.assertEqual(path.stat().st_mode & 0o077, 0)

    def test_package_descriptor_matches_uncached_provider_and_rejects_invalid_base(self):
        self.run_pass()
        self.assertEqual(self.providers.package_descriptor('codex', self.sid), native.package_descriptor('codex', self.sid))
        self.rows[0]['payload']['history_base'] = {'thread_id': 'fabricated-external-base'}
        self.write()
        self.assertEqual(self.run_pass()['pending'],1)
        listed = self.providers.list_sessions('codex')
        self.assertEqual(listed[0]['native_id'], self.sid)
        self.assertIn('error', listed[0])
        self.assertFalse(native.package_descriptor('codex',self.sid)['available'])

    def test_bad_codex_session_does_not_block_healthy_upload_and_can_recover(self):
        bad_id = str(uuid.uuid4())
        bad = self.path.with_name('rollout-' + bad_id + '.jsonl')
        rows = json.loads(json.dumps(self.rows))
        rows[0]['payload'].update(id=bad_id, history_base={'thread_id': 'missing-base'})
        bad.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        result = self.run_pass()
        self.assertEqual((result['synced'], result['pending']), (1, 1))
        failed = [row for row in result['sessions'] if row.get('native_id') == bad_id][0]
        self.assertEqual(failed['status'], 'error')
        self.assertEqual(self.local()['status'], 'synced')
        self.assertNotIn(('codex', bad_id), self.providers.cache.entries)
        rows[0]['payload'].pop('history_base')
        bad.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        result = self.run_pass()
        self.assertEqual((result['synced'], result['pending']), (2, 0))
        self.assertIsNone(self.state.session('codex', bad_id)['last_error'])

    def test_duplicate_claude_skips_both_copies_and_uploads_healthy_session(self):
        home = self.root / 'claude'
        duplicate_id, healthy_id = str(uuid.uuid4()), str(uuid.uuid4())
        def write(project, sid, text):
            path = home / 'projects' / project / (sid + '.jsonl')
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({'type': 'user', 'sessionId': sid,
                'uuid': str(uuid.uuid4()), 'parentUuid': None,
                'message': {'role': 'user', 'content': text}}) + '\n')
            return path
        first = write('a', duplicate_id, 'original copy')
        with patch.dict(os.environ, CLAUDE_CONFIG_DIR=str(home)):
            self.assertEqual(self.collector.run(days=None, providers=['claude'])['synced'], 1)
            second = write('b', duplicate_id, 'different copy')
            write('c', healthy_id, 'healthy session')
            originals = (first.read_bytes(), second.read_bytes())
            result = self.collector.run(days=None, providers=['claude'])
            self.assertEqual((result['synced'], result['pending']), (1, 1))
            failed = [row for row in result['sessions'] if row.get('native_id') == duplicate_id][0]
            self.assertEqual(failed['error'], 'ambiguous_native_id')
            self.assertNotIn(('claude', duplicate_id), self.providers.cache.entries)
            self.assertEqual((first.read_bytes(), second.read_bytes()), originals)
            self.assertEqual(self.state.session('claude', healthy_id)['status'], 'synced')

    def test_claude_companion_changes_invalidate_cache(self):
        home = self.root / 'claude'
        path = home / 'projects' / 'fixture' / (self.sid + '.jsonl')
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({'type':'user','sessionId':self.sid,'uuid':str(uuid.uuid4()),'parentUuid':None,
                                    'message':{'role':'user','content':'synthetic question'}}) + '\n')
        with patch.dict(os.environ, CLAUDE_CONFIG_DIR=str(home)):
            cache = self.providers.cache
            cache.list_sessions('claude')
            first = cache.signature('claude', self.sid)
            result = path.with_suffix('') / 'tool-results' / 'fixture.txt'
            result.parent.mkdir(parents=True); result.write_text('synthetic result')
            cache.list_sessions('claude')
            self.assertNotEqual(cache.signature('claude', self.sid), first)
            self.assertEqual(cache.raw_files('claude', self.sid), native.raw_files('claude', self.sid))
            self.assertEqual(cache.package_descriptor('claude', self.sid), native.package_descriptor('claude', self.sid))

    def test_complete_record_appearing_in_partial_suffix_is_detected_at_full_verification(self):
        with self.path.open('ab') as out: out.write(b'{} ')
        self.run_pass()
        fingerprint = self.providers.cache.entries[('codex', self.sid)][1]['fingerprint']
        raw = self.path.read_bytes(); self.path.write_bytes(raw[:-1] + b'\n')
        with patch.object(self.providers.cache, 'fingerprint', return_value=fingerprint):
            self.clock[0] += 301
            with patch.object(native, '_metadata', wraps=native._metadata) as parsed:
                self.run_pass(); self.assertGreater(parsed.call_count, 0)

    def test_watch_wakes_for_new_record_before_sixty_second_interval(self):
        self.run_pass()
        self.state.set('settings', {'enabled':True,'endpoint':self.peer.url,'client_id':'synthetic'})
        private_json(self.state.root/'credentials.json', {'access_token':'fixture-access','refresh_token':'fixture-refresh',
                    'refresh_state':'ready','expires_at':time.time()+600})
        process = subprocess.Popen([sys.executable,str(Path(__file__).resolve().parents[1]/'csl'),'sync',
                                    '--home',str(self.state.root),'watch','--all','--provider','codex','--interval','60'],
                                   env=dict(os.environ,CODEX_HOME=str(self.home)), stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,text=True)
        try:
            # A successful first pass is flushed before watch enters its idle wait.
            ready, _, _ = select.select([process.stdout], [], [], 10)
            self.assertTrue(ready, 'watch did not finish its first pass')
            self.assertIn('"synced":1',process.stdout.readline())
            start = time.monotonic()
            with self.path.open('a') as out:
                out.write(json.dumps({'type':'response_item','payload':{'type':'message','role':'user',
                               'content':[{'type':'input_text','text':'synthetic wake evidence'}]}})+'\n')
            received = False
            while time.monotonic()-start < 15:
                if any(row.get('text')=='synthetic wake evidence' for rows in list(self.peer.views.values()) for row in list(rows)):
                    received = True; break
                if process.poll() is not None: break
                time.sleep(.02)
            self.assertTrue(received,'watch did not wake for changed source within 15 seconds')
            self.assertLess(time.monotonic()-start,60)
        finally:
            process.terminate()
            process.communicate(timeout=5)

    def test_change_hints_detect_creation_removal_and_do_not_retain_deleted_entries(self):
        self.run_pass(); self.assertFalse(self.providers.changed())
        other = self.path.with_name('rollout-' + str(uuid.uuid4()) + '.jsonl')
        other.write_bytes(self.path.read_bytes())
        self.assertTrue(self.providers.changed())
        other.unlink()
        self.path.unlink()
        self.assertTrue(self.providers.changed())
        self.providers.list_sessions('codex')
        self.assertFalse(self.providers.changed())

    def test_mismatched_derived_index_is_rebuilt_instead_of_remaining_pending(self):
        self.run_pass()
        directory = self.providers.cache.entries[('codex', self.sid)][0]
        with closing(sqlite3.connect(directory/'view.sqlite')) as db, db:
            value = json.loads(db.execute('SELECT value FROM meta WHERE id=1').fetchone()[0])
            value['sha256'] = '0'*64
            db.execute('UPDATE meta SET value=? WHERE id=1',(json.dumps(value),))
        with self.assertRaises(SyncError): self.providers.view_records('codex',self.sid)
        self.assertEqual(self.run_pass()['synced'],1)
        self.assertEqual(self.providers.view_records('codex',self.sid),native.view_records('codex',self.sid))

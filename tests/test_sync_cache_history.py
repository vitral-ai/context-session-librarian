"""History-base cache dependencies; every fixture is synthetic."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import csl_sync_providers as native
from csl_sync_engine import Collector, Providers
from csl_sync_http import HTTP, Registry
from csl_sync_state import State
from sync_support import FakeTokens, Peer
from test_codex_history import write_rollout


class HistoryCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.home=self.root/'native'
        self.base,self.bid,position=write_rollout(self.home,archived=True,label='BASE')
        self.leaf,self.sid,_=write_rollout(self.home,base=position,label='LEAF')
        self.env=patch.dict(os.environ,CODEX_HOME=str(self.home));self.env.start();self.addCleanup(self.env.stop)
        self.state=State(self.root/'state');self.addCleanup(self.state.close)
        self.peer=Peer();self.addCleanup(self.peer.close)
        self.providers=Providers(self.state.root);self.clock=[1000.0]
        self.providers.cache.clock=lambda:self.clock[0]
        self.collector=Collector(self.state,Registry(self.peer.url,FakeTokens(),http=HTTP(sleep=lambda _:None)),self.providers)

    def run_pass(self):return self.collector.run(days=None,providers=['codex'])
    def local(self):return self.state.session('codex',self.sid)
    def entry(self):return self.providers.cache.entries[('codex',self.sid)][1]

    def test_base_changes_while_leaf_unchanged_and_cached_view_matches_native(self):
        self.run_pass();leaf=self.leaf.read_bytes();generation=self.local()['view']['generation']
        self.assertTrue(next(r for r in self.run_pass()['sessions'] if r.get('native_id')==self.sid)['unchanged'])
        self.base.write_bytes(self.base.read_bytes().replace(b'BASE-ANSWER',b'NEXT-ANSWER'))
        self.assertTrue(self.providers.changed())
        self.assertEqual(self.run_pass()['pending'],0)
        self.assertEqual(self.leaf.read_bytes(),leaf)
        self.assertNotEqual(self.local()['view']['generation'],generation)
        self.assertEqual(self.providers.view_records('codex',self.sid),native.view_records('codex',self.sid))
        self.assertEqual(self.providers.package_descriptor('codex',self.sid),native.package_descriptor('codex',self.sid))
        self.assertIn('NEXT-ANSWER',json.dumps(self.providers.view_records('codex',self.sid)))

    def test_hidden_base_rewrite_is_detected_by_periodic_full_verification(self):
        self.run_pass()
        stamps={str(paths[0]):entry['fingerprint'] for _,entry,paths,_ in self.providers.cache.entries.values()}
        self.base.write_bytes(self.base.read_bytes().replace(b'BASE-ANSWER',b'NEXT-ANSWER'))
        with patch.object(self.providers.cache,'fingerprint',side_effect=lambda paths,home:stamps[str(paths[0])]):
            self.assertTrue(next(r for r in self.run_pass()['sessions'] if r.get('native_id')==self.sid)['unchanged'])
            self.clock[0]+=301
            self.assertEqual(self.run_pass()['pending'],0)
        self.assertIn('NEXT-ANSWER',json.dumps(self.providers.view_records('codex',self.sid)))

    def test_archived_base_move_invalidates_dependency_paths(self):
        self.run_pass();signature=self.local()['verified_cache']
        moved=self.home/'sessions'/self.base.name;moved.parent.mkdir(exist_ok=True)
        self.base.rename(moved)
        self.assertTrue(self.providers.changed())
        self.assertEqual(self.run_pass()['pending'],0)
        self.assertNotEqual(self.local()['verified_cache'],signature)
        self.assertIn(str(moved),[row['path'] for row in self.entry()['inventory']])

    def test_compressed_sibling_is_an_input_even_when_native_prefers_plain(self):
        sibling=Path(str(self.base)+'.zst');sibling.write_bytes(b'ignored-compressed-sibling-v1')
        self.run_pass()
        self.assertIn(str(sibling),[row['path'] for row in self.entry()['graph_inputs']])
        stamps={str(paths[0]):entry['fingerprint'] for _,entry,paths,_ in self.providers.cache.entries.values()}
        sibling.write_bytes(b'ignored-compressed-sibling-v2')
        with patch.object(self.providers.cache,'fingerprint',side_effect=lambda paths,home:stamps[str(paths[0])]):
            self.clock[0]+=301
            with patch.object(self.providers.cache,'prepare',wraps=self.providers.cache.prepare) as prepared:
                self.assertEqual(self.run_pass()['pending'],0)
                self.assertTrue(any(call.args[1]==self.leaf for call in prepared.call_args_list))

    @unittest.skipUnless(shutil.which('zstd'),'optional zstd CLI for compressed fixture')
    def test_selected_compressed_base_is_bound_and_rewrites_invalidate_leaf(self):
        sibling=Path(str(self.base)+'.zst');original=self.base.read_bytes()
        sibling.write_bytes(subprocess.run(['zstd','-q','-c'],input=original,capture_output=True,check=True).stdout)
        self.base.unlink();self.run_pass();generation=self.local()['view']['generation']
        self.assertEqual(self.providers.view_records('codex',self.sid),native.view_records('codex',self.sid))
        sibling.write_bytes(subprocess.run(['zstd','-q','-c'],input=original.replace(b'BASE-ANSWER',b'NEXT-ANSWER'),capture_output=True,check=True).stdout)
        self.assertEqual(self.run_pass()['pending'],0)
        self.assertNotEqual(self.local()['view']['generation'],generation)

    def test_changed_base_graph_discovers_new_ancestor_while_terminal_leaf_unchanged(self):
        _,_,first=write_rollout(self.home,label='ROOTA')
        _,_,second=write_rollout(self.home,label='ROOTB')
        middle,_,ref=write_rollout(self.home,base=first,label='MID')
        terminal,tid,_=write_rollout(self.home,base=ref,label='END')
        self.run_pass();old=terminal.read_bytes()
        middle.write_bytes(middle.read_bytes().replace(first['thread_id'].encode(),second['thread_id'].encode()))
        self.assertEqual(self.run_pass()['pending'],0)
        self.assertEqual(old,terminal.read_bytes())
        entry=self.providers.cache.entries[('codex',tid)][1]
        self.assertIn(second['thread_id'],entry['graph_rollouts'])
        self.assertNotIn(first['thread_id'],entry['graph_rollouts'])
        self.assertEqual(self.providers.view_records('codex',tid),native.view_records('codex',tid))

    def test_missing_base_or_missing_view_anchor_is_rejected(self):
        self.run_pass();generation=self.local()['view']['generation']
        self.base.write_bytes(self.base.read_bytes().replace(b'BASE-ANSWER',b'NEXT-ANSWER'))
        view=self.providers.view_records
        def missing(provider,sid,**kwargs):
            page=view(provider,sid,**kwargs)
            if sid==self.sid:page['source_files']=page['source_files'][:1]
            return page
        with patch.object(self.providers,'view_records',side_effect=missing):
            self.assertGreater(self.run_pass()['pending'],0)
        self.assertEqual(self.local()['view']['generation'],generation)
        self.base.unlink()
        self.assertEqual(self.run_pass()['sessions'][0]['error'],'history_base_missing')

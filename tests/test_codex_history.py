"""Synthetic native lineage fixtures; no personal data, credentials or models."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from unittest.mock import patch
import zipfile

import csl_codex_history as h
import csl_provider_bundle as bundle
import csl_sync_providers as p

STAMP = '2026-01-01T00:00:00Z'


def native_history_items(test, root, home, sid, last_turn):
    """Rebuild disposable projection caches through Codex, never SQL or a turn.

    Codex 0.169's thread/read reads a SQLite projection, not raw JSONL. A bounded
    ephemeral fork rebuilds all referenced projections using Codex's own parser.
    No turn/start, credentials, hooks, or remote model provider are supplied.
    """
    env = {'PATH': os.environ['PATH'], 'HOME': str(root), 'CODEX_HOME': str(home), 'LANG': 'C.UTF-8'}
    responses = queue.Queue()
    args = ['codex', 'app-server', '-c', 'model_provider="csl_offline"', '-c', 'model="synthetic"',
            '-c', 'model_providers.csl_offline={name="offline",base_url="http://127.0.0.1:1/v1",wire_api="responses"}']
    process = subprocess.Popen(args, env=env, cwd=root, stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    def reader():
        for line in process.stdout:
            responses.put(json.loads(line))
        responses.put(None)
    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    def call(number, method, params):
        process.stdin.write(json.dumps({'id': number, 'method': method, 'params': params}) + '\n')
        process.stdin.flush()
        while True:
            reply = responses.get(timeout=30)
            if reply is None:
                raise AssertionError('native process closed')
            if reply.get('id') == number:
                # Do not echo native history or local paths in failure reports.
                if 'error' in reply:
                    raise AssertionError('native operation rejected')
                return reply['result']
    try:
        call(1, 'initialize', {'clientInfo': {'name': 'csl_lineage_test', 'version': '1'},
                               'capabilities': {'experimentalApi': True}})
        process.stdin.write('{"method":"initialized","params":{}}\n')
        process.stdin.flush()
        initial = call(2, 'thread/read', {'threadId': sid, 'includeTurns': True})
        call(3, 'thread/fork', {'threadId': sid, 'lastTurnId': last_turn, 'ephemeral': True,
                               'excludeTurns': True, 'modelProvider': 'csl_offline',
                               'model': 'synthetic', 'cwd': str(root)})
        result = call(4, 'thread/read', {'threadId': sid, 'includeTurns': True})
        items = [item for turn in result['thread']['turns'] for item in turn['items']]
        return items, len(initial['thread']['turns'])
    finally:
        process.stdin.close()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=10)
        thread.join(timeout=5)
        process.stdout.close()


def write_rollout(home, *, owner=None, rollout=None, base=None, label='BASE',
                  archived=False, opaque=False):
    owner = owner or str(uuid.uuid4())
    rollout = rollout or owner
    suffix = owner if rollout == owner else owner + '_' + rollout
    path = home / ('archived_sessions' if archived else 'sessions') / (
        'rollout-2026-01-01T00-00-00-' + suffix + '.jsonl')
    meta = {'id': owner, 'session_id': owner, 'timestamp': STAMP, 'cwd': str(home),
            'originator': 'csl_test', 'source': 'cli', 'cli_version': '0.169.0',
            'model_provider': 'openai', 'history_mode': 'paginated'}
    if base:
        meta['history_base'] = base
    turn = str(uuid.uuid4())
    rows = [{'type': 'session_meta', 'payload': meta},
            {'type': 'event_msg', 'payload': {'type': 'task_started', 'turn_id': turn}},
            {'type': 'response_item', 'payload': {'type': 'message', 'role': 'user',
                'content': [{'type': 'input_text', 'text': label + '-QUESTION'}]}},
            {'type': 'event_msg', 'payload': {'type': 'item_completed', 'thread_id': owner,
                'turn_id': turn, 'item': {'type': 'UserMessage', 'id': 'u-' + turn,
                'client_id': None, 'content': [{'type': 'text', 'text': label + '-QUESTION', 'text_elements': []}]}}},
            {'type': 'response_item', 'payload': {'type': 'message', 'role': 'assistant',
                'content': [{'type': 'output_text', 'text': label + '-ANSWER'}]}},
            {'type': 'event_msg', 'payload': {'type': 'item_completed', 'thread_id': owner,
                'turn_id': turn, 'item': {'type': 'AgentMessage', 'id': 'a-' + turn,
                'phase': 'final_answer', 'content': [{'type': 'Text', 'text': label + '-ANSWER'}]}}},
            {'type': 'event_msg', 'payload': {'type': 'task_complete', 'turn_id': turn,
                'last_agent_message': label + '-ANSWER'}}]
    if opaque:
        rows.insert(-1, {'type': 'response_item', 'payload': {
            'type': 'reasoning', 'encrypted_content': 'synthetic-opaque-ciphertext'}})
    start = base['end_ordinal_exclusive'] if base else 0
    for index, row in enumerate(rows):
        row.update(timestamp=STAMP, ordinal=start + index)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b''.join((json.dumps(row) + '\n').encode() for row in rows))
    position = {'thread_id': rollout, 'end_byte_offset': path.stat().st_size,
                'end_ordinal_exclusive': start + len(rows)}
    return path, owner, position


class HistoryBaseTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.home = self.root / 'source'
        self.cwd = self.root / 'project-with-longer-name'
        self.cwd.mkdir()
        self.base, self.base_id, self.position = write_rollout(self.home, archived=True, opaque=True)
        self.child, self.sid, _ = write_rollout(self.home, base=self.position, label='CHILD')

    def change_meta(self, path, change):
        rows = [json.loads(line) for line in path.read_bytes().splitlines()]
        change(rows[0])
        path.write_bytes(b''.join((json.dumps(r) + '\n').encode() for r in rows))

    def export(self):
        path = self.root / ('native-' + str(uuid.uuid4()) + '.zip')
        with contextlib.redirect_stdout(io.StringIO()):
            bundle.export_native('codex', self.sid, path, home=self.home)
        return path

    def restore(self, package=None, target=None, **kwargs):
        package = package or self.export()
        target = target or self.root / ('target-' + str(uuid.uuid4()))
        with zipfile.ZipFile(package) as archive, patch.dict(os.environ, {'CODEX_HOME': str(target)}), contextlib.redirect_stdout(io.StringIO()):
            result = bundle.import_native(archive, bundle.validate(archive), self.cwd, **kwargs)
        return target, result[0]

    def test_inventory_exact_bounded_prefix_and_stable_keys(self):
        original = self.base.read_bytes()
        with self.base.open('ab') as out:
            out.write(b'{"future":"not inherited"}\n{incomplete')
        files = p.raw_files('codex', self.sid, home=self.home)
        self.assertEqual([f['file_key'] for f in files], ['main', 'base-' + self.base_id])
        self.assertEqual(files[1]['role'], 'context')
        self.assertEqual(files[1]['mode'], 'append')
        self.assertEqual(files[1]['complete_length'], len(original))
        self.assertEqual(files[1]['sha256'], hashlib.sha256(original).hexdigest())
        self.assertGreater(files[1]['size'], files[1]['complete_length'])
        self.assertTrue(p.package_descriptor('codex', self.sid, home=self.home)['available'])

    def test_view_inherits_and_binds_every_file(self):
        result = p.view_records('codex', self.sid, home=self.home)
        text = json.dumps(result['records'])
        self.assertIn('BASE-ANSWER', text)
        self.assertIn('CHILD-ANSWER', text)
        self.assertEqual(len(result['source_files']), 2)
        self.assertTrue(result['records'][0]['inherited'])
        self.assertFalse(result['records'][-1]['inherited'])

    def test_view_pagination_and_required_prefix_mutation(self):
        first = p.view_records('codex', self.sid, home=self.home, limit=1)
        next_page = p.view_records('codex', self.sid, home=self.home, position=first['position'])
        self.assertEqual(next_page['records'][0]['seq'], 2)
        self.base.write_bytes(self.base.read_bytes().replace(b'BASE-QUESTION', b'EDIT-QUESTION'))
        reset = p.view_records('codex', self.sid, home=self.home, position=next_page['position'])
        self.assertTrue(reset['reset_required'])

    def test_tool_correlation_across_base_and_child(self):
        rows = [json.loads(line) for line in self.base.read_bytes().splitlines()]
        rows[2]['payload'] = {'type': 'function_call', 'name': 'read', 'call_id': 'synthetic-call', 'arguments': '{}'}
        self.base.write_bytes(b''.join((json.dumps(r) + '\n').encode() for r in rows))
        self.position['end_byte_offset'] = self.base.stat().st_size
        self.change_meta(self.child, lambda r: r['payload'].update(history_base=self.position))
        rows = [json.loads(line) for line in self.child.read_bytes().splitlines()]
        rows[2]['payload'] = {'type': 'function_call_output', 'call_id': 'synthetic-call', 'output': 'ONLY-TOOL-EVIDENCE'}
        self.child.write_bytes(b''.join((json.dumps(r) + '\n').encode() for r in rows))
        result = p.view_records('codex', self.sid, home=self.home)['records']
        tool = next(r for r in result if r['kind'] == 'tool_result')
        self.assertEqual(tool['call_id'], 'synthetic-call')
        self.assertIn('ONLY-TOOL-EVIDENCE', tool['text'])

    def test_three_level_chain_and_independent_restore(self):
        _, _, position = write_rollout(self.home, base=self.position, label='MIDDLE', archived=True)
        self.child.unlink()
        self.child, self.sid, _ = write_rollout(self.home, base=position, label='CHILD')
        originals = {path: path.read_bytes() for path in self.home.rglob('*.jsonl')}
        target, installed = self.restore()
        nodes = h.lineage(target, Path(installed['path']))
        self.assertEqual(len(nodes), 3)
        self.assertNotEqual(installed['id'], self.sid)
        old_ids = {h.identity(path)[1] for path in originals}
        self.assertFalse(old_ids & {n.rollout_id for n in nodes})
        for n in nodes[1:]:
            self.assertIsNotNone(n.meta['history_base'])
            self.assertEqual(n.meta['history_mode'], 'paginated')
        self.assertIn(b'synthetic-opaque-ciphertext', nodes[0].path.read_bytes())
        for path, contents in originals.items():
            self.assertEqual(path.read_bytes(), contents)

    def test_v3_package_raw_members_and_recomputed_offsets(self):
        package = self.export()
        with zipfile.ZipFile(package) as archive:
            entry = bundle.validate(archive)['sessions'][0]
            by_key = {item['file_key']: archive.read(item['file']) for item in entry['files']}
        self.assertEqual(by_key['main'], self.child.read_bytes())
        self.assertEqual(by_key['base-' + self.base_id], self.base.read_bytes())
        target, installed = self.restore(package)
        nodes = h.lineage(target, Path(installed['path']))
        self.assertEqual(nodes[-1].meta['history_base']['end_byte_offset'], nodes[0].path.stat().st_size)
        self.assertNotEqual(nodes[-1].meta['history_base']['end_byte_offset'], self.position['end_byte_offset'])
        self.assertEqual(nodes[-1].meta['history_base']['end_ordinal_exclusive'], self.position['end_ordinal_exclusive'])

    def test_repeated_imports_create_disjoint_graphs(self):
        package = self.export()
        target, a = self.restore(package)
        _, b = self.restore(package, target)
        one = {n.rollout_id for n in h.lineage(target, Path(a['path']))}
        two = {n.rollout_id for n in h.lineage(target, Path(b['path']))}
        self.assertFalse(one & two)

    def test_revert_owner_and_physical_rollout_are_distinct(self):
        old = self.base
        physical_id = str(uuid.uuid4())
        self.base = old.with_name(old.name.replace(self.base_id, self.base_id + '_' + physical_id))
        old.rename(self.base)
        self.position['thread_id'] = physical_id
        self.change_meta(self.child, lambda r: r['payload'].update(history_base=self.position))
        self.assertEqual(p.raw_files('codex', self.sid, home=self.home)[1]['file_key'], 'base-' + physical_id)
        target, installed = self.restore()
        nodes = h.lineage(target, Path(installed['path']))
        self.assertNotEqual(nodes[0].rollout_id, physical_id)
        self.assertEqual(nodes[-1].meta['history_base']['thread_id'], nodes[0].rollout_id)

    def test_current_revert_discovery_uses_stable_owner(self):
        new = self.child.with_name(self.child.name.replace(self.sid, self.sid + '_' + str(uuid.uuid4())))
        self.child.rename(new)
        ids = [r['native_id'] for r in p.list_sessions('codex', home=self.home)]
        self.assertIn(self.sid, ids)
        self.assertEqual(p.raw_files('codex', self.sid, home=self.home)[0]['path'], str(new))

    def test_missing_base_is_unavailable_not_partial_success(self):
        self.base.unlink()
        self.assertEqual(p.package_descriptor('codex', self.sid, home=self.home)['reason'], 'history_base_missing')
        with self.assertRaisesRegex(p.ProviderError, 'history_base_missing'):
            p.raw_files('codex', self.sid, home=self.home)

    def test_bad_boundaries_and_ordinals_reject(self):
        original = self.child.read_bytes()
        for key, value in [('end_byte_offset', 7), ('end_byte_offset', self.position['end_byte_offset'] + 99),
                           ('end_ordinal_exclusive', self.position['end_ordinal_exclusive'] + 1)]:
            with self.subTest(key=key, value=value):
                self.child.write_bytes(original)
                self.change_meta(self.child, lambda r: r['payload']['history_base'].update({key: value}))
                with self.assertRaises(h.HistoryError):
                    h.lineage(self.home, self.child)

    def test_cycle_and_depth_limit(self):
        self.change_meta(self.child, lambda r: r['payload']['history_base'].update(thread_id=self.sid))
        with self.assertRaisesRegex(h.HistoryError, 'history_base_cycle'):
            h.lineage(self.home, self.child)
        self.change_meta(self.child, lambda r: r['payload'].update(history_base=self.position))
        with patch.object(h, 'MAX_DEPTH', 0), self.assertRaisesRegex(h.HistoryError, 'depth_limit'):
            h.lineage(self.home, self.child)

    def test_invalid_reference_shapes_are_explicit(self):
        for value in [{'thread_id': '../../bad'}, {**self.position, 'unknown': True},
                      {**self.position, 'end_byte_offset': True}]:
            with self.subTest(value=value), self.assertRaises(h.HistoryError):
                h.reference(value)

    def test_nonpaginated_ancestor_and_malformed_prefix(self):
        raw = self.base.read_bytes()
        self.change_meta(self.base, lambda r: r['payload'].update(history_mode='legacy'))
        with self.assertRaisesRegex(h.HistoryError, 'not_paginated'):
            h.lineage(self.home, self.child)
        self.base.write_bytes(raw.replace(b'"ordinal": 0', b'"ordinal": 9'))
        with self.assertRaisesRegex(h.HistoryError, 'ordinal_mismatch'):
            h.lineage(self.home, self.child)

    def test_symlink_base_rejected(self):
        moved = self.root / 'external.jsonl'
        self.base.rename(moved)
        self.base.symlink_to(moved)
        with self.assertRaisesRegex(h.HistoryError, 'symlink'):
            h.lineage(self.home, self.child)

    def test_conflicting_copies_not_silently_selected(self):
        duplicate = self.home / 'sessions' / self.base.name
        duplicate.parent.mkdir(exist_ok=True)
        duplicate.write_bytes(self.base.read_bytes())
        self.assertEqual(len(h.lineage(self.home, self.child)), 2)
        duplicate.write_bytes(duplicate.read_bytes().replace(b'BASE-ANSWER', b'EDIT-ANSWER'))
        with self.assertRaisesRegex(h.HistoryError, 'ambiguous_history_base'):
            h.lineage(self.home, self.child)

    def test_incomplete_primary_tail_is_not_packaged(self):
        complete = self.child.read_bytes()
        with self.child.open('ab') as out:
            out.write(b'{"unfinished":')
        self.assertEqual(p.raw_files('codex', self.sid, home=self.home)[0]['complete_length'], len(complete))
        self.assertTrue(p.package_descriptor('codex', self.sid, home=self.home)['available'])

    def test_title_override_does_not_silently_discard_lineage(self):
        with self.assertRaisesRegex(ValueError, 'title override'):
            self.restore(name='unsupported')

    def test_import_detaches_account_binding_but_keeps_context_window(self):
        self.change_meta(self.child, lambda r: r['payload'].update(account_scoped=True,
            creator_user_id='synthetic-user', creator_account_id='synthetic-account',
            context_window={'window_id': 'synthetic-window'}))
        _, installed = self.restore()
        meta, _ = h.header(Path(installed['path']))
        self.assertFalse(meta['payload']['account_scoped'])
        self.assertNotIn('creator_user_id', meta['payload'])
        self.assertNotIn('creator_account_id', meta['payload'])
        self.assertEqual(meta['payload']['context_window'], {'window_id': 'synthetic-window'})

    def test_attached_thread_profile_is_explicitly_unverified(self):
        self.change_meta(self.child, lambda r: r['payload'].update(attached_thread=True))
        self.assertEqual(p.package_descriptor('codex', self.sid, home=self.home)['reason'],
                         'history_base_attached_thread_unverified')

    def test_missing_compression_decoder_is_explicit(self):
        import builtins
        original = builtins.__import__
        def without_zstd(name, *args, **kwargs):
            if name == 'compression':
                raise ImportError('synthetic unavailable decoder')
            return original(name, *args, **kwargs)
        with patch('builtins.__import__', side_effect=without_zstd), patch('shutil.which', return_value=None):
            with self.assertRaisesRegex(h.HistoryError, 'zstd_decoder_unavailable'):
                with h.decoded(self.root / 'synthetic.jsonl.zst'):
                    pass

    def test_failed_publication_rolls_back_entire_graph(self):
        package = self.export()
        target = self.root / 'failed'
        real_link = os.link
        calls = []
        def fail(source, destination):
            calls.append(destination)
            if len(calls) == 2:
                raise OSError('synthetic publication failure')
            return real_link(source, destination)
        with patch('os.link', side_effect=fail), self.assertRaises(OSError):
            self.restore(package, target)
        self.assertFalse(list(target.rglob('*.jsonl')))

    def run_export_cli(self, *flags):
        env = {**os.environ, 'HOME': str(self.root), 'CODEX_HOME': str(self.home),
               'CLAUDE_CONFIG_DIR': str(self.root / 'claude')}
        return subprocess.run([sys.executable, str(Path(__file__).resolve().parents[1] / 'csl'),
            'export', str(self.child), '--provider', 'codex', '--to', str(self.root / 'cli.zip'),
            *flags], env=env, capture_output=True, text=True)

    def test_raw_cli_uses_v3_and_portable_rejects_missing_context(self):
        portable = self.run_export_cli()
        self.assertNotEqual(portable.returncode, 0)
        self.assertIn('single-session --raw', portable.stderr)
        self.assertFalse((self.root / 'cli.zip').exists())
        raw = self.run_export_cli('--raw')
        self.assertEqual(raw.returncode, 0, raw.stderr)
        with zipfile.ZipFile(self.root / 'cli.zip') as archive:
            manifest = bundle.validate(archive)
            self.assertEqual(manifest['version'], 3)
            self.assertEqual(len(manifest['sessions'][0]['files']), 2)

    def test_raw_cli_rejects_scrub_instead_of_ignoring_it(self):
        result = self.run_export_cli('--raw', '--scrub-secrets')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('cannot be scrubbed', result.stderr)
        self.assertFalse((self.root / 'cli.zip').exists())

    def test_forged_graph_is_rejected(self):
        package = self.export()
        with zipfile.ZipFile(package) as archive:
            contents = {name: archive.read(name) for name in archive.namelist()}
        manifest = json.loads(contents['manifest.json'])
        manifest['sessions'][0]['codex_history']['nodes'][0]['decoded_length'] += 1
        contents['manifest.json'] = json.dumps(manifest).encode()
        bad = self.root / 'forged.zip'
        with zipfile.ZipFile(bad, 'w') as archive:
            for name, body in contents.items():
                archive.writestr(name, body)
        with self.assertRaisesRegex(h.HistoryError, 'manifest_mismatch'):
            self.restore(bad)

    @unittest.skipUnless(shutil.which('zstd'), 'optional zstd CLI for synthetic compressed base')
    def test_compressed_base_is_raw_preserved_and_decoded_on_restore(self):
        raw = self.base.read_bytes()
        compressed = Path(str(self.base) + '.zst')
        compressed.write_bytes(subprocess.run(['zstd', '-q', '-c'], input=raw, capture_output=True, check=True).stdout)
        self.base.unlink()
        files = p.raw_files('codex', self.sid, home=self.home)
        self.assertEqual(files[1]['mode'], 'rewrite')
        self.assertEqual(files[1]['sha256'], hashlib.sha256(compressed.read_bytes()).hexdigest())
        view = p.view_records('codex', self.sid, home=self.home)
        self.assertEqual(view['records'][0]['raw_offset']['offset'], 0)
        self.assertIn('native_pointer', view['records'][0])
        target, installed = self.restore()
        nodes = h.lineage(target, Path(installed['path']))
        self.assertIn(b'BASE-ANSWER', nodes[0].path.read_bytes())

    @unittest.skipUnless(shutil.which('zstd'), 'optional zstd CLI')
    def test_compressed_tail_is_secret_reviewed_even_outside_inheritance(self):
        from csl_secrets import SecretReview
        secret = 'ghp_' + 'Z' * 30
        raw = self.base.read_bytes() + ('{"tail":"' + secret + '"}\n').encode()
        compressed = Path(str(self.base) + '.zst')
        compressed.write_bytes(subprocess.run(['zstd', '-q', '-c'], input=raw, capture_output=True, check=True).stdout)
        self.base.unlink()
        findings = []
        def inspect(review):
            findings.extend(review.findings)
            raise ValueError('synthetic review cancellation')
        with patch.object(SecretReview, 'resolve', inspect), self.assertRaisesRegex(ValueError, 'cancellation'):
            self.export()
        self.assertIn(secret, findings)

    def test_same_owner_revert_chain_keeps_separate_physical_identity(self):
        self.child.unlink()
        self.child, self.sid, _ = write_rollout(self.home, owner=self.base_id,
            rollout=str(uuid.uuid4()), base=self.position, label='REVERT')
        target, installed = self.restore()
        nodes = h.lineage(target, Path(installed['path']))
        self.assertEqual(nodes[0].owner, nodes[1].owner)
        self.assertNotEqual(nodes[0].rollout_id, nodes[1].rollout_id)

    @unittest.skipUnless(shutil.which('zstd'), 'optional zstd CLI')
    def test_corrupt_compressed_base_rejected(self):
        compressed = Path(str(self.base) + '.zst')
        compressed.write_bytes(b'invalid-zstd')
        self.base.unlink()
        with self.assertRaises(h.HistoryError):
            h.lineage(self.home, self.child)

    @unittest.skipUnless(os.environ.get('CSL_RUN_CODEX_NATIVE_TESTS') == '1' and shutil.which('codex'),
                         'opt in to offline native Codex thread/read')
    def test_native_loader_reads_inherited_and_local_history(self):
        target, installed = self.restore()
        last_turn = next(row['payload']['turn_id'] for row, _, _, _ in h.lines(self.child)
                         if row.get('payload', {}).get('type') == 'task_complete')
        items, _ = native_history_items(self, self.root, target, installed['id'], last_turn)
        text = json.dumps(items)
        for marker in ('BASE-QUESTION', 'BASE-ANSWER', 'CHILD-QUESTION', 'CHILD-ANSWER'):
            self.assertIn(marker, text)

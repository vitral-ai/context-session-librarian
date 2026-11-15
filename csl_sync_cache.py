"""Private, periodically verified collector snapshots; never provider credentials.

Metadata is only an optimization hint. Changed inputs are frozen and verified
before use; unchanged inputs retain their previous completeness timestamp.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import time

from csl_sync_state import SyncError, private_directory, private_json, read_private_json

EMPTY = hashlib.sha256(b'').hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()


def hash_prefix(path, length):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        while length:
            chunk = stream.read(min(length, 1024 * 1024))
            if not chunk: raise SyncError('source_changed')
            h.update(chunk); length -= len(chunk)
    return h.hexdigest()


class CachedProviders:
    def __init__(self, module, state_root, *, verify_seconds=300, clock=time.time):
        self.module, self.clock, self.verify_seconds = module, clock, verify_seconds
        self.root = private_directory(Path(state_root) / 'provider-cache')
        self.entries, self.hashes, self.reused, self.listed = {}, {}, set(), {}

    def sources(self, provider, path, home, entry=None):
        paths = [path]
        if provider == 'codex':
            history = self.module.csl_codex_history
            try:
                # Cached graph topology is a hint only: changed inputs force
                # fresh lineage resolution before freezing or rendering.
                rollouts = entry.get('graph_rollouts') if entry else None
                if rollouts is None:
                    rollouts = ([n.rollout_id for n in history.lineage(home, path)[:-1]]
                                if history.has_base(path) else [])
                for rollout in rollouts:
                    candidates = []
                    for tree in ('sessions', 'archived_sessions'):
                        for candidate in (home/tree).rglob('rollout-*.jsonl*'):
                            ids = history.identity(candidate)
                            if ids and ids[1] == rollout:
                                history._safe(candidate, home)
                                candidates.append(candidate)
                    if not candidates: raise SyncError('history_base_missing')
                    # Include ignored compressed siblings and alternate copies:
                    # their arrival/change can alter native candidate selection.
                    paths.extend(sorted(candidates))
            except history.HistoryError as error:
                raise SyncError(str(error)) from None
            paths = list(dict.fromkeys(paths))
        if provider == 'claude':
            directory = path.with_suffix('')
            if directory.is_dir():
                for child in sorted(directory.rglob('*')):
                    if child.is_symlink(): raise SyncError('symlink_source')
                    if child.is_file() and child.relative_to(directory).parts[0] in ('subagents', 'tool-results'):
                        paths.append(child)
        for source in paths: self.module._safe(source, home)
        return paths

    def fingerprint(self, paths, home):
        result = []
        for path in paths:
            st = path.stat()
            result.append([path.relative_to(home).as_posix(), st.st_dev, st.st_ino,
                           st.st_size, st.st_mtime_ns, st.st_ctime_ns])
        return result

    def list_sessions(self, provider):
        if provider not in ('codex', 'claude'): return self.module.list_sessions(provider)
        home = self.module.provider_home(provider)
        rows, seen = [], set()
        discovered = self.module._paths(provider, home)
        self.listed[provider] = (home, discovered)
        for path in discovered:
            key = hashlib.sha256(encoded([provider, str(home), str(path)])).hexdigest()
            directory = private_directory(self.root / key)
            info_path = directory / 'entry.json'
            entry = read_private_json(info_path)
            paths = self.sources(provider, path, home, entry)
            stamp = self.fingerprint(paths, home)
            elapsed = self.clock() - entry['checked_at'] if entry else -1
            reusable = bool(entry and entry.get('schema') == 2 and entry.get('index_valid', True))
            reuse = bool(reusable and entry.get('fingerprint') == stamp and
                         0 <= elapsed < self.verify_seconds)
            if not reuse and reusable and entry.get('fingerprint') == stamp:
                # Expired or backwards wall clock: verify bytes, not merely stat.
                if self.verify_entry(entry):
                    entry['checked_at'] = self.clock()
                    private_json(info_path, entry)
                    reuse = True
            if not reuse:
                entry = self.prepare(provider, path, home, paths, stamp, directory, entry)
                private_json(info_path, entry)
            paths = [home / row[0] for row in entry['fingerprint']]
            sid = entry['metadata']['native_id']
            if sid in seen: raise SyncError('ambiguous_native_id')
            seen.add(sid)
            self.entries[(provider, sid)] = (directory, entry, paths, home)
            if reuse: self.reused.add((provider, sid))
            else: self.reused.discard((provider, sid))
            row = dict(entry['metadata'])
            # Activity is an observation, not a cached claim about a process.
            row['state'] = 'archived' if 'archived_sessions' in path.parts else (
                'active' if 0 <= self.clock() - path.stat().st_mtime <= 300 else 'idle')
            rows.append(row)
        for key in list(self.entries):
            if key[0] == provider and key[1] not in seen:
                self.entries.pop(key)
                self.reused.discard(key)
        return sorted(rows, key=lambda r: (r['updated_at'] or '', r['native_id']), reverse=True)

    def changed(self):
        """A cheap wake hint, never evidence of integrity or completeness."""
        for provider, (home, previous_paths) in self.listed.items():
            if self.module._paths(provider, home) != previous_paths:
                return True
            for (p, _), (_, entry, paths, root) in self.entries.items():
                if p != provider or root != home: continue
                try:
                    actual = self.sources(provider, paths[0], home, entry)
                    if self.fingerprint(actual, home) != entry['fingerprint']: return True
                except (OSError, SyncError, ValueError): return True
        return False

    def load_hints(self, providers=('codex', 'claude')):
        """Supervisor-only: small metadata files, never transcript parsing."""
        self.entries, self.listed = {}, {}
        for provider in providers:
            if provider in ('codex', 'claude'):
                home = self.module.provider_home(provider)
                self.listed[provider] = (home, self.module._paths(provider, home))
        for path in self.root.glob('*/entry.json'):
            entry = read_private_json(path)
            provider = entry.get('metadata', {}).get('provider') if entry else None
            if provider not in self.listed or not entry.get('inventory') or entry.get('schema') != 2: continue
            home = self.listed[provider][0]
            primary = Path(entry['inventory'][0]['path'])
            if not primary.is_relative_to(home) or not primary.is_file(): continue
            sources = [Path(item['path']) for item in entry['inventory']]
            self.entries[(provider, entry['metadata']['native_id'])] = (path.parent, entry, sources, home)

    def verify_entry(self, entry):
        try:
            if entry.get('graph_rollouts'):
                for item in entry['graph_inputs']:
                    path = Path(item['path'])
                    if (path.is_symlink() or path.stat().st_size != item['size'] or
                            hash_prefix(path, item['size']) != item['sha256']):
                        return False
                return True
            for item in entry['inventory']:
                path = Path(item['path'])
                if path.is_symlink() or path.stat().st_size != item['size']:
                    return False
                if hash_prefix(path, item['complete_length']) != item['sha256']:
                    return False
                if item['mode'] == 'append':
                    with path.open('rb') as stream:
                        stream.seek(item['complete_length'])
                        if b'\n' in stream.read(item['size'] - item['complete_length']):
                            return False
            return True
        except OSError: return False

    def prepare(self, provider, path, home, paths, stamp, directory, previous):
        # Re-resolve references after any dependency hint changed. A base may
        # introduce another ancestor while the leaf itself remains untouched.
        paths = self.sources(provider, path, home)
        stamp = self.fingerprint(paths, home)
        stage = directory / 'staging'
        shutil.rmtree(stage, ignore_errors=True)
        private_directory(stage)
        try:
            inputs = []
            for source in paths:
                target = stage / source.relative_to(home)
                private_directory(target.parent)
                fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(fd, 'rb') as incoming, target.open('wb') as output:
                    os.fchmod(output.fileno(), 0o600)
                    size = os.fstat(incoming.fileno()).st_size
                    left = size
                    h = hashlib.sha256()
                    while left:
                        chunk = incoming.read(min(left, 1024 * 1024))
                        if not chunk: raise SyncError('source_changed')
                        output.write(chunk); h.update(chunk); left -= len(chunk)
                if hash_prefix(source, size) != h.hexdigest(): raise SyncError('source_changed')
                inputs.append({'path': str(source), 'size': size, 'sha256': h.hexdigest()})
            frozen = stage / path.relative_to(home)
            nodes = (self.module.csl_codex_history.lineage(stage, frozen)
                     if provider == 'codex' and self.module.csl_codex_history.has_base(frozen) else None)
            inventory = self.module._files(provider, frozen, stage)
            evidence = {}
            metadata = self.module._metadata(provider, frozen, stage, 300, files=inventory, evidence=evidence)
            # Keep the inventory frozen for this pass, but fence the originals
            # in the engine. Growth after this prefix is deferred to next pass.
            for item in inventory:
                item['path'] = str(home / Path(item['path']).relative_to(stage))
            binding = hashlib.sha256(encoded([{'file_key': f['file_key'], 'length': f['complete_length'],
                                               'sha256': f['sha256']} for f in inventory])).hexdigest()
            has_conversation = self.index_view(provider, frozen, inventory[0], directory, previous, nodes=nodes, binding=binding)
            reason = None if has_conversation else 'no_conversation'
            package = ({'available': False, 'reason': reason, 'descriptor_version': 1, 'manifest': None, 'members': []}
                       if reason else self.module._package_mapping(provider, metadata['native_id'], frozen.suffix, metadata, inventory, nodes=nodes))
            return {'schema': 2, 'metadata': metadata, 'inventory': inventory, 'package': package,
                    'index_valid': True,
                    'view_binding': binding,
                    'graph_rollouts': [n.rollout_id for n in nodes[:-1]] if nodes else [],
                    'graph_inputs': inputs if nodes else [],
                    'fingerprint': stamp, 'checked_at': self.clock(),
                    'content_signature': hashlib.sha256(encoded(inventory)).hexdigest()}
        finally:
            shutil.rmtree(stage, ignore_errors=True)

    def index_view(self, provider, path, primary, directory, previous, *, nodes=None, binding=None):
        db = sqlite3.connect(directory / 'view.sqlite')
        (directory / 'view.sqlite').chmod(0o600)
        try:
            db.execute('CREATE TABLE IF NOT EXISTS records (seq INTEGER PRIMARY KEY, body TEXT, digest TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY, value TEXT)')
            saved = db.execute('SELECT value FROM meta WHERE id=1').fetchone()
            saved = json.loads(saved[0]) if saved else None
            append = bool(not nodes and saved and saved['length'] <= primary['complete_length'] and
                          hash_prefix(path, saved['length']) == saved['sha256'])
            with db:
                if not append:
                    db.execute('DELETE FROM records')
                    saved = {'length': 0, 'seq': 0, 'tools': {}}
                h = self.hashes.get(str(directory)) if append else None
                if h is None:
                    h = hashlib.sha256()
                    if append:
                        for (body,) in db.execute('SELECT body FROM records ORDER BY seq'):
                            h.update(body.encode() + b'\n')
                else: h = h.copy()
                seq, tools = saved['seq'], saved['tools']
                has_conversation = saved.get('has_conversation', False)
                events = (self.module._codex_lineage_events(nodes, 16000) if nodes else
                          self.module._view(provider, path, 16000, primary['complete_length'],
                                            start_offset=saved['length'], tools=tools))
                for event in events:
                    seq += 1
                    has_conversation = has_conversation or event['kind'] in ('text','tool_call','tool_result','image','document')
                    body = encoded({'seq': seq, **event})
                    h.update(body + b'\n')
                    db.execute('INSERT INTO records VALUES (?,?,?)', (seq, body.decode(), h.hexdigest()))
                value = {'length': primary['complete_length'], 'sha256': primary['sha256'], 'seq': seq, 'tools': tools,
                         'has_conversation': has_conversation, 'binding': binding}
                db.execute('INSERT OR REPLACE INTO meta VALUES (1,?)', (json.dumps(value),))
            self.hashes[str(directory)] = h.copy()
            return has_conversation
        finally: db.close()

    def signature(self, provider, native_id):
        found = self.entries.get((provider, native_id))
        return found[1]['content_signature'] if found else None

    def unchanged(self, provider, native_id, signature):
        return ((provider, native_id) in self.reused and signature is not None and
                signature == self.signature(provider, native_id))

    def raw_files(self, provider, native_id):
        found = self.entries.get((provider, native_id))
        if not found: return self.module.raw_files(provider, native_id)
        _, entry, paths, home = found
        actual = self.sources(provider, paths[0], home, entry)
        if [p.relative_to(home) for p in actual] != [p.relative_to(home) for p in paths]:
            raise SyncError('source_changed')
        if entry.get('graph_rollouts'):
            # The leaf may append beyond its frozen prefix, but ancestry or
            # candidate changes require re-resolution, even outside a cutoff.
            if self.fingerprint(actual, home)[1:] != entry['fingerprint'][1:]:
                raise SyncError('source_changed')
        for item in entry['inventory']:
            if item['mode'] == 'rewrite' and Path(item['path']).stat().st_size != item['complete_length']:
                raise SyncError('source_changed')
        return entry['inventory']

    def package_descriptor(self, provider, native_id):
        found = self.entries.get((provider, native_id))
        return found[1]['package'] if found else self.module.package_descriptor(provider, native_id)

    def view_records(self, provider, native_id, *, position=None, limit=500, text_limit=16000):
        found = self.entries.get((provider, native_id))
        if not found:
            return self.module.view_records(provider, native_id, position=position, limit=limit, text_limit=text_limit)
        directory, entry, _, _ = found
        if text_limit != 16000 or not 1 <= limit <= 1000: raise SyncError('invalid_view_limit')
        target = position.get('next_seq') if isinstance(position, dict) else 1
        if (position is not None and (not isinstance(position, dict) or position.get('version') != 1 or
                position.get('provider') != provider or position.get('native_id') != native_id or
                position.get('text_limit') != text_limit)) or type(target) is not int or target < 1:
            raise SyncError('invalid_view_position')
        db = sqlite3.connect(directory / 'view.sqlite')
        try:
            saved = db.execute('SELECT value FROM meta WHERE id=1').fetchone()
            if (not saved or json.loads(saved[0])['sha256'] != entry['inventory'][0]['sha256'] or
                    json.loads(saved[0]).get('binding') != entry.get('view_binding')):
                entry['checked_at'] = 0
                entry['index_valid'] = False
                private_json(directory / 'entry.json', entry)
                raise SyncError('cache_recovery_required')
            prior = db.execute('SELECT digest FROM records WHERE seq=?', (target - 1,)).fetchone()
            digest = EMPTY if target == 1 else prior[0] if prior else None
            reset = bool(position and digest != position.get('prefix_sha256'))
            if reset: target, digest = 1, EMPTY
            page = [] if reset else db.execute('SELECT seq,body,digest FROM records WHERE seq>=? ORDER BY seq LIMIT ?', (target, limit)).fetchall()
            records = [json.loads(row[1]) for row in page]
            end = page[-1][0] + 1 if page else target
            if page: digest = page[-1][2]
            count = db.execute('SELECT max(seq) FROM records').fetchone()[0] or 0
            primary = entry['inventory'][0]
            result = {'records': records, 'position': {'version': 1, 'provider': provider, 'native_id': native_id,
                    'next_seq': end, 'prefix_sha256': digest, 'text_limit': text_limit},
                    'reset_required': reset, 'complete': not reset and end > count,
                    'reason': 'view_prefix_changed' if reset else None,
                    'source_file': {'file_key': primary['file_key'], 'length': primary['complete_length'], 'sha256': primary['sha256']}}
            if entry.get('graph_rollouts'):
                result['source_files'] = [{'file_key': f['file_key'], 'length': f['complete_length'],
                                           'sha256': f['sha256']} for f in entry['inventory']]
            return result
        finally: db.close()

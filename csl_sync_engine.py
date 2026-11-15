"""Opt-in, bounded snapshot publisher. Provider parsing remains provider-owned."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import urllib.parse
import uuid

from csl_sync_state import SyncError, now, private_directory
from csl_version import __version__

CHUNK = 8 * 1024 * 1024
EMPTY_HASH = hashlib.sha256(b'').hexdigest()
PROVIDERS = ('codex', 'claude', 'gemini', 'antigravity')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def segment(value):
    if not isinstance(value, str) or not value or len(value) > 2048 or value in ('.', '..') or any(ord(c) < 32 for c in value):
        raise SyncError('invalid_provider_identifier')
    return urllib.parse.quote(value, safe='')


def prefix(path, length):
    h = hashlib.sha256()
    with open(path, 'rb') as source:
        while length:
            data = source.read(min(1024 * 1024, length))
            if not data:
                raise SyncError('source_changed')
            h.update(data)
            length -= len(data)
    return h.hexdigest()


def safe_id(value):
    value = str(value)
    return value if len(value) <= 128 and not any(c in value for c in '/\\\r\n\t') else 'id-' + hashlib.sha256(value.encode()).hexdigest()[:16]


class Providers:
    """Lazy integration with the separately owned, documented provider interface."""
    def __init__(self):
        try:
            import csl_sync_providers
        except ImportError:
            raise SyncError('provider_interface_unavailable') from None
        self.module = csl_sync_providers

    def list_sessions(self, provider):
        return self.module.list_sessions(provider)

    def raw_files(self, provider, native_id):
        return self.module.raw_files(provider, native_id)

    def view_records(self, provider, native_id, **kwargs):
        return self.module.view_records(provider, native_id, **kwargs)

    def package_descriptor(self, provider, native_id):
        function = getattr(self.module, 'package_descriptor', None)
        return function(provider, native_id) if function else None


class Collector:
    def __init__(self, state, registry, providers, *, chunk_size=CHUNK):
        self.state, self.registry, self.providers = state, registry, providers
        self.chunk_size = min(max(chunk_size, 1), CHUNK)
        self.spool = private_directory(state.root / 'snapshots')

    def register(self):
        config = self.state.get('settings', {})
        body = {'label': config.get('label', 'CSL device'), 'platform': os.uname().sysname,
                'kind': config.get('kind', 'local'), 'environment': config.get('environment'), 'csl_version': __version__}
        return self.registry.call('PUT', '/devices/' + segment(self.state.get('device_id')), body=body,
                                  key=digest(body))

    def selected(self, provider, row, days, tracked):
        explicit = self.state.selection(provider, row['native_id'])
        if explicit is not None:
            return explicit
        if (provider, row['native_id']) in tracked or days is None:
            return True
        try:
            updated = datetime.fromisoformat(row['updated_at'].replace('Z', '+00:00'))
            return updated >= datetime.now(timezone.utc) - timedelta(days=days)
        except (ValueError, TypeError, AttributeError):
            return False  # Unknown age requires explicit inclusion, not an invented timestamp.

    def run(self, *, days=30, providers=PROVIDERS):
        # Called under the collector lock; only our private orphaned snapshots are removed.
        for path in self.spool.iterdir():
            if path.is_file() and not path.is_symlink():
                path.unlink()
        self.register()
        self.limits = self.registry.call('GET', '/limits')
        maximum_chunk = self.limits.get('max_chunk_bytes')
        if type(maximum_chunk) is int and maximum_chunk > 0:
            self.chunk_size = min(self.chunk_size, maximum_chunk)
        tracked, active, results = self.state.tracked(), [], []
        for provider in providers:
            try:
                rows = self.providers.list_sessions(provider)
            except Exception as error:
                code = getattr(error, 'code', 'provider_unavailable')
                results.append({'provider': provider, 'status': 'error', 'error': safe_id(code)})
                continue
            for metadata in rows:
                native_id = metadata['native_id']
                if not self.selected(provider, metadata, days, tracked):
                    continue
                if metadata.get('state') == 'active' and metadata.get('updated_at'):
                    active.append({'provider': provider, 'native_id': native_id, 'last_write_at': metadata['updated_at']})
                try:
                    self.sync_session(provider, native_id, metadata)
                    results.append({'provider': provider, 'native_id': safe_id(native_id), 'status': 'synced'})
                except Exception as error:
                    code = getattr(error, 'code', 'sync_failed')
                    local = self.state.session(provider, native_id)
                    local.update(status='error', last_error=safe_id(code))
                    self.state.save_session(provider, native_id, local)
                    results.append({'provider': provider, 'native_id': safe_id(native_id), 'status': 'error', 'error': safe_id(code)})
                    if code in ('credential_revoked', 'session_revoked', 'sign_in_required', 'refresh_outcome_unknown'):
                        raise SyncError(code) from None
        self.registry.call('POST', '/devices/' + segment(self.state.get('device_id')) + '/heartbeat',
                           body={'active_sessions': active}, key=str(uuid.uuid4()))
        self.state.set('last_pass_at', now())
        self.state.set('last_error', next((r['error'] for r in results if 'error' in r), None))
        return {'device_id': self.state.get('device_id'), 'sessions': results,
                'synced': sum(r['status'] == 'synced' for r in results), 'pending': sum(r['status'] != 'synced' for r in results)}

    def capture(self, item):
        key = item.get('file_key')
        if not isinstance(key, str) or not re.fullmatch(r'[A-Za-z0-9._-]{1,128}', key) or key in ('.', '..'):
            raise SyncError('invalid_file_key')
        length = item.get('complete_length')
        if type(length) is not int or length < 0 or item.get('mode') not in ('append', 'rewrite'):
            raise SyncError('invalid_provider_inventory')
        try:
            fd = os.open(item['path'], os.O_RDONLY | os.O_NOFOLLOW)
        except OSError:
            raise SyncError('source_unavailable') from None
        tmp_fd, temporary = tempfile.mkstemp(prefix='raw-', dir=self.spool)
        os.fchmod(tmp_fd, 0o600)
        h = hashlib.sha256()
        try:
            with os.fdopen(fd, 'rb') as source, os.fdopen(tmp_fd, 'wb') as target:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size < length:
                    raise SyncError('source_changed')
                left = length
                while left:
                    data = source.read(min(1024 * 1024, left))
                    if not data:
                        raise SyncError('source_changed')
                    target.write(data); h.update(data); left -= len(data)
                target.flush()
            if h.hexdigest() != item['sha256']:
                raise SyncError('source_changed')
            return {**item, 'spool': temporary, 'identity': [info.st_dev, info.st_ino]}
        except BaseException:
            Path(temporary).unlink(missing_ok=True)
            raise

    def check_sources(self, files):
        for item in files:
            try:
                fd = os.open(item['path'], os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(fd, 'rb') as source:
                    info = os.fstat(source.fileno())
                    if [info.st_dev, info.st_ino] != item['identity'] or info.st_size < item['complete_length']:
                        raise SyncError('source_changed')
                    h = hashlib.sha256(); left = item['complete_length']
                    while left:
                        data = source.read(min(left, 1024 * 1024))
                        if not data: raise SyncError('source_changed')
                        h.update(data); left -= len(data)
                if h.hexdigest() != item['sha256']:
                    raise SyncError('source_changed')
            except OSError:
                raise SyncError('source_changed') from None

    def build_view(self, provider, native_id, local, files):
        view = local.setdefault('view', {})
        view.setdefault('generation', str(uuid.uuid4()))
        position, sequence = view.get('position'), view.get('last_seq', 0)
        lengths = {f['file_key']: f['complete_length'] for f in files}
        snapshots = {f['file_key']: {'file_key': f['file_key'], 'length': f['complete_length'],
                                    'sha256': f['sha256']} for f in files}
        pages = []
        resets = 0
        def bind_sources(page):
            anchors = page.get('source_files', [page.get('source_file')])
            if not isinstance(anchors, list) or not anchors:
                raise SyncError('source_changed')
            keys = set()
            for anchor in anchors:
                if (not isinstance(anchor, dict) or anchor.get('file_key') in keys or
                        snapshots.get(anchor.get('file_key')) != anchor):
                    raise SyncError('source_changed')
                keys.add(anchor['file_key'])
            if (provider == 'antigravity' or provider == 'codex' and len(files)>1) and ('source_files' not in page or keys != set(snapshots)):
                raise SyncError('source_changed')
            if page.get('source_file') is not None and page['source_file'] not in anchors:
                raise SyncError('source_changed')
        for _ in range(100000):
            page = self.providers.view_records(provider, native_id, position=position, limit=100)
            bind_sources(page)
            if page.get('reset_required'):
                if resets: raise SyncError('source_changed')
                for p in pages: Path(p).unlink(missing_ok=True)
                pages = []; resets += 1; position = None; sequence = 0
                view.update(generation=str(uuid.uuid4()), position=None, last_seq=0)
                continue
            records = page.get('records')
            if not isinstance(records, list) or len(records) > 100:
                raise SyncError('invalid_provider_view')
            eligible = []
            for record in records:
                offset = record.get('raw_offset') or {}
                value = offset.get('byte_offset', offset.get('offset'))
                if offset.get('file_key') not in lengths or type(value) is not int or value < 0:
                    raise SyncError('invalid_provider_offset')
                if value >= lengths[offset['file_key']]:
                    break
                eligible.append(record)
            bounded_at_snapshot = len(eligible) < len(records)
            if bounded_at_snapshot:
                if not eligible: break
                page = self.providers.view_records(provider, native_id, position=position, limit=len(eligible))
                bind_sources(page)
                if page.get('reset_required') or page.get('records') != eligible:
                    raise SyncError('source_changed')
            if not eligible: break
            wire = []
            for record in eligible:
                if (not isinstance(record.get('text'), str) or len(record['text']) > 16000 or
                        record.get('role') not in ('user','assistant','system','tool') or
                        record.get('kind') not in ('text','tool_call','tool_result','image','document','notice')):
                    raise SyncError('invalid_provider_view')
                normalized = dict(record)
                if type(record.get('seq')) is not int or record['seq'] != sequence + 1:
                    raise SyncError('invalid_provider_sequence')
                sequence += 1; normalized['seq'] = sequence
                offset = dict(record['raw_offset'])
                if 'byte_offset' in offset: offset['offset'] = offset.pop('byte_offset')
                normalized['raw_offset'] = offset
                wire.append(normalized)
            fd, name = tempfile.mkstemp(prefix='view-', dir=self.spool)
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, 'w') as target:
                json.dump({'records': wire, 'position': page['position']}, target, separators=(',', ':'))
            pages.append(name); position = page['position']
            if bounded_at_snapshot or page.get('complete', False): break
        else:
            raise SyncError('view_page_budget_exceeded')
        return pages, sequence

    def sync_session(self, provider, native_id, metadata):
        local = self.state.session(provider, native_id)
        local.update(status='syncing', last_error=None)
        files, pages = [], []
        try:
            inventory = self.providers.raw_files(provider, native_id)
            if not inventory: raise SyncError('source_incomplete')
            seen = set()
            for item in inventory:
                if item.get('file_key') in seen: raise SyncError('duplicate_file_key')
                maximum = getattr(self, 'limits', {}).get('max_file_bytes')
                if type(maximum) is int and item.get('complete_length', 0) > maximum:
                    raise SyncError('limit_exceeded', status=413)
                seen.add(item.get('file_key')); files.append(self.capture(item))
            pages, last_seq = self.build_view(provider, native_id, local, files)
            self.check_sources(files)
            # Detect newly appearing/disappearing companions as well as changed
            # bytes. A DB-only fence cannot detect a WAL-only transaction.
            def signature(rows):
                return {row['file_key']: (row['mode'], row['complete_length'], row['sha256']) for row in rows}
            refreshed = self.providers.raw_files(provider, native_id)
            if len(refreshed) != len(files) or signature(refreshed) != signature(files):
                raise SyncError('source_changed')
            planned = []
            for item in files:
                key = item['file_key']; previous = local.setdefault('files', {}).get(key, {})
                offset = previous.get('offset', 0)
                reuse = previous and previous.get('identity') == item['identity'] and previous.get('mode') == item['mode']
                if item['mode'] == 'rewrite':
                    reuse = reuse and previous.get('sha256') == item['sha256'] and previous.get('length') == item['complete_length']
                else:
                    reuse = (reuse and item['complete_length'] >= previous.get('length', 0) and
                             offset <= item['complete_length'] and prefix(item['spool'], offset) == previous.get('offset_sha256', EMPTY_HASH))
                if not reuse:
                    previous = {'revision': str(uuid.uuid4()), 'offset': 0, 'offset_sha256': EMPTY_HASH}
                previous.update(identity=item['identity'], mode=item['mode'], length=item['complete_length'], sha256=item['sha256'])
                local['files'][key] = previous
                planned.append({'file_key': key, 'role': item['role'], 'mode': 'replace' if item['mode'] == 'rewrite' else 'append',
                                'revision': previous['revision'], 'length': item['complete_length'], 'sha256': item['sha256']})
            local['files'] = {key: value for key, value in local['files'].items() if key in seen}
            package = self.providers.package_descriptor(provider, native_id)
            if isinstance(package, dict) and package.get('available') and {m['file_key'] for m in package.get('members', [])} != seen:
                raise SyncError('source_changed')
            if isinstance(package, dict) and isinstance(package.get('manifest'), dict):
                local.setdefault('package_created_at', now())
                package = {**package, 'manifest': {**package['manifest'], 'created_at': local['package_created_at']}}
            snapshot = {'files': planned, 'view': {'generation': local['view']['generation'], 'last_seq': last_seq}, 'package': package}
            snapshot_id = str(uuid.uuid5(uuid.NAMESPACE_URL, self.state.get('device_id') + '/' + provider + '/' + native_id + '/' + digest(snapshot)))
            local.update(snapshot_id=snapshot_id, status='syncing')
            self.state.save_session(provider, native_id, local)  # Revision/generation fixed before the first remote write.
            path = '/sessions/' + segment(provider) + '/' + segment(native_id)
            body = {k: metadata.get(k) for k in ['title','cwd','repository','created_at','updated_at','model','record_count','byte_count','parent','origin','state']}
            captured_bytes = sum(f['complete_length'] for f in files)
            if body['byte_count'] != captured_bytes:
                raise SyncError('source_changed')  # Ask the provider for a fresh consistent inventory next pass.
            body['byte_count'] = captured_bytes
            body.update(snapshot, device_id=self.state.get('device_id'), snapshot_id=snapshot_id)
            self.registry.call('PUT', path, body=body, key=digest(body))
            remote_files, cursor = {}, None
            for _ in range(10000):
                suffix = '/files?limit=100' + ('&cursor=' + urllib.parse.quote(cursor, safe='') if cursor else '')
                remote = self.registry.call('GET', path + suffix)
                if not isinstance(remote.get('items'), list): raise SyncError('invalid_server_response')
                remote_files.update({(r['file_key'], r['revision']): r for r in remote.get('items', [])})
                next_cursor = remote.get('next_cursor')
                if not next_cursor: break
                if not isinstance(next_cursor, str) or next_cursor == cursor:
                    raise SyncError('invalid_server_cursor')
                cursor = next_cursor
            else:
                raise SyncError('registry_page_budget_exceeded')
            for item in files:
                checkpoint = local['files'][item['file_key']]
                self.upload(path, item, checkpoint, remote_files.get((item['file_key'], checkpoint['revision'])),
                            on_progress=lambda: self.state.save_session(provider, native_id, local))
                self.state.save_session(provider, native_id, local)
            for page_path in pages:
                page = json.loads(Path(page_path).read_text())
                payload = {'generation': local['view']['generation'], 'first_seq': page['records'][0]['seq'], 'records': page['records']}
                self.registry.call('PUT', path + '/view', body=payload, key=digest(payload))
                local['view'].update(position=page['position'], last_seq=page['records'][-1]['seq'])
                self.state.save_session(provider, native_id, local)
            local.update(status='synced', last_synced_at=now(), last_error=None)
            self.state.save_session(provider, native_id, local)
        except SyncError:
            self.state.save_session(provider, native_id, local)
            raise
        except OSError:
            raise SyncError('source_unavailable') from None
        finally:
            for item in files: Path(item['spool']).unlink(missing_ok=True)
            for page in pages: Path(page).unlink(missing_ok=True)

    def upload(self, path, item, checkpoint, remote, *, on_progress=lambda: None):
        offset = remote.get('length', 0) if remote else 0
        if type(offset) is not int or not 0 <= offset <= item['complete_length']:
            checkpoint.update(revision=str(uuid.uuid4()), offset=0, offset_sha256=EMPTY_HASH)
            raise SyncError('remote_prefix_mismatch')
        if remote and prefix(item['spool'], offset) != remote.get('sha256'):
            checkpoint.update(revision=str(uuid.uuid4()), offset=0, offset_sha256=EMPTY_HASH)
            raise SyncError('remote_prefix_mismatch')
        h = hashlib.sha256()
        checkpoint.update(offset=offset, offset_sha256=remote['sha256'] if remote else EMPTY_HASH)
        on_progress()
        with open(item['spool'], 'rb') as source:
            left = offset
            while left:
                part = source.read(min(left, 1024 * 1024)); h.update(part); left -= len(part)
            empty = item['complete_length'] == 0 and not remote
            while offset < item['complete_length'] or empty:
                data = source.read(min(self.chunk_size, item['complete_length'] - offset))
                end = offset + len(data)
                headers = {'X-Start': str(offset), 'X-End': str(end), 'X-Content-SHA256': hashlib.sha256(data).hexdigest()}
                if offset: headers['X-Prefix-SHA256'] = h.hexdigest()
                suffix = '/files/' + segment(item['file_key']) + '/content?' + urllib.parse.urlencode({
                    'revision': checkpoint['revision'], 'mode': 'replace' if item['mode'] == 'rewrite' else 'append'})
                key = digest({'path': path, 'revision': checkpoint['revision'], 'file_key': item['file_key'],
                              'mode': item['mode'], 'prefix': headers.get('X-Prefix-SHA256'),
                              'start': offset, 'end': end, 'sha256': headers['X-Content-SHA256']})
                try:
                    receipt = self.registry.call('PUT', path + suffix, binary=data, headers=headers, key=key)
                except SyncError as error:
                    if error.code == 'prefix_mismatch':
                        checkpoint.update(revision=str(uuid.uuid4()), offset=0, offset_sha256=EMPTY_HASH)
                    raise
                h.update(data)
                if receipt.get('length') != end or receipt.get('sha256') != h.hexdigest() or receipt.get('revision') != checkpoint['revision']:
                    raise SyncError('upload_receipt_mismatch')
                offset = end; empty = False
                checkpoint.update(offset=offset, offset_sha256=h.hexdigest())
                on_progress()

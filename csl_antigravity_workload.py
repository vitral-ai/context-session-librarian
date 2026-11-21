"""Antigravity CLI 1.5.9 sanitized workload projection; view-only, never a raw upload."""
import hashlib
from pathlib import Path
import re

import csl_antigravity as native
from csl_sync_state import SyncError
from csl_workload_provider import PreparedProvider
from csl_workload_stream import Bounds, ProjectionStore, Redactor, encoded

PROFILE = 'antigravity-1.5.9-sqlite-text-v1'
PROFILE_MARKER = 'workload_profile_v1'
SOURCE = re.compile(r'conversations/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.db')
VIEW_FIELDS = ('time', 'role', 'kind', 'text', 'tool', 'call_id', 'truncated', 'native_pointer')


def source_id(relative):
    match = SOURCE.fullmatch(relative) if isinstance(relative, str) else None
    if match is None: raise SyncError('workload_source_refused')
    return match.group(1)


class WorkloadAntigravityProvider(PreparedProvider):
    """Reads private copies of conversations/<uuid>.db and uploads only redacted view text."""
    provider = 'antigravity'
    profile = PROFILE

    def __init__(self, sources, store, *, bounds=None, extra_literals=(), capture=None):
        if capture is not None: raise SyncError('workload_source_refused')
        self.capture = None
        self.bounds = bounds or Bounds()
        if not isinstance(sources, tuple) or not 1 <= len(sources) <= self.bounds.files:
            raise SyncError('workload_source_limit')
        store_root = Path(store).expanduser().absolute()
        for source in sources:
            if not isinstance(source, tuple) or len(source) != 2: raise SyncError('workload_source_refused')
            root, relative = source
            root = Path(root).expanduser().absolute()
            source_id(relative)
            if root.resolve() != root or store_root.is_relative_to(root) or root.is_relative_to(store_root):
                raise SyncError('workload_source_refused')
        self.sources = sources
        self.store = ProjectionStore(store_root, self.bounds)
        self.scratch = self.store.private_root / 'antigravity-read'
        self.scratch.mkdir(mode=0o700, exist_ok=True)
        self.redactor = Redactor(self.bounds, extra_literals)
        self.prepared = {}

    @property
    def policy_fingerprint(self):
        return hashlib.sha256(encoded({'profile': self.profile,
                                       'redaction_sha256': self.redactor.fingerprint})).hexdigest()

    def _read(self, root, relative):
        sid = source_id(relative)
        try:
            # Private scratch under the collector state: the image root filesystem is read-only.
            with native.snapshot(Path(root), sid, scratch=self.scratch) as (db, files):
                info = native.metadata(db, sid)
                rows = [{name: row.get(name) for name in VIEW_FIELDS} for row in native.views(db, 16000)]
        except native.FormatError as error:
            code = str(error) if str(error) == 'antigravity_source_changing' else 'workload_antigravity_format_refused'
            raise SyncError(code) from None
        if len(rows) > self.bounds.records: raise SyncError('workload_view_limit')
        return sid, info, files, rows

    def view_records(self, provider, native_id, **options):
        # The engine requires every Antigravity view page to bind its source files.
        page = super().view_records(provider, native_id, **options)
        page['source_files'] = [page['source_file']]
        return page

    def list_sessions(self, provider):
        if provider != self.provider: raise SyncError('unsupported_provider')
        entries = {}
        for root, relative in self.sources:
            sid, info, files, rows = self._read(root, relative)
            if sid in entries: raise SyncError(self.duplicate_code)
            clean = self.redactor.clean(rows)
            output, view, title = bytearray(), [], None
            for row in clean:
                if row['kind'] not in ('text', 'tool_call', 'tool_result', 'notice') or row['role'] not in (
                        'user', 'assistant', 'system'):
                    raise SyncError('workload_antigravity_format_refused')
                offset = len(output)
                line = encoded({**row, PROFILE_MARKER: self.profile}) + b'\n'
                if len(line) > self.bounds.record_bytes or len(output) + len(line) > self.bounds.output_bytes:
                    raise SyncError('workload_output_limit')
                output.extend(line)
                if title is None and row['role'] == 'user' and row['kind'] == 'text': title = row['text'][:200]
                view.append({'seq': len(view) + 1, **row, 'raw_offset': {'file_key': 'main', 'offset': offset}})
            output = bytes(output)
            metadata = {'provider': self.provider, 'native_id': sid, 'title': (title or sid)[:200],
                'cwd': None, 'repository': None, 'created_at': info['created_at'], 'updated_at': info['updated_at'],
                'model': None, 'record_count': info['record_count'], 'byte_count': len(output), 'parent': None,
                'origin': 'native', 'state': 'idle'}
            source_key = str(Path(root).absolute()) + '/' + relative
            path = self.store.publish(source_key, output)
            cursor = {'files': [{key: item[key] for key in ('file_key', 'complete_length', 'sha256')} for item in files]}
            self.store.checkpoint(source_key, cursor, self.policy_fingerprint, output)
            descriptor = {'file_key': 'main', 'path': str(path), 'relative_path': 'session.jsonl',
                'mode': 'append', 'complete_length': len(output), 'size': len(output),
                'sha256': hashlib.sha256(output).hexdigest(), 'role': 'transcript'}
            entries[sid] = {'metadata': metadata, 'file': descriptor, 'view': view, 'source_cursor': cursor,
                'projection_version': 1, 'policy_sha256': self.policy_fingerprint}
        self.prepared = entries
        return [entry['metadata'] for entry in entries.values()]

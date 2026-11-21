"""Shared sanitized spool/view boundary; no native discovery or upload authority."""
import hashlib
from pathlib import Path

from csl_sync_state import SyncError
from csl_workload_stream import Bounds, ProjectionStore, Redactor, encoded, read_complete, verify_prefix

EMPTY = hashlib.sha256(b'').hexdigest()


class PreparedProvider:
    """One locked collector, one format, explicit transcript sources only."""
    provider = None
    duplicate_code = 'workload_duplicate_native_id'
    pair_view_revisions = True

    @property
    def supported_providers(self):
        return (self.provider,)

    @property
    def policy_fingerprint(self):
        return self.redactor.fingerprint

    def __init__(self, sources, store, *, bounds=None, extra_literals=(), capture=None):
        if capture is not None:
            from csl_workload_launch_binding import LaunchObservation
            if not isinstance(capture, LaunchObservation): raise TypeError()
        self.capture = capture
        self.bounds = bounds or Bounds()
        if not isinstance(sources, tuple) or not 1 <= len(sources) <= self.bounds.files:
            raise SyncError('workload_source_limit')
        self.sources = sources
        store_root = Path(store).expanduser().absolute()
        for source in sources:
            if not isinstance(source, tuple) or len(source) != 2:
                raise SyncError('workload_source_refused')
            root, relative = source
            root = Path(root).expanduser().absolute()
            if (not isinstance(relative, str) or not relative.endswith('.jsonl')
                    or root.resolve() != root or store_root.is_relative_to(root)
                    or root.is_relative_to(store_root)):
                raise SyncError('workload_source_refused')
        self.store = ProjectionStore(store_root, self.bounds)
        self.redactor = Redactor(self.bounds, extra_literals)
        self.prepared = {}

    def project(self, records):
        raise NotImplementedError

    def list_sessions(self, provider):
        if provider != self.provider:
            raise SyncError('unsupported_provider')
        entries = {}
        for root, relative in self.sources:
            records, cursor = (read_complete(root, relative, self.bounds) if self.capture is None else
                               read_complete(root, relative, self.bounds, capture=self.capture))
            metadata, output, view = self.project(records)
            if self.capture is not None: self.capture.header(records)
            sid = metadata['native_id']
            if sid in entries:
                raise SyncError(self.duplicate_code)
            if self.capture is None: verify_prefix(root, relative, cursor)
            else: verify_prefix(root, relative, cursor, capture=self.capture)
            source_key = str(Path(root).absolute()) + '/' + relative
            path = self.store.publish(source_key, output)
            self.store.checkpoint(source_key, cursor, self.policy_fingerprint, output)
            descriptor = {'file_key': 'main', 'path': str(path), 'relative_path': 'session.jsonl',
                'mode': 'append', 'complete_length': len(output), 'size': len(output),
                'sha256': hashlib.sha256(output).hexdigest(), 'role': 'transcript'}
            entries[sid] = {'metadata': metadata, 'file': descriptor, 'view': view,
                'source_cursor': cursor, 'projection_version': 1, 'policy_sha256': self.policy_fingerprint}
        self.prepared = entries
        accepted = [entry['metadata'] for entry in entries.values()]
        if self.capture is not None: self.capture.admitted(self, accepted)
        return accepted

    def _entry(self, provider, native_id):
        if provider != self.provider or native_id not in self.prepared:
            raise SyncError('workload_session_unprepared')
        return self.prepared[native_id]

    def raw_files(self, provider, native_id):
        return [dict(self._entry(provider, native_id)['file'])]

    def view_records(self, provider, native_id, *, position=None, limit=100):
        entry = self._entry(provider, native_id)
        records = entry['view']
        target = position.get('next_seq') if isinstance(position, dict) else 1
        if (position is not None and not isinstance(position, dict)
                or type(target) is not int or target < 1 or type(limit) is not int or not 1 <= limit <= 1000
                or position is not None and (position.get('version') != 1
                    or position.get('provider') != provider or position.get('native_id') != native_id)):
            raise SyncError('invalid_view_position')
        def digest(rows):
            return hashlib.sha256(b''.join(encoded(row)+b'\n' for row in rows)).hexdigest()
        prefix = digest(records[:target-1])
        reset = bool(position and (target > len(records)+1 or position.get('prefix_sha256') != prefix))
        page = [] if reset else records[target-1:target-1+limit]
        end = 1 if reset else target + len(page)
        source = entry['file']
        return {'records': page, 'position': {'version': 1, 'provider': provider, 'native_id': native_id,
                'next_seq': end, 'prefix_sha256': EMPTY if reset else digest(records[:end-1])},
            'reset_required': reset, 'complete': not reset and end > len(records),
            'source_file': {'file_key': 'main', 'length': source['complete_length'], 'sha256': source['sha256']}}

    def package_descriptor(self, provider, native_id):
        self._entry(provider, native_id)
        return {'available': False, 'reason': 'workload_view_only', 'descriptor_version': 1,
                'manifest': None, 'members': []}

"""Fixture-side binding of the first accepted session in an owned producer run."""

from dataclasses import dataclass
from uuid import UUID

from csl_workload_runner import sources
from csl_workload_stream import verify_prefix


@dataclass(frozen=True)
class SessionBirth:
    native_id: str
    relative: str
    device: int
    inode: int


class FreshCodexBirth:
    """Use only inside the trusted driver around its guarded producer launch."""

    def __init__(self, root):
        self.root = root
        if any(root.iterdir()):
            raise ValueError('producer_source_not_empty')
        self.birth = None

    def bind_accepted(self, adapter, accepted):
        """Bind the exact provider output before admitting an observation."""
        selected = sources(self.root, files=adapter.bounds.files)
        if (adapter.provider != 'codex' or len(selected) != 1
                or adapter.sources != ((self.root, selected[0]),)
                or len(accepted) != 1 or len(adapter.prepared) != 1):
            raise ValueError('producer_source_ambiguous')
        metadata, = accepted
        native_id = metadata['native_id']
        if str(UUID(native_id)) != native_id:
            raise ValueError('producer_session_invalid')
        entry = adapter.prepared.get(native_id)
        if entry is None or entry['metadata'] != metadata:
            raise ValueError('producer_session_unqualified')
        cursor = entry['source_cursor']
        verify_prefix(self.root, selected[0], cursor)
        current = SessionBirth(native_id, selected[0], cursor['device'], cursor['inode'])
        if self.birth is not None and self.birth != current:
            raise ValueError('producer_session_changed')
        self.birth = current
        return current

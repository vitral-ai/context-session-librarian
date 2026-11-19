"""Bounded, deterministic workload text projection. Never uploads source bytes."""
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import stat
import tempfile

from csl_canonical import strict_json
from csl_secrets import CREDENTIAL_KEY, PATTERNS, REPLACEMENT, RULES_VERSION
from csl_sync_state import SyncError, private_directory, private_json

VERSION = 1
OPAQUE = frozenset({'encrypted_content', 'signature', 'thoughtSignature',
                    'inlineData', 'inline_data', 'fileData', 'file_data'})
IDENTITIES = frozenset({'id', 'sessionId', 'projectHash', 'type', 'role',
                        '$rewindTo', 'call_id', 'tool_use_id'})


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(',', ':')).encode('utf-8')


def checked_text(value):
    try:
        value.encode('utf-8')
    except UnicodeError:
        raise SyncError('workload_record_refused') from None


@dataclass(frozen=True)
class Bounds:
    source_bytes: int = 16 * 1024 * 1024
    record_bytes: int = 1024 * 1024
    output_bytes: int = 16 * 1024 * 1024
    spool_bytes: int = 32 * 1024 * 1024
    records: int = 20000
    files: int = 32
    values: int = 200000
    depth: int = 32

    def __post_init__(self):
        if any(type(value) is not int or value < 1 for value in vars(self).values()):
            raise SyncError('workload_invalid_bounds')


class StructureBudget:
    """Validate every descendant before selection or credential replacement."""
    def __init__(self, bounds):
        self.bounds, self.remaining = bounds, bounds.values

    def check(self, item, depth=0):
        self.remaining -= 1
        if self.remaining < 0 or depth > self.bounds.depth:
            raise SyncError('workload_structure_limit')
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise SyncError('workload_record_refused')
                checked_text(key)
                self.check(child, depth+1)
        elif isinstance(item, list):
            for child in item: self.check(child, depth+1)
        elif isinstance(item, str):
            checked_text(item)
        elif isinstance(item, float) and not math.isfinite(item):
            raise SyncError('workload_record_refused')
        elif item is not None and type(item) not in (bool, int, float):
            raise SyncError('workload_record_refused')


def opened_source(root, relative, *, capture=None):
    """Open beneath an explicit root using anchored, no-symlink directory FDs."""
    if not isinstance(relative, str):
        raise SyncError('workload_source_refused')
    parts = PurePosixPath(relative)
    if (not relative or not parts.parts or parts.is_absolute()
            or '..' in parts.parts or str(parts) != relative or '\\' in relative):
        raise SyncError('workload_source_refused')
    directory = None
    try:
        root = Path(root).absolute()
        if root.resolve() != root:
            raise SyncError('workload_source_refused')
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        if capture is not None: capture.root(directory)
        for part in parts.parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        fd = os.open(parts.parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            os.close(fd)
            raise SyncError('workload_source_refused')
        return os.fdopen(fd, 'rb')
    except (OSError, ValueError, TypeError):
        raise SyncError('workload_source_refused') from None
    finally:
        if directory is not None:
            os.close(directory)


def read_complete(root, relative, bounds, *, capture=None):
    records, digest, length, lines = [], hashlib.sha256(), 0, 0
    budget = StructureBudget(bounds)
    budget.check(records)
    try:
        with (opened_source(root, relative) if capture is None else opened_source(root, relative, capture=capture)) as source:
            initial = os.fstat(source.fileno())
            if initial.st_size > bounds.source_bytes:
                raise SyncError('workload_source_limit')
            remaining = initial.st_size
            while remaining:
                line = source.readline(min(remaining, bounds.record_bytes + 1))
                if not line:
                    raise SyncError('source_changed')
                if len(line) > bounds.record_bytes:
                    raise SyncError('workload_record_limit')
                if not line.endswith(b'\n'):
                    break
                remaining -= len(line)
                length += len(line)
                digest.update(line)
                lines += 1
                if lines > bounds.records:
                    raise SyncError('workload_record_limit')
                if line.strip():
                    value = strict_json(line)
                    if not isinstance(value, dict):
                        raise SyncError('workload_record_refused')
                    budget.check(value, depth=1)
                    records.append(value)
            # A fixed complete prefix may grow, but may not be rewritten while read.
            source.seek(0)
            check, left = hashlib.sha256(), length
            while left:
                chunk = source.read(min(left, 65536))
                if not chunk:
                    raise SyncError('source_changed')
                check.update(chunk)
                left -= len(chunk)
            if check.digest() != digest.digest():
                raise SyncError('source_changed')
            current = os.fstat(source.fileno())
            if current.st_size == initial.st_size and current.st_mtime_ns != initial.st_mtime_ns:
                raise SyncError('source_changed')
        return records, {'device': initial.st_dev, 'inode': initial.st_ino,
                         'complete_length': length, 'sha256': digest.hexdigest(),
                         'partial': length < initial.st_size}
    except (ValueError, UnicodeError, RecursionError, OSError):
        raise SyncError('workload_record_refused') from None


def verify_prefix(root, relative, cursor, *, capture=None):
    """Revalidate the captured prefix, allowing a producer to append meanwhile."""
    with (opened_source(root, relative) if capture is None else opened_source(root, relative, capture=capture)) as source:
        info = os.fstat(source.fileno())
        if (info.st_dev != cursor['device'] or info.st_ino != cursor['inode']
                or info.st_size < cursor['complete_length']):
            raise SyncError('source_changed')
        digest, left = hashlib.sha256(), cursor['complete_length']
        while left:
            chunk = source.read(min(left, 65536))
            if not chunk:
                raise SyncError('source_changed')
            digest.update(chunk)
            left -= len(chunk)
        if digest.hexdigest() != cursor['sha256']:
            raise SyncError('source_changed')


class Redactor:
    """Readable-text pattern policy; opaque fields refuse, never pass through."""
    def __init__(self, bounds, extra_literals=()):
        if (not isinstance(extra_literals, tuple) or len(extra_literals) > 128
                or any(not isinstance(s, str) or not 1 <= len(s) <= 4096 for s in extra_literals)):
            raise SyncError('workload_invalid_policy')
        self.bounds = bounds
        self.extra = tuple(sorted(set(extra_literals), key=lambda s: (-len(s), s)))
        self.fingerprint = hashlib.sha256(encoded({'projection': VERSION,
            'rules': RULES_VERSION, 'extra_literals': self.extra})).hexdigest()

    def text(self, value):
        for _, pattern, group in PATTERNS:
            def replace(match):
                selected = group if group is not None else (1 if match.group(1) is not None else 2)
                start, end = match.span(selected)
                return match.group(0)[:start-match.start()] + REPLACEMENT + match.group(0)[end-match.start():]
            value = pattern.sub(replace, value)
        for literal in self.extra:
            value = value.replace(literal, REPLACEMENT)
        return value

    def clean(self, value):
        StructureBudget(self.bounds).check(value)
        remaining = [self.bounds.values]
        def visit(item, key=None, depth=0):
            remaining[0] -= 1
            if remaining[0] < 0 or depth > self.bounds.depth:
                raise SyncError('workload_structure_limit')
            if key and CREDENTIAL_KEY.fullmatch(key):
                return REPLACEMENT
            if isinstance(item, dict):
                if isinstance(item.get('type'), str) and item['type'] in {
                        'image', 'document', 'audio', 'video', 'base64', 'input_image', 'input_file'}:
                    raise SyncError('workload_opaque_or_key_refused')
                result = {}
                for name, child in item.items():
                    if isinstance(name, str): checked_text(name)
                    if (not isinstance(name, str) or self.text(name) != name or name in OPAQUE):
                        raise SyncError('workload_opaque_or_key_refused')
                    result[name] = visit(child, name, depth+1)
                return result
            if isinstance(item, list):
                return [visit(child, key, depth+1) for child in item]
            if isinstance(item, str):
                checked_text(item)
                cleaned = self.text(item)
                if key in IDENTITIES and cleaned != item:
                    raise SyncError('workload_identity_refused')
                return cleaned
            if isinstance(item, float) and not math.isfinite(item):
                raise SyncError('workload_record_refused')
            if item is None or type(item) in (bool, int, float):
                return item
            raise SyncError('workload_record_refused')
        return visit(value)


class ProjectionStore:
    """Private sanitized spool with bounded total storage and stable append inode."""
    def __init__(self, root, bounds):
        root = Path(root).expanduser().absolute()
        if root.resolve() != root:
            raise SyncError('workload_spool_refused')
        self.private_root = private_directory(root)
        self.root = private_directory(self.private_root / 'streams')
        self.checkpoints = private_directory(self.private_root / 'source-cursors')
        self.bounds = bounds

    def publish(self, source_key, data):
        if len(data) > self.bounds.output_bytes:
            raise SyncError('workload_output_limit')
        target = self.root / (hashlib.sha256(source_key.encode()).hexdigest() + '.jsonl')
        total, count, previous = 0, 0, b''
        for entry in self.root.iterdir():
            count += 1
            info = entry.lstat()
            if (count > self.bounds.files or not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.getuid() or info.st_mode & 0o077):
                raise SyncError('workload_spool_refused')
            total += info.st_size
            if entry == target:
                if info.st_size > self.bounds.output_bytes:
                    raise SyncError('workload_spool_limit')
                with opened_source(self.root, entry.name) as existing:
                    previous = existing.read(self.bounds.output_bytes + 1)
        if (total - len(previous) + len(data) > self.bounds.spool_bytes
                or not target.exists() and count >= self.bounds.files):
            raise SyncError('workload_spool_limit')
        if previous and data.startswith(previous):
            fd = os.open(target, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
            with os.fdopen(fd, 'ab') as stream:
                stream.write(data[len(previous):])
                stream.flush()
                os.fsync(stream.fileno())
        else:
            if total + len(data) > self.bounds.spool_bytes:
                raise SyncError('workload_spool_limit')
            fd, temporary = tempfile.mkstemp(prefix='.projection-', dir=self.root)
            try:
                os.fchmod(fd, 0o600)
                with os.fdopen(fd, 'wb') as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, target)
            finally:
                Path(temporary).unlink(missing_ok=True)
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return target

    def checkpoint(self, source_key, cursor, policy, stream):
        name = hashlib.sha256(source_key.encode()).hexdigest() + '.json'
        private_json(self.checkpoints / name, {'projection_version': VERSION,
            'policy_sha256': policy, 'source_cursor': cursor,
            'sanitized_length': len(stream), 'sanitized_sha256': hashlib.sha256(stream).hexdigest()})

"""Unwired Linux execution codec/capture/reservation. No import or Start caller."""
from dataclasses import dataclass
import fcntl
import hashlib
import os
from pathlib import Path
import re
import stat
import time
import tempfile
import uuid
import zipfile

from csl_backup_core import canonical
from csl_canonical import strict_json
import csl_durable as durable
from csl_transfer import inspect_bundle

MAX_MESSAGE = 16 * 1024
FIELDS = frozenset('schema_version operation execution_id import_key provider source_id package_size package_sha256 owner host_id assignment_id generation boot_id cwd cwd_device cwd_inode authority'.split())
SHA = re.compile(r'[0-9a-f]{64}')


class ExecutionError(ValueError):
    pass


def _uint(text, maximum=(1 << 64) - 1, positive=False):
    return isinstance(text, str) and re.fullmatch(r'0|[1-9][0-9]*', text) is not None and len(text) <= 20 and int(text) <= maximum and (not positive or int(text) > 0)


def _uuid(text):
    try:
        return isinstance(text, str) and str(uuid.UUID(text)) == text
    except (ValueError, TypeError, AttributeError):
        return False


@dataclass(frozen=True)
class Request:
    encoded: bytes

    @property
    def value(self):
        return strict_json(self.encoded)

    @property
    def commitment(self):
        return hashlib.sha256(self.encoded).hexdigest()


def request(raw):
    if not isinstance(raw, bytes) or len(raw) > MAX_MESSAGE:
        raise ExecutionError('execution_request_limit')
    try:
        v = strict_json(raw)
        if not isinstance(v, dict) or set(v) != FIELDS or type(v['schema_version']) is not int or v['schema_version'] != 1 or v['operation'] not in ('import', 'lookup'):
            raise ValueError()
        for field, prefix in [('execution_id', 're_'), ('import_key', 'ri_')]:
            if not isinstance(v[field], str) or not re.fullmatch(prefix + r'[0-9a-f]{64}', v[field]):
                raise ValueError()
        if v['provider'] not in ('codex', 'claude') or not isinstance(v['source_id'], str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', v['source_id']):
            raise ValueError()
        if type(v['package_size']) is not int or not 0 < v['package_size'] <= 4 * 1024**3 or not isinstance(v['package_sha256'], str) or not SHA.fullmatch(v['package_sha256']):
            raise ValueError()
        if not isinstance(v['owner'], str) or not re.fullmatch(r'[0-9a-f]{24}', v['owner']) or not isinstance(v['host_id'], str) or not re.fullmatch(r'ses_[A-Za-z0-9_-]{1,128}', v['host_id']):
            raise ValueError()
        if not _uuid(v['assignment_id']) or not _uuid(v['boot_id']) or not _uint(v['generation'], positive=True):
            raise ValueError()
        cwd = v['cwd']
        if not isinstance(cwd, str) or '\0' in cwd or len(cwd.encode()) > 4096 or not Path(cwd).is_absolute() or str(Path(cwd)) != cwd or '..' in Path(cwd).parts or cwd.startswith('//'):
            raise ValueError()
        if not _uint(v['cwd_device']) or not _uint(v['cwd_inode'], positive=True):
            raise ValueError()
        a = v['authority']
        if not isinstance(a, dict) or set(a) != {'boot_id', 'start_boottime_ns', 'end_boottime_ns'} or a['boot_id'] != v['boot_id'] or not all(_uint(a[x], (1 << 63) - 1) for x in ('start_boottime_ns', 'end_boottime_ns')) or int(a['end_boottime_ns']) <= int(a['start_boottime_ns']):
            raise ValueError()
        return Request(canonical(v))
    except (ValueError, TypeError, KeyError, UnicodeError, OverflowError):
        raise ExecutionError('execution_request_invalid') from None


class Guard:
    """Injected original native check, never inferred from request metadata."""
    def __init__(self, req, native_check, clock):
        bound = req.value['authority']
        self._bound = (bound['boot_id'], int(bound['start_boottime_ns']), int(bound['end_boottime_ns']))
        self.native_check, self.clock = native_check, clock
        self.last = self._bound[1]

    def __call__(self):
        self.native_check()  # Known authoritative loss wins over clock diagnostics.
        boot, now = self.clock()
        if type(now) is not int or boot != self._bound[0] or now < self.last or now >= self._bound[2]:
            raise ExecutionError('execution_original_bound_lost')
        self.last = now


def linux_clock():
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip(), time.clock_gettime_ns(time.CLOCK_BOOTTIME)


class Capture:
    def __init__(self, stream, sha, size):
        self.stream, self.sha, self.size = stream, sha, size

    def rewind(self):
        seals = fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL
        if fcntl.fcntl(self.stream.fileno(), fcntl.F_GET_SEALS) & seals != seals:
            raise ExecutionError('execution_capture_unsealed')
        self.stream.seek(0)
        return self.stream

    def close(self):
        self.stream.close()


def capture(fd, req, check, max_bytes):
    """Borrow FD5 at offset0; inspect ONLY the owned hash-qualified sealed copy."""
    v = req.value
    if type(max_bytes) is not int or not 0 < v['package_size'] <= max_bytes:
        raise ExecutionError('execution_package_limit')
    check()
    before = os.fstat(fd)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink < 1 or before.st_size != v['package_size'] or fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE != os.O_RDONLY or os.lseek(fd, 0, os.SEEK_CUR) != 0:
        raise ExecutionError('execution_package_fd_invalid')
    check()
    out = os.memfd_create('csl-execution-capture', os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        os.fchmod(out, 0o600)
        digest, offset = hashlib.sha256(), 0
        while offset <= v['package_size']:
            check()
            chunk = os.pread(fd, min(1024 * 1024, v['package_size'] + 1 - offset), offset)
            check()
            if not chunk:
                break
            offset += len(chunk)
            if offset > v['package_size']:
                raise ExecutionError('execution_package_changed')
            digest.update(chunk)
            view = memoryview(chunk)
            while view:
                check()
                n = os.write(out, view)
                if n <= 0:
                    raise ExecutionError('execution_capture_short_write')
                view = view[n:]
        after = os.fstat(fd)
        identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        if offset != v['package_size'] or digest.hexdigest() != v['package_sha256'] or identity(before) != identity(after):
            raise ExecutionError('execution_package_changed')
        check()
        fcntl.fcntl(out, fcntl.F_ADD_SEALS, fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL)
        result = Capture(os.fdopen(out, 'rb'), digest.hexdigest(), offset)
        out = -1
        result.rewind()
        return result
    finally:
        if out >= 0:
            os.close(out)


def plan(captured, req, check, staging_fd):
    """Validate the whole sealed package, reserve every graph ID; no rewriting."""
    check()
    durable.directory(staging_fd)
    if captured.sha != req.value['package_sha256'] or captured.size != req.value['package_size']:
        raise ExecutionError('execution_capture_conflict')
    with zipfile.ZipFile(captured.rewind()) as archive:
        manifest = inspect_bundle(archive)
        check()
        sessions = manifest['sessions']
        v = req.value
        if manifest['mode'] != 'raw' or len(sessions) != 1 or sessions[0].get('provider', 'codex') != v['provider'] or sessions[0]['id'] != v['source_id']:
            raise ExecutionError('execution_package_profile_conflict')
        entry = sessions[0]
        graph = entry.get('codex_history')
        if graph:
            import csl_codex_history as history
            check()
            with tempfile.TemporaryDirectory(prefix='.graph-', dir=f'/proc/self/fd/{staging_fd}') as scratch:
                scratch = Path(scratch).resolve(strict=True)
                parent = scratch.parent.stat()
                held = durable.directory(staging_fd)
                if (parent.st_dev, parent.st_ino) != (held.st_dev, held.st_ino):
                    raise ExecutionError('execution_staging_parent_changed')
                check()
                nodes = history.unpack(archive, entry, scratch, check=check)
                mapping = history.reserve_mapping(nodes)
        else:
            mapping = {'rollouts': {entry['id']: str(uuid.uuid4())}, 'owners': {}}
            mapping['owners'][entry['id']] = mapping['rollouts'][entry['id']]
        check()
        return {'mapping': mapping, 'package_sha256': captured.sha, 'package_size': captured.size, 'source_id': entry['id'], 'provider': v['provider']}


def _plan(value, req):
    try:
        v = req.value
        if not isinstance(value, dict) or set(value) != {'mapping', 'package_sha256', 'package_size', 'source_id', 'provider'} or any(value[k] != v[k] for k in ('package_sha256', 'package_size', 'source_id', 'provider')):
            raise ValueError()
        mapping = value['mapping']
        if not isinstance(mapping, dict) or set(mapping) != {'rollouts', 'owners'}:
            raise ValueError()
        for name in ('rollouts', 'owners'):
            ids = mapping[name]
            if not isinstance(ids, dict) or not 1 <= len(ids) <= 33 or len(set(ids.values())) != len(ids):
                raise ValueError()
            if any(not isinstance(k, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', k) or not _uuid(v) for k, v in ids.items()):
                raise ValueError()
        if v['source_id'] not in mapping['owners'] or not set(mapping['owners'].values()) <= set(mapping['rollouts'].values()) or (set(mapping['owners'].values()) | set(mapping['rollouts'].values())) & (set(mapping['owners']) | set(mapping['rollouts'])):
            raise ValueError()
        return value
    except (KeyError, ValueError, TypeError):
        raise ExecutionError('execution_plan_conflict') from None


def _intent_key(req):
    # operation and current authority aren't immutable intent: lookup is read-only.
    value = req.value
    return {k: v for k, v in value.items() if k != 'operation'}


def lookup(fd, req, check):
    check()
    raw = durable.read(fd, req.value['import_key'] + '.json', MAX_MESSAGE)
    row = strict_json(raw)
    if not isinstance(row, dict) or set(row) != {'v', 'revision', 'intent', 'plan', 'state'} or type(row['v']) is not int or row['v'] != 1 or type(row['revision']) is not int or row['revision'] != 1 or not isinstance(row['intent'], dict) or {k:v for k,v in row['intent'].items() if k != 'authority'} != {k:v for k,v in _intent_key(req).items() if k != 'authority'} or row['state'] != 'reserved':
        raise ExecutionError('execution_journal_conflict')
    _plan(row['plan'], req)
    original = dict(row['intent'], operation='import')
    request(canonical(original))
    check()
    return row  # A reserved mapping is never an imported or native result.


def reserve(fd, req, prepared, check):
    if req.value['operation'] != 'import':
        raise ExecutionError('execution_plan_conflict')
    prepared = _plan(strict_json(canonical(prepared)), req)
    with durable.locked(fd, check):
        try:
            prior = lookup(fd, req, check)
        except FileNotFoundError:
            prior = None
        if prior is not None:
            if prior['intent'] != _intent_key(req):
                raise ExecutionError('execution_original_bound_changed')
            check()
            return prior
        check()
        row = {'v': 1, 'revision': 1, 'intent': _intent_key(req), 'plan': prepared, 'state': 'reserved'}
        durable.create(fd, req.value['import_key'] + '.json', row, check, MAX_MESSAGE)
        return lookup(fd, req, check)

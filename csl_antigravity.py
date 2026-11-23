"""Read-only Antigravity CLI SQLite/protobuf profile observed on CLI 1.5.9.

No Google login, native executable, network, protobuf runtime or model is needed.
Field numbers were checked against the official 1.5.9 binary's descriptors and
native-generated synthetic sessions. Unknown steps remain opaque raw evidence.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import struct
import sys
import tempfile
from urllib.parse import quote, unquote, urlsplit
from urllib.request import url2pathname
import uuid
from csl_sync_fs import WINDOWS, open_stream, reparse


class FormatError(ValueError):
    pass


def _varint(body, offset):
    value = 0
    for shift in range(0, 70, 7):
        if offset >= len(body):
            raise FormatError('invalid_antigravity_protobuf')
        item = body[offset]; offset += 1
        value |= (item & 127) << shift
        if item < 128:
            if shift == 63 and item > 1:
                raise FormatError('invalid_antigravity_protobuf')
            return value, offset
    raise FormatError('invalid_antigravity_protobuf')


def fields(body):
    """Bounded protobuf wire reader; unknown fields can be retained unchanged."""
    if not isinstance(body, bytes) or len(body) > 64 * 1024 * 1024:
        raise FormatError('unsupported_antigravity_blob')
    offset = count = 0
    while offset < len(body):
        count += 1
        if count > 100000:
            raise FormatError('oversized_antigravity_message')
        tag, offset = _varint(body, offset)
        number, wire = tag >> 3, tag & 7
        if not 1 <= number < 2**29:
            raise FormatError('invalid_antigravity_protobuf')
        if wire == 0:
            value, offset = _varint(body, offset)
        elif wire in (1, 2, 5):
            if wire == 2:
                size, offset = _varint(body, offset)
            else:
                size = 8 if wire == 1 else 4
            if size > len(body) - offset:
                raise FormatError('invalid_antigravity_protobuf')
            value = body[offset:offset+size]; offset += size
        else:
            raise FormatError('unsupported_antigravity_wire_type')
        yield number, wire, value


def integer(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128); value >>= 7
    return bytes(result) + bytes([value])


def encode(rows):
    out = bytearray()
    for number, wire, value in rows:
        out.extend(integer((number << 3) | wire))
        if wire == 0:
            out.extend(integer(value))
        elif wire == 2:
            out.extend(integer(len(value))); out.extend(value)
        else:
            out.extend(value)
    return bytes(out)


def first(body, number, default=b''):
    return next((v for n, _, v in fields(body) if n == number), default)


def text(body, number):
    value = first(body, number)
    if not isinstance(value, bytes):
        raise FormatError('invalid_antigravity_text')
    try:
        return value.decode('utf-8')
    except UnicodeError:
        raise FormatError('invalid_antigravity_text') from None


def timestamp(body):
    if not body:
        return None
    seconds = first(body, 1, 0); nanos = first(body, 2, 0)
    try:
        return datetime.fromtimestamp(seconds + nanos / 1e9, timezone.utc).isoformat()
    except (TypeError, ValueError, OverflowError):
        raise FormatError('invalid_antigravity_timestamp') from None


def _regular(path, root):
    try:
        current = root
        for part in path.relative_to(root).parts:
            current = current / part
            if reparse(current):
                raise FormatError('antigravity_symlink_source')
        if not path.is_file():
            raise FormatError('antigravity_source_missing')
    except (OSError, ValueError) as exc:
        if isinstance(exc, FormatError):
            raise
        raise FormatError('antigravity_source_missing') from None


def _hash(path,length):
    digest=hashlib.sha256()
    with open_stream(path) as stream:
        while length:
            chunk=stream.read(min(length,1024*1024))
            if not chunk:raise FormatError('antigravity_source_changing')
            digest.update(chunk);length-=len(chunk)
    return digest.hexdigest()


def _wal_length(path):
    """Last complete committed WAL frame, following SQLite's native checksums."""
    size=path.stat().st_size
    if not size:return 0
    with open_stream(path) as stream:
        header=stream.read(32)
        if len(header)!=32:raise FormatError('incomplete_antigravity_wal')
        magic,version,page_size=struct.unpack('>III',header[:12])
        if magic not in (0x377f0682,0x377f0683) or version!=3007000 or page_size<512 or page_size>65536 or page_size&(page_size-1):
            raise FormatError('invalid_antigravity_wal')
        order='>' if magic&1 else '<'
        def checksum(data,state):
            a,b=state
            for x,y in struct.iter_unpack(order+'II',data):
                a=(a+x+b)&0xffffffff;b=(b+y+a)&0xffffffff
            return a,b
        state=checksum(header[:24],(0,0))
        if state!=struct.unpack('>II',header[24:32]):raise FormatError('invalid_antigravity_wal')
        complete=32
        while True:
            frame=stream.read(24+page_size)
            if len(frame)!=24+page_size or frame[8:16]!=header[16:24]:break
            expected=checksum(frame[:8]+frame[24:],state)
            if expected!=struct.unpack('>II',frame[16:24]):break
            state=expected
            if int.from_bytes(frame[4:8],'big'):complete=stream.tell()
        return complete


def inventory(root, native_id):
    if not isinstance(native_id, str) or not re.fullmatch(r'[a-fA-F0-9-]{36}', native_id):
        raise FormatError('invalid_antigravity_id')
    try:
        uuid.UUID(native_id)
    except ValueError:
        raise FormatError('invalid_antigravity_id') from None
    main = root / 'conversations' / (native_id + '.db')
    _regular(main, root)
    paths = [main]
    wal = Path(str(main) + '-wal')
    if wal.exists() or wal.is_symlink():
        _regular(wal, root); paths.append(wal)
    # A rollback journal can contain uncommitted old pages. Do not guess recovery.
    journal = Path(str(main) + '-journal')
    if journal.exists() and journal.stat().st_size:
        raise FormatError('antigravity_transaction_in_progress')
    result = []
    for index, path in enumerate(paths):
        before = path.stat()
        complete=before.st_size if index==0 else _wal_length(path)
        digest=_hash(path,complete)
        after = path.stat()
        if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
            raise FormatError('antigravity_source_changing')
        result.append({'file_key': 'main' if index == 0 else 'sqlite-wal',
                       'path': str(path),
                       'relative_path': 'session.db' if index == 0 else 'session/session.db-wal',
                       'mode': 'rewrite', 'complete_length': complete, 'size': after.st_size,
                       'sha256': digest, 'role': 'transcript' if index == 0 else 'context'})
    return result


def _schema(db):
    if db.execute("SELECT 1 FROM sqlite_master WHERE type IN ('view','trigger') OR upper(ltrim(sql)) LIKE 'CREATE VIRTUAL TABLE%' LIMIT 1").fetchone():
        raise FormatError('unsupported_antigravity_schema')
    required = {'trajectory_meta': {'trajectory_id', 'cascade_id'},
                'steps': {'idx', 'step_type', 'status', 'step_payload', 'step_format'},
                'trajectory_metadata_blob': {'id', 'data'}}
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for table, columns in required.items():
        if table not in tables or not columns <= {r[1] for r in db.execute('PRAGMA table_info(' + table + ')')}:
            raise FormatError('unsupported_antigravity_schema')
    if db.execute('SELECT 1 FROM steps WHERE step_format != 0 LIMIT 1').fetchone():
        raise FormatError('unsupported_antigravity_step_format')


def read_only_uri(path):
    # Escape before SQLite parses the query or authority, including Win32 extended paths.
    absolute = Path(path).absolute()
    filename = str(absolute) if WINDOWS else os.fsencode(absolute)
    return 'file:' + quote(filename, safe='/:') + '?mode=ro'


@contextmanager
def snapshot(root, native_id, scratch=None):
    """Copy exact DB/WAL bytes into private storage; SQL never opens the origin."""
    files = inventory(root, native_id)
    with tempfile.TemporaryDirectory(prefix='csl-agy-read-', dir=scratch) as tmp:
        path = Path(tmp) / 'session.db'
        for item in files:
            dest = path if item['file_key'] == 'main' else Path(str(path) + '-wal')
            with open_stream(item['path']) as source, dest.open('xb') as target:
                os.chmod(dest, 0o600);left=item['complete_length']
                while left:
                    chunk=source.read(min(left,1024*1024))
                    if not chunk:raise FormatError('antigravity_source_changing')
                    target.write(chunk);left-=len(chunk)
            if _hash(dest,item['complete_length']) != item['sha256']:
                raise FormatError('antigravity_source_changing')
        if files != inventory(root, native_id):
            raise FormatError('antigravity_source_changing')
        db = None
        try:
            db = sqlite3.connect(read_only_uri(path), uri=True)
            db.execute('PRAGMA query_only=ON'); db.execute('PRAGMA trusted_schema=OFF')
            _schema(db)
            meta = db.execute('SELECT cascade_id FROM trajectory_meta').fetchall()
            if not meta or any(r[0] != native_id for r in meta):
                raise FormatError('antigravity_identity_mismatch')
            yield db, files
            if files != inventory(root, native_id):
                raise FormatError('antigravity_source_changing')
        except sqlite3.Error:
            raise FormatError('invalid_antigravity_database') from None
        finally:
            if db is not None:
                db.close()


def _metadata_blob(db):
    row = db.execute("SELECT data FROM trajectory_metadata_blob WHERE id='main'").fetchone()
    return row[0] if row and isinstance(row[0], bytes) else b''


def metadata(db, native_id):
    blob = _metadata_blob(db)
    cwd = None
    uri = text(blob, 7)
    if not uri:
        workspace = first(blob, 1)
        uri = text(workspace, 1) if workspace else ''
    if uri:
        parts = urlsplit(uri)
        if parts.scheme == 'file' and parts.netloc in ('', 'localhost'):
            try:
                cwd = (url2pathname(parts.path) if WINDOWS
                       else unquote(parts.path, encoding=sys.getfilesystemencoding(), errors='replace'))
            except (OSError, ValueError):
                pass
    created = timestamp(first(blob, 2)); updated = created
    count = 0; title = None
    for idx, kind, payload in db.execute('SELECT idx,step_type,step_payload FROM steps ORDER BY idx'):
        count += 1
        updated = timestamp(first(first(payload, 5), 1)) or updated
        if kind == 14 and title is None:
            user = first(payload, 19)
            title = (text(user, 2) or text(user, 1))[:200]
    parent = text(blob, 5) or None
    imported = False
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='csl_import_provenance'").fetchone():
        row = db.execute('SELECT source_id FROM csl_import_provenance LIMIT 1').fetchone()
        if row:
            parent = row[0]; imported = True
    return {'provider': 'antigravity', 'native_id': native_id, 'title': title or native_id,
            'cwd': cwd, 'created_at': created, 'updated_at': updated, 'model': None,
            'record_count': count, 'parent': {'provider': 'antigravity', 'native_id': parent} if parent else None,
            'origin': 'imported' if imported else 'native'}


def _tool(body):
    return {'id': text(body, 1) or None, 'name': text(body, 2) or None,
            'arguments': text(body, 3) or text(body, 4)}


def views(db, text_limit):
    calls = set()
    for idx, kind, status, payload in db.execute('SELECT idx,step_type,status,step_payload FROM steps ORDER BY idx'):
        meta = first(payload, 5)
        stamp = timestamp(first(meta, 1))
        block = 0
        def row(role, category, value, tool=None, call_id=None):
            nonlocal block
            result = {'time': stamp, 'role': role, 'kind': category, 'text': value[:text_limit],
                      'tool': tool, 'call_id': call_id, 'truncated': len(value) > text_limit,
                      'raw_offset': {'file_key': 'main', 'offset': 0},
                      'native_pointer': '/steps/' + str(idx), 'block_index': block}
            block += 1
            return result
        if kind == 14:
            user = first(payload, 19)
            value = text(user, 2) or text(user, 1)
            if value:
                yield row('user', 'text', value)
            else:
                yield row('user', 'notice', 'User content retained in native raw step; text projection unavailable.')
        elif kind == 15:
            response = first(payload, 20)
            value = text(response, 8) or text(response, 1)
            if value:
                yield row('assistant', 'text', value)
            for number, wire, body in fields(response):
                if number == 7 and wire == 2:
                    tool = _tool(body)
                    if tool['id'] not in calls:
                        calls.add(tool['id'])
                        yield row('assistant', 'tool_call', tool['arguments'], tool['name'], tool['id'])
        elif kind in (8, 21, 38):
            call_body = first(meta, 4)
            part = first(payload, {8: 14, 21: 28, 38: 47}[kind])
            if kind == 38:
                call_body = first(part, 2) or call_body
            tool = _tool(call_body)
            if tool['id'] and tool['id'] not in calls:
                calls.add(tool['id'])
                yield row('assistant', 'tool_call', tool['arguments'], tool['name'], tool['id'])
            if kind == 8:
                value = text(part, 9) or text(part, 4)
            elif kind == 21:
                output = first(part, 21) or first(part, 26)
                value = (text(output, 1) or text(output, 2)) if output else (text(part, 4) + text(part, 5))
                code = first(part, 6, None)
                if code is not None:
                    code &= 0xffffffff
                    if code & 0x80000000:code -= 0x100000000
                    value += '\n[exit_code=' + str(code) + ']'
            else:
                value = text(part, 3)
            if status in (3,7) and (value or tool['id']):
                if status==7:value+='\n[native_step_status=error]'
                yield row('tool', 'tool_result', value, tool['name'], tool['id'])
            elif not tool['id']:
                yield row('system', 'notice', 'Native tool step retained in raw; correlation unavailable.')
        else:
            yield row('system', 'notice', 'Native step type ' + str(kind) + ' retained in raw; not projected as executable content.')


def package_reason(db):
    blob = _metadata_blob(db)
    if text(blob, 5):
        return 'antigravity_parent_context_not_packaged'
    if db.execute('SELECT 1 FROM steps WHERE status IN (1,2,8,9,11) LIMIT 1').fetchone():
        return 'antigravity_session_not_quiescent'
    columns={r[1] for r in db.execute('PRAGMA table_info(steps)')}
    if 'has_subtrajectory' in columns and db.execute('SELECT 1 FROM steps WHERE has_subtrajectory != 0 LIMIT 1').fetchone():
        return 'antigravity_subtrajectory_not_packaged'
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='parent_references'").fetchone():
        if db.execute('SELECT 1 FROM parent_references LIMIT 1').fetchone():
            return 'antigravity_parent_context_not_packaged'
    if not db.execute('SELECT 1 FROM steps WHERE step_type=14 LIMIT 1').fetchone():
        return 'no_conversation'
    return None


def rewrite_copy(source, destination, source_id, new_id, cwd):
    """Independent native copy. Historical blobs stay raw except local authority/IDs."""
    db = sqlite3.connect(read_only_uri(source), uri=True)
    out = sqlite3.connect(destination)
    try:
        _schema(db)
        if any(row[0] != source_id for row in db.execute('SELECT cascade_id FROM trajectory_meta')):
            raise FormatError('antigravity_identity_mismatch')
        if package_reason(db):
            raise FormatError(package_reason(db))
        db.backup(out)
        uri = Path(cwd).resolve().as_uri().encode()
        out.execute('UPDATE trajectory_meta SET cascade_id=?', (new_id,))
        # New trajectory identifiers prevent accidental links to the source graph.
        id_map = {r[0]: str(uuid.uuid4()) for r in out.execute('SELECT trajectory_id FROM trajectory_meta')}
        for old, new in id_map.items():
            out.execute('UPDATE trajectory_meta SET trajectory_id=? WHERE trajectory_id=?', (new, old))
        blob = _metadata_blob(out)
        rows = [r for r in fields(blob) if r[0] not in (1,4,5,6,7,8,10,11,12,18,21,22,23)]
        rows += [(1, 2, encode([(1, 2, uri)])), (6, 2, new_id.encode()), (7, 2, uri),
                 (18, 2, b'default-cli-project')]
        out.execute("UPDATE trajectory_metadata_blob SET data=? WHERE id='main'", (encode(rows),))
        for idx, payload in out.execute('SELECT idx,step_payload FROM steps').fetchall():
            changed = []
            for n, w, value in fields(payload):
                if n==133:
                    continue  # Stored historical permission decisions are not grants.
                if n == 5 and w == 2:
                    changed_meta = []
                    for mn, mw, mv in fields(value):
                        if mn == 20 and mw == 2:
                            mv = encode([(sn, sw, id_map.get(sv.decode(), sv.decode()).encode() if sn==1 and sw==2 else new_id.encode() if sn==4 and sw==2 else sv) for sn, sw, sv in fields(mv)])
                        changed_meta.append((mn, mw, mv))
                    value = encode(changed_meta)
                elif n == 19 and w == 2:
                    # Persisted source configuration is not current permission authority.
                    value = encode([r for r in fields(value) if r[0] not in (12, 13)])
                changed.append((n, w, value))
            out.execute('UPDATE steps SET step_payload=? WHERE idx=?', (encode(changed), idx))
            columns={r[1] for r in out.execute('PRAGMA table_info(steps)')}
            if 'metadata' in columns:
                out.execute('UPDATE steps SET metadata=? WHERE idx=?',(first(encode(changed),5),idx))
            if 'permissions' in columns:
                out.execute('UPDATE steps SET permissions=NULL WHERE idx=?',(idx,))
        if out.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='executor_metadata'").fetchone():
            out.execute('DELETE FROM executor_metadata')
        out.execute('CREATE TABLE IF NOT EXISTS csl_import_provenance (source_id TEXT NOT NULL)')
        out.execute('DELETE FROM csl_import_provenance')
        out.execute('INSERT INTO csl_import_provenance VALUES (?)', (source_id,))
        out.commit()
        out.execute('PRAGMA journal_mode=DELETE')
    except sqlite3.Error:
        raise FormatError('invalid_antigravity_database') from None
    finally:
        out.close(); db.close()
    os.chmod(destination, 0o600)

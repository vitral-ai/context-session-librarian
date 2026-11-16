"""Private collector state. No transcript text is stored in its journal."""
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import uuid
import hashlib


class SyncError(Exception):
    def __init__(self, code, *, status=None, retryable=False, retry_after=None):
        self.code, self.status = code, status
        self.retryable, self.retry_after = retryable, retry_after
        super().__init__(code)  # Never include paths, HTTP bodies or credentials.


def now():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def display_id(value):
    value = str(value)
    return value if len(value) <= 128 and not any(c in value for c in '/\\\r\n\t') else 'id-' + hashlib.sha256(value.encode()).hexdigest()[:16]


def private_directory(path):
    path = Path(path).expanduser().absolute()
    if path.is_symlink():
        raise SyncError('unsafe_state_directory')
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    info = path.stat()
    if info.st_uid != os.getuid() or not stat.S_ISDIR(info.st_mode):
        raise SyncError('unsafe_state_directory')
    path.chmod(0o700)
    return path


def private_json(path, data):
    path = Path(path)
    directory = private_directory(path.parent)
    fd, temporary = tempfile.mkstemp(prefix='.write-', dir=directory)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'w') as output:
            json.dump(data, output, separators=(',', ':'))
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def read_private_json(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    except OSError:
        raise SyncError('unsafe_credentials_file') from None
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if info.st_uid != os.getuid() or not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
            raise SyncError('unsafe_credentials_file')
        raw = stream.read(1024 * 1024 + 1)
    try:
        if len(raw) > 1024 * 1024:
            raise ValueError()
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError()
        return value
    except (ValueError, UnicodeError):
        raise SyncError('invalid_credentials_file') from None


@contextmanager
def exclusive(root, name='collector.lock'):
    root = private_directory(root)
    try:
        fd = os.open(root / name, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    except OSError:
        raise SyncError('unsafe_lock_file') from None
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SyncError('collector_busy') from None
        yield fd
    finally:
        os.close(fd)


class State:
    def __init__(self, root):
        self.root = private_directory(root)
        path = self.root / 'state.sqlite'
        if path.is_symlink():
            raise SyncError('unsafe_state_file')
        self.db = sqlite3.connect(path)
        path.chmod(0o600)
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sessions (
                provider TEXT NOT NULL, native_id TEXT NOT NULL, data TEXT NOT NULL,
                PRIMARY KEY(provider,native_id));
            CREATE TABLE IF NOT EXISTS selections (
                provider TEXT NOT NULL, native_id TEXT NOT NULL, included INTEGER NOT NULL,
                PRIMARY KEY(provider,native_id));
        ''')
        if self.get('device_id') is None:
            self.set('device_id', str(uuid.uuid4()))

    def close(self):
        self.db.close()

    def get(self, key, default=None):
        row = self.db.execute('SELECT value FROM config WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key, value):
        with self.db:
            self.db.execute('INSERT INTO config VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                            (key, json.dumps(value, separators=(',', ':'))))

    def session(self, provider, native_id):
        row = self.db.execute('SELECT data FROM sessions WHERE provider=? AND native_id=?',
                              (provider, native_id)).fetchone()
        return json.loads(row[0]) if row else {'files': {}, 'view': {}, 'status': 'pending'}

    def save_session(self, provider, native_id, value):
        with self.db:
            self.db.execute('INSERT INTO sessions VALUES(?,?,?) ON CONFLICT(provider,native_id) DO UPDATE SET data=excluded.data',
                            (provider, native_id, json.dumps(value, separators=(',', ':'))))

    def selection(self, provider, native_id):
        row = self.db.execute('SELECT included FROM selections WHERE provider=? AND native_id=?',
                              (provider, native_id)).fetchone()
        return bool(row[0]) if row else None

    def select(self, provider, native_id, included):
        with self.db:
            self.db.execute('INSERT INTO selections VALUES(?,?,?) ON CONFLICT(provider,native_id) DO UPDATE SET included=excluded.included',
                            (provider, native_id, int(included)))

    def tracked(self):
        return set(self.db.execute('SELECT provider,native_id FROM sessions'))

    def reset_checkpoints(self):
        # Fresh authorization can select a different account/registry; preserve opt-outs.
        with self.db:
            self.db.execute('UPDATE sessions SET data=?', (json.dumps({'files': {}, 'view': {}, 'status': 'pending'}),))

    def status(self):
        rows = []
        for provider, native_id, raw in self.db.execute('SELECT provider,native_id,data FROM sessions ORDER BY provider,native_id'):
            item = json.loads(raw)
            rows.append({'provider': provider, 'native_id': display_id(native_id), 'status': item.get('status'),
                         'synced_bytes': sum(f.get('offset', 0) for f in item.get('files', {}).values()),
                         'planned_bytes': sum(f.get('length', 0) for f in item.get('files', {}).values()),
                         'last_synced_at': item.get('last_synced_at'), 'last_error': item.get('last_error'),
                         'included': self.selection(provider, native_id)})
        return {'device_id': self.get('device_id'), 'last_error': self.get('last_error'),
                'last_pass_at': self.get('last_pass_at'), 'sessions': rows}

"""Compact wake via an existing local Claude Code peer socket.

No listener or permission grant. Native IPC is version-dependent; the ordinary
session-bound hook remains the fallback. A socket write is an emission attempt,
not proof of model consumption. Failed writes leave the hook fallback eligible.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import stat
import struct

from csl_bridge_store import BridgeError, Store, compact_message, decode, encode
from csl_bridge_mailbox import now, timestamp

WAKE_TEXT = 'CSL: new messages'


def owned_json(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
            raise BridgeError('Untrusted native peer metadata')
        with os.fdopen(fd, 'r') as stream:
            fd = None
            data = stream.read(16385)
        if len(data) > 16384:
            raise BridgeError('Native peer metadata too large')
        return json.loads(data)
    finally:
        if fd is not None:
            os.close(fd)


def start_ticks(pid):
    return Path('/proc', str(pid), 'stat').read_text().rsplit(')', 1)[1].split()[19]


def endpoint(config, session):
    matches = []
    for path in (Path(config) / 'sessions').glob('*.json'):
        if not path.stem.isdecimal():
            continue
        record = owned_json(path)
        if record.get('sessionId') == session and record.get('kind') == 'interactive':
            pid = record.get('pid')
            if type(pid) is not int or str(pid) != path.stem:
                raise BridgeError('Native peer PID mismatch')
            try:
                if start_ticks(pid) != record.get('procStart'):
                    continue
            except FileNotFoundError:
                continue
            matches.append(record)
    if len(matches) != 1:
        raise BridgeError('Expected exactly one live native session')
    record = matches[0]
    if record.get('peerProtocol') != 1:
        raise BridgeError('Unsupported native peer protocol')
    path = Path(record['messagingSocketPath'])
    expected = Path('/run/user', str(os.getuid()), 'cc-socks', str(record['pid']) + '.sock')
    if path != expected or path.resolve() != path:
        raise BridgeError('Unexpected native peer socket')
    info = path.lstat()
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
        raise BridgeError('Untrusted native peer socket')
    digest = hashlib.sha256(str(path).encode()).hexdigest()
    key_path = Path(config) / 'sessions' / (str(record['pid']) + '.' + digest + '.key')
    key = owned_json(key_path)
    if key_path.stat().st_mode & 0o077 or not re.fullmatch('[0-9a-f]{32}', key.get('peerToken', '')):
        raise BridgeError('Invalid native peer key')
    for field in ('procStart', 'pidDomain'):
        if key.get(field) != record.get(field):
            raise BridgeError('Native peer identity changed')
    return record, path, key['peerToken']


@contextmanager
def native_connection(session, config=None):
    record, path, token = endpoint(config or Path.home() / '.claude', session)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(1)
        connection.connect(str(path))
        pid, uid, _ = struct.unpack('3i', connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        if (pid, uid) != (record['pid'], os.getuid()) or start_ticks(pid) != record['procStart']:
            raise BridgeError('Native peer changed before delivery')
        yield connection, token


def send_native(connection, token, session, text):
    connection.sendall((encode({'type': 'auth', 'token': token}) + '\n' + encode({
        'type': 'user', 'session_id': session, 'from': 'csl-mailbox',
        'priority': 'next', 'message': {'content': text}}) + '\n').encode())


def wake(session, config=None):
    with native_connection(session, config) as (connection, token):
        send_native(connection, token, session, WAKE_TEXT)


def notify(home, agent, message_id, config=None):
    """Emit one compact preview per session/message, shared with hook fallback."""
    store = Store(home)
    try:
        session = store.agents().get(agent)
        if not session:
            raise BridgeError('Unknown notification endpoint')
        rows = store.rows("SELECT * FROM mail_messages WHERE id=? AND direction='in' "
                          'AND target=? AND acknowledged_at IS NULL', (message_id, agent))
        if not rows:
            return False
        row = rows[0]
        envelope = decode(row['envelope'])
        if envelope.get('expires_at') and timestamp(envelope['expires_at']) <= timestamp(now()):
            return False
        text = compact_message(row['source'], message_id, envelope['body'], target=agent if row['peer'] == 'agent-hub' else None)
        with native_connection(session, config) as (connection, token):
            # Share the hook's emission ledger. A bounded socket send is serialized
            # with hooks; failure rolls back and leaves their fallback eligible.
            with store.transaction() as db:
                db.execute('CREATE TABLE IF NOT EXISTS mail_surfaced '
                           '(agent TEXT,session TEXT,id TEXT,surfaced_at TEXT,PRIMARY KEY(agent,session,id))')
                if db.execute('SELECT 1 FROM mail_surfaced WHERE agent=? AND session=? AND id=?',
                              (agent, session, message_id)).fetchone():
                    return False
                send_native(connection, token, session, text)
                db.execute('INSERT INTO mail_surfaced VALUES(?,?,?,?)',
                           (agent, session, message_id, now()))
        return True
    finally:
        store.close()

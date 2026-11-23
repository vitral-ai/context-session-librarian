"""Small, single-owner TLS agent hub. Native runtimes and credentials stay local."""
import hashlib
import hmac
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import select
import socket
import sqlite3
import ssl
import threading
import time

from csl_bridge_store import BridgeError, decode, encode, identifier, name


class Hub:
    def __init__(self, database, machines):
        self.database, self.machines = str(database), machines
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS agents(name TEXT PRIMARY KEY, record TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS approvals(source TEXT, target TEXT, PRIMARY KEY(source,target));
                CREATE TABLE IF NOT EXISTS envelopes(id TEXT PRIMARY KEY, source TEXT, target TEXT,
                    machine TEXT, packet TEXT NOT NULL, state TEXT NOT NULL, receipt TEXT, created REAL);
            ''')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.database, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def authenticate(self, token):
        digest = hashlib.sha256(token.encode()).hexdigest()
        for machine, record in self.machines.items():
            if hmac.compare_digest(digest, record['sha256']):
                return machine, record.get('admin') is True
        raise BridgeError('Invalid machine token')

    def call(self, token, request):
        machine, admin = self.authenticate(token)
        if not isinstance(request, dict):
            raise BridgeError('Expected an object')
        action = request.get('action')
        with self.connect() as db:
            # Serialize registry/allow-list reads with enqueue and revocation.
            db.execute('BEGIN IMMEDIATE')
            def agent(key):
                row = db.execute('SELECT record FROM agents WHERE name=?', (name(key),)).fetchone()
                if not row:
                    raise BridgeError('Unknown agent: ' + key)
                return decode(row['record'])
            if action == 'register':
                record = {k: request[k] for k in ('name', 'session', 'cli', 'machine')}
                name(record['name']); identifier(record['session']); name(record['machine'])
                if record['cli'] not in ('codex', 'claude') or record['machine'] not in self.machines:
                    raise BridgeError('Unknown CLI or machine')
                old = db.execute('SELECT record FROM agents WHERE name=?', (record['name'],)).fetchone()
                if (record['machine'] != machine or old and decode(old['record'])['machine'] != machine) and not admin:
                    raise BridgeError('Cannot register another machine\'s agent')
                # A changed binding must be approved again; old queued envelopes keep old pins.
                if old and decode(old['record']) != record:
                    db.execute('DELETE FROM approvals WHERE source=? OR target=?', (record['name'], record['name']))
                db.execute('INSERT OR REPLACE INTO agents VALUES(?,?)', (record['name'], encode(record)))
                return record
            if action in ('allow', 'deny', 'remove'):
                if not admin:
                    raise BridgeError('Registry approvals require the owner machine')
                if action == 'remove':
                    key = name(request['name'])
                    db.execute('DELETE FROM agents WHERE name=?', (key,))
                    db.execute('DELETE FROM approvals WHERE source=? OR target=?', (key, key))
                else:
                    source, target = agent(request['source'])['name'], agent(request['target'])['name']
                    pairs = [(source, target)] + ([(target, source)] if request.get('both') is True else [])
                    for pair in pairs:
                        db.execute(('INSERT OR IGNORE INTO approvals VALUES(?,?)' if action == 'allow'
                                    else 'DELETE FROM approvals WHERE source=? AND target=?'), pair)
                return {'state': action + 'ed'}
            if action == 'list':
                return {'agents': [decode(r[0]) for r in db.execute('SELECT record FROM agents ORDER BY name')]}
            if action == 'show':
                return agent(request['name'])
            if action == 'permissions':
                return {'approvals': [dict(r) for r in db.execute('SELECT * FROM approvals ORDER BY source,target')]}
            if action == 'send':
                source, target = agent(request['source']), agent(request['target'])
                if source['machine'] != machine:
                    raise BridgeError('Source agent does not belong to authenticated machine')
                if not db.execute('SELECT 1 FROM approvals WHERE source=? AND target=?',
                                  (source['name'], target['name'])).fetchone():
                    raise BridgeError('Unapproved pair: ' + source['name'] + ' -> ' + target['name'])
                mid, text = identifier(request['id']), request['text']
                if not isinstance(text, str) or not text.strip() or len(text.encode()) > 10000:
                    raise BridgeError('Message must contain 1..10000 UTF-8 bytes')
                reply = request.get('in_reply_to')
                if reply is not None:
                    original = db.execute('SELECT source,target FROM envelopes WHERE id=?', (identifier(reply),)).fetchone()
                    if not original or (original['source'], original['target']) != (target['name'], source['name']):
                        raise BridgeError('Reply must reverse the original envelope endpoints')
                packet = {'id': mid, 'source': source, 'target': target, 'text': text, 'in_reply_to': reply}
                previous = db.execute('SELECT packet,state,receipt FROM envelopes WHERE id=?', (mid,)).fetchone()
                if previous:
                    if {k: v for k, v in decode(previous['packet']).items() if k != 'created_at'} != packet:
                        raise BridgeError('Message ID collision')
                    return {'id': mid, 'state': previous['state'], 'receipt': decode(previous['receipt']) if previous['receipt'] else None}
                from datetime import datetime, timezone
                packet['created_at'] = datetime.now(timezone.utc).isoformat()
                db.execute('INSERT INTO envelopes VALUES(?,?,?,?,?,?,NULL,?)',
                           (mid, source['name'], target['name'], target['machine'], encode(packet), 'queued', time.time()))
                return {'id': mid, 'state': 'queued'}
            if action == 'pull':
                packets, size, targets = [], 0, set()
                for row in db.execute("SELECT packet FROM envelopes WHERE machine=? AND state='queued' ORDER BY created", (machine,)):
                    packet = decode(row['packet'])
                    if packet['target']['name'] in targets:
                        continue
                    current = db.execute('SELECT record FROM agents WHERE name=?', (packet['target']['name'],)).fetchone()
                    approved = db.execute('SELECT 1 FROM approvals WHERE source=? AND target=?',
                                          (packet['source']['name'], packet['target']['name'])).fetchone()
                    origin = db.execute('SELECT record FROM agents WHERE name=?', (packet['source']['name'],)).fetchone()
                    if current and origin and decode(origin['record']) == packet['source'] and decode(current['record']) == packet['target'] and approved:
                        length = len(encode(packet).encode())
                        if size + length > 58000:
                            break
                        packets.append(packet)
                        targets.add(packet['target']['name'])
                        size += length
                    if len(packets) == 16:
                        break
                return {'envelopes': packets}
            if action in ('ack', 'receipt'):
                row = db.execute('SELECT * FROM envelopes WHERE id=?', (identifier(request['id']),)).fetchone()
                if not row:
                    raise BridgeError('Unknown message')
                packet = decode(row['packet'])
                if action == 'ack':
                    if row['machine'] != machine:
                        raise BridgeError('Only recipient machine can acknowledge delivery')
                    receipt = request['receipt']
                    if not isinstance(receipt, dict) or receipt.get('state') != 'delivered':
                        raise BridgeError('Only actual delivered receipts settle an envelope')
                    if row['state'] != 'delivered':
                        db.execute("UPDATE envelopes SET state='delivered',receipt=? WHERE id=?", (encode(receipt), row['id']))
                    return {'id': row['id'], 'state': 'delivered'}
                if machine not in (packet['source']['machine'], packet['target']['machine']):
                    raise BridgeError('Not an envelope endpoint')
                return {'id': row['id'], 'state': row['state'], 'receipt': decode(row['receipt']) if row['receipt'] else None}
            raise BridgeError('Unknown hub action')


def server(hub, address, certificate, key, ssh_port=None):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(certificate, key)
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass  # Never log Authorization or envelope content.

        def do_POST(self):
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if self.path != '/v1' or not 0 < length <= 65536:
                    raise BridgeError('Invalid endpoint or request size')
                auth = self.headers.get('Authorization', '')
                if not auth.startswith('Bearer '):
                    raise BridgeError('Missing machine token')
                result = hub.call(auth[7:], decode(self.rfile.read(length)))
                status, body = 200, {'ok': True, 'data': result}
            except (BridgeError, ValueError, KeyError, TypeError) as error:
                status, body = 400, {'ok': False, 'error': str(error)}
            except sqlite3.Error:
                status, body = 503, {'ok': False, 'error': 'Hub storage unavailable; retry same message ID'}
            raw = encode(body).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
    class Server(ThreadingHTTPServer):
        daemon_threads = True
        slots = threading.BoundedSemaphore(64)
        def process_request(self, connection, client):
            if not self.slots.acquire(blocking=False):
                self.shutdown_request(connection)
                return
            try:
                super().process_request(connection, client)
            except BaseException:
                self.slots.release()
                raise

        def process_request_thread(self, connection, client):
            try:
                connection.settimeout(10)
                # SSH sends a plaintext version; TLS sends a ClientHello. One
                # approved service owns 443; sshd stays on loopback/local port 22.
                first = connection.recv(1, socket.MSG_PEEK)
                if ssh_port and first == b'S':
                    with socket.create_connection(('127.0.0.1', ssh_port), timeout=5) as remote:
                        remote.settimeout(None); connection.settimeout(None)
                        while True:
                            ready, _, _ = select.select([connection, remote], [], [], 300)
                            if not ready:
                                continue
                            for source in ready:
                                data = source.recv(65536)
                                if not data:
                                    return
                                (remote if source is connection else connection).sendall(data)
                else:
                    with context.wrap_socket(connection, server_side=True) as tls:
                        self.finish_request(tls, client)
            except (OSError, ValueError):
                pass
            finally:
                self.shutdown_request(connection)
                self.slots.release()
    return Server(address, Handler)

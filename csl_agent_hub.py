"""Small, single-owner TLS agent hub. Native runtimes and credentials stay local."""
import hashlib
import hmac
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from pathlib import Path
import select
import socket
import sqlite3
import ssl
import threading
import time
import uuid
from datetime import datetime, timezone

from csl_bridge_store import BridgeError, decode, encode, identifier, message_selector, name


NOTICE_SOURCE = {'name': 'hub-notifier', 'session': 'a38597e0-058a-5a72-9f71-50340e8242d3',
                 'cli': 'hub', 'machine': 'hub'}
COORDINATOR = 'laptop-orchestrator'


class Hub:
    def __init__(self, database, machines, history_retention=200):
        if type(history_retention) is not int or not 1 <= history_retention <= 10000:
            raise BridgeError('History retention must be 1..10000')
        self.history_retention = history_retention
        self.database, self.machines = str(database), machines
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS history(id TEXT PRIMARY KEY, source TEXT, target TEXT,
                    source_machine TEXT, target_machine TEXT, time TEXT, state TEXT, body TEXT,
                    pair_a TEXT, pair_b TEXT, sequence INTEGER);
                CREATE INDEX IF NOT EXISTS history_pair ON history(pair_a,pair_b,sequence);
                CREATE TABLE IF NOT EXISTS history_migrations(version INTEGER PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS hub_notices(id TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS agents(name TEXT PRIMARY KEY, record TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS approvals(source TEXT, target TEXT, PRIMARY KEY(source,target));
                CREATE TABLE IF NOT EXISTS envelopes(id TEXT PRIMARY KEY, source TEXT, target TEXT,
                    machine TEXT, packet TEXT NOT NULL, state TEXT NOT NULL, receipt TEXT, created REAL);
            ''')
            if not db.execute('SELECT 1 FROM history_migrations WHERE version=1').fetchone():
                for row in db.execute('SELECT packet,state FROM envelopes ORDER BY created'):
                    packet = decode(row['packet'])
                    self.remember(db, packet, row['state'])
                db.execute('INSERT INTO history_migrations VALUES(1)')
            for row in db.execute('SELECT DISTINCT pair_a,pair_b FROM history').fetchall():
                self.trim_history(db, row['pair_a'], row['pair_b'])

    def trim_history(self, db, a, b):
        db.execute('DELETE FROM history WHERE pair_a=? AND pair_b=? AND id NOT IN '
                   '(SELECT id FROM history WHERE pair_a=? AND pair_b=? ORDER BY sequence DESC LIMIT ?)',
                   (a, b, a, b, self.history_retention))

    def remember(self, db, packet, state):
        source, target = packet['source'], packet['target']
        a, b = sorted((source['name'], target['name']))
        old = db.execute('SELECT source,target,body FROM history WHERE id=?', (packet['id'],)).fetchone()
        if old and (old['source'], old['target'], old['body']) != (source['name'], target['name'], packet['text']):
            raise BridgeError('Message ID collision')
        sequence = db.execute('SELECT COALESCE(MAX(sequence),0)+1 FROM history').fetchone()[0]
        db.execute('INSERT OR IGNORE INTO history VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                   (packet['id'], source['name'], target['name'], source.get('machine'),
                    target.get('machine'), packet.get('created_at') or datetime.now(timezone.utc).isoformat(),
                    state, packet['text'], a, b, sequence))
        self.trim_history(db, a, b)


    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.database, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def refuse_send(self, db, machine, source, target, mid, reason, packet=None):
        """Commit one hub-authored notice before returning the original refusal."""
        if packet is not None:
            try:
                self.remember(db, packet, 'refused')
            except BridgeError:
                pass  # A refused ID collision must not overwrite the original history.
            db.commit()
        coordinator = db.execute('SELECT record FROM agents WHERE name=?', (COORDINATOR,)).fetchone()
        if coordinator:
            recipient = decode(coordinator['record'])
            # Retries of the same refusal never generate duplicate coordinator turns.
            notice_id = str(uuid.uuid5(uuid.NAMESPACE_URL,
                encode([machine, source, target, mid, reason])))
            created = datetime.now(timezone.utc).isoformat()
            packet = {'id': notice_id, 'source': NOTICE_SOURCE, 'target': recipient,
                      'text': f'refused: {source} -> {target}, {reason}, {created}, message id {mid}',
                      'in_reply_to': None, 'created_at': created}
            db.execute('INSERT OR IGNORE INTO envelopes VALUES(?,?,?,?,?,?,NULL,?)',
                       (notice_id, NOTICE_SOURCE['name'], COORDINATOR, recipient['machine'],
                        encode(packet), 'queued', time.time()))
            db.execute('INSERT OR IGNORE INTO hub_notices VALUES(?)', (notice_id,))
            self.remember(db, packet, 'queued')
            # Raising inside connect() normally rolls back; the durable notice must survive.
            db.commit()
            suffix = 'Coordinator laptop-orchestrator has been notified (notice queued).'
        else:
            suffix = 'Coordinator notification unavailable: laptop-orchestrator is not registered.'
        raise BridgeError(reason + '. ' + suffix + ' Do not retry through another channel.')

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
        policy = self.machines[machine]
        if policy.get('scope') == 'notices':
            from csl_ci_notice import notice_request
            request = notice_request(policy, request)
            machine, admin = policy['source_machine'], False
            action = 'send'
        elif action == 'notice':
            raise BridgeError('A repository-scoped notice credential is required')
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
                if record['name'] == NOTICE_SOURCE['name']:
                    raise BridgeError('Reserved hub notification identity')
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
            if action == 'history':
                source = agent(request['source'])
                if not admin and source['machine'] != machine:
                    raise BridgeError('History requires an agent on the authenticated machine')
                mid, partner = request.get('id'), request.get('with')
                if bool(mid) == bool(partner):
                    raise BridgeError('History requires exactly one of with or id')
                last = request.get('last', min(5, self.history_retention))
                if type(last) is not int or not 1 <= last <= self.history_retention:
                    raise BridgeError('History last must be 1..' + str(self.history_retention))
                if mid:
                    selector = message_selector(mid)
                    if len(selector) == 36:
                        rows = db.execute('SELECT * FROM history WHERE id=?', (selector,)).fetchall()
                    else:
                        # Prefix ambiguity is scoped to history this endpoint may read.
                        rows = db.execute('SELECT * FROM history WHERE id LIKE ? AND '
                                          '(? OR (source=? AND source_machine=?) OR '
                                          '(target=? AND target_machine=?)) LIMIT 2',
                                          (selector + '%', admin, source['name'], machine,
                                           source['name'], machine)).fetchall()
                        if len(rows) > 1:
                            raise BridgeError('Ambiguous message ID prefix; use more characters')
                else:
                    a, b = sorted((source['name'], name(partner)))
                    rows = db.execute('SELECT * FROM history WHERE pair_a=? AND pair_b=? '
                                      'ORDER BY sequence DESC LIMIT ?', (a, b, last)).fetchall()[::-1]
                result = []
                for row in rows:
                    own = (row['source'] == source['name'] and row['source_machine'] == machine or
                           row['target'] == source['name'] and row['target_machine'] == machine)
                    if not admin and not own:
                        raise BridgeError('History is private to the pair endpoints and owner')
                    result.append({k: row[k] for k in ('id','source','target','time','state','body')})
                return {'messages': result, 'retention': self.history_retention}
            if action == 'send':
                source_name, target_name = name(request['source']), name(request['target'])
                mid, text = identifier(request['id']), request['text']
                if not isinstance(text, str) or not text.strip() or len(text.encode()) > 10000:
                    raise BridgeError('Message must contain 1..10000 UTF-8 bytes')
                source = agent(source_name)
                if source['machine'] != machine:
                    raise BridgeError('Source agent does not belong to authenticated machine')
                target_row = db.execute('SELECT record FROM agents WHERE name=?', (target_name,)).fetchone()
                target = decode(target_row['record']) if target_row else {'name': target_name, 'machine': None}
                attempt = {'id': mid, 'source': source, 'target': target, 'text': text}
                if (policy.get('scope') == 'notices' and request['source'] == 'ci-notices'
                        and not db.execute('SELECT 1 FROM agents WHERE name=?',
                                           (request['target'],)).fetchone()):
                    logging.getLogger(__name__).warning(
                        'CI notice dropped: unknown target=%s id=%s', request['target'], request['id'])
                    self.remember(db, attempt, 'dropped')
                    return {'id': request['id'], 'state': 'dropped', 'reason': 'unknown notice target'}
                if not target_row:
                    self.refuse_send(db, machine, source_name, target_name, mid,
                                     'Unknown agent: ' + target_name, attempt)
                if not db.execute('SELECT 1 FROM approvals WHERE source=? AND target=?',
                                  (source_name, target_name)).fetchone():
                    self.refuse_send(db, machine, source_name, target_name, mid,
                                     'Unapproved pair: ' + source_name + ' -> ' + target_name, attempt)
                reply = request.get('in_reply_to')
                if reply is not None:
                    selector = message_selector(reply)
                    originals = db.execute('SELECT id,source,target FROM envelopes WHERE id LIKE ? '
                                           'AND source=? AND target=? LIMIT 2',
                                           (selector + '%' if len(selector) < 36 else selector,
                                            target['name'], source['name'])).fetchall()
                    if len(originals) > 1:
                        raise BridgeError('Ambiguous message ID prefix; use more characters')
                    original = originals[0] if originals else None
                    if original:
                        reply = original['id']
                    if not original or (original['source'], original['target']) != (target['name'], source['name']):
                        raise BridgeError('Reply must reverse the original envelope endpoints')
                packet = {'id': mid, 'source': source, 'target': target, 'text': text, 'in_reply_to': reply}
                previous = db.execute('SELECT packet,state,receipt FROM envelopes WHERE id=?', (mid,)).fetchone()
                if previous:
                    if {k: v for k, v in decode(previous['packet']).items() if k != 'created_at'} != packet:
                        raise BridgeError('Message ID collision')
                    return {'id': mid, 'state': previous['state'], 'receipt': decode(previous['receipt']) if previous['receipt'] else None}
                packet['created_at'] = datetime.now(timezone.utc).isoformat()
                db.execute('INSERT INTO envelopes VALUES(?,?,?,?,?,?,NULL,?)',
                           (mid, source['name'], target['name'], target['machine'], encode(packet), 'queued', time.time()))
                self.remember(db, packet, 'queued')
                db.execute("UPDATE history SET state='queued',source_machine=?,target_machine=? WHERE id=?",
                           (source['machine'], target['machine'], mid))
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
                    system_notice = (packet['source'] == NOTICE_SOURCE and
                        db.execute('SELECT 1 FROM hub_notices WHERE id=?', (packet['id'],)).fetchone())
                    valid_origin = (origin and decode(origin['record']) == packet['source'] and approved)
                    if current and decode(current['record']) == packet['target'] and (system_notice or valid_origin):
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
                    db.execute("UPDATE history SET state='delivered' WHERE id=?", (row['id'],))
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

"""Persistent CSL stream. Standard library; no pool client, lease or governor dependency."""
import argparse, concurrent.futures, fcntl, hashlib, json, os, queue, selectors, signal, socket, sqlite3, subprocess, sys, threading, time, uuid
from pathlib import Path
from contextlib import contextmanager
MAX = 65536
STOP = threading.Event()

def wire(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)

def load(path):
    return json.loads(Path(path).read_text())

def log(event, **fields):
    print(wire({'event': event, **fields}), file=sys.stderr, flush=True)

def private_dir(path):
    path = Path(path)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.resolve() != path or path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077:
        raise ValueError('Private canonical directory required')
    return path

def read_line(stream):
    raw = stream.readline(MAX + 1)
    if not raw:
        raise EOFError()
    if len(raw) > MAX or not raw.endswith(b'\n'):
        raise ValueError('Frame too large or incomplete')
    from csl_bridge_store import decode
    return decode(raw)

def local(home, request, timeout=55):
    deadline = time.monotonic() + timeout
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect(str(Path(home) / 'light.sock'))
            s.sendall((wire(request) + '\n').encode())
            with s.makefile('rb') as reader:
                return read_line(reader)
    except (OSError, EOFError, ValueError):
        if request.get('action') != 'exchange':
            raise
        node = Node(load(Path(home) / 'config.json'))
        try:
            return node.exchange(request['peer'], request['request'], request['origin'], timeout=max(0, deadline - time.monotonic()))
        finally:
            node.pool.shutdown(wait=False)

@contextmanager
def sql(path, **kwargs):
    db = sqlite3.connect(path, **kwargs)
    try:
        with db:
            yield db
    finally:
        db.close()

def stop_process(process):
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=2)
    except ProcessLookupError:
        pass

class Journal:

    def __init__(self, path):
        self.path = Path(path)
        private_dir(self.path.parent)
        with self.db() as db:
            db.executescript('CREATE TABLE IF NOT EXISTS rpc(direction TEXT,peer TEXT,id TEXT,payload TEXT,hash TEXT,state TEXT,result TEXT,error TEXT,created REAL,updated REAL,PRIMARY KEY(direction,peer,id));CREATE TABLE IF NOT EXISTS routes(agent TEXT PRIMARY KEY,peer TEXT,thread TEXT);')
            db.execute('CREATE TABLE IF NOT EXISTS delivery_retries(peer TEXT,id TEXT,attempts INTEGER,next REAL,PRIMARY KEY(peer,id))')
            db.execute('CREATE TABLE IF NOT EXISTS tombstones(direction TEXT,peer TEXT,id TEXT,hash TEXT,result TEXT,updated REAL,PRIMARY KEY(direction,peer,id))')
        self.path.chmod(0o600)

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def put(self, direction, peer, identifier, payload):
        raw = wire(payload)
        digest = hashlib.sha256(raw.encode()).hexdigest()
        now = time.time()
        with self.db() as db:
            old = db.execute('SELECT * FROM tombstones WHERE direction=? AND peer=? AND id=?', (direction, peer, identifier)).fetchone()
            if old:
                if old['hash'] != digest:
                    raise ValueError('Message id collision')
                return {**dict(old), 'payload': raw, 'state': 'done'}
            db.execute('INSERT OR IGNORE INTO rpc(direction,peer,id,payload,hash,state,result,error,created,updated) VALUES(?,?,?,?,?,?,NULL,NULL,?,?)',
                       (direction, peer, identifier, raw, digest, 'queued', now, now))
            row = db.execute('SELECT * FROM rpc WHERE direction=? AND peer=? AND id=?',
                             (direction, peer, identifier)).fetchone()
            if row['hash'] != digest:
                raise ValueError('Message id collision')
            return dict(row)

    def get(self, direction, peer, identifier):
        with self.db() as db:
            row = db.execute('SELECT * FROM rpc WHERE direction=? AND peer=? AND id=?', (direction, peer, identifier)).fetchone()
            if row:
                return dict(row)
            old = db.execute('SELECT * FROM tombstones WHERE direction=? AND peer=? AND id=?', (direction, peer, identifier)).fetchone()
            return {**dict(old), 'state':'done', 'payload':'null'} if old else None

    def finish(self, direction, peer, identifier, result):
        with self.db() as db:
            db.execute("UPDATE rpc SET state='done',result=?,error=NULL,updated=? WHERE direction=? AND peer=? AND id=?", (wire(result), time.time(), direction, peer, identifier))
            if direction == 'in':
                db.execute('DELETE FROM delivery_retries WHERE peer=? AND id=?',(peer,identifier))

    def retry_due(self, peer, identifier):
        with self.db() as db:
            row = db.execute('SELECT next FROM delivery_retries WHERE peer=? AND id=?',(peer,identifier)).fetchone()
            return row is None or row[0] <= time.time()

    def error(self, direction, peer, identifier, error):
        with self.db() as db:
            row = db.execute('SELECT attempts FROM delivery_retries WHERE peer=? AND id=?',(peer,identifier)).fetchone()
            count = row[0] if row else 0
            delay = min(60, 2 ** min(6, count))
            db.execute('INSERT INTO delivery_retries VALUES(?,?,?,?) ON CONFLICT(peer,id)DO UPDATE SET attempts=excluded.attempts,next=excluded.next',(peer,identifier,count+1,time.time()+delay))
            db.execute('UPDATE rpc SET error=?,updated=? WHERE direction=? AND peer=? AND id=?',(type(error).__name__,time.time(),direction,peer,identifier))

    def pending(self, direction, peer=None):
        with self.db() as db:
            sql = "SELECT * FROM rpc WHERE direction=? AND state!='done'"
            args = [direction]
            if direction == 'in':
                sql += ' AND NOT EXISTS(SELECT 1 FROM delivery_retries r WHERE r.peer=rpc.peer AND r.id=rpc.id AND r.next>?)'
                args.append(time.time())
            if peer is not None:
                sql += ' AND peer=?'
                args.append(peer)
            return [dict(r) for r in db.execute(sql + ' ORDER BY created LIMIT 256', args)]

    def prune(self, days=30):
        """Compact old payloads; permanent small tombstones still refuse ID reuse."""
        with self.db() as db:
            rows = db.execute("SELECT rowid,* FROM rpc WHERE state='done' AND updated<? LIMIT 500", (time.time()-max(1,days)*86400,)).fetchall()
            for row in rows:
                db.execute('INSERT OR IGNORE INTO tombstones VALUES(?,?,?,?,?,?)', (row['direction'],row['peer'],row['id'],row['hash'],row['result'],row['updated']))
                db.execute('DELETE FROM rpc WHERE rowid=?',(row['rowid'],))

    def route(self, agent, peer, thread):
        with self.db() as db:
            old = db.execute('SELECT peer FROM routes WHERE agent=?', (agent,)).fetchone()
            if old and old['peer'] != peer:
                raise ValueError('Agent route identity collision')
            db.execute('INSERT INTO routes VALUES(?,?,?) ON CONFLICT(agent) DO UPDATE SET thread=excluded.thread', (agent, peer, thread))

    def resolve(self, selector):
        with self.db() as db:
            rows = db.execute('SELECT * FROM routes WHERE agent=? OR thread=?', (selector, selector)).fetchall()
            if len(rows) != 1:
                raise ValueError('Unknown or ambiguous agent; not queued')
            return dict(rows[0])

def native_turns(client, thread):
    cursor = None
    while True:
        params = {'threadId':thread,'limit':20,'itemsView':'full','sortDirection':'desc'}
        if cursor:
            params['cursor'] = cursor
        page = client.call('thread/turns/list', params)
        yield from page['data']
        following = page.get('nextCursor')
        if not following:
            return
        if following == cursor:
            raise RuntimeError('Native pagination did not advance')
        cursor = following


class Native:

    def __init__(self, node):
        self.node = node
        self.clients = {}
        self.locks = {}
        self.connection_lock = threading.Lock()

    def client(self, home):
        with self.connection_lock:
            return self._client(home)

    def _client(self, home):
        config = self.node.bridge_config(home)
        path = config.get('runtime_socket')
        if not path:
            return None
        from csl_bridge_rpc import Runtime
        old = self.clients.get(home)
        if old is None or not old.connected:
            if old:
                old.close()
            old = Runtime(path, self.node.config.get('proxy_executable'))
            self.clients[home] = old
            threading.Thread(target=self.drain, args=(old,), daemon=True).start()
        return old

    def drain(self, client):
        while client.connected and (not STOP.is_set()):
            try:
                client.events.get(timeout=1)
            except queue.Empty:
                pass

    def inject(self, agent, request, session=None):
        home, thread = self.node.agent(agent)
        client = self.client(home)
        if client is None:
            return {'state': 'mail_ready_for_client', 'request_id': request['id']}
        lock = self.locks.setdefault(thread, threading.Lock())
        with lock:
            try:
                response = client.call('thread/read', {'threadId': thread, 'includeTurns': False})
            except Exception:
                response = client.call('thread/resume', {'threadId': thread, 'excludeTurns': True})
            active = None
            for turn in native_turns(client, thread):
                if turn.get('status') == 'inProgress':
                    active = turn
                if any((item.get('type') == 'userMessage' and item.get('clientId') == request['id'] for item in turn.get('items', []))):
                    return {'state': 'delivered', 'request_id': request['id'], 'thread_id': thread, 'turn_id': turn['id'], 'reconciled': True}
            from csl_bridge_store import frame_message
            text = request.get('text') or request.get('envelope', {}).get('body', '')
            text = frame_message(request['source'], request.get('header_task_id', request['id']), session or request.get('session'), text)
            params = {'threadId': thread, 'clientUserMessageId': request['id'], 'input': [{'type': 'text', 'text': text}]}
            if active:
                result = client.call('turn/steer', {**params, 'expectedTurnId': active['id']})
                turn = result['turnId']
            else:
                result = client.call('turn/start', params)
                turn = result['turn']['id']
            return {'state': 'delivered', 'request_id': request['id'], 'thread_id': thread, 'turn_id': turn}

class Channel:
    """Reader never performs pipe writes; a bounded writer owns backpressure."""
    LIVENESS = 20
    def __init__(self, node, peer, reader, writer, close):
        self.node, self.peer = node, peer
        self.reader, self.writer, self.close_fn = reader, writer, close
        self.closed = False
        self.sent, self.acked = {}, set()
        self.state_lock = threading.Lock()
        self.outgoing = queue.Queue(maxsize=64)
        self.last_received = time.monotonic()
        self.revoked = False
        self.writer_thread = threading.Thread(target=self.write_loop, daemon=True)
        self.writer_thread.start()

    def send(self, value):
        raw = (wire(value) + '\n').encode()
        if len(raw) > MAX:
            raise ValueError('Frame too large')
        if self.closed:
            return False
        try:
            self.outgoing.put_nowait(raw)
            return True
        except queue.Full:
            # Durable frames replay on the next connection. Never block a reader.
            self.close()
            return False

    def write_loop(self):
        try:
            while not self.closed and not STOP.is_set():
                try:
                    raw = self.outgoing.get(timeout=.2)
                except queue.Empty:
                    continue
                view = memoryview(raw)
                while view and not self.closed:
                    count = self.writer.write(view)
                    if not count:
                        raise EOFError('Stream closed')
                    view = view[count:]
                self.writer.flush()
        except (OSError, EOFError, ValueError):
            self.close()

    def close(self):
        if not self.closed:
            self.closed = True
            try:
                self.close_fn()
            except OSError:
                pass

    def hello(self, frame):
        config = self.node.config['links'][self.peer]
        if frame.get('node') != self.peer:
            raise ValueError('Wrong environment identity')
        for agent, thread in frame.get('agents', {}).items():
            if self.node.is_router:
                if agent not in config.get('agents', []):
                    raise ValueError('Unconfigured source agent')
                pinned = config.get('agent_threads', {}).get(agent)
                if pinned and pinned != thread:
                    raise ValueError('Enrolled thread changed')
            if agent in self.node.agents():
                if self.node.agents()[agent] != thread:
                    raise ValueError('Local agent identity collision')
                continue
            self.node.journal.route(agent, self.peer, thread)

    def run(self):
        with self.node.channel_lock:
            previous = self.node.channels.get(self.peer)
            if previous:
                previous.close()
            self.node.channels[self.peer] = self
        self.send({'type': 'hello', 'node': self.node.config['node'], 'agents': self.node.advertise()})
        log('connected', peer=self.peer)
        threading.Thread(target=self.pump, daemon=True).start()
        try:
            while not STOP.is_set() and not self.closed:
                frame = read_line(self.reader)
                self.last_received = time.monotonic()
                kind = frame.get('type')
                if kind == 'hello':
                    self.hello(frame)
                elif kind in ('request', 'event'):
                    try:
                        self.node.authenticate(self.peer, frame)
                        row = self.node.journal.put('in', self.peer, frame['id'], frame)
                    except (ValueError, KeyError, TypeError):
                        from csl_bridge_store import identifier
                        identifier(frame.get('id'))
                        self.send({'type': 'response', 'id': frame['id'], 'result':
                                   {'ok': False, 'data': {'error': 'Frame refused'}}})
                        continue
                    self.send({'type': 'ack', 'id': frame['id']})
                    if row['state'] == 'done':
                        self.send({'type': 'response', 'id': frame['id'], 'result': json.loads(row['result'])})
                    else:
                        self.node.schedule(self.peer, frame['id'])
                elif kind == 'response':
                    self.node.journal.finish('out', self.peer, frame['id'], frame['result'])
                    with self.state_lock:
                        self.sent.pop(frame['id'], None)
                        self.acked.discard(frame['id'])
                elif kind == 'ack':
                    with self.state_lock:
                        self.acked.add(frame['id'])
                elif kind == 'ping':
                    self.send({'type': 'pong'})
                elif kind == 'revoked':
                    self.revoked = True
                    return
                elif kind != 'pong':
                    raise ValueError('Unknown frame')
        except (OSError, EOFError, ValueError, KeyError, TypeError) as error:
            log('disconnected', peer=self.peer, error=type(error).__name__)
        finally:
            self.close()
            with self.node.channel_lock:
                if self.node.channels.get(self.peer) is self:
                    self.node.channels.pop(self.peer, None)

    def pump(self):
        last_hello = 0
        try:
            while not self.closed and not STOP.is_set():
                now = time.monotonic()
                if now - self.last_received > self.LIVENESS:
                    self.close()
                    return
                if now - last_hello > 3:
                    self.send({'type': 'hello', 'node': self.node.config['node'], 'agents': self.node.advertise()})
                    self.send({'type': 'ping'})
                    last_hello = now
                rows = self.node.journal.pending('out', self.peer)
                pending = {r['id'] for r in rows}
                with self.state_lock:
                    self.sent = {i: t for i, t in self.sent.items() if i in pending}
                    self.acked.intersection_update(pending)
                for row in rows:
                    if self.outgoing.qsize() >= 32:
                        break
                    with self.state_lock:
                        due = now - self.sent.get(row['id'], -100) >= (60 if row['id'] in self.acked else 5)
                    if not due:
                        continue
                    if self.send(json.loads(row['payload'])):
                        with self.state_lock:
                            self.sent[row['id']] = now
                STOP.wait(.1)
        except (OSError, ValueError):
            self.close()

class Node:

    def __init__(self, config):
        self.config = config
        self.is_router = config.get('router', False)
        self.home = private_dir(config['home'])
        sys.path.append(config.get('csl_source', str(Path(__file__).resolve().parent)))
        self.journal = Journal(self.home / 'light.sqlite')
        self.channels = {}
        self.channel_lock = threading.Lock()
        self.executing = set()
        self.exec_lock = threading.Lock()
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=32)
        self.native = Native(self)
        self.link_workers = {}
        self.serving = False
        for agent, thread in self.agents().items():
            self.journal.route(agent, config['node'], thread)

    def bridge_config(self, home):
        with sql('file:' + str(Path(home) / 'bridge.sqlite') + '?mode=ro', uri=True) as db:
            return {k: json.loads(v) for k, v in db.execute('SELECT key,value FROM config')}

    def agents(self):
        result = {}
        for bridge in self.config['bridges'].values():
            with sql('file:' + str(Path(bridge['home']) / 'bridge.sqlite') + '?mode=ro', uri=True) as db:
                for agent, thread in db.execute('SELECT name,thread FROM agents'):
                    if agent in bridge['agents']:
                        result[agent] = thread
        return result

    def advertise(self):
        result = self.agents()
        if self.is_router:
            with self.journal.db() as db:
                result.update({r['agent']: r['thread'] for r in db.execute('SELECT agent,thread FROM routes WHERE thread IS NOT NULL')})
        return result

    def agent(self, selector):
        for bridge in self.config['bridges'].values():
            with sql('file:' + str(Path(bridge['home']) / 'bridge.sqlite') + '?mode=ro', uri=True) as db:
                rows = db.execute('SELECT name,thread FROM agents WHERE name=? OR thread=?', (selector, selector)).fetchall()
                if len(rows) == 1 and rows[0][0] in bridge['agents']:
                    return (bridge['home'], rows[0][1])
        raise ValueError('Unknown local registered agent')

    def authenticate(self, peer, frame):
        from csl_bridge_store import validate
        if frame.get('type') == 'event':
            from csl_bridge_engine import validate_event
            validate_event(frame['event'])
            request = frame['event']
        else:
            request = validate(frame['request'])
        if frame['id'] != request['id']:
            raise ValueError('Request id mismatch')
        source = request.get('source')
        link = self.config['links'][peer]
        if self.is_router:
            if source and source not in link.get('agents', []):
                raise ValueError('Wrong authenticated source')
            thread = link.get('agent_threads', {}).get(source)
            if source and not thread:
                with self.journal.db() as db:
                    row = db.execute('SELECT thread FROM routes WHERE agent=? AND peer=?', (source, peer)).fetchone()
                thread = row['thread'] if row else None
            if 'session' in request and request['session'] != thread:
                raise ValueError('Sender session differs from enrolled thread')
            frame['origin'] = peer
            frame['source_session'] = thread
            if thread and 'session' in request:
                request['session'] = thread

    def schedule(self, peer, identifier):
        key = (peer, identifier)
        with self.exec_lock:
            if key in self.executing or len(self.executing) >= 32 or sum(p == peer for p, _ in self.executing) >= 4:
                return
            if not self.journal.retry_due(peer, identifier):
                return
            self.executing.add(key)
        self.pool.submit(self.deliver, peer, identifier)

    def mirror(self, home, peer, request):
        from csl_bridge_store import Store, encode
        from csl_bridge_mailbox import schema
        store = Store(home)
        schema(store)
        try:
            if request['action'] == 'receipt':
                row = store.rows('SELECT persisted_at,acknowledged_at FROM mail_messages WHERE id=?', (request['message_id'],))
                return {'state': 'persisted', **dict(row[0])} if row else {'state': 'not_found'}
            if request['action'] == 'ack':
                with store.transaction() as db:
                    db.execute('UPDATE mail_messages SET acknowledged_at=coalesce(acknowledged_at,?) WHERE id=? AND direction=? AND source=? AND target=?', (request['acknowledged_at'], request['message_id'], 'out', request['target'], request['source']))
                return {'state': 'acknowledged', 'message_id': request['message_id']}
            digest = hashlib.sha256(encode(request).encode()).hexdigest()
            stamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            with store.transaction() as db:
                row = db.execute('SELECT fingerprint,direction FROM mail_messages WHERE id=?', (request['id'],)).fetchone()
                if row and (row['fingerprint'] != digest or row['direction'] != 'in'):
                    raise ValueError('Mailbox id collision')
                db.execute('INSERT OR IGNORE INTO mail_messages(id,direction,peer,source,target,fingerprint,envelope,persisted_at)VALUES(?,?,?,?,?,?,?,?)', (request['id'], 'in', peer, request['source'], request['target'], digest, encode(request['envelope']), stamp))
            return {'state': 'persisted', 'message_id': request['id'], 'persisted_at': stamp, 'acknowledged_at': None}
        finally:
            store.db.close()

    def dispatch(self, peer, frame):
        from csl_bridge_transport import local_call
        if frame.get('type') == 'event':
            return self.dispatch_event(peer, frame)
        request = frame['request']
        target = request.get('target')
        bridges = self.config['bridges']
        destination = frame.get('destination')
        if target and target not in self.agents() and self.is_router:
            route = self.journal.resolve(target)
            if route['peer'] == peer:
                raise ValueError('Routing loop')
            forwarded = self.exchange(route['peer'], request, peer, destination=route['peer'], session=frame.get('source_session'), timeout=0)
            if forwarded['ok'] and forwarded['data'].get('state') == 'queued':
                raise RuntimeError('Destination queued; await its native receipt')
            if not forwarded['ok']:
                raise ValueError(forwarded['data'].get('error', 'Forwarding refused'))
            return forwarded['data']
        if target:
            home, _ = self.agent(target)
        else:
            selected = bridges.get(destination) or bridges.get(frame.get('origin')) or next(iter(bridges.values()))
            home = selected['home']
        trusted = frame.get('origin',peer) if peer == self.config['node'] else (peer if self.is_router else frame.get('origin', peer))
        if request['op'] == 'mail':
            result = self.mirror(home, trusted, request)
            if request['action'] == 'send':
                self.native.inject(target, request, frame.get('source_session'))
            return result
        if request['op'] in ('followup', 'send_message'):
            if self.native.client(home) is None:
                raise ValueError('Followup requires an existing Codex endpoint; use send for client-only agents')
            from csl_bridge_store import encode
            core_request = request
            raw = encode(core_request)
            digest = hashlib.sha256(raw.encode()).hexdigest()
            with sql(Path(home) / 'bridge.sqlite') as db:
                old = db.execute('SELECT peer,hash FROM operations WHERE id=?', (request['id'],)).fetchone()
                if old and (old[0] != trusted or old[1] != digest):
                    raise ValueError('Existing native operation identity collision')
                db.execute('INSERT OR IGNORE INTO operations VALUES(?,?,?,?,?,?)', (request['id'], trusted, digest, raw, 'invoking', encode({'state': 'uncertain', 'request_id': request['id']})))
                if request['op'] == 'followup':
                    db.execute('INSERT OR IGNORE INTO tasks VALUES(?,?,?,?,NULL,?)', (request['id'], trusted, request['source'], target, 'pending'))
            result = self.native.inject(target, request, frame.get('source_session'))
            with sql(Path(home) / 'bridge.sqlite') as db:
                db.execute('UPDATE operations SET state=?,result=? WHERE id=?', ('delivered', encode(result), request['id']))
                if request['op'] == 'followup':
                    db.execute('UPDATE tasks SET turn=?,status=? WHERE id=?', (result['turn_id'], 'running', request['id']))
            return result
        return local_call(home, {'local_op': '_incoming', 'trusted_peer': trusted, 'request': request})

    def dispatch_event(self, peer, frame):
        from csl_bridge_engine import validate_event
        from csl_bridge_store import encode
        from csl_bridge_transport import local_call
        event = frame['event']
        validate_event(event)
        if event['target'] not in self.agents():
            if not self.is_router:
                raise ValueError('Unknown event recipient')
            destination = self.journal.resolve(event['target'])['peer']
            if destination == peer:
                raise ValueError('Event routing loop')
            self.journal.put('out', destination, frame['id'], frame)
            return {'state': 'forwarded'}
        home, _ = self.agent(event['target'])
        with sql(Path(home)/'bridge.sqlite') as db:
            row = db.execute("SELECT peer,request,result FROM operations WHERE id=? AND peer LIKE 'outbound:%'",(event['task_id'],)).fetchone()
            if not row:
                raise ValueError('Event has no delegated task')
            request = json.loads(row[1])
            if request.get('source') != event['target'] or request.get('target') != event['source']:
                raise ValueError('Event actors differ from delegated task')
            receipt = json.loads(row[2]) if row[2] else {}
            if not receipt.get('turn_id'):
                with self.journal.db() as outbox:
                    reply = outbox.execute("SELECT result FROM rpc WHERE direction='out' AND id=? AND state='done'",(event['task_id'],)).fetchone()
                if reply:
                    receipt = json.loads(reply[0]).get('data',{})
            if not receipt.get('turn_id'):
                raise RuntimeError('Waiting for original native acceptance receipt')
            if receipt['turn_id'] != event['turn_id']:
                raise ValueError('Event turn differs from delegated turn')
            prior = db.execute('SELECT body FROM inbound WHERE id=?',(event['id'],)).fetchone()
            if prior and prior[0] != encode(event):
                raise ValueError('Event id collision')
            db.execute("INSERT OR IGNORE INTO inbound(id,peer,body,delivery)VALUES(?,?,?,'uncertain')",(event['id'],row[0].removeprefix('outbound:'),encode(event)))
        if event['text']:
            self.native.inject(event['target'], {'id':event['id'],'source':event['source'],
                'text':event['text'],'header_task_id':event['task_id']}, frame.get('source_session') or event.get('session'))
        with sql(Path(home)/'bridge.sqlite') as db:
            db.execute("UPDATE inbound SET delivery='delivered' WHERE id=?",(event['id'],))
        try:
            local_call(home,{'local_op':'_light_events_changed'},timeout=1)
        except (ValueError,OSError):
            pass
        return {'state':'delivered','event_id':event['id']}

    def finish_pending(self):
        """Recover native completions even without a separately running connector."""
        from csl_bridge_store import Store
        from csl_bridge_engine import publish_task_completion
        for bridge in self.config['bridges'].values():
            home = bridge['home']
            store = Store(home)
            try:
                pending = store.rows("SELECT target,turn FROM tasks WHERE status IN ('running','pending') AND turn IS NOT NULL")
                for agent in {row['target'] for row in pending} & set(bridge['agents']):
                    client = self.native.client(home)
                    if client is None:
                        continue
                    turns = native_turns(client, self.agents()[agent])
                    wanted = {r['turn']for r in pending if r['target']==agent}
                    for turn in turns:
                        if turn['id'] in wanted and turn['status'] in ('completed','failed','interrupted'):
                            answer = '\n'.join(item.get('text','')for item in turn.get('items',[])if item.get('type')=='agentMessage')[-12000:]
                            publish_task_completion(store,agent,self.agents()[agent],turn['id'],turn['status'],answer)
            finally:
                store.close()

    def queue_events(self):
        """Push existing CSL progress/final envelopes through the durable stream."""
        with self.journal.db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS event_cursors(home TEXT PRIMARY KEY,seq INTEGER)')
        for bridge in self.config['bridges'].values():
            home = bridge['home']
            with self.journal.db() as db:
                cursor = db.execute('SELECT seq FROM event_cursors WHERE home=?',(home,)).fetchone()
            after = cursor[0] if cursor else 0
            with sql(Path(home)/'bridge.sqlite') as db:
                rows = db.execute('SELECT seq,body FROM outbound WHERE seq>? ORDER BY seq LIMIT 100',(after,)).fetchall()
            for seq,body in rows:
                event = json.loads(body)
                with self.journal.db() as db:
                    original = db.execute("SELECT peer FROM rpc WHERE direction='in' AND id=?",(event['task_id'],)).fetchone()
                if original:
                    peer = original[0]
                    existing = self.journal.get('out',peer,event['id'])
                    frame = json.loads(existing['payload'])if existing else {'type':'event','id':event['id'],'event':event,'source_session':self.agents().get(event['source'])}
                    self.journal.put('out',peer,event['id'],frame)
                with self.journal.db() as db:
                    db.execute('INSERT INTO event_cursors VALUES(?,?) ON CONFLICT(home)DO UPDATE SET seq=excluded.seq',(home,seq))

    def deliver(self, peer, identifier):
        try:
            row = self.journal.get('in', peer, identifier)
            frame = json.loads(row['payload'])
            try:
                result = {'ok': True, 'data': self.dispatch(peer, frame)}
            except __import__('csl_bridge_store').BridgeError:
                raise
            except (ValueError, KeyError) as error:
                result = {'ok': False, 'data': {'error': str(error)}}
            self.journal.finish('in', peer, identifier, result)
            with self.channel_lock:
                channel = self.channels.get(peer)
            if channel:
                channel.send({'type': 'response', 'id': identifier, 'result': result})
        except Exception as error:
            self.journal.error('in', peer, identifier, error)
        finally:
            with self.exec_lock:
                self.executing.discard((peer, identifier))

    def exchange(self, peer, request, origin, destination=None, session=None, timeout=52):
        from csl_bridge_store import validate
        validate(request)
        is_local = peer == self.config['node']
        if not is_local and peer not in self.config['links']:
            if self.is_router:
                peer = self.journal.resolve(peer)['peer']
            else:
                destination = destination or peer
                peer = self.config['upstream']
        source = request.get('source')
        if session is None and source:
            try:
                session = self.agents()[source]
            except KeyError:
                if not self.is_router:
                    raise ValueError('Unknown local source')
        if is_local and request['op'] != 'mail':
            raise ValueError('Use send for messages within the same light node')
        if source in self.agents() and request['op'] in ('followup', 'send_message'):
            from csl_bridge_store import Store
            source_home, _ = self.agent(source)
            store = Store(source_home)
            try:
                existing = store.rows('SELECT peer,hash FROM operations WHERE id=?',(request['id'],))
                if existing:
                    from csl_bridge_store import encode
                    if not existing[0]['peer'].startswith('outbound:') or existing[0]['hash'] != hashlib.sha256(encode(request).encode()).hexdigest():
                        raise ValueError('Outbound task id collision')
                else:
                    store.reserve('outbound:' + peer, request)
            finally:
                store.close()
        frame = {'type': 'request', 'id': request['id'], 'request': request, 'origin': origin, 'destination': destination or peer, 'source_session': session}
        row = self.journal.put('out', peer, request['id'], frame)
        if is_local:
            self.journal.put('in',peer,request['id'],json.loads(row['payload']))
            if self.serving:
                self.schedule(peer,request['id'])
        end = time.monotonic() + timeout
        while not STOP.is_set():
            row = self.journal.get('out', peer, request['id'])
            if row['state'] == 'done':
                result = json.loads(row['result'])
                if source in self.agents() and request['op'] in ('followup','send_message') and result.get('ok'):
                    with sql(Path(source_home)/'bridge.sqlite') as db:
                        db.execute('UPDATE operations SET state=?,result=? WHERE id=?', (result['data'].get('state','delivered'), wire(result['data']),request['id']))
                return result
            if time.monotonic() >= end:
                break
            time.sleep(0.05)
        return {'ok': True, 'data': {'state': 'queued', 'request_id': request['id'], 'peer': peer, 'offline': peer not in self.channels}}

    def ssh_loop(self, peer, config):
        delay = 1
        while not STOP.is_set() and self.config['links'].get(peer) == config:
            key = Path(config['key'])
            if (self.home / ('revoked-' + peer)).exists():
                return
            if not key.exists():
                time.sleep(1)
                continue
            argv = ['ssh', '-T', '-oBatchMode=yes', '-oIdentitiesOnly=yes', '-oStrictHostKeyChecking=yes', '-oClearAllForwardings=yes', '-oForwardAgent=no', '-oControlMaster=no', '-oConnectTimeout=5', '-oServerAliveInterval=5', '-oServerAliveCountMax=2', '-oUserKnownHostsFile=' + config.get('known_hosts', str(Path.home() / '.ssh/known_hosts')), '-i', str(key), config.get('endpoint', peer), config.get('remote_command', '')]
            if config.get('connect_command'):
                argv = list(config['connect_command'])
            try:
                process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, start_new_session=True)
                channel = Channel(self, peer, process.stdout, process.stdin, lambda process=process: stop_process(process))

                def revoke():
                    while process.poll() is None and (not STOP.is_set()):
                        if not key.exists():
                            stop_process(process)
                            return
                        time.sleep(1)
                threading.Thread(target=revoke, daemon=True).start()
                channel.run()
                if channel.revoked:
                    atomic_disabled = self.home / ('revoked-' + peer)
                    atomic_disabled.write_text('Guest key marker revoked; re-enroll to clear.\n')
                    return
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            except OSError as error:
                log('connect_failed', peer=peer, error=type(error).__name__)
            STOP.wait(delay)
            delay = min(5, delay + 1)

    def pump_local(self):
        for row in self.journal.pending('out',self.config['node']):
            incoming = self.journal.get('in',self.config['node'],row['id'])
            if incoming and incoming['state'] == 'done':
                self.journal.finish('out',self.config['node'],row['id'],json.loads(incoming['result']))

    def sync_links(self):
        """Reload owner config without dropping unaffected streams."""
        candidate = load(self.home / 'config.json')
        if candidate['node'] != self.config['node'] or candidate['home'] != str(self.home):
            raise ValueError('Changing node identity requires an explicit restart')
        old = self.config['links']
        self.config = candidate
        for peer in set(old) | set(candidate['links']):
            current = candidate['links'].get(peer)
            if old.get(peer) != current:
                channel = self.channels.get(peer)
                if channel:
                    channel.close()
            if not current:
                continue
            for agent in current.get('agents', []):
                thread = current.get('agent_threads', {}).get(agent)
                if thread:
                    self.journal.route(agent, peer, thread)
            worker = self.link_workers.get(peer)
            if (current.get('endpoint') or current.get('connect_command')) and (worker is None or not worker.is_alive()):
                worker = threading.Thread(target=self.ssh_loop, args=(peer, current), daemon=True)
                self.link_workers[peer] = worker
                worker.start()

    def retry_loop(self):
        last_prune = 0
        while not STOP.is_set():
            if time.monotonic() - last_prune > 3600:
                if self.config.get('retention_days', 0) > 0:
                    self.journal.prune(self.config['retention_days'])
                last_prune = time.monotonic()
            try:
                self.sync_links()
                self.pump_local()
            except (OSError, ValueError, KeyError) as error:
                log('config_reload_deferred', error=type(error).__name__)
            for row in self.journal.pending('in'):
                self.schedule(row['peer'], row['id'])
            for step in (self.finish_pending, self.queue_events):
                try:
                    step()
                except Exception as error:
                    log('event_delivery_deferred',step=step.__name__,error=type(error).__name__)
            for agent, thread in self.agents().items():
                self.journal.route(agent, self.config['node'], thread)
            STOP.wait(1)

    def handle(self, conn):
        with conn:
            conn.settimeout(60)
            reader = conn.makefile('rb')
            request = read_line(reader)
            if request.get('action') == 'attach':
                peer = request['peer']
                if peer not in self.config['links']:
                    raise ValueError('Unknown attach peer')
                conn.settimeout(None)
                Channel(self, peer, reader, conn.makefile('wb', buffering=0), lambda: conn.shutdown(socket.SHUT_RDWR)).run()
                return
            try:
                action = request['action']
                if action == 'exchange':
                    result = self.exchange(request['peer'], request['request'], request['origin'])
                elif action == 'agents':
                    result = {'ok': True, 'data': self.agents()}
                else:
                    raise ValueError('Unknown local action')
            except Exception as error:
                result = {'ok': False, 'data': {'error': str(error)}}
            try:
                conn.sendall((wire(result) + '\n').encode())
            except (BrokenPipeError, ConnectionResetError):
                pass

    def run(self):
        lock = (self.home / 'broker.lock').open('a')
        try:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError('A light broker already owns this home') from None
            self.serving = True
            self._serve()
        finally:
            if self.serving:
                self.close()
            lock.close()

    def close(self):
        for channel in list(self.channels.values()):
            channel.close()
        for client in self.native.clients.values():
            client.close()
        self.pool.shutdown(wait=False,cancel_futures=True)
        (self.home/'light.sock').unlink(missing_ok=True)

    def _serve(self):
        sockpath = self.home / 'light.sock'
        if sockpath.exists():
            sockpath.unlink()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sockpath))
            sockpath.chmod(0o600)
            server.listen(32)
            server.settimeout(1)
            threading.Thread(target=self.retry_loop, daemon=True).start()
            while not STOP.is_set():
                try:
                    conn, _ = server.accept()
                    threading.Thread(target=self.handle, args=(conn,), daemon=True).start()
                except socket.timeout:
                    pass

def relay_duplex(left_read, left_write, right_read, right_write, alive):
    """Independent bounded pumps: a blocked stdout cannot stop stdin draining."""
    stopped = threading.Event()
    def pump(reader, writer):
        try:
            while not stopped.is_set():
                raw = reader(65536)
                if not raw:
                    break
                view = memoryview(raw)
                while view:
                    count = writer(view)
                    if not count:
                        raise EOFError()
                    view = view[count:]
        except (OSError, EOFError):
            pass
        finally:
            stopped.set()
    for reader, writer in ((left_read, right_write), (right_read, left_write)):
        threading.Thread(target=pump, args=(reader, writer), daemon=True).start()
    while not stopped.wait(.2):
        if not alive():
            break


def attach(home, peer, key_file):
    if os.environ.get('SSH_ORIGINAL_COMMAND', '') not in ('', 'csl-light-stream'):
        raise ValueError('Only the fixed stream is allowed')
    key = Path(key_file)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(str(Path(home) / 'light.sock'))
        if not key.exists():
            # Only a trusted local attach process emits this control frame.
            sys.stdout.buffer.write((wire({'type': 'revoked'}) + '\n').encode())
            sys.stdout.buffer.flush()
            return
        original = hashlib.sha256(key.read_bytes()).digest()
        s.sendall((wire({'action': 'attach', 'peer': peer}) + '\n').encode())
        def alive():
            if not key.exists() or hashlib.sha256(key.read_bytes()).digest() != original:
                return False
            return not STOP.is_set()
        relay_duplex(lambda n: os.read(sys.stdin.fileno(), n),
                     lambda v: os.write(sys.stdout.fileno(), v), s.recv, s.send, alive)
        s.shutdown(socket.SHUT_RDWR)

def main(argv=None):
    os.umask(0o077)
    p = argparse.ArgumentParser()
    p.add_argument('--home', type=Path, default=Path.home() / '.local/share/csl-light')
    sub = p.add_subparsers(dest='command', required=True)
    sub.add_parser('run')
    init = sub.add_parser('init')
    init.add_argument('--node', required=True)
    init.add_argument('--bridge-home', type=Path, action='append', required=True)
    init.add_argument('--agent', action='append', required=True)
    init.add_argument('--router', action='store_true')
    init.add_argument('--service', action='store_true')
    enroll = sub.add_parser('enroll')
    enroll.add_argument('enroll_args', nargs=argparse.REMAINDER)
    e = sub.add_parser('exchange')
    e.add_argument('--peer', required=True)
    e.add_argument('--origin', required=True)
    a = sub.add_parser('attach')
    a.add_argument('--peer', required=True)
    a.add_argument('--key-file', required=True)
    for name in ('send', 'followup'):
        c = sub.add_parser(name)
        c.add_argument('target')
        c.add_argument('file', type=Path)
        c.add_argument('--from', dest='source')
        c.add_argument('--id')
    args = p.parse_args(argv)
    if args.command == 'init':
        from enroll_light import initialize
        print(wire(initialize(args.home, args.node, args.bridge_home, args.agent, args.router, args.service)))
        return
    if args.command == 'enroll':
        from enroll_light import main as enroll_main
        enroll_main(['--home', str(args.home), *args.enroll_args])
        return
    if args.command == 'run':
        STOP.clear()
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, lambda *_: STOP.set())
        Node(load(args.home / 'config.json')).run()
        return
    if args.command == 'attach':
        attach(args.home, args.peer, args.key_file)
        return
    if args.command == 'exchange':
        request = read_line(sys.stdin.buffer)
        print(wire(local(args.home, {'action': 'exchange', 'peer': args.peer, 'origin': args.origin, 'request': request})))
        return
    config = load(args.home / 'config.json')
    sys.path.append(config.get('csl_source', str(Path(__file__).resolve().parent)))
    from csl_bridge_store import validate
    source = args.source or config['default_agent']
    identifier = args.id or str(uuid.uuid4())
    target = args.target
    node = Node(config)
    route = node.journal.resolve(target)
    target = route['agent']
    origin = next((key for key, b in config['bridges'].items() if source in b['agents']))
    from csl_bridge import reviewed_text
    text = reviewed_text(args.file, False)
    if args.command == 'followup':
        request = {'version': 1, 'op': 'followup', 'id': identifier, 'source': source, 'target': target, 'text': text, 'session': node.agents()[source]}
    else:
        request = {'version': 1, 'op': 'mail', 'message_version': 1, 'action': 'send', 'id': identifier, 'source': source, 'target': target, 'envelope': {'kind': 'message', 'body': text, 'in_reply_to': None, 'grant_ref': None, 'created_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'expires_at': None}}
    validate(request)
    print(wire(local(args.home, {'action': 'exchange', 'peer': route['peer'], 'origin': origin, 'request': request})))
if __name__ == '__main__':
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: STOP.set())
    try:
        main()
    except Exception as error:
        log('refused', error=type(error).__name__, errno=getattr(error, 'errno', None))
        raise SystemExit(1)

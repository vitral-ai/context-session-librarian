"""Persistent CSL stream. Standard library; no pool client, lease or governor dependency."""
import argparse, contextlib, concurrent.futures, fcntl, hashlib, json, os, queue, selectors, signal, socket, sqlite3, subprocess, sys, threading, time, uuid
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
            return row is None or row[0] <= time.monotonic()

    def error(self, direction, peer, identifier, error):
        with self.db() as db:
            row = db.execute('SELECT attempts FROM delivery_retries WHERE peer=? AND id=?',(peer,identifier)).fetchone()
            count = row[0] if row else 0
            delay = min(60, 2 ** min(6, count))
            db.execute('INSERT INTO delivery_retries VALUES(?,?,?,?) ON CONFLICT(peer,id)DO UPDATE SET attempts=excluded.attempts,next=excluded.next',(peer,identifier,count+1,time.monotonic()+delay))
            db.execute('UPDATE rpc SET error=?,updated=? WHERE direction=? AND peer=? AND id=?',(type(error).__name__,time.time(),direction,peer,identifier))

    def pending(self, direction, peer=None):
        with self.db() as db:
            sql = "SELECT * FROM rpc WHERE direction=? AND state!='done'"
            args = [direction]
            if direction == 'in':
                sql += ' AND NOT EXISTS(SELECT 1 FROM delivery_retries r WHERE r.peer=rpc.peer AND r.id=rpc.id AND r.next>?)'
                args.append(time.monotonic())
            if peer is not None:
                sql += ' AND peer=?'
                args.append(peer)
            return [dict(r) for r in db.execute(sql + ' ORDER BY created LIMIT 256', args)]

    def reset_retries(self):
        # These are scheduling hints, not delivery records; reset at broker startup.
        with self.db() as db:
            db.execute('DELETE FROM delivery_retries')

    def ready_out(self, peer, deadlines=None):
        """Scan IDs before the window; load only eligible payloads (at most 256)."""
        deadlines = deadlines or {}
        now, selected = time.monotonic(), []
        with self.db() as db:
            cursor = db.execute("SELECT id FROM rpc WHERE direction='out' AND peer=? "
                                "AND state!='done' ORDER BY created",(peer,))
            for row in cursor:
                if deadlines.get(row['id'],0) <= now:
                    selected.append(row['id'])
                    if len(selected) == 256:
                        break
            if not selected:
                return []
            marks = ','.join('?' for _ in selected)
            return [dict(row) for row in db.execute(
                "SELECT * FROM rpc WHERE direction='out' AND peer=? AND state!='done' "
                "AND id IN (" + marks + ") ORDER BY created",[peer,*selected])]

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
        binding = request.get('_native_binding')
        if binding:
            home, thread, purpose, fingerprint = binding
            from csl_bridge_store import Store
            from csl_bridge_event_state import EventState
            store = Store(home)
            try:
                with store.transaction() as db:
                    EventState.reserve_id(db, request['id'], purpose, fingerprint, thread)
            finally:
                store.close()
        else:
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
            else:
                status = response.get('thread', {}).get('status')
                if isinstance(status, dict) and status.get('type') == 'notLoaded':
                    response = client.call('thread/resume', {'threadId': thread, 'excludeTurns': True})
            page = client.call('thread/turns/list', {'threadId':thread,'limit':20,'itemsView':'full','sortDirection':'desc'})
            turns = page.get('data', [])
            for turn in turns:
                if any((item.get('type') == 'userMessage' and item.get('clientId') == request['id'] for item in turn.get('items', []))):
                    return {'state': 'delivered', 'request_id': request['id'], 'thread_id': thread, 'turn_id': turn['id'], 'reconciled': True}
            from csl_bridge_store import frame_message
            text = request.get('text') or request.get('envelope', {}).get('body', '')
            text = frame_message(request['source'], request.get('header_task_id', request['id']), session or request.get('session'), text)
            params = {'threadId': thread, 'clientUserMessageId': request['id'], 'input': [{'type': 'text', 'text': text}]}
            active = next((turn for turn in turns if turn.get('status') == 'inProgress'), None)
            if active:
                result = client.call('turn/steer', {**params, 'expectedTurnId': active['id']})
                turn = result['turnId']
            else:
                result = client.call('turn/start', params)
                turn = result['turn']['id']
            return {'state': 'delivered', 'request_id': request['id'], 'thread_id': thread, 'turn_id': turn}

    def event_available(self, home, event, thread):
        try:
            selected, registered = self.node.agent(event['target'])
        except ValueError:
            return False
        return (Path(selected).resolve() == Path(home).resolve() and registered == thread and self.client(home) is not None)

    def send_event(self, home, claim, event, session=None):
        """A NEW granted effect only; never called by uncertain recovery."""
        from csl_bridge_store import Store, frame_message
        from csl_bridge_event_state import digest, EventConflict
        store = Store(home)
        try:
            with store.transaction() as db:
                reserved = db.execute('SELECT * FROM event_native_ids WHERE id=?', (claim['client_id'],)).fetchone()
                if 'key' in claim:
                    effect = db.execute('SELECT * FROM event_effects WHERE key=?', (claim['key'],)).fetchone()
                    if (not effect or effect['state'] != 'uncertain' or effect['attempt'] != claim['number'] or
                            effect['client_id'] != claim['client_id'] or effect['thread'] != claim['thread']):
                        raise EventConflict('Native final grant changed')
                    binding = digest([effect['key'], effect['peer'], effect['task'], effect['projection']])
                    body = db.execute('SELECT body FROM inbound WHERE seq=?', (effect['representative'],)).fetchone()[0]
                    purpose = 'final'
                else:
                    effect = db.execute('SELECT * FROM event_progress_effects WHERE wire_id=?', (claim['client_id'],)).fetchone()
                    if (not effect or effect['state'] != 'uncertain' or effect['number'] != claim['number'] or effect['thread'] != claim['thread']):
                        raise EventConflict('Native progress grant changed')
                    body = db.execute('SELECT body FROM inbound WHERE id=?', (claim['client_id'],)).fetchone()[0]
                    binding, purpose = hashlib.sha256(body.encode()).hexdigest(), 'progress'
                if (not reserved or (reserved['purpose'], reserved['binding'], reserved['thread']) != (purpose, binding, claim['thread']) or
                        json.loads(body) != event):
                    raise EventConflict('Native event purpose/payload binding differs')
        finally:
            store.close()
        client = self.client(home)
        if client is None:
            return {'state': 'mail_ready_for_client', 'request_id': claim['client_id']}
        thread = claim['thread']
        with self.locks.setdefault(thread, threading.Lock()):
            from csl_bridge_rpc import RuntimeRejected
            # Metadata failures occur BEFORE user input: a new claim may retry.
            try:
                try:
                    response = client.call('thread/read', {'threadId': thread, 'includeTurns': False})
                except Exception:
                    response = client.call('thread/resume', {'threadId': thread, 'excludeTurns': True})
                else:
                    if response.get('thread', {}).get('status', {}).get('type') == 'notLoaded':
                        response = client.call('thread/resume', {'threadId': thread, 'excludeTurns': True})
                turns = client.call('thread/turns/list', {'threadId': thread, 'limit': 20, 'itemsView': 'full', 'sortDirection': 'desc'}).get('data', [])[:20]
                active = next((turn for turn in turns if turn.get('status') == 'inProgress'), None)
                if response.get('thread', {}).get('status', {}).get('type') == 'active' and active is None:
                    raise RuntimeRejected('Active recipient turn is not observable')
            except Exception as error:
                raise RuntimeRejected('Event metadata unavailable before user input') from error
            params = {'threadId': thread, 'clientUserMessageId': claim['client_id'],
                      'input': [{'type': 'text', 'text': frame_message(event['source'], event['task_id'], session or event.get('session'), event['text'])}]}
            if active:
                result = client.call('turn/steer', {**params, 'expectedTurnId': active['id']})
                turn = result['turnId']
            else:
                turn = client.call('turn/start', params)['turn']['id']
            return {'state': 'delivered', 'request_id': claim['client_id'], 'thread_id': thread, 'turn_id': turn}

    def find_event_acceptance(self, home, thread, ids):
        """Evidence only, exact recorded context; a miss NEVER injects or resumes."""
        from csl_bridge_rpc import RuntimeRejected, RuntimeUnavailable
        client = self.client(home)
        if client is None:
            return {}
        try:
            turns = client.call('thread/turns/list', {'threadId': thread, 'limit': 20,
                'itemsView': 'full', 'sortDirection': 'desc'}).get('data', [])[:20]
        except (RuntimeRejected, RuntimeUnavailable, OSError):
            return {}
        found = {}
        wanted = set(ids)
        for turn in turns:
            for item in turn.get('items', []):
                value = item.get('clientId')
                if item.get('type') == 'userMessage' and value in wanted:
                    found.setdefault(value, set()).add(turn['id'])
        return {value: next(iter(turns)) for value, turns in found.items() if len(turns) == 1}


class Channel:
    """Reader never performs pipe writes; a bounded writer owns backpressure."""
    LIVENESS = 20
    def __init__(self, node, peer, reader, writer, close):
        self.node, self.peer = node, peer
        self.reader, self.writer, self.close_fn = reader, writer, close
        self.closed = False
        self.retry_at = {}
        self.retry_lock = threading.Lock()
        self.outgoing = queue.Queue(maxsize=64)
        self.last_received = time.monotonic()
        self.first_frame_deadline = None
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
            if value.get('type') in ('ack','pong','ping','hello'):
                return False  # Advisory; the next resend/heartbeat elicits another.
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
                self.first_frame_deadline = None
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
                    with self.retry_lock:
                        self.retry_at.pop(frame['id'],None)
                elif kind == 'ack':
                    if self.node.channels.get(self.peer) is self:
                        self.acknowledged(frame['id'])
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

    def acknowledged(self, identifier):
        with self.retry_lock:
            if identifier in self.retry_at:
                self.retry_at[identifier] = time.monotonic()+60

    def reserve_send(self, identifier):
        with self.retry_lock:
            now = time.monotonic()
            if self.retry_at.get(identifier,0) > now:
                return False
            self.retry_at[identifier] = now+5
            return True

    def pump(self):
        last_hello = 0
        try:
            while not self.closed and not STOP.is_set():
                try:
                    now = time.monotonic()
                    if now - self.last_received > self.LIVENESS and (self.first_frame_deadline is None or now > self.first_frame_deadline):
                        self.close()
                        return
                    if now - last_hello > 3:
                        self.send({'type': 'hello', 'node': self.node.config['node'], 'agents': self.node.advertise()})
                        self.send({'type': 'ping'})
                        last_hello = now
                    with self.retry_lock:
                        deadlines = dict(self.retry_at)
                    for row in self.node.journal.ready_out(self.peer,deadlines):
                        if self.outgoing.qsize() >= 32:
                            break
                        if self.reserve_send(row['id']):
                            self.send(json.loads(row['payload']))
                    STOP.wait(.1)
                except sqlite3.OperationalError as error:
                    if (getattr(error, 'sqlite_errorcode', 0) & 255) not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                        raise
                    log('journal_busy', loop='pump', peer=self.peer)
                    STOP.wait(.25)

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
        candidates, threads = [], {}
        for bridge in self.config['bridges'].values():
            home = bridge['home']
            with sql('file:' + str(Path(home) / 'bridge.sqlite') + '?mode=ro', uri=True) as db:
                for agent, thread in db.execute('SELECT name,thread FROM agents'):
                    if agent not in bridge['agents']:
                        continue
                    namespace = str((Path(home)/'bridge.sqlite').resolve())
                    threads.setdefault(thread, set()).add(namespace)
                    if selector in (agent, thread):
                        candidates.append((home, thread, namespace))
        identities = {(namespace, thread) for _, thread, namespace in candidates}
        if len(identities) != 1:
            raise ValueError('Unknown or ambiguous local registered agent')
        home, thread, _ = candidates[0]
        if len(threads[thread]) != 1:
            raise ValueError('Ambiguous recipient home/thread binding')
        return home, thread

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
                row = store.rows('SELECT id AS message_id,persisted_at,acknowledged_at FROM mail_messages WHERE id=?', (request['message_id'],))
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
                target = db.execute('SELECT thread FROM agents WHERE name=?', (request['target'],)).fetchone()
                if target:
                    from csl_bridge_event_state import EventState
                    EventState.reserve_id(db, request['id'], 'mail', digest, target[0])
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
                bound = {**request, '_native_binding': (home, self.agent(target)[1], 'mail', hashlib.sha256(wire(request).encode()).hexdigest())}
                self.native.inject(target, bound, frame.get('source_session'))
            return result
        if request['op'] in ('followup', 'send_message'):
            if self.native.client(home) is None:
                raise ValueError('Followup requires an existing Codex endpoint; use send for client-only agents')
            from csl_bridge_store import encode
            core_request = request
            raw = encode(core_request)
            digest = hashlib.sha256(raw.encode()).hexdigest()
            from csl_bridge_store import Store
            from csl_bridge_event_state import EventState
            store = Store(home)
            try:
                with store.transaction() as db:
                    old = db.execute('SELECT peer,hash FROM operations WHERE id=?', (request['id'],)).fetchone()
                    if old and (old['peer'] != trusted or old['hash'] != digest):
                        raise ValueError('Existing native operation identity collision')
                    thread = db.execute('SELECT thread FROM agents WHERE name=?', (target,)).fetchone()[0]
                    EventState.reserve_id(db, request['id'], 'operation', digest, thread)
                    db.execute('INSERT OR IGNORE INTO operations VALUES(?,?,?,?,?,?)', (request['id'], trusted, digest, raw, 'invoking', encode({'state':'uncertain','request_id':request['id']})))
                    if request['op'] == 'followup':
                        db.execute('INSERT OR IGNORE INTO tasks VALUES(?,?,?,?,NULL,?)', (request['id'], trusted, request['source'], target, 'pending'))
                bound = {**request, '_native_binding': (home, thread, 'operation', digest)}
                result = self.native.inject(target, bound, frame.get('source_session'))
                with store.transaction() as db:
                    db.execute('UPDATE operations SET state=?,result=? WHERE id=?', ('delivered', encode(result), request['id']))
                    if request['op'] == 'followup':
                        db.execute('UPDATE tasks SET turn=?,status=? WHERE id=?', (result['turn_id'], 'running', request['id']))
                return result
            finally:
                store.close()
        if request['op'] == 'receipt':
            # A routed event's canonical logical peer may be an alias for this
            # physical hub. Reuse only its earlier authenticated exact route;
            # the connector still applies that peer's existing receipt ACL.
            from csl_bridge_store import Store, decode
            with contextlib.closing(Store(home)) as store:
                if store.rows("SELECT 1 FROM sqlite_master WHERE name='event_catalog'"):
                    rows = store.rows('SELECT peer,task FROM event_catalog WHERE wire_id=?', (request['request_id'],))
                    if rows and rows[0]['peer'] != trusted:
                        saved = store.rows('SELECT route FROM event_original_routes WHERE peer=? AND task=?', (rows[0]['peer'], rows[0]['task']))
                        expected = {'physical_peer': peer, 'origin': trusted, 'destination': trusted}
                        if saved and decode(saved[0]['route']) == expected:
                            trusted = rows[0]['peer']
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
        from csl_bridge_engine import confirm_event_turn
        from csl_bridge_store import Store, BridgeError
        store = Store(home)
        from csl_bridge_event_delivery import EventDelivery
        delivery = EventDelivery(store)
        try:
            matches = store.rows("SELECT * FROM operations WHERE id=? AND peer LIKE 'outbound:%'", (event['task_id'],))
            if not matches:
                raise ValueError('Event has no delegated task')
            original = matches[0]
            logical = original['peer'].removeprefix('outbound:')
            config = store.peers().get(logical, {})
            path = config.get('peer', logical) if config.get('transport') == 'light' else logical
            # The immediate authenticated link is not necessarily the routed origin.
            if self.is_router:
                route = self.journal.resolve(event['source'])
                origin = peer
                if route['peer'] != peer or path != peer:
                    raise ValueError('Event peer differs from delegated path')
            else:
                if path != peer:
                    raise ValueError('Event peer differs from delegated path')
                origin = frame.get('origin', peer)
                if origin != peer and not self.config['links'][peer].get('trusted_router'):
                    raise ValueError('Untrusted event origin')
            route_context = {'physical_peer': peer, 'origin': origin, 'destination': origin}
            cached_route = delivery.history.cached_route(logical, event['task_id'])
            request = json.loads(original['request'])
            receipt = json.loads(original['result']) if original['result'] else {}
            try:
                confirmed = confirm_event_turn(request, original['state'], receipt, event)
            except BridgeError as error:
                raise ValueError(str(error)) from error
            rpc = self.journal.get('out', peer, event['task_id'])
            payload = json.loads(rpc['payload']) if rpc else None
            # No original work is resent. Compacted/missing provenance stays deferred.
            if payload is not None:
                if (payload.get('type') != 'request' or payload.get('id') != event['task_id'] or
                        payload.get('request') != request or
                        hashlib.sha256(wire(payload).encode()).hexdigest() != rpc['hash'] or
                        payload.get('destination', peer) != origin):
                    raise ValueError('Original RPC binding differs from event path')
            elif (not self.is_router and self.config['links'][peer].get('trusted_router') and
                  cached_route != route_context):
                raise RuntimeError('Waiting for original routed RPC provenance')
            if not confirmed:
                reply = json.loads(rpc['result']) if rpc and rpc['state'] == 'done' and rpc['result'] else {}
                recovered = reply.get('data', {})
                if (not payload or reply.get('ok') is not True or
                        recovered.get('state') != 'delivered' or
                        recovered.get('request_id') != event['task_id']):
                    if cached_route == route_context:
                        receipt = delivery.history.recover_original(logical, event, route_context)
                        recovered = receipt
                    else:
                        raise RuntimeError('Waiting for original native acceptance receipt')
                try:
                    if not confirm_event_turn(request, 'delivered', recovered, event):
                        raise RuntimeError('Waiting for original native acceptance receipt')
                    # Separate databases are never locked together. Recheck immutable
                    # identity and current outcome before publishing recovered proof.
                    with store.transaction() as db:
                        current = db.execute('SELECT * FROM operations WHERE id=?', (event['task_id'],)).fetchone()
                        if (current['peer'] != original['peer'] or current['hash'] != original['hash'] or
                                current['request'] != original['request']):
                            raise ValueError('Original operation changed during recovery')
                        latest = json.loads(current['result']) if current['result'] else {}
                        if not confirm_event_turn(request, current['state'], latest, event):
                            db.execute("UPDATE operations SET state='delivered',result=? WHERE id=?",
                                       (encode(recovered), event['task_id']))
                except BridgeError as error:
                    raise ValueError(str(error)) from error
                try:
                    local_call(home, {'local_op': '_light_events_changed'}, timeout=1)
                except (ValueError, OSError):
                    pass
            current_receipt = store.rows('SELECT result FROM operations WHERE id=? AND peer=?', (event['task_id'], original['peer']))
            receipt = json.loads(current_receipt[0]['result']) if current_receipt and current_receipt[0]['result'] else {}
            sender = self.event_sender(peer, frame, receipt)
            admitted = delivery.accept(logical, event, route=route_context)
            def selected_session(canonical):
                # Use the first admitted frame's authenticated header, not a later
                # alias's optional metadata. Compaction never invents a session.
                saved = self.journal.get('in', peer, canonical['id'])
                payload = json.loads(saved['payload']) if saved else None
                if (payload and payload.get('event') == canonical and
                        hashlib.sha256(wire(payload).encode()).hexdigest() == saved['hash']):
                    return self.event_sender(peer, payload, receipt)
                if admitted.get('created') and canonical['id'] == event['id']:
                    return sender
                return canonical.get('session')
            callbacks = {
                'available': lambda canonical, thread: self.native.event_available(home, canonical, thread),
                'send': lambda claim, canonical: self.native.send_event(home, claim, canonical, selected_session(canonical)),
                'find': lambda thread, ids: self.native.find_event_acceptance(home, thread, ids),
            }
            if event['kind'] == 'final':
                result = delivery.final(logical, event['task_id'], event['id'], **callbacks)
            else:
                result = delivery.progress(event['id'], **callbacks)
            try:
                local_call(home, {'local_op': '_light_events_changed'}, timeout=1)
            except (ValueError, OSError):
                pass
            return result
        finally:
            store.close()

    def event_sender(self, peer, frame, receipt):
        event = frame['event']
        link = self.config['links'].get(peer, {})
        if self.is_router:
            enrolled = self.journal.resolve(event['source'])
            sender = link.get('agent_threads', {}).get(event['source']) or enrolled.get('thread')
        elif link.get('trusted_router'):
            sender = frame.get('source_session')
        else:
            try:
                enrolled = self.journal.resolve(event['source'])
                sender = enrolled.get('thread') if enrolled['peer'] == peer else None
            except ValueError:
                sender = None
        if (sender and receipt.get('thread_id') and sender != receipt['thread_id']) or (sender and event.get('session') and sender != event['session']):
            raise ValueError('Authenticated sender thread differs from original task')
        return sender or event.get('session')

    def finish_pending(self):
        """Recover native completions even without a separately running connector."""
        from csl_bridge_store import Store
        from csl_bridge_engine import publish_task_completion, retry_publications
        from csl_bridge_event_history import EventHistory
        from csl_bridge_event_state import EventConflict, EventDeferred
        from csl_bridge_rpc import RuntimeRejected, RuntimeUnavailable
        for bridge in self.config['bridges'].values():
            home = bridge['home']
            store = None
            try:
                store = Store(home)
                retry_publications(store)
                pending = [task for task in EventHistory(store).publication_tasks(include_cached=False) if task['turn'] is not None]
                agents = {name: thread for name, thread in store.agents().items() if name in bridge['agents']}
                for agent in {row['target'] for row in pending} & set(agents):
                    client = self.native.client(home)
                    if client is None:
                        continue
                    turns = client.call('thread/turns/list',{'threadId':agents[agent],'limit':20,'itemsView':'full','sortDirection':'desc'})['data']
                    wanted = {r['turn']for r in pending if r['target']==agent}
                    for turn in turns:
                        if turn['id'] in wanted and turn['status'] in ('completed','failed','interrupted'):
                            answer = '\n'.join(item.get('text','')for item in turn.get('items',[])if item.get('type')=='agentMessage')[-12000:]
                            publish_task_completion(store,agent,agents[agent],turn['id'],turn['status'],answer)
            except (EventConflict, EventDeferred, ValueError, RuntimeRejected, RuntimeUnavailable, OSError, sqlite3.Error) as error:
                log('publication_home_deferred', error=type(error).__name__)
            finally:
                if store is not None:
                    store.close()

    def queue_events(self):
        """Push existing CSL progress/final envelopes through the durable stream."""
        from csl_bridge_store import encode
        with self.journal.db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS event_cursors(home TEXT PRIMARY KEY,seq INTEGER)')
        for bridge in self.config['bridges'].values():
            home = bridge['home']
            with self.journal.db() as db:
                cursor = db.execute('SELECT seq FROM event_cursors WHERE home=?',(home,)).fetchone()
            after = cursor[0] if cursor else 0
            with sql(Path(home)/'bridge.sqlite') as db:
                rows = db.execute('SELECT seq,peer,body FROM outbound WHERE seq>? ORDER BY seq LIMIT 100',(after,)).fetchall()
            blocked = False
            for seq,event_peer,body in rows:
                event = json.loads(body)
                with self.journal.db() as db:
                    candidates = db.execute("SELECT * FROM rpc WHERE direction='in' AND id=? AND state='done'", (event['task_id'],)).fetchall()
                    light_seen = db.execute("SELECT 1 FROM rpc WHERE direction='in' AND id=? UNION ALL SELECT 1 FROM tombstones WHERE direction='in' AND id=? LIMIT 1", (event['task_id'], event['task_id'])).fetchone()
                with sql(Path(home)/'bridge.sqlite') as db:
                    db.row_factory = sqlite3.Row
                    operation = db.execute('SELECT * FROM operations WHERE id=?', (event['task_id'],)).fetchone()
                    configured = db.execute('SELECT config FROM peers WHERE name=?', (event_peer,)).fetchone()
                try:
                    transport = json.loads(configured[0]) if configured else {}
                except (ValueError, TypeError):
                    transport = {}
                fallback = (not light_seen and operation and operation['peer'] == event_peer and
                            isinstance(transport, dict) and transport.get('transport') in ('ssh', 'operator', 'lab'))
                originals = []
                for candidate in candidates:
                    payload = json.loads(candidate['payload'])
                    reply = json.loads(candidate['result'])
                    request = payload.get('request', {})
                    if (operation and payload.get('type') == 'request' and
                            encode(request) == operation['request'] and
                            operation['peer'] == payload.get('origin', candidate['peer']) and
                            hashlib.sha256(wire(payload).encode()).hexdigest() == candidate['hash'] and
                            reply.get('ok') is True and
                            reply.get('data', {}).get('request_id') == event['task_id']):
                        from csl_bridge_engine import confirm_event_turn
                        try:
                            if (confirm_event_turn(request, operation['state'],
                                                   json.loads(operation['result']) if operation['result'] else {}, event)
                                    and confirm_event_turn(request, operation['state'], reply.get('data'), event)):
                                originals.append(candidate)
                        except __import__('csl_bridge_store').BridgeError:
                            continue
                if len(originals) == 1:
                    peer = originals[0]['peer']
                    existing = self.journal.get('out',peer,event['id'])
                    frame = json.loads(existing['payload'])if existing else {'type':'event','id':event['id'],'event':event,'origin':self.config['node'],'source_session':self.agents().get(event['source'])}
                    self.journal.put('out',peer,event['id'],frame)
                elif not fallback:
                    # Keep the contiguous cursor before a genuine light/unknown gap.
                    # Later confirmed rows can still enter the idempotent journal
                    # in this bounded batch without consuming that missing event.
                    blocked = True
                if not blocked:
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
                    try:
                        from csl_bridge_transport import local_call
                        local_call(source_home, {'local_op': '_light_events_changed'}, timeout=1)
                    except (OSError, ValueError):
                        pass
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
                # Owner certificate/admission can wait before app-level hello exists.
                channel.first_frame_deadline = time.monotonic() + 90

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
            if not current or (self.home / ('revoked-' + peer)).exists():
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

    def schedule_event(self, home, job):
        physical = job.get('transport_peer', job['peer'])
        if physical not in self.config['links']:
            return  # Legacy activation remains the owning connector's policy.
        peer = physical
        token = 'event:' + hashlib.sha256(wire([str(Path(home).resolve()), job['key']]).encode()).hexdigest()
        key = (peer, token)
        with self.exec_lock:
            if key in self.executing or len(self.executing) >= 32 or sum(p == peer for p, _ in self.executing) >= 4:
                return
            if not self.journal.retry_due(peer, token):
                return
            self.executing.add(key)
        self.pool.submit(self.deliver_event_job, home, peer, token, job)

    def deliver_event_job(self, home, peer, token, job):
        from csl_bridge_store import Store
        from csl_bridge_event_delivery import EventDelivery
        from csl_bridge_event_state import EventConflict
        from csl_bridge_transport import local_call
        store = Store(home)
        try:
            delivery = EventDelivery(store)
            rows = store.rows('SELECT body FROM inbound WHERE id=?', (job['wire_id'],))
            if not rows:
                raise EventConflict('Event work has no original row')
            event = json.loads(rows[0]['body'])
            saved_route = delivery.history.cached_route(job['peer'], event['task_id'])
            physical = saved_route['physical_peer'] if saved_route else peer
            saved = self.journal.get('in', physical, event['id'])
            payload = json.loads(saved['payload']) if saved else None
            def session(canonical):
                if payload and payload.get('event') == canonical and hashlib.sha256(wire(payload).encode()).hexdigest() == saved['hash']:
                    original = store.rows('SELECT result FROM operations WHERE id=?', (canonical['task_id'],))
                    receipt = json.loads(original[0]['result']) if original and original[0]['result'] else {}
                    return self.event_sender(physical, payload, receipt)
                return canonical.get('session')
            callbacks = {
                'available': lambda canonical, thread: self.native.event_available(home, canonical, thread),
                'send': lambda claim, canonical: self.native.send_event(home, claim, canonical, session(canonical)),
                'find': lambda thread, ids: self.native.find_event_acceptance(home, thread, ids),
            }
            if event['kind'] == 'final':
                result = delivery.final(job['peer'], event['task_id'], event['id'], **callbacks)
            else:
                result = delivery.progress(event['id'], **callbacks)
            self.journal.finish('in', peer, token, {'ok': True, 'data': result})
            if saved:
                response = {'ok': True, 'data': result}
                self.journal.finish('in', physical, event['id'], response)
                with self.channel_lock:
                    channel = self.channels.get(physical)
                if channel:
                    channel.send({'type': 'response', 'id': event['id'], 'result': response})
            try:
                local_call(home, {'local_op': '_light_events_changed'}, timeout=1)
            except (OSError, ValueError):
                pass
        except EventConflict:
            with store.transaction() as db:
                db.execute('UPDATE event_delivery_jobs SET dirty=0 WHERE key=?', (job['key'],))
            self.journal.finish('in', peer, token, {'ok': False, 'data': {'error': 'Event identity refused'}})
        except Exception as error:
            self.journal.error('in', peer, token, error)
        finally:
            store.close()
            with self.exec_lock:
                self.executing.discard((peer, token))

    def maintain_events(self):
        from csl_bridge_store import Store
        from csl_bridge_event_delivery import EventDelivery
        homes = {bridge['home'] for bridge in self.config['bridges'].values()}
        for home in homes:
            store = Store(home)
            try:
                delivery = EventDelivery(store)
                for job in delivery.housekeeping():
                    catalog = store.rows('SELECT task FROM event_catalog WHERE wire_id=?', (job['wire_id'],))
                    route = delivery.history.cached_route(job['peer'], catalog[0]['task']) if catalog else None
                    job['transport_peer'] = route['physical_peer'] if route else job['peer']
                    if job['transport_peer'] in self.config['links']:
                        self.schedule_event(home, job)
            finally:
                store.close()

    def retry_loop(self):
        last_prune = 0
        while not STOP.is_set():
            try:
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
                for step in (self.finish_pending, self.queue_events, self.maintain_events):
                    try:
                        step()
                    except Exception as error:
                        log('event_delivery_deferred',step=step.__name__,error=type(error).__name__)
                for agent, thread in self.agents().items():
                    self.journal.route(agent, self.config['node'], thread)
            except sqlite3.OperationalError as error:
                if (getattr(error, 'sqlite_errorcode', 0) & 255) not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                    raise
                log('journal_busy', loop='retry')
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
            self.journal.reset_retries()
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

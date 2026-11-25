"""Opt-in CSL NATS transport. The core CLI does not import nats-py at startup."""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import ssl
import stat
import sys
import time
from urllib.parse import urlsplit
import uuid

from csl_bridge_rpc import RuntimeRejected, RuntimeUnavailable
from csl_bridge_store import BridgeError, Store, contains_findings, encode, frame_message, identifier
from csl_version import build_version

NATIVE_PEER = '_nats'  # Outside the public bridge peer namespace.
CI_SOURCE = 'ci-notices'
RESERVED_AGENTS = {'owner', 'csl-control', 'csl-ci', CI_SOURCE, 'hub-notifier'}
MAX_BODY = 48 * 1024
MAX_DIGEST = 512 * 1024
MESSAGE_KINDS = ('message', 'reply', 'ci', 'system', 'reminder')
STREAM = 'CSL_MSG'
LOG = logging.getLogger(__name__)


def agent_name(value, *, registration=False):
    if (not isinstance(value, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{1,62}', value)
            or registration and value in RESERVED_AGENTS):
        raise BridgeError('Invalid NATS agent name')
    return value


def tenant_name(value):
    if not isinstance(value, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', value):
        raise BridgeError('Invalid NATS tenant')
    return value


def private_file(path):
    path = Path(path).expanduser()
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise BridgeError('NATS configuration and credentials must be owned private files')
    return path


def config(path):
    try:
        settings = json.loads(private_file(path).read_text(encoding='utf-8'))
        if set(settings) != {'tenant', 'servers', 'cafile', 'connector_home', 'agents'}:
            raise ValueError
        tenant_name(settings['tenant'])
        if not isinstance(settings['servers'], list) or not settings['servers']:
            raise ValueError
        for server in settings['servers']:
            url = urlsplit(server)
            if (url.scheme not in ('tls', 'wss') or not url.hostname or url.username or url.password or
                    url.query or url.fragment or (url.scheme == 'tls' and url.path not in ('', '/'))):
                raise ValueError
        settings['connector_home'] = Path(settings['connector_home']).expanduser().absolute()
        for agent, endpoint in settings['agents'].items():
            agent_name(agent, registration=True)
            fields = {'credentials', 'session'}
            if endpoint.get('cli', 'codex') == 'claude':
                fields |= {'cli', 'mailbox_home', 'claude_config'}
                for key in ('mailbox_home', 'claude_config'):
                    endpoint[key] = Path(endpoint[key]).expanduser().absolute()
            elif endpoint.get('cli') == 'codex':
                fields.add('cli')
            if set(endpoint) != fields:
                raise ValueError
            identifier(endpoint['session'])
            endpoint['credentials'] = private_file(endpoint['credentials'])
        return settings
    except (AttributeError, KeyError, ValueError, TypeError) as error:
        raise BridgeError('Invalid NATS adapter configuration') from error


def body_text(value):
    try:
        if isinstance(value, str) and len(value.encode('utf-8')) <= MAX_BODY:
            return value
    except UnicodeError:
        pass
    raise BridgeError('NATS message body must be UTF-8 text of at most 48 KiB')


def validate_envelope(envelope, agent):
    if (not isinstance(envelope, dict) or type(envelope.get('v')) is not int or envelope['v'] != 1
            or envelope.get('to') != agent):
        raise BridgeError('Invalid NATS inbox envelope')
    identifier(envelope.get('id'))
    agent_name(envelope.get('from'))
    body_text(envelope.get('body'))
    if envelope.get('kind') not in MESSAGE_KINDS or envelope.get('items') or envelope.get('attachments'):
        raise BridgeError('Unsupported NATS inbox envelope')


def delivery_members(envelope, agent, size):
    if not isinstance(envelope, dict) or envelope.get('kind') != 'digest':
        validate_envelope(envelope, agent)
        return []
    items = envelope.get('items')
    if (size > MAX_DIGEST or envelope.get('from') != 'csl-control' or envelope.get('body') != ''
            or not isinstance(items, list) or not items):
        raise BridgeError('Invalid NATS digest')
    validate_envelope(dict(envelope, kind='system', items=[]), agent)
    seen = {envelope['id']}
    members = []
    for item in items:
        validate_envelope(item, agent)
        message_id = item['id']
        if message_id in seen:
            raise BridgeError('Duplicate message in NATS digest')
        seen.add(message_id)
        members.append(message_id)
    return members


def delivery_text(envelope):
    header = frame_message(envelope['from'], envelope['id'], None, envelope['body'])
    if envelope.get('kind') != 'digest':
        return header
    entries = [frame_message(item['from'], item['id'], None, item['body']) for item in envelope['items']]
    return header + '\n\n'.join(entries)


class ClaudeInbox:
    def __init__(self, store, tenant, agent, session):
        self.store = store
        self.route = (tenant, agent, session)
        with store.transaction() as db:
            self.require_session(db)
            db.execute('''CREATE TABLE IF NOT EXISTS nats_claude_inbox (
                seq INTEGER PRIMARY KEY, tenant TEXT NOT NULL, agent TEXT NOT NULL, session TEXT NOT NULL,
                id TEXT NOT NULL, fingerprint TEXT NOT NULL, body TEXT NOT NULL,
                woke INTEGER NOT NULL DEFAULT 0, surfaced INTEGER NOT NULL DEFAULT 0,
                UNIQUE (tenant, agent, id))''')

    def require_session(self, db):
        row = db.execute('SELECT thread FROM agents WHERE name=?', (self.route[1],)).fetchone()
        if not row or row['thread'] != self.route[2]:
            raise BridgeError('NATS Claude inbox does not match the registered session')

    def stage(self, envelope, fingerprint):
        with self.store.transaction() as db:
            self.require_session(db)
            row = db.execute('''SELECT session,fingerprint,surfaced FROM nats_claude_inbox
                WHERE tenant=? AND agent=? AND id=?''', (*self.route[:2], envelope['id'])).fetchone()
            if row:
                if (row['session'], row['fingerprint']) != (self.route[2], fingerprint):
                    raise BridgeError('NATS Claude inbox ID collision')
                return self.route[2] if row['surfaced'] else None
            db.execute('INSERT INTO nats_claude_inbox (tenant,agent,session,id,fingerprint,body) VALUES (?,?,?,?,?,?)',
                       (*self.route, envelope['id'], fingerprint, delivery_text(envelope)))
        return None

    def wake_waiting(self, claude_config):
        from csl_bridge_claude_notify import wake
        with self.store.transaction() as db:
            self.require_session(db)
            first = db.execute('''SELECT seq,woke FROM nats_claude_inbox
                WHERE tenant=? AND agent=? AND session=? AND surfaced=0 ORDER BY seq LIMIT 1''', self.route).fetchone()
            if first and not first['woke']:
                wake(self.route[2], claude_config)
                db.execute('UPDATE nats_claude_inbox SET woke=1 WHERE seq=?', (first['seq'],))

    def surface(self, render, emit, limit):
        with self.store.transaction() as db:
            self.require_session(db)
            rows = db.execute('''SELECT seq,body FROM nats_claude_inbox
                WHERE tenant=? AND agent=? AND session=? AND surfaced=0 ORDER BY seq LIMIT ?''',
                              (*self.route, limit)).fetchall()
            if not rows:
                return
            selected = [rows[0]]
            size = len(rows[0]['body'].encode('utf-8'))
            for row in rows[1:]:
                size += len(row['body'].encode('utf-8')) + 2
                if size > MAX_DIGEST:
                    break
                selected.append(row)
            emit(render('\n\n'.join(row['body'] for row in selected)))
            db.executemany("UPDATE nats_claude_inbox SET surfaced=1,body='' WHERE seq=?",
                           ((row['seq'],) for row in selected))


class Client:
    def __init__(self, connection, tenant, agent):
        self.connection = connection
        self.root = f'csl.{tenant}'
        self.agent = agent
        self.inbox = f'_INBOX.csl.{tenant}.{agent}'
        self.consumer = f'agent-{agent}'

    @classmethod
    async def connect(cls, settings, agent):
        try:
            import nats
        except ImportError:
            raise BridgeError('Install nats-py[nkeys] in the CSL Python environment') from None
        endpoint = settings['agents'].get(agent)
        if endpoint is None:
            raise BridgeError('Sender is not configured for NATS')
        tls = ssl.create_default_context(cafile=str(Path(settings['cafile']).expanduser()))
        inbox = f"_INBOX.csl.{settings['tenant']}.{agent}"
        connection = await nats.connect(
            servers=settings['servers'], tls=tls, user_credentials=str(endpoint['credentials']),
            inbox_prefix=inbox, connect_timeout=3, max_reconnect_attempts=2,
        )
        return cls(connection, settings['tenant'], agent)

    async def request(self, operation, body, *, headers=None):
        message = await self.connection.request(
            f'{self.root}.{operation}.{self.agent}', encode(body).encode('utf-8'), headers=headers, timeout=12,
        )
        result = json.loads(message.data)
        if not isinstance(result, dict):
            raise BridgeError('Invalid response from NATS control')
        if 'error' in result:
            raise BridgeError(str(result.get('message') or result['error']))
        return result

    async def pull(self):
        return await self.connection.jetstream().pull_subscribe_bind(
            consumer=self.consumer, stream=STREAM, inbox_prefix=(self.inbox + '.').encode(),
        )

    async def receipt(self, message_id, state):
        result = await self.request('ctl.receipt', {'id': message_id, 'state': state})
        if result.get('id') != message_id or result.get('state') not in (state, 'acked', 'dropped'):
            raise BridgeError('Unexpected NATS delivery receipt')
        return result['state']

    async def send(self, envelope):
        return await self.request('send', envelope, headers={'Nats-Msg-Id': envelope['id']})


class Ledger:
    def __init__(self, store, tenant, agent, thread):
        self.store = store
        self.key = (tenant, agent)
        self.thread = thread
        with store.transaction() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS nats_delivered (
                tenant TEXT, agent TEXT, id TEXT, thread TEXT NOT NULL,
                fingerprint TEXT NOT NULL, turn TEXT NOT NULL, ack_subject TEXT,
                ack_state TEXT NOT NULL DEFAULT 'delivered', delivered_at INTEGER NOT NULL,
                PRIMARY KEY (tenant, agent, id))''')
            db.execute('''CREATE TABLE IF NOT EXISTS nats_digest_members (
                tenant TEXT NOT NULL, agent TEXT NOT NULL, digest_id TEXT NOT NULL, member_id TEXT NOT NULL,
                PRIMARY KEY (tenant, agent, digest_id, member_id))''')

    def get(self, message_id, fingerprint):
        rows = self.store.rows('SELECT * FROM nats_delivered WHERE tenant=? AND agent=? AND id=?',
                               (*self.key, message_id))
        if not rows:
            return None
        row = rows[0]
        if (row['thread'], row['fingerprint']) != (self.thread, fingerprint):
            raise BridgeError('NATS receipt does not match its original thread or envelope')
        return row

    def delivered(self, message_id, fingerprint, turn, members=()):
        with self.store.transaction() as db:
            db.execute('''INSERT INTO nats_delivered
                (tenant,agent,id,thread,fingerprint,turn,delivered_at) VALUES (?,?,?,?,?,?,?)''',
                       (*self.key, message_id, self.thread, fingerprint, identifier(turn), int(time.time())))
            db.executemany('INSERT INTO nats_digest_members VALUES (?,?,?,?)',
                           ((*self.key, message_id, member) for member in members))

    def receipt_ids(self, message_id):
        rows = self.store.rows('''SELECT member_id FROM nats_digest_members
            WHERE tenant=? AND agent=? AND digest_id=?''', (*self.key, message_id))
        return [*(row['member_id'] for row in rows), message_id]

    def mark(self, message_id, state, subject=None):
        with self.store.transaction() as db:
            db.execute('''UPDATE nats_delivered SET ack_state=?,ack_subject=?
                WHERE tenant=? AND agent=? AND id=? AND thread=?''',
                       (state, subject, *self.key, message_id, self.thread))

    def pending(self):
        return self.store.rows('''SELECT id,ack_state,ack_subject FROM nats_delivered
            WHERE tenant=? AND agent=? AND thread=? AND ack_state IN ('ack_intent','broker_acked')
            ORDER BY delivered_at LIMIT 32''', (*self.key, self.thread))


def ci_completion_fingerprint(source, target, body):
    if source == CI_SOURCE:
        return hashlib.sha256(encode([source, target, body]).encode('utf-8')).hexdigest()
    return None


def same_ci_completion(operation, expected):
    request = operation['request']
    if operation['peer'] == NATIVE_PEER:
        actual = request.get('ci_completion')
    elif operation['peer'] == 'agent-hub' and request.get('op') == 'followup':
        actual = ci_completion_fingerprint(request['source'], request['target'], request['text'])
    else:
        actual = None
    # Only CI has a shared sender identity and a canonical ID/body across transports.
    if expected is None or actual != expected:
        raise BridgeError('Native operation ID collision; CI completion does not match')


def deliver_codex(connector, thread, envelope, fingerprint):
    target = envelope['to']
    if connector.stop.is_set() or connector.client_only or connector.agents.get(target) != thread:
        return None
    with connector.locks[target]:
        if connector.stop.is_set() or connector.store.agents().get(target) != thread:
            return None
        request = {
            'version': 1, 'op': 'followup', 'id': envelope['id'], 'source': envelope['from'],
            'target': target, 'text': '', 'nats_fingerprint': fingerprint,
        }
        completion = None
        if envelope.get('kind') == 'ci' and not envelope.get('reply_to'):
            completion = ci_completion_fingerprint(envelope['from'], target, envelope['body'])
        if completion:
            request['ci_completion'] = completion
        old = connector.store.native_operation(envelope['id'], thread)
        if old:
            if old['peer'] != NATIVE_PEER:
                same_ci_completion(old, completion)
            else:
                # Reuse the exact persisted request, including pre-upgrade rows.
                original = old['request']
                if 'ci_completion' not in original:
                    request.pop('ci_completion', None)
                if request != original:
                    raise BridgeError('NATS operation ID collision; envelope differs')
                request = original
            if old['peer'] != NATIVE_PEER or old['state'] != 'pending':
                receipt = Store.native_delivery(old, envelope['id'], thread)
                return receipt['turn_id'] if receipt else None
        else:
            connector.store.reserve(NATIVE_PEER, request)
        text = delivery_text(envelope)
        result = connector.invoke(NATIVE_PEER, request, native_text=text)
        receipt = Store.native_delivery({'state': result['state'], 'result': result}, envelope['id'], thread)
        return receipt['turn_id'] if receipt else None


def validate_ack_subject(subject, consumer):
    pattern = rf'\$JS\.ACK\.(?:[^.\s*<>]+\.[^.\s*<>]+\.)?{STREAM}\.{re.escape(consumer)}(?:\.[0-9]+){{5}}'
    if not isinstance(subject, str) or not re.fullmatch(pattern, subject):
        raise BridgeError('Unexpected JetStream acknowledgment subject')
    return subject


async def confirm_acked(client, ledger, message_id):
    for receipt_id in ledger.receipt_ids(message_id):
        await client.receipt(receipt_id, 'acked')
    ledger.mark(message_id, 'acked')


async def recover_acks(client, ledger, expected_errors):
    for row in ledger.pending():
        try:
            if row['ack_state'] == 'ack_intent':
                subject = validate_ack_subject(row['ack_subject'], client.consumer)
                await client.connection.request(subject, b'+ACK', timeout=1)
                ledger.mark(row['id'], 'broker_acked')
            await confirm_acked(client, ledger, row['id'])
        except expected_errors:
            LOG.debug('NATS acknowledgment still pending for %s', row['id'])


async def receive_one(client, ledger, message, deliver, *, retry_delay=60):
    if message.subject != f'{client.root}.agent.{client.agent}.inbox':
        raise BridgeError('Unexpected NATS inbox subject')
    subject = validate_ack_subject(message.reply, client.consumer)
    envelope = json.loads(message.data)
    members = delivery_members(envelope, client.agent, len(message.data))
    message_id = envelope['id']
    fingerprint = hashlib.sha256(encode(envelope).encode('utf-8')).hexdigest()
    row = ledger.get(message_id, fingerprint)
    if row is None:
        turn = await asyncio.to_thread(deliver, envelope, fingerprint)
        if turn is None:
            await message.nak(delay=retry_delay)
            return False
        ledger.delivered(message_id, fingerprint, turn, members)
    if row is None or row['ack_state'] != 'acked':
        for receipt_id in ledger.receipt_ids(message_id):
            await client.receipt(receipt_id, 'delivered')
        ledger.mark(message_id, 'ack_intent', subject)
        await message.ack_sync(timeout=3)
        ledger.mark(message_id, 'broker_acked')
        await confirm_acked(client, ledger, message_id)
    else:
        await message.ack_sync(timeout=3)
    return True


async def receive_loop(connector, settings, agent, thread, claude=None):
    from functools import partial
    from nats.errors import Error, TimeoutError as NatsTimeout
    errors = (Error, OSError, ValueError, RuntimeRejected, RuntimeUnavailable, sqlite3.Error)
    wake_errors = errors + (KeyError, TypeError)
    ledger = Ledger(connector.store, settings['tenant'], agent, thread)
    deliver = claude.stage if claude else partial(deliver_codex, connector, thread)
    batch_size, retry_delay = (16, 5) if claude else (1, 60)
    while not connector.stop.is_set():
        client = None
        try:
            client = await Client.connect(settings, agent)
            await client.request('ctl.hello', {'client_version': build_version()})
            inbox = await client.pull()
            next_recovery = next_wake = 0
            while not connector.stop.is_set() and not client.connection.is_closed:
                if time.monotonic() >= next_recovery:
                    await recover_acks(client, ledger, errors)
                    next_recovery = time.monotonic() + 15
                try:
                    messages = await inbox.fetch(batch_size, timeout=2)
                except NatsTimeout:
                    messages = []
                for message in messages:
                    try:
                        await receive_one(client, ledger, message, deliver, retry_delay=retry_delay)
                    except errors as error:
                        LOG.warning('NATS inbox %s left unacknowledged (%s)', agent, type(error).__name__)
                if claude and time.monotonic() >= next_wake:
                    try:
                        await asyncio.to_thread(claude.wake_waiting, settings['agents'][agent]['claude_config'])
                    except wake_errors as error:
                        LOG.debug('NATS Claude wake for %s unavailable (%s)', agent, type(error).__name__)
                    next_wake = time.monotonic() + 5
        except errors as error:
            LOG.warning('NATS adapter %s will reconnect (%s)', agent, type(error).__name__)
        finally:
            if client:
                await client.connection.close()
        await asyncio.to_thread(connector.stop.wait, 5)


def start(connector, config_path):
    settings = config(config_path)
    if connector.client_only or settings['connector_home'] != connector.store.home:
        raise BridgeError('NATS adapter requires the configured native Codex connector home')
    try:
        import nats  # noqa: F401
    except ImportError:
        raise BridgeError('Install nats-py[nkeys] in the connector Python environment') from None
    for agent, endpoint in settings['agents'].items():
        if endpoint.get('cli') == 'claude':
            store = Store(endpoint['mailbox_home'])
            try:
                ClaudeInbox(store, settings['tenant'], agent, endpoint['session'])
            finally:
                store.close()
        elif connector.agents.get(agent) != endpoint['session']:
            raise BridgeError(f'NATS registration does not match the local thread for {agent}')

    def run(name, endpoint):
        thread = endpoint['session']
        if endpoint.get('cli') != 'claude':
            asyncio.run(receive_loop(connector, settings, name, thread))
            return
        store = Store(endpoint['mailbox_home'])
        try:
            inbox = ClaudeInbox(store, settings['tenant'], name, thread)
            asyncio.run(receive_loop(connector, settings, name, thread, inbox))
        finally:
            store.close()

    for agent, endpoint in settings['agents'].items():
        connector.spawn(run, agent, endpoint)


def remember_send(store, tenant, envelope):
    """Preserve creation time and build identity when an uncertain send is retried."""
    with store.transaction() as db:
        db.execute('''CREATE TABLE IF NOT EXISTS nats_outbox (
            tenant TEXT, agent TEXT, id TEXT, envelope TEXT NOT NULL,
            PRIMARY KEY (tenant, agent, id))''')
        key = (tenant, envelope['from'], envelope['id'])
        row = db.execute('SELECT envelope FROM nats_outbox WHERE tenant=? AND agent=? AND id=?', key).fetchone()
        if row:
            old = json.loads(row[0])
            envelope.update(created_at=old['created_at'], client_version=old['client_version'])
            if old != envelope:
                raise BridgeError('NATS send ID collision; reuse the original target and body')
            return old
        db.execute('INSERT INTO nats_outbox VALUES (?,?,?,?)', (*key, encode(envelope)))
        return envelope


def send_command(args):
    settings = config(args.nats_config)
    source = agent_name(args.source or os.environ.get('CSL_AGENT'), registration=True)
    target = agent_name(args.to, registration=True)
    if source == target or source not in settings['agents']:
        raise BridgeError('NATS sender must be locally configured and different from the target')
    text = args.text
    if text is None:
        if str(args.text_file) == '-':
            data = sys.stdin.buffer.read(MAX_BODY + 1)
        else:
            with args.text_file.open('rb') as stream:
                data = stream.read(MAX_BODY + 1)
        try:
            text = data.decode('utf-8')
        except UnicodeError:
            raise BridgeError('NATS message body must be UTF-8') from None
    body_text(text)
    if contains_findings(text):
        raise BridgeError('Possible secrets detected; select redacted text before sending')
    if args.urgent != bool(args.reason):
        raise BridgeError('Use --urgent and --reason together')
    message_id = identifier(args.id) if args.id else str(uuid.uuid4())
    envelope = {'v': 1, 'id': message_id, 'from': source, 'to': target, 'kind': 'message',
                'reply_to': None, 'urgency': 'urgent' if args.urgent else 'normal',
                'created_at': datetime.now(timezone.utc).isoformat(), 'body': text,
                'client_version': build_version(), 'attachments': [], 'items': []}
    if args.reason:
        envelope['reason'] = args.reason
    store = Store(settings['connector_home'])
    try:
        envelope = remember_send(store, settings['tenant'], envelope)
    finally:
        store.close()
    print('message_id=' + message_id, file=sys.stderr)

    async def send():
        client = await Client.connect(settings, source)
        try:
            await client.request('ctl.hello', {'client_version': build_version()})
            return await client.send(envelope)
        finally:
            await client.connection.close()

    try:
        result = asyncio.run(send())
    except BridgeError:
        raise
    except Exception as error:
        # The same persisted envelope is safe to retry with the printed UUID.
        raise BridgeError(f'NATS send unconfirmed ({type(error).__name__}); retry with --id {message_id}') from None
    if result.get('id') != message_id or result.get('state') not in ('queued', 'delivered', 'acked', 'refused', 'dropped'):
        raise BridgeError(f'Unexpected NATS send receipt; retry with --id {message_id}')
    print(encode(result))

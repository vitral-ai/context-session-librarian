"""Opt-in CSL NATS transport. The core CLI does not import nats-py at startup."""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shlex
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
WAKE_RETRY_SECONDS = 30
MESSAGE_KINDS = ('message', 'reply', 'ci', 'system', 'reminder')
STREAM = 'CSL_MSG'
OUTBOX_SECONDS = 15 * 60
LOG = logging.getLogger(__name__)


class ControlRejected(BridgeError):
    def __init__(self, code, message):
        super().__init__(message)
        self.uncertain = code == 'invalid' and message == 'control operation unavailable'


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


def digest_entry(item, config_path):
    text = frame_message(item['from'], item['id'], None, item['body'])
    if item['from'] in RESERVED_AGENTS or item['from'] == item['to']:
        return text
    command = shlex.join(['csl', 'reply', item['id'], '--from', item['to'], '--to', item['from'],
                          '--nats-config', str(config_path), '--text', '...'])
    return text + '\nReply via NATS: ' + command


def delivery_parts(envelope, config_path):
    if envelope.get('kind') == 'digest':
        return [digest_entry(item, config_path) for item in envelope['items']]
    return [frame_message(envelope['from'], envelope['id'], None, envelope['body'])]


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

    def stage(self, envelope, fingerprint, *, config_path):
        with self.store.transaction() as db:
            self.require_session(db)
            row = db.execute('''SELECT session,fingerprint,surfaced FROM nats_claude_inbox
                WHERE tenant=? AND agent=? AND id=?''', (*self.route[:2], envelope['id'])).fetchone()
            if row:
                if (row['session'], row['fingerprint']) != (self.route[2], fingerprint):
                    raise BridgeError('NATS Claude inbox ID collision')
                return self.route[2] if row['surfaced'] else None
            parts = delivery_parts(envelope, config_path)
            db.execute('INSERT INTO nats_claude_inbox (tenant,agent,session,id,fingerprint,body) VALUES (?,?,?,?,?,?)',
                       (*self.route, envelope['id'], fingerprint, encode(parts)))
        return None

    def wake_waiting(self, claude_config):
        from csl_bridge_claude_notify import wake
        with self.store.transaction() as db:
            self.require_session(db)
            first = db.execute('''SELECT seq,woke FROM nats_claude_inbox
                WHERE tenant=? AND agent=? AND session=? AND surfaced=0 ORDER BY seq LIMIT 1''', self.route).fetchone()
            now = int(time.time())
            if first and (first['woke'] == 0 or 0 < first['woke'] <= now - WAKE_RETRY_SECONDS):
                wake(self.route[2], claude_config)
                db.execute('UPDATE nats_claude_inbox SET woke=? WHERE seq=?',
                           (-1 if first['woke'] else now, first['seq']))

    def surface(self, render, emit, limit):
        with self.store.transaction() as db:
            self.require_session(db)
            rows = db.execute('''SELECT seq,body FROM nats_claude_inbox
                WHERE tenant=? AND agent=? AND session=? AND surfaced=0 ORDER BY seq LIMIT ?''',
                              (*self.route, limit)).fetchall()
            updates, parts, output = [], [], None
            for row in rows:
                pending = json.loads(row['body']) if row['body'].startswith('[') else [row['body']]
                count = 0
                for part in pending:
                    candidate = render('\n\n'.join([*parts, part]))
                    if len(candidate.encode('utf-8')) + 1 > MAX_DIGEST:
                        if output is None:
                            raise BridgeError('Claude inbox member exceeds the hook output budget')
                        break
                    parts.append(part)
                    output = candidate
                    count += 1
                remaining = pending[count:]
                if count:
                    updates.append((not remaining, encode(remaining) if remaining else '', row['seq']))
                if remaining:
                    break
            if output is not None:
                emit(output)
                db.executemany('UPDATE nats_claude_inbox SET surfaced=?,body=?,woke=0 WHERE seq=?', updates)


class Client:
    def __init__(self, connection, tenant, agent, broker=None):
        self.connection = connection
        self.root = f'csl.{tenant}'
        self.agent = agent
        self.inbox = f'_INBOX.csl.{tenant}.{agent}'
        self.consumer = f'agent-{agent}'
        self.broker = broker

    @classmethod
    async def connect(cls, settings, agent, *, server=None):
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
            servers=[server] if server else settings['servers'], tls=tls, user_credentials=str(endpoint['credentials']),
            inbox_prefix=inbox, connect_timeout=3, max_reconnect_attempts=2, allow_reconnect=server is None,
        )
        server_id = getattr(connection, '_server_info', {}).get('server_id')
        broker = f'{server} {server_id}' if server and server_id else None
        return cls(connection, settings['tenant'], agent, broker)

    @classmethod
    async def connect_active(cls, settings, agent):
        try:
            from nats.errors import Error
        except ImportError:
            raise BridgeError('Install nats-py[nkeys] in the CSL Python environment') from None

        for server in settings['servers']:
            client = None
            try:
                client = await cls.connect(settings, agent, server=server)
                await client.request('ctl.hello', {'client_version': build_version()}, timeout=3)
                return client
            except (Error, OSError):
                if client:
                    await client.connection.close()
            except BaseException:
                if client:
                    await client.connection.close()
                raise
        raise OSError('No configured NATS broker answered the control hello')

    async def request(self, operation, body, *, headers=None, timeout=12):
        message = await self.connection.request(
            f'{self.root}.{operation}.{self.agent}', encode(body).encode('utf-8'), headers=headers, timeout=timeout,
        )
        result = json.loads(message.data)
        if not isinstance(result, dict):
            raise BridgeError('Invalid response from NATS control')
        if 'error' in result:
            raise ControlRejected(result['error'], str(result.get('message') or result['error']))
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

    async def send(self, envelope, *, timeout=12):
        return await self.request('send', envelope, headers={'Nats-Msg-Id': envelope['id']}, timeout=timeout)


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
            if 'ack_broker' not in {row['name'] for row in db.execute('PRAGMA table_info(nats_delivered)')}:
                db.execute('ALTER TABLE nats_delivered ADD COLUMN ack_broker TEXT')

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

    def mark(self, message_id, state, broker, subject=None):
        with self.store.transaction() as db:
            db.execute('''UPDATE nats_delivered SET ack_state=?,ack_subject=?,ack_broker=?
                WHERE tenant=? AND agent=? AND id=? AND thread=?''',
                       (state, subject, broker, *self.key, message_id, self.thread))

    def pending(self, broker):
        return self.store.rows('''SELECT id,ack_state,ack_subject FROM nats_delivered
            WHERE tenant=? AND agent=? AND thread=? AND ack_state IN ('ack_intent','broker_acked')
            AND ack_broker=? ORDER BY delivered_at LIMIT 32''', (*self.key, self.thread, broker))


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


def digest_ci_operation(store, message_id, thread, expected):
    if not store.rows("SELECT 1 FROM sqlite_master WHERE name='nats_digest_ci' AND type='table'"):
        return None
    rows = store.rows('SELECT * FROM nats_digest_ci WHERE id=?', (message_id,))
    if not rows:
        return None
    alias = rows[0]
    if not expected or alias['fingerprint'] != expected:
        raise BridgeError('Native digest member ID collision; CI completion does not match')
    parent = store.native_operation(alias['digest_id'], thread)
    if not parent or message_id not in parent['request'].get('digest_items', []):
        raise BridgeError('Native digest member is missing its pinned operation')
    receipt = Store.native_delivery(parent, alias['digest_id'], thread)
    return dict(receipt, request_id=message_id) if receipt else {'state': 'queued'}


def envelope_ci(envelope):
    if envelope.get('kind') == 'ci' and not envelope.get('reply_to'):
        return ci_completion_fingerprint(envelope['from'], envelope['to'], envelope['body'])
    return None


def reserve_digest(store, request, thread, items):
    from csl_bridge_event_state import EventState

    selected, aliases, prior_turn = [], [], None
    for item in items:
        item_id = item['id']
        completion = envelope_ci(item)
        old = store.native_operation(item_id, thread)
        if old:
            same_ci_completion(old, completion)
            receipt = Store.native_delivery(old, item_id, thread)
            if receipt is None:
                return None
        else:
            receipt = digest_ci_operation(store, item_id, thread, completion)
            if receipt and receipt['state'] != 'delivered':
                return None
        if receipt:
            prior_turn = receipt['turn_id']
            continue
        selected.append(item_id)
        if completion:
            aliases.append((item_id, request['id'], completion))
    request['digest_items'] = selected
    with store.transaction() as db:
        store.reserve(NATIVE_PEER, request, transaction=db)
        if aliases:
            db.execute('''CREATE TABLE IF NOT EXISTS nats_digest_ci
                (id TEXT PRIMARY KEY, digest_id TEXT NOT NULL, fingerprint TEXT NOT NULL)''')
            digest_binding = hashlib.sha256(encode(request).encode()).hexdigest()
            for item_id, _, _ in aliases:
                EventState.reserve_id(db, item_id, 'operation', digest_binding, thread)
            db.executemany('INSERT INTO nats_digest_ci VALUES (?,?,?)', aliases)
        if not selected:
            result = {'state': 'delivered', 'request_id': request['id'],
                      'thread_id': thread, 'turn_id': prior_turn}
            db.execute("UPDATE operations SET state='delivered', result=? WHERE id=?",
                       (encode(result), request['id']))
    return request


def deliver_codex(connector, thread, envelope, fingerprint, *, config_path):
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
        completion = envelope_ci(envelope)
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
                if 'digest_items' in original:
                    request['digest_items'] = original['digest_items']
                if request != original:
                    raise BridgeError('NATS operation ID collision; envelope differs')
                request = original
            if old['peer'] != NATIVE_PEER or old['state'] != 'pending':
                receipt = Store.native_delivery(old, envelope['id'], thread)
                return receipt['turn_id'] if receipt else None
        else:
            member = digest_ci_operation(connector.store, envelope['id'], thread, completion)
            if member:
                return member.get('turn_id')
            if envelope.get('kind') == 'digest':
                if reserve_digest(connector.store, request, thread, envelope['items']) is None:
                    return None
                if not request['digest_items']:
                    operation = connector.store.native_operation(envelope['id'], thread)
                    return Store.native_delivery(operation, envelope['id'], thread)['turn_id']
            else:
                connector.store.reserve(NATIVE_PEER, request)
        if 'digest_items' in request:
            selected = set(request['digest_items'])
            envelope = dict(envelope, items=[item for item in envelope['items'] if item['id'] in selected])
        text = '\n\n'.join(delivery_parts(envelope, config_path))
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
    ledger.mark(message_id, 'acked', client.broker)


async def recover_acks(client, ledger, expected_errors):
    for row in ledger.pending(client.broker):
        try:
            if row['ack_state'] == 'ack_intent':
                subject = validate_ack_subject(row['ack_subject'], client.consumer)
                await client.connection.request(subject, b'+ACK', timeout=1)
                ledger.mark(row['id'], 'broker_acked', client.broker)
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
    if row is None or row['ack_state'] != 'acked' or row['ack_broker'] != client.broker or client.broker is None:
        for receipt_id in ledger.receipt_ids(message_id):
            await client.receipt(receipt_id, 'delivered')
        ledger.mark(message_id, 'ack_intent', client.broker, subject)
        await message.ack_sync(timeout=3)
        ledger.mark(message_id, 'broker_acked', client.broker)
        await confirm_acked(client, ledger, message_id)
    else:
        await message.ack_sync(timeout=3)
    return True


async def receive_loop(connector, settings, agent, thread, config_path, claude=None):
    from functools import partial
    from nats.errors import Error, TimeoutError as NatsTimeout
    from csl_nats_renewal import renew_if_due

    errors = (Error, OSError, ValueError, RuntimeRejected, RuntimeUnavailable, sqlite3.Error)
    wake_errors = errors + (KeyError, TypeError)
    ledger = Ledger(connector.store, settings['tenant'], agent, thread)
    delivery = claude.stage if claude else partial(deliver_codex, connector, thread)
    deliver = partial(delivery, config_path=config_path)
    batch_size, retry_delay = (16, 5) if claude else (1, 60)
    credentials = settings['agents'][agent]['credentials']
    next_renewal = 0
    prepare_outbox(connector.store)
    while not connector.stop.is_set():
        client = None
        try:
            client = await Client.connect_active(settings, agent)
            inbox = await client.pull()
            next_recovery = next_wake = 0
            while not connector.stop.is_set() and not client.connection.is_closed:
                if time.monotonic() >= next_renewal:
                    next_renewal = time.monotonic() + 300
                    try:
                        if await renew_if_due(client, credentials):
                            break
                    except errors as error:
                        LOG.warning('NATS renewal for %s deferred (%s)', agent, type(error).__name__)
                if time.monotonic() >= next_recovery:
                    await recover_acks(client, ledger, errors)
                    await replay_outbox(client, ledger, errors)
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
    config_path = Path(config_path).expanduser().absolute()
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
            asyncio.run(receive_loop(connector, settings, name, thread, config_path))
            return
        store = Store(endpoint['mailbox_home'])
        try:
            inbox = ClaudeInbox(store, settings['tenant'], name, thread)
            asyncio.run(receive_loop(connector, settings, name, thread, config_path, inbox))
        finally:
            store.close()

    for agent, endpoint in settings['agents'].items():
        connector.spawn(run, agent, endpoint)


def prepare_outbox(store):
    with store.transaction() as db:
        db.execute('''CREATE TABLE IF NOT EXISTS nats_outbox (
            tenant TEXT, agent TEXT, id TEXT, envelope TEXT NOT NULL,
            PRIMARY KEY (tenant, agent, id))''')
        columns = {row['name'] for row in db.execute('PRAGMA table_info(nats_outbox)')}
        for name, definition in (('thread', 'TEXT'), ('saved_at', 'INTEGER'), ('retry_after', 'INTEGER NOT NULL DEFAULT 0'),
                                 ('broker', 'TEXT'), ('state', 'TEXT')):
            if name not in columns:
                db.execute(f'ALTER TABLE nats_outbox ADD COLUMN {name} {definition}')
        db.execute('UPDATE nats_outbox SET saved_at=COALESCE(unixepoch(json_extract(envelope, \'$.created_at\')), ?) '
                   'WHERE saved_at IS NULL', (int(time.time()),))


def remember_send(store, tenant, envelope, thread=None):
    """Preserve creation time and build identity when an uncertain send is retried."""
    prepare_outbox(store)
    now = int(time.time())
    with store.transaction() as db:
        db.execute('DELETE FROM nats_outbox WHERE saved_at <= ?', (now - OUTBOX_SECONDS,))
        key = (tenant, envelope['from'], envelope['id'])
        row = db.execute('SELECT envelope,thread FROM nats_outbox WHERE tenant=? AND agent=? AND id=?', key).fetchone()
        if row:
            old = json.loads(row['envelope'])
            envelope.update(created_at=old['created_at'], client_version=old['client_version'])
            if old != envelope or row['thread'] not in (None, thread):
                raise BridgeError('NATS send ID collision; reuse the original thread, target and body')
            if row['thread'] is None and thread is not None:
                db.execute('UPDATE nats_outbox SET thread=? WHERE tenant=? AND agent=? AND id=?', (thread, *key))
            return old
        db.execute('INSERT INTO nats_outbox (tenant,agent,id,envelope,thread,saved_at,retry_after) VALUES (?,?,?,?,?,?,?)',
                   (*key, encode(envelope), thread, now, now + 30))
        return envelope


async def post_saved(client, store, key, thread, envelope, *, replay=False):
    message_id = envelope['id']
    args = (*key, message_id, thread)
    try:
        result = await client.send(envelope, timeout=3 if replay else 12)
        state = result.get('state')
        if result.get('id') != message_id or state not in ('queued', 'delivered', 'acked', 'refused', 'dropped'):
            raise BridgeError(f'Unexpected NATS send receipt; retry with --id {message_id}')
    except ControlRejected as error:
        if not error.uncertain and not replay:
            with store.transaction() as db:
                db.execute('UPDATE nats_outbox SET state=\'refused\' WHERE tenant=? AND agent=? AND id=? AND thread=?', args)
        raise
    with store.transaction() as db:
        db.execute('UPDATE nats_outbox SET broker=?,state=? WHERE tenant=? AND agent=? AND id=? AND thread=?',
                   (client.broker, state, *args))
    return result


async def replay_outbox(client, ledger, expected_errors):
    if not client.broker:
        return
    now = int(time.time())
    with ledger.store.transaction() as db:
        db.execute('DELETE FROM nats_outbox WHERE saved_at <= ?', (now - OUTBOX_SECONDS,))
        rows = db.execute('''SELECT id,envelope FROM nats_outbox WHERE tenant=? AND agent=? AND thread=?
            AND COALESCE(state,'') NOT IN ('refused','dropped') AND broker IS NOT ? AND retry_after<=?
            ORDER BY saved_at,id LIMIT 4''', (*ledger.key, ledger.thread, client.broker, now)).fetchall()
        db.executemany('UPDATE nats_outbox SET retry_after=? WHERE tenant=? AND agent=? AND id=?',
                       ((now + 60, *ledger.key, row['id']) for row in rows))
    for row in rows:
        try:
            await post_saved(client, ledger.store, ledger.key, ledger.thread, json.loads(row['envelope']), replay=True)
        except expected_errors as error:
            LOG.debug('NATS outbox %s still pending (%s)', row['id'], type(error).__name__)


def send_command(args):
    settings = config(args.nats_config)
    source = agent_name(args.source or os.environ.get('CSL_AGENT'), registration=True)
    reply_to = None
    if args.is_reply:
        if not args.to or not args.message_id or args.in_reply_to:
            raise BridgeError('NATS reply requires the full positional message UUID and --to')
        reply_to = identifier(args.message_id)
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
    urgent, reason = getattr(args, 'urgent', False), getattr(args, 'reason', None)
    if urgent != bool(reason):
        raise BridgeError('Use --urgent and --reason together')
    message_id = identifier(args.id) if args.id else str(uuid.uuid4())
    envelope = {'v': 1, 'id': message_id, 'from': source, 'to': target, 'kind': 'reply' if reply_to else 'message',
                'reply_to': reply_to, 'urgency': 'urgent' if urgent else 'normal',
                'created_at': datetime.now(timezone.utc).isoformat(), 'body': text,
                'client_version': build_version(), 'attachments': [], 'items': []}
    if reason:
        envelope['reason'] = reason
    thread = settings['agents'][source]['session']
    store = Store(settings['connector_home'])
    print('message_id=' + message_id, file=sys.stderr)

    async def send():
        client = await Client.connect_active(settings, source)
        try:
            return await post_saved(client, store, (settings['tenant'], source), thread, envelope)
        finally:
            await client.connection.close()

    def keep_tls_log(record):
        return (record.levelno != logging.WARNING or
                record.getMessage() != 'returning true from eof_received() has no effect when using ssl')

    # asyncio reports the stream reader's ignored half-close flag during TLS shutdown.
    asyncio_log = logging.getLogger('asyncio')
    asyncio_log.addFilter(keep_tls_log)
    try:
        envelope = remember_send(store, settings['tenant'], envelope, thread)
        result = asyncio.run(send())
    except BridgeError:
        raise
    except Exception as error:
        # The same persisted envelope is safe to retry with the printed UUID.
        raise BridgeError(f'NATS send unconfirmed ({type(error).__name__}); retry with --id {message_id}') from None
    finally:
        asyncio_log.removeFilter(keep_tls_log)
        store.close()
    print(encode(result))

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
from csl_bridge_store import BridgeError, Store, contains_findings, encode, frame_message, identifier, name
from csl_version import build_version

NATIVE_PEER = '_nats'  # Outside the public bridge peer namespace.
MAX_BODY = 48 * 1024
STREAM = 'CSL_MSG'
LOG = logging.getLogger(__name__)


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
        tenant = name(settings['tenant'])
        if not isinstance(settings['servers'], list) or not settings['servers']:
            raise ValueError
        for server in settings['servers']:
            url = urlsplit(server)
            if (url.scheme not in ('tls', 'wss') or not url.hostname or url.username or url.password or
                    url.query or url.fragment or (url.scheme == 'tls' and url.path not in ('', '/'))):
                raise ValueError
        settings['connector_home'] = Path(settings['connector_home']).expanduser().absolute()
        for agent, endpoint in settings['agents'].items():
            name(agent)
            if agent == 'owner' or set(endpoint) != {'credentials', 'session'}:
                raise ValueError
            identifier(endpoint['session'])
            endpoint['credentials'] = private_file(endpoint['credentials'])
        settings['tenant'] = tenant
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

    def get(self, message_id, fingerprint):
        rows = self.store.rows('SELECT * FROM nats_delivered WHERE tenant=? AND agent=? AND id=?',
                               (*self.key, message_id))
        if not rows:
            return None
        row = rows[0]
        if (row['thread'], row['fingerprint']) != (self.thread, fingerprint):
            raise BridgeError('NATS receipt does not match its original thread or envelope')
        return row

    def delivered(self, message_id, fingerprint, turn):
        with self.store.transaction() as db:
            db.execute('''INSERT INTO nats_delivered
                (tenant,agent,id,thread,fingerprint,turn,delivered_at) VALUES (?,?,?,?,?,?,?)''',
                       (*self.key, message_id, self.thread, fingerprint, identifier(turn), int(time.time())))

    def mark(self, message_id, state, subject=None):
        with self.store.transaction() as db:
            db.execute('''UPDATE nats_delivered SET ack_state=?,ack_subject=?
                WHERE tenant=? AND agent=? AND id=? AND thread=?''',
                       (state, subject, *self.key, message_id, self.thread))

    def pending(self):
        return self.store.rows('''SELECT id,ack_state,ack_subject FROM nats_delivered
            WHERE tenant=? AND agent=? AND thread=? AND ack_state IN ('ack_intent','broker_acked')
            ORDER BY delivered_at LIMIT 32''', (*self.key, self.thread))


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
        old = connector.store.reserve(NATIVE_PEER, request)
        if old and old['state'] != 'pending':
            result = old['result'] or {}
        else:
            text = frame_message(envelope['from'], envelope['id'], None, envelope['body'])
            result = connector.invoke(NATIVE_PEER, request, native_text=text)
        if result.get('state') == 'delivered' and result.get('thread_id') == thread:
            return result['turn_id']
        return None


def validate_ack_subject(subject, consumer):
    pattern = rf'\$JS\.ACK\.(?:[^.\s*<>]+\.[^.\s*<>]+\.)?{STREAM}\.{re.escape(consumer)}(?:\.[0-9]+){{5}}'
    if not isinstance(subject, str) or not re.fullmatch(pattern, subject):
        raise BridgeError('Unexpected JetStream acknowledgment subject')
    return subject


async def confirm_acked(client, ledger, message_id):
    await client.receipt(message_id, 'acked')
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


async def receive_one(client, ledger, message, deliver):
    if message.subject != f'{client.root}.agent.{client.agent}.inbox':
        raise BridgeError('Unexpected NATS inbox subject')
    subject = validate_ack_subject(message.reply, client.consumer)
    envelope = json.loads(message.data)
    if (not isinstance(envelope, dict) or type(envelope.get('v')) is not int or envelope['v'] != 1 or
            envelope.get('to') != client.agent or envelope.get('kind') not in ('message', 'reply', 'ci', 'system', 'reminder') or
            envelope.get('items') or envelope.get('attachments')):
        raise BridgeError('Envelope is unsupported by the first NATS adapter slice')
    message_id = identifier(envelope.get('id'))
    name(envelope.get('from'))
    body_text(envelope.get('body'))
    fingerprint = hashlib.sha256(encode(envelope).encode('utf-8')).hexdigest()
    row = ledger.get(message_id, fingerprint)
    if row is None:
        turn = await asyncio.to_thread(deliver, envelope, fingerprint)
        if turn is None:
            await message.nak(delay=60)
            return False
        ledger.delivered(message_id, fingerprint, turn)
    if row is None or row['ack_state'] != 'acked':
        await client.receipt(message_id, 'delivered')
        ledger.mark(message_id, 'ack_intent', subject)
        await message.ack_sync(timeout=3)
        ledger.mark(message_id, 'broker_acked')
        await confirm_acked(client, ledger, message_id)
    else:
        await message.ack_sync(timeout=3)
    return True


async def receive_loop(connector, settings, agent, thread):
    from functools import partial
    from nats.errors import Error, TimeoutError as NatsTimeout
    errors = (Error, OSError, ValueError, RuntimeRejected, RuntimeUnavailable, sqlite3.Error)
    ledger = Ledger(connector.store, settings['tenant'], agent, thread)
    deliver = partial(deliver_codex, connector, thread)
    while not connector.stop.is_set():
        client = None
        try:
            client = await Client.connect(settings, agent)
            await client.request('ctl.hello', {'client_version': build_version()})
            inbox = await client.pull()
            next_recovery = 0
            while not connector.stop.is_set() and not client.connection.is_closed:
                if time.monotonic() >= next_recovery:
                    await recover_acks(client, ledger, errors)
                    next_recovery = time.monotonic() + 15
                try:
                    messages = await inbox.fetch(1, timeout=2)
                except NatsTimeout:
                    continue
                for message in messages:
                    try:
                        await receive_one(client, ledger, message, deliver)
                    except errors as error:
                        LOG.warning('NATS inbox %s left unacknowledged (%s)', agent, type(error).__name__)
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
        if connector.agents.get(agent) != endpoint['session']:
            raise BridgeError(f'NATS registration does not match the local thread for {agent}')
    def run(name, thread):
        asyncio.run(receive_loop(connector, settings, name, thread))

    for agent, endpoint in settings['agents'].items():
        connector.spawn(run, agent, endpoint['session'])


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
    source = name(args.source or os.environ.get('CSL_AGENT'))
    target = name(args.to)
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

"""Task-independent durable mail. No runtime or model API is imported here."""
from datetime import datetime, timezone
import hashlib
import threading
import uuid

from csl_bridge_store import BridgeError, Store, contains_findings, decode, encode, identifier, name


class MailRejected(BridgeError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__('Mailbox rejected: ' + reason)


def now():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError()
        return parsed
    except (TypeError, ValueError):
        raise MailRejected('invalid_timestamp') from None


def validate_mail(request):
    base = {'version', 'op', 'id', 'message_version', 'action', 'source', 'target'}
    action = request.get('action')
    extra = {'send': {'envelope'}, 'ack': {'message_id', 'acknowledged_at'},
             'receipt': {'message_id'}}
    if request.get('message_version') != 1 or type(request.get('message_version')) is not int:
        raise MailRejected('unsupported_message_version')
    if action not in extra or set(request) != base | extra[action]:
        raise MailRejected('invalid_mail_fields')
    identifier(request['id']); name(request['source']); name(request['target'])
    if action == 'send':
        envelope = request['envelope']
        if not isinstance(envelope, dict) or set(envelope) != {'kind', 'body', 'in_reply_to', 'grant_ref', 'created_at', 'expires_at'}:
            raise MailRejected('invalid_envelope')
        if envelope['kind'] not in ('message', 'request', 'reply'):
            raise MailRejected('invalid_kind')
        body = envelope['body']
        if not isinstance(body, str) or not body.strip() or len(body) > 12000 or any(ord(c) < 32 and c not in '\r\n\t' for c in body):
            raise MailRejected('invalid_body')
        if envelope['kind'] == 'reply':
            identifier(envelope['in_reply_to'])
        elif envelope['in_reply_to'] is not None:
            raise MailRejected('unexpected_reply_reference')
        if envelope['grant_ref'] is not None:
            name(envelope['grant_ref'])
        timestamp(envelope['created_at'])
        if envelope['expires_at'] is not None:
            timestamp(envelope['expires_at'])
    else:
        identifier(request['message_id'])
        if action == 'ack':
            timestamp(request['acknowledged_at'])
    return request


def schema(store):
    with store.lock:
        store.db.executescript('''
            CREATE TABLE IF NOT EXISTS mail_messages (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
                direction TEXT NOT NULL, peer TEXT NOT NULL, source TEXT NOT NULL,
                target TEXT NOT NULL, fingerprint TEXT NOT NULL, envelope TEXT NOT NULL,
                queued_at TEXT, persisted_at TEXT, acknowledged_at TEXT);
            CREATE TABLE IF NOT EXISTS mail_grants (
                ref TEXT PRIMARY KEY, peer TEXT NOT NULL, source TEXT NOT NULL,
                target TEXT NOT NULL, permissions TEXT NOT NULL, scope TEXT NOT NULL,
                expires_at TEXT, revoked INTEGER NOT NULL DEFAULT 0);
        ''')


def grant(store, ref, peer, source, target, permissions, scope, expires_at=None, revoke=False):
    """Owner/operator configuration only. Deliberately absent from MCP and wire."""
    schema(store); name(ref)
    if revoke:
        with store.transaction() as db:
            if not db.execute('SELECT ref FROM mail_grants WHERE ref=?', (ref,)).fetchone():
                raise MailRejected('unknown_grant')
            db.execute('UPDATE mail_grants SET revoked=1 WHERE ref=?', (ref,))
        return {'grant_ref': ref, 'revoked': True}
    name(peer); name(source); name(target)
    if peer not in store.peers() or target not in store.agents():
        raise MailRejected('unknown_grant_endpoint')
    if not permissions or not set(permissions) <= {'communicate', 'activate', 'work'}:
        raise MailRejected('invalid_grant_permissions')
    if not isinstance(scope, str) or not scope.strip() or len(scope) > 2000:
        raise MailRejected('invalid_grant_scope')
    if expires_at is not None:
        timestamp(expires_at)
    with store.transaction() as db:
        db.execute('INSERT INTO mail_grants VALUES(?,?,?,?,?,?,?,0) ON CONFLICT(ref) DO UPDATE SET '
                   'peer=excluded.peer,source=excluded.source,target=excluded.target,permissions=excluded.permissions,'
                   'scope=excluded.scope,expires_at=excluded.expires_at,revoked=0',
                   (ref, peer, source, target, encode(sorted(set(permissions))), scope, expires_at))
    return {'grant_ref': ref, 'configured': True}


class Mailbox:
    def __init__(self, connector):
        self.connector = connector
        self.store = connector.store
        self.delivery_lock = threading.RLock()
        self.queue_lock = threading.RLock()
        schema(self.store)

    def message(self, message_id):
        identifier(message_id)
        rows = self.store.rows('SELECT * FROM mail_messages WHERE id=?', (message_id,))
        return rows[0] if rows else None

    def authority(self, row):
        reference = decode(row['envelope'])['grant_ref']
        none = lambda reason: {'permissions': [], 'scope': None, 'authority': 'none', 'reason': reason}
        if reference is None:
            return none('no_grant_reference')
        grants = self.store.rows('SELECT * FROM mail_grants WHERE ref=?', (reference,))
        if not grants:
            return none('unknown_grant')
        item = grants[0]
        if (item['peer'], item['source'], item['target']) != (row['peer'], row['source'], row['target']):
            return none('grant_endpoint_mismatch')
        if item['revoked']:
            return none('grant_revoked')
        if item['expires_at'] and timestamp(item['expires_at']) <= timestamp(now()):
            return none('grant_expired')
        return {'authority': 'owner_grant', 'permissions': decode(item['permissions']),
                'scope': item['scope'], 'reason': 'locally_resolved', 'executes_work': False, 'activates_model': False}

    def view(self, row):
        envelope = decode(row['envelope'])
        own = self.connector.identity
        sender, recipient = ((own, row['source']), (row['peer'], row['target'])) if row['direction'] == 'out' else ((row['peer'], row['source']), (own, row['target']))
        replies = [r['id'] for r in self.store.rows('SELECT id,envelope FROM mail_messages WHERE peer=? AND direction=? AND source=? AND target=?',
                   (row['peer'], 'in' if row['direction'] == 'out' else 'out', row['target'], row['source']))
                   if decode(r['envelope'])['in_reply_to'] == row['id']]
        result = {'id': row['id'], 'cursor': row['seq'], 'from': {'peer': sender[0], 'agent': sender[1]},
                  'to': {'peer': recipient[0], 'agent': recipient[1]}, **envelope,
                  'queued_at': row['queued_at'], 'persisted_at': row['persisted_at'],
                  'acknowledged_at': row['acknowledged_at'], 'reply_ids': replies[:100], 'reply_count': len(replies),
                  'facts': {'queued': row['queued_at'] is not None, 'persisted': row['persisted_at'] is not None,
                            'acknowledged': row['acknowledged_at'] is not None, 'replied': bool(replies)},
                  'expired': bool(envelope['expires_at'] and timestamp(envelope['expires_at']) <= timestamp(now()))}
        if row['direction'] == 'in':
            result['authority'] = self.authority(row)
        else:
            operations = self.store.rows('SELECT state,result FROM operations WHERE id=?', (row['id'],))
            if operations:
                result['delivery'] = {'state': operations[0]['state'], 'result': decode(operations[0]['result']) if operations[0]['result'] else None}
        return result

    def incoming(self, peer, request):
        validate_mail(request)
        config = self.connector.peers.get(peer)
        if not config or 'mail' not in config['allow_ops'] or request['source'] not in config.get('allow_sources', []) or request['target'] not in config['allow_agents']:
            return {'state': 'rejected', 'reason': 'unauthorized_sender', 'request_id': request['id']}
        if request['target'] not in self.connector.agents:
            return {'state': 'undeliverable', 'reason': 'unknown_recipient', 'request_id': request['id']}
        action = request['action']
        if action != 'send':
            row = self.message(request['message_id'])
            direction = 'out' if action == 'ack' else row['direction'] if row else 'in'
            source, target = (row['target'], row['source']) if row and direction == 'out' else (row['source'], row['target']) if row else (None, None)
            if action == 'receipt' and not row:
                return {'state': 'not_found', 'message_id': request['message_id']}
            if not row or row['direction'] != direction or row['peer'] != peer or (source, target) != (request['source'], request['target']):
                return {'state': 'rejected', 'reason': 'unknown_or_mismatched_message', 'request_id': request['id']}
            if action == 'receipt':
                return self.receipt(row)
            try:
                self.store.reserve('mail-in:' + peer, request)
            except BridgeError:
                return {'state': 'rejected', 'reason': 'id_collision', 'request_id': request['id']}
            with self.store.transaction() as db:
                db.execute('UPDATE mail_messages SET acknowledged_at=coalesce(acknowledged_at,?) WHERE id=?',
                           (request['acknowledged_at'], row['id']))
                result = {'state': 'acknowledged', 'message_id': row['id']}
                db.execute('UPDATE operations SET state=?,result=? WHERE id=?', ('delivered', encode(result), request['id']))
            return result
        envelope = request['envelope']
        if envelope['kind'] == 'reply':
            original = self.message(envelope['in_reply_to'])
            if not original or original['direction'] != 'out' or original['peer'] != peer or (original['target'], original['source']) != (request['source'], request['target']):
                return {'state': 'rejected', 'reason': 'invalid_reply_reference', 'request_id': request['id']}
        try:
            self.store.reserve('mail-in:' + peer, request)
        except BridgeError:
            return {'state': 'rejected', 'reason': 'id_collision', 'request_id': request['id']}
        fingerprint = hashlib.sha256(encode(request).encode()).hexdigest()
        with self.store.transaction() as db:
            old = db.execute('SELECT * FROM mail_messages WHERE id=?', (request['id'],)).fetchone()
            if old and (old['direction'] != 'in' or old['fingerprint'] != fingerprint):
                return {'state': 'rejected', 'reason': 'id_collision', 'request_id': request['id']}
            if not old and envelope['expires_at'] and timestamp(envelope['expires_at']) <= timestamp(now()):
                result = {'state': 'expired', 'reason': 'message_expired_before_persistence', 'message_id': request['id']}
                db.execute('UPDATE operations SET state=?,result=? WHERE id=?', ('expired', encode(result), request['id']))
                return result
            db.execute('INSERT OR IGNORE INTO mail_messages(id,direction,peer,source,target,fingerprint,envelope,persisted_at) VALUES(?,?,?,?,?,?,?,?)',
                       (request['id'], 'in', peer, request['source'], request['target'], fingerprint, encode(envelope), now()))
            row = dict(db.execute('SELECT * FROM mail_messages WHERE id=?', (request['id'],)).fetchone())
            result = self.receipt(row)
            db.execute('UPDATE operations SET state=?,result=? WHERE id=?', ('delivered', encode(result), request['id']))
        return result

    def receipt(self, row):
        return {'state': 'persisted', 'message_id': row['id'], 'persisted_at': row['persisted_at'],
                'acknowledged_at': row['acknowledged_at']}

    def queue(self, source, peer, target, message_id, kind, body, in_reply_to=None, grant_ref=None, expires_at=None):
        name(source); name(peer); name(target); identifier(message_id)
        if source not in self.connector.agents or peer not in self.connector.peers:
            raise MailRejected('unknown_local_endpoint')
        if contains_findings(body):
            raise MailRejected('possible_secrets')
        content = {'version': 1, 'op': 'mail', 'message_version': 1, 'action': 'send',
                   'id': message_id, 'source': source, 'target': target,
                   'envelope': {'kind': kind, 'body': body, 'in_reply_to': in_reply_to,
                                'grant_ref': grant_ref, 'created_at': now(), 'expires_at': expires_at}}
        reserved = self.store.rows('SELECT peer,request FROM operations WHERE id=?', (message_id,))
        if reserved and reserved[0]['peer'] == 'mail-out:' + peer:
            prior = decode(reserved[0]['request'])
            if prior.get('op') == 'mail' and prior.get('action') == 'send':
                content['envelope']['created_at'] = prior['envelope']['created_at']
        validate_mail(content)
        fingerprint = hashlib.sha256(encode({'peer': peer, **content, 'envelope': {k: v for k, v in content['envelope'].items() if k != 'created_at'}}).encode()).hexdigest()
        old = self.message(message_id)
        if old:
            if old['direction'] != 'out' or old['fingerprint'] != fingerprint:
                raise MailRejected('id_collision')
            return self.view(old)
        if kind == 'reply':
            original = self.message(in_reply_to)
            if not original or original['direction'] != 'in' or original['peer'] != peer or (original['target'], original['source']) != (source, target):
                raise MailRejected('invalid_reply_reference')
        with self.store.transaction() as db:
            try:
                self.store.reserve('mail-out:' + peer, content, transaction=db)
            except BridgeError:
                raise MailRejected('id_collision') from None
            db.execute('INSERT INTO mail_messages(id,direction,peer,source,target,fingerprint,envelope,queued_at) VALUES(?,?,?,?,?,?,?,?)',
                       (message_id, 'out', peer, source, target, fingerprint, encode(content['envelope']), content['envelope']['created_at']))
        return self.view(self.message(message_id))

    def flush(self, peer):
        from csl_bridge_transport import exchange
        with self.delivery_lock:
            for operation in self.store.rows("SELECT * FROM operations WHERE peer=? AND state IN ('pending','uncertain')", ('mail-out:' + peer,)):
                request = decode(operation['request'])
                try:
                    result = None
                    envelope = request.get('envelope', {})
                    expired = bool(envelope.get('expires_at') and timestamp(envelope['expires_at']) <= timestamp(now()))
                    if operation['state'] == 'uncertain':
                        check = {'version': 1, 'op': 'mail', 'message_version': 1, 'action': 'receipt',
                                 'id': str(uuid.uuid4()), 'source': request['source'], 'target': request['target'],
                                 'message_id': request['id'] if request['action'] == 'send' else request['message_id']}
                        result = exchange(self.connector.peers[peer], self.connector.identity, check)
                        if not isinstance(result, dict):
                            raise BridgeError('Invalid mail receipt')
                        if result.get('state') == 'not_found':
                            result = None  # confirmed absent, resend the same immutable operation
                        elif request['action'] == 'ack' and result.get('state') == 'persisted':
                            if result.get('acknowledged_at'):
                                result = {'state': 'acknowledged', 'message_id': request['message_id']}
                            else:
                                result = None  # recipient explicitly confirms no acknowledgement
                    if result is None:
                        result = ({'state': 'expired', 'reason': 'message_expired_before_confirmed_delivery', 'message_id': request['id']}
                                  if expired else exchange(self.connector.peers[peer], self.connector.identity, request))
                    if not isinstance(result, dict) or result.get('state') not in ('persisted', 'acknowledged', 'rejected', 'undeliverable', 'expired'):
                        raise BridgeError('Invalid mail receipt')
                    if result['state'] == 'persisted' and (result.get('message_id') != request['id'] or not result.get('persisted_at')):
                        raise BridgeError('Invalid persisted receipt')
                    if result['state'] == 'acknowledged' and (request['action'] != 'ack' or result.get('message_id') != request['message_id']):
                        raise BridgeError('Invalid acknowledgement receipt')
                    if result.get('persisted_at'):
                        timestamp(result['persisted_at'])
                    if result.get('acknowledged_at'):
                        timestamp(result['acknowledged_at'])
                    result = {key: value for key, value in result.items() if key in
                              {'state', 'message_id', 'persisted_at', 'acknowledged_at', 'reason'}}
                    if 'reason' in result and (not isinstance(result['reason'], str) or len(result['reason']) > 128):
                        raise BridgeError('Invalid receipt reason')
                except BridgeError:
                    result = {'state': 'uncertain', 'request_id': request['id'], 'reason': 'transport_or_receipt_uncertain'}
                state = 'delivered' if result['state'] in ('persisted', 'acknowledged') else result['state']
                with self.store.transaction() as db:
                    db.execute('UPDATE operations SET state=?,result=? WHERE id=?', (state, encode(result), request['id']))
                    if result['state'] == 'persisted':
                        db.execute('UPDATE mail_messages SET persisted_at=?,acknowledged_at=coalesce(acknowledged_at,?) WHERE id=?',
                                   (result['persisted_at'], result.get('acknowledged_at'), request['id']))

    def local(self, request):
        with self.queue_lock:
            return self._local(request)

    def _local(self, request):
        try:
            action = request['local_op']
            common = {'local_op', 'agent'}
            fields = {'mail_send': {'peer', 'target', 'id', 'kind', 'body', 'grant_ref', 'expires_at'},
                      'mail_reply': {'id', 'in_reply_to', 'body', 'grant_ref', 'expires_at'},
                      'mail_receive': {'after', 'limit', 'outbox'}, 'mail_ack': {'message_id'}}
            required = {'mail_send': {'peer', 'target', 'id', 'body'}, 'mail_reply': {'id', 'in_reply_to', 'body'},
                        'mail_receive': set(), 'mail_ack': {'message_id'}}
            if action not in fields or not common | required[action] <= set(request) or not set(request) <= common | fields[action]:
                raise MailRejected('invalid_mail_fields')
            agent = name(request['agent'])
            if agent not in self.connector.agents:
                raise MailRejected('unknown_local_agent')
            if action in ('mail_send', 'mail_reply'):
                peer, target = request.get('peer'), request.get('target')
                original = None
                if action == 'mail_reply':
                    original = self.message(request['in_reply_to'])
                    if not original or original['direction'] != 'in' or original['target'] != agent:
                        raise MailRejected('invalid_reply_reference')
                    peer, target = original['peer'], original['source']
                with self.queue_lock:
                    return self.queue(agent, peer, target, request['id'], 'reply' if original else request.get('kind', 'message'),
                                      request['body'], request.get('in_reply_to'), request.get('grant_ref'), request.get('expires_at'))
            if action == 'mail_receive':
                after, limit = request.get('after', 0), request.get('limit', 20)
                if type(after) is not int or not 0 <= after <= 2**63-1 or type(limit) is not int or not 1 <= limit <= 100 or type(request.get('outbox', False)) is not bool:
                    raise MailRejected('invalid_mail_cursor_or_limit')
                direction = 'out' if request.get('outbox', False) else 'in'
                column = 'source' if direction == 'out' else 'target'
                rows = self.store.rows('SELECT * FROM mail_messages WHERE direction=? AND '+column+'=? AND seq>? ORDER BY seq LIMIT ?', (direction, agent, after, limit))
                messages, cursor = [], after
                for row in rows:
                    view = self.view(row)
                    if len(encode({'messages': messages + [view], 'cursor': row['seq']}).encode()) > 60000:
                        break  # byte-bounded page; the next cursor never skips a message
                    messages.append(view); cursor = row['seq']
                return {'messages': messages, 'cursor': cursor}
            if action == 'mail_ack':
                row = self.message(request['message_id'])
                if not row or row['direction'] != 'in' or row['target'] != agent:
                    raise MailRejected('unknown_or_mismatched_message')
                stamp = row['acknowledged_at'] or now()
                ack_id = str(uuid.uuid5(uuid.NAMESPACE_URL, 'csl-mail-ack/'+self.connector.identity+'/'+agent+'/'+row['id']))
                reserved = self.store.rows('SELECT request FROM operations WHERE id=?', (ack_id,))
                if reserved:
                    stamp = decode(reserved[0]['request']).get('acknowledged_at', stamp)
                wire = {'version': 1, 'op': 'mail', 'message_version': 1, 'action': 'ack', 'id': ack_id,
                        'source': agent, 'target': row['source'], 'message_id': row['id'], 'acknowledged_at': stamp}
                with self.store.transaction() as db:
                    try:
                        self.store.reserve('mail-out:' + row['peer'], wire, transaction=db)
                    except BridgeError:
                        raise MailRejected('id_collision') from None
                    db.execute('UPDATE mail_messages SET acknowledged_at=coalesce(acknowledged_at,?) WHERE id=?', (stamp, row['id']))
                return self.view(self.message(row['id']))
            raise MailRejected('unknown_mail_operation')
        except MailRejected as exc:
            return {'state': 'rejected', 'reason': exc.reason}
        except (BridgeError, KeyError, TypeError, ValueError):
            return {'state': 'rejected', 'reason': 'invalid_mail_request'}

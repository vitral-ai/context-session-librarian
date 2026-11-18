"""Inert event-effect transaction primitives; no transport imports or activation.

Consumers must supply independently correlated identities and complete historical
seeding before setting seed_complete/pending. This module does not implement that
admission/upgrade integration and is deliberately unused by production consumers.
All ID purposes and claims are scoped to this Store's exact bridge.sqlite.
"""
import hashlib
import uuid

from csl_bridge_store import BridgeError, decode, encode, identifier, name


class EventConflict(ValueError):
    """Permanent identity refusal; adapters must not schedule an infinite retry."""


class EventDeferred(BridgeError):
    """Unresolved durable evidence; never permission for another Native effect."""


SETTLED = ('native_delivered', 'wait_delivered', 'legacy_suppressed')


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


class EventState:
    """Canonical final claims; progress keeps its separate per-wire domain."""

    def __init__(self, store):
        self.store = store
        # Not called from Store.__init__: this contribution is inert until the
        # reviewed consumer/old-home integration explicitly activates it.
        with store.transaction() as db:
            for statement in (
                '''CREATE TABLE IF NOT EXISTS event_effects(
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,key TEXT UNIQUE NOT NULL,
                    peer TEXT NOT NULL,task TEXT NOT NULL,projection TEXT NOT NULL,
                    representative INTEGER NOT NULL,state TEXT NOT NULL DEFAULT 'seeding',
                    seed_complete INTEGER NOT NULL DEFAULT 0,agent TEXT NOT NULL,
                    thread TEXT,client_id TEXT,attempt INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 0,outcome TEXT,
                    receipt_observed INTEGER NOT NULL DEFAULT 0,UNIQUE(peer,task))''',
                'CREATE INDEX IF NOT EXISTS event_effect_work ON event_effects(state,seq)',
                '''CREATE TABLE IF NOT EXISTS event_attempts(
                    key TEXT NOT NULL,number INTEGER NOT NULL,mode TEXT NOT NULL,
                    thread TEXT NOT NULL,client_id TEXT,state TEXT NOT NULL,outcome TEXT,
                    PRIMARY KEY(key,number))''',
                '''CREATE TABLE IF NOT EXISTS event_native_ids(
                    id TEXT PRIMARY KEY,purpose TEXT NOT NULL,binding TEXT NOT NULL,
                    thread TEXT NOT NULL)''',
                '''CREATE TABLE IF NOT EXISTS event_history_barriers(
                    seq INTEGER PRIMARY KEY,peer TEXT,wire_id TEXT,body_hash TEXT NOT NULL,
                    reason TEXT NOT NULL)''',
                'CREATE INDEX IF NOT EXISTS event_barrier_scope ON event_history_barriers(peer)',
                '''CREATE TABLE IF NOT EXISTS event_recovery_scans(
                    consumer TEXT PRIMARY KEY,after_seq INTEGER NOT NULL,horizon INTEGER NOT NULL)''',
            ):
                db.execute(statement)

    @staticmethod
    def reserve_id(db, value, purpose, binding, thread):
        """Call inside the SAME transaction as operation/mail/alias reservation.

        Existing own-purpose rows must match their immutable fingerprint. A final
        effect can never borrow an ordinary/progress/mail acceptance ID. Ordinary
        consumers must call here too; checking final allocation alone is insufficient.
        """
        identifier(value); identifier(thread)
        if purpose not in ('operation', 'mail', 'progress', 'final'):
            raise EventConflict('Unknown Native ID purpose')
        old = db.execute('SELECT * FROM event_native_ids WHERE id=?', (value,)).fetchone()
        if old and (old['purpose'], old['binding'], old['thread']) != (purpose, binding, thread):
            raise EventConflict('Native client ID purpose/binding collision')
        checks = [('operations', 'hash', 'operation'), ('inbound', 'body', 'progress')]
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='mail_messages'").fetchone():
            checks.append(('mail_messages', 'fingerprint', 'mail'))
        for table, column, allowed in checks:
            row = db.execute('SELECT ' + column + ' FROM ' + table + ' WHERE id=?', (value,)).fetchone()
            if row:
                fingerprint = hashlib.sha256(row[0].encode()).hexdigest() if table == 'inbound' else row[0]
                if table == 'inbound':
                    try:
                        progress = decode(row[0]).get('kind') == 'progress'
                    except (BridgeError, AttributeError):
                        progress = False
                    if not progress:
                        raise EventConflict('Native client ID belongs to a final or malformed event')
                if purpose != allowed or fingerprint != binding:
                    raise EventConflict('Native client ID collides with another stored message')
        db.execute('INSERT OR IGNORE INTO event_native_ids VALUES(?,?,?,?)',
                   (value, purpose, binding, thread))

    @staticmethod
    def barrier(db, seq, peer, wire_id, raw_body, reason):
        """Commit together with the catalog watermark, never silently skip damage.

        peer must come from independent durable metadata; None suppresses the home.
        This helper intentionally provides no body-derived narrowing/removal API.
        """
        try:
            name(peer)
        except BridgeError:
            peer = None
        raw = raw_body if isinstance(raw_body, bytes) else raw_body.encode()
        fingerprint = hashlib.sha256(raw).hexdigest()
        old = db.execute('SELECT * FROM event_history_barriers WHERE seq=?', (seq,)).fetchone()
        if old and (old['peer'], old['wire_id'], old['body_hash'], old['reason']) != (peer, wire_id, fingerprint, reason):
            raise EventConflict('Historical barrier identity changed')
        db.execute('INSERT OR IGNORE INTO event_history_barriers VALUES(?,?,?,?,?)',
                   (seq, peer, wire_id, fingerprint, reason))

    @staticmethod
    def checked(db, key, revision):
        row = db.execute('SELECT * FROM event_effects WHERE key=?', (key,)).fetchone()
        if row is None or row['revision'] != revision:
            raise EventDeferred('Event identity changed; re-read its durable outcome')
        if not row['seed_complete'] or row['state'] in ('seeding', 'deferred'):
            raise EventDeferred('Historical/correlation proof is incomplete')
        if row['state'] in ('conflict', 'rejected'):
            raise EventConflict('Canonical event identity is conflicted')
        barrier = db.execute('SELECT 1 FROM event_history_barriers WHERE peer IS NULL OR peer=? LIMIT 1',
                             (row['peer'],)).fetchone()
        if barrier:
            raise EventDeferred('Unresolved historical suppression barrier')
        return row

    @staticmethod
    def recipient(db, row):
        registered = db.execute('SELECT thread FROM agents WHERE name=?', (row['agent'],)).fetchone()
        if not row['thread'] or not registered or registered[0] != row['thread']:
            raise EventDeferred('Recipient binding is unknown or changed')

    def claim_native(self, key, revision):
        """One committed winner; caller invokes outside the returned transaction."""
        with self.store.transaction() as db:
            row = self.checked(db, key, revision)
            if row['state'] in SETTLED:
                return None
            if row['state'] != 'pending':
                raise EventDeferred('No fresh Native effect is permitted')
            self.recipient(db, row)
            value = row['client_id'] or str(uuid.uuid4())
            self.reserve_id(db, value, 'final', row['projection'], row['thread'])
            number = row['attempt'] + 1
            changed = db.execute("UPDATE event_effects SET state='uncertain',client_id=?,attempt=?,revision=revision+1 WHERE key=? AND state='pending' AND revision=?",
                                 (value, number, key, revision)).rowcount
            if changed != 1:
                raise EventDeferred('Another event claimant won')
            db.execute('INSERT INTO event_attempts VALUES(?,?,?,?,?,?,NULL)',
                       (key, number, 'native', row['thread'], value, 'uncertain'))
            return {'key': key, 'number': number, 'client_id': value,
                    'thread': row['thread'], 'revision': revision + 1}

    def observe_wait(self, key, revision):
        """Claim+observation commit together. Old-cursor reads never reopen effects."""
        with self.store.transaction() as db:
            row = self.checked(db, key, revision)
            if row['state'] in SETTLED:
                return {'state': row['state'], 'history_read': True}
            if row['state'] == 'receipt_available':
                db.execute('UPDATE event_effects SET receipt_observed=1 WHERE key=?', (key,))
                return {'state': 'receipt_available', 'narrative_accepted': False}
            if row['state'] != 'pending':
                raise EventDeferred('An unresolved Native attempt holds this cursor')
            self.recipient(db, row)
            number = row['attempt'] + 1
            outcome = encode({'state': 'wait_delivered', 'native_accepted': False})
            db.execute("UPDATE event_effects SET state='wait_delivered',attempt=?,revision=revision+1,outcome=? WHERE key=? AND state='pending' AND revision=?",
                       (number, outcome, key, revision))
            db.execute('INSERT INTO event_attempts VALUES(?,?,?,?,?,?,?)',
                       (key, number, 'wait', row['thread'], None, 'wait_delivered', outcome))
            return {'state': 'wait_delivered', 'history_read': False}

    def finish_native(self, claim, result, *, definitely_unaccepted=False):
        """Ambiguous failures stay uncertain; only actual typed acceptance settles."""
        with self.store.transaction() as db:
            row = self.checked(db, claim['key'], claim['revision'])
            attempt = db.execute('SELECT * FROM event_attempts WHERE key=? AND number=?',
                                 (claim['key'], claim['number'])).fetchone()
            if (row['state'] != 'uncertain' or not attempt or attempt['mode'] != 'native' or
                    row['attempt'] != claim['number'] or row['thread'] != claim['thread'] or
                    row['client_id'] != claim['client_id'] or attempt['thread'] != claim['thread'] or
                    attempt['client_id'] != claim['client_id']):
                raise EventDeferred('Attempt binding changed; do not reinterpret it')
            state = 'uncertain'
            if (isinstance(result, dict) and result.get('state') == 'delivered' and
                    result.get('request_id') == claim['client_id'] and
                    result.get('thread_id') == claim['thread'] and result.get('turn_id')):
                identifier(result['turn_id'])
                state = 'native_delivered'
            elif definitely_unaccepted and isinstance(result, dict) and result.get('state') in ('rejected', 'mail_ready_for_client'):
                state = 'pending'
            outcome = encode(result)
            db.execute('UPDATE event_attempts SET state=?,outcome=? WHERE key=? AND number=?',
                       (state, outcome, claim['key'], claim['number']))
            db.execute('UPDATE event_effects SET state=?,outcome=?,revision=revision+1 WHERE key=?',
                       (state, outcome, claim['key']))
            return state

    def recovery_batch(self, consumer, limit=32):
        """Fixed sweep horizon rotates past uncertainty even under new arrivals.

        Caller revalidates correlation/attempt before promotion and searches only
        the recorded thread/client ID, within its existing 20/200-turn ceiling.
        This function performs no Native/history reads.
        """
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ValueError('Recovery batch must be between 1 and 32')
        with self.store.transaction() as db:
            maximum = db.execute('SELECT coalesce(max(seq),0) FROM event_effects').fetchone()[0]
            scan = db.execute('SELECT * FROM event_recovery_scans WHERE consumer=?', (consumer,)).fetchone()
            after, horizon = (scan['after_seq'], scan['horizon']) if scan else (0, maximum)
            # Bound candidates BEFORE eligibility filters. Otherwise many barred
            # rows could turn LIMIT 32 into an unbounded scan for 32 matches.
            rows = db.execute("SELECT * FROM event_effects WHERE seq>? AND seq<=? AND state='uncertain' ORDER BY seq LIMIT ?",
                              (after, horizon, limit)).fetchall()
            selected = []
            for row in rows:
                if not row['seed_complete']:
                    continue
                attempt = db.execute('SELECT mode,state FROM event_attempts WHERE key=? AND number=?',
                                     (row['key'], row['attempt'])).fetchone()
                if not attempt or (attempt['mode'], attempt['state']) != ('native', 'uncertain'):
                    continue
                if db.execute('SELECT 1 FROM event_history_barriers WHERE peer IS NULL OR peer=? LIMIT 1',
                              (row['peer'],)).fetchone():
                    continue
                selected.append(dict(row))
            if len(rows) < limit:
                after, horizon = 0, maximum
            else:
                after = rows[-1]['seq']
            db.execute('INSERT INTO event_recovery_scans VALUES(?,?,?) ON CONFLICT(consumer) DO UPDATE SET after_seq=excluded.after_seq,horizon=excluded.horizon',
                       (consumer, after, horizon))
            return selected

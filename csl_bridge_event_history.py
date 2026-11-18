"""Inactive bounded history helpers; transport admission and Native remain separate.

Local preparation depends on the separately reviewed event-state contribution.
No production consumer imports or opens this module. Old and new effect writers
must never share a home during eventual activation.
"""
import hashlib

from csl_bridge_engine import confirm_event_turn, event_id, validate_event
from csl_bridge_event_state import EventConflict, EventDeferred, EventState, digest
from csl_bridge_store import BridgeError, decode, encode, identifier, name


def raw_hash(raw):
    return hashlib.sha256(raw if isinstance(raw, bytes) else raw.encode()).hexdigest()


def canonical_key(peer, task):
    return digest([peer, task, 'final'])


class EventHistory:
    def __init__(self, store):
        self.store = store
        self.effects = EventState(store)
        with store.transaction() as db:
            for statement in (
                '''CREATE TABLE IF NOT EXISTS event_catalog(
                    seq INTEGER PRIMARY KEY,wire_id TEXT NOT NULL,raw_hash TEXT NOT NULL,
                    peer TEXT,task TEXT,kind TEXT,key TEXT,delivery TEXT NOT NULL,
                    legacy INTEGER NOT NULL,conflict TEXT)''',
                'CREATE INDEX IF NOT EXISTS event_catalog_key ON event_catalog(key,seq)',
                'CREATE UNIQUE INDEX IF NOT EXISTS event_catalog_wire ON event_catalog(wire_id)',
                '''CREATE TABLE IF NOT EXISTS event_catalog_meta(
                    id INTEGER PRIMARY KEY CHECK(id=1),watermark INTEGER,version INTEGER NOT NULL)''',
                'INSERT OR IGNORE INTO event_catalog_meta VALUES(1,NULL,1)',
                '''CREATE TABLE IF NOT EXISTS event_seed_state(
                    key TEXT PRIMARY KEY,cursor INTEGER,signature TEXT,projection TEXT,
                    delivered INTEGER NOT NULL DEFAULT 0,uncertain INTEGER NOT NULL DEFAULT 0,
                    conflict INTEGER NOT NULL DEFAULT 0,representative INTEGER,thread_snapshot TEXT,
                    sealed INTEGER NOT NULL DEFAULT 0,resume_state TEXT,sender TEXT)''',
                '''CREATE TABLE IF NOT EXISTS event_legacy_attempts(
                    seq INTEGER PRIMARY KEY,key TEXT NOT NULL,wire_id TEXT NOT NULL,
                    raw_hash TEXT NOT NULL,thread TEXT)''',
                'CREATE INDEX IF NOT EXISTS event_legacy_key ON event_legacy_attempts(key,seq)',
                '''CREATE TABLE IF NOT EXISTS event_publications(
                    seq INTEGER PRIMARY KEY,wire_id TEXT NOT NULL,kind TEXT NOT NULL,
                    key TEXT,target TEXT NOT NULL)''',
                "CREATE UNIQUE INDEX IF NOT EXISTS event_final_representative ON event_publications(key) WHERE kind='final'",
                'CREATE INDEX IF NOT EXISTS event_publication_wait ON event_publications(target,seq)',
                '''CREATE TABLE IF NOT EXISTS event_history_conflicts(
                    seq INTEGER NOT NULL,wire_id TEXT NOT NULL,raw_hash TEXT NOT NULL,
                    reason TEXT NOT NULL,PRIMARY KEY(seq,wire_id,raw_hash,reason))''',
                '''CREATE TABLE IF NOT EXISTS event_original_routes(
                    peer TEXT NOT NULL,task TEXT NOT NULL,signature TEXT NOT NULL,route TEXT NOT NULL,receipt TEXT NOT NULL,
                    PRIMARY KEY(peer,task))''',
                '''CREATE TABLE IF NOT EXISTS event_admission_conflicts(
                    peer TEXT NOT NULL,task TEXT NOT NULL,wire_id TEXT NOT NULL,raw_hash TEXT NOT NULL,reason TEXT NOT NULL,
                    PRIMARY KEY(peer,wire_id,raw_hash,reason))''',
                '''CREATE TABLE IF NOT EXISTS event_final_conflicts(
                    task TEXT NOT NULL,wire_id TEXT NOT NULL,raw_hash TEXT NOT NULL,
                    reason TEXT NOT NULL,PRIMARY KEY(task,wire_id,raw_hash,reason))''',
            ):
                db.execute(statement)

    @staticmethod
    def complete(db):
        watermark = db.execute('SELECT watermark FROM event_catalog_meta WHERE id=1').fetchone()[0]
        tail = db.execute('SELECT max(seq) FROM inbound').fetchone()[0]
        return watermark == tail

    @staticmethod
    def note(db, row, reason):
        db.execute('INSERT OR IGNORE INTO event_history_conflicts VALUES(?,?,?,?)',
                   (row['seq'], row['id'], raw_hash(row['body']), reason))

    def index_row(self, db, row, *, legacy):
        """For future admission: call in its inbound/cursor write transaction.

        Snapshot old delivery before any effect. Repeated indexing cannot replace
        an old uncertain/delivered snapshot with today's less-conservative state.
        """
        fingerprint = raw_hash(row['body'])
        prior = db.execute('SELECT * FROM event_catalog WHERE seq=?', (row['seq'],)).fetchone()
        if prior:
            if (prior['wire_id'], prior['peer'], prior['raw_hash']) != (row['id'], row['peer'], fingerprint):
                self.note(db, row, 'Historical raw identity changed')
                old = db.execute('SELECT 1 FROM event_history_barriers WHERE seq=?', (row['seq'],)).fetchone()
                if old:
                    # Widening is conservative; original catalog/hash remain intact.
                    db.execute('UPDATE event_history_barriers SET peer=NULL WHERE seq=?', (row['seq'],))
                else:
                    self.effects.barrier(db, row['seq'], None, row['id'], row['body'], 'Historical raw identity changed')
            return
        peer, task, kind, key, conflict = row['peer'], None, None, None, None
        try:
            name(peer)
            event = decode(row['body']); validate_event(event)
            if event['id'] != identifier(row['id']):
                raise BridgeError('Wire/body ID differs')
            if row['delivery'] not in ('pending', 'uncertain', 'delivered', 'rejected'):
                raise BridgeError('Unknown historical delivery state')
            task, kind = event['task_id'], event['kind']
            key = canonical_key(peer, task) if kind == 'final' else None
            collision = db.execute('SELECT 1 FROM operations WHERE id=?', (row['id'],)).fetchone()
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='mail_messages'").fetchone():
                collision = collision or db.execute('SELECT 1 FROM mail_messages WHERE id=?', (row['id'],)).fetchone()
            reserved = db.execute('SELECT purpose,binding FROM event_native_ids WHERE id=?', (row['id'],)).fetchone()
            if reserved and (kind != 'progress' or reserved['purpose'] != 'progress' or reserved['binding'] != fingerprint):
                collision = True
            if collision:
                conflict = 'Historical cross-kind client ID collision'
        except (BridgeError, TypeError, AttributeError) as error:
            conflict = str(error)
            self.effects.barrier(db, row['seq'], peer, row['id'], row['body'], conflict)
        db.execute('INSERT INTO event_catalog VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (row['seq'], row['id'], fingerprint, peer, task, kind, key,
                    row['delivery'], int(legacy), conflict))
        if conflict:
            self.note(db, row, conflict)
        if key:
            effect = db.execute('SELECT state,seed_complete FROM event_effects WHERE key=?', (key,)).fetchone()
            if effect and effect['seed_complete']:
                db.execute('UPDATE event_seed_state SET resume_state=? WHERE key=?', (effect['state'], key))
                db.execute('UPDATE event_effects SET seed_complete=0,revision=revision+1 WHERE key=?', (key,))
        if kind == 'progress':
            event = decode(row['body'])
            db.execute('INSERT OR IGNORE INTO event_publications VALUES(?,?,?,?,?)',
                       (row['seq'], row['id'], kind, None, event['target']))

    def catalog_step(self, limit=128):
        if type(limit) is not int or not 1 <= limit <= 128:
            raise ValueError('Catalog step must contain 1..128 rows')
        with self.store.transaction() as db:
            watermark = db.execute('SELECT watermark FROM event_catalog_meta WHERE id=1').fetchone()[0]
            query = 'SELECT * FROM inbound' + (' WHERE seq>?' if watermark is not None else '') + ' ORDER BY seq LIMIT ?'
            args = (watermark, limit) if watermark is not None else (limit,)
            rows = db.execute(query, args).fetchall()
            for row in rows:
                self.index_row(db, row, legacy=True)
            if rows:
                db.execute('UPDATE event_catalog_meta SET watermark=? WHERE id=1', (rows[-1]['seq'],))
            return {'processed': len(rows), 'complete': self.complete(db)}

    @staticmethod
    def proof(db, peer, event):
        original = db.execute('SELECT * FROM operations WHERE id=? AND peer=?',
                              (event['task_id'], 'outbound:' + peer)).fetchone()
        if not original:
            return {'state': 'deferred', 'binding': None, 'thread': None, 'sender': None}
        try:
            request = decode(original['request'])
            if raw_hash(original['request']) != original['hash']:
                raise BridgeError('Original request fingerprint differs')
            receipt = decode(original['result']) if original['result'] else {}
            confirmed = confirm_event_turn(request, original['state'], receipt, event)
            thread = request.get('session')
            if thread:
                identifier(thread)
            sender = receipt.get('thread_id') if confirmed else None
            if sender:
                identifier(sender)
            return {'state': 'confirmed' if confirmed else 'deferred',
                    'binding': digest([peer, original['hash'], original['request']]),
                    'thread': thread, 'sender': sender}
        except (BridgeError, TypeError, AttributeError):
            return {'state': 'conflict', 'binding': None, 'thread': None, 'sender': None}

    @staticmethod
    def projection(peer, event, proof):
        header = event.get('session')
        if proof['sender']:
            if header and header != proof['sender']:
                raise EventConflict('Sender header differs from original Native receipt')
            header = proof['sender']
        # Optional headers merge only with independently established sender proof;
        # otherwise exact presence/value stays part of this conservative projection.
        return digest([peer, {k: v for k, v in event.items() if k not in ('id', 'session')},
                       header, proof['binding']])

    def seed_step(self, key, limit=64):
        if type(limit) is not int or not 1 <= limit <= 64:
            raise ValueError('Seed step must contain 1..64 aliases')
        with self.store.transaction() as db:
            if not self.complete(db):
                raise EventDeferred('Complete the bounded home catalog before a claim')
            first = db.execute('SELECT c.*,i.body FROM event_catalog c JOIN inbound i USING(seq) WHERE c.key=? ORDER BY c.seq LIMIT 1', (key,)).fetchone()
            if not first:
                raise EventConflict('Unknown canonical final key')
            if raw_hash(first['body']) != first['raw_hash']:
                self.effects.barrier(db, first['seq'], None, first['wire_id'], first['body'], 'Raw body changed during seed')
                return {'processed': 0, 'complete': False, 'state': 'conflict'}
            event = decode(first['body'])
            proof = self.proof(db, first['peer'], event)
            signature = digest(proof['binding'])
            state = db.execute('SELECT * FROM event_seed_state WHERE key=?', (key,)).fetchone()
            effect = db.execute('SELECT * FROM event_effects WHERE key=?', (key,)).fetchone()
            if state and state['sealed']:
                if state['signature'] != signature:
                    self.note(db, {'seq': first['seq'], 'id': first['wire_id'], 'body': first['body']}, 'Sealed original authority changed: ' + signature)
                    db.execute("UPDATE event_effects SET state='conflict',revision=revision+1 WHERE key=?", (key,))
                    return {'processed': 0, 'complete': True, 'state': 'conflict'}
                if effect['seed_complete']:
                    if effect['state'] == 'deferred' and proof['state'] == 'confirmed' and not (state['uncertain'] or state['delivered']):
                        try:
                            self.projection(first['peer'], event, proof)
                        except EventConflict:
                            self.note(db, {'seq': first['seq'], 'id': first['wire_id'], 'body': first['body']}, 'Recovered original contradicts sender header')
                            db.execute("UPDATE event_effects SET state='conflict',revision=revision+1 WHERE key=?", (key,))
                            return {'processed': 0, 'complete': True, 'state': 'conflict'}
                        updated = 'receipt_available' if not event['text'] else 'pending'
                        db.execute('UPDATE event_effects SET state=?,revision=revision+1 WHERE key=?', (updated, key))
                        return {'processed': 0, 'complete': True, 'state': updated}
                    return {'processed': 0, 'complete': True, 'state': effect['state']}
            if state is None or state['signature'] != signature:
                registered = db.execute('SELECT thread FROM agents WHERE name=?', (event['target'],)).fetchone()
                thread = proof['thread'] or (registered[0] if registered else None)
                db.execute('INSERT INTO event_seed_state(key,signature,thread_snapshot,sender)VALUES(?,?,?,?) ON CONFLICT(key)DO UPDATE SET cursor=NULL,signature=excluded.signature,projection=NULL,delivered=0,uncertain=0,conflict=0,representative=NULL,sender=excluded.sender',
                           (key, signature, thread, proof['sender']))
                db.execute("INSERT INTO event_effects(key,peer,task,projection,representative,agent,thread)VALUES(?,?,?,?,?,?,?) ON CONFLICT(key)DO UPDATE SET state='seeding',seed_complete=0,revision=revision+1",
                           (key, first['peer'], first['task'], '', first['seq'], event['target'], thread))
                state = db.execute('SELECT * FROM event_seed_state WHERE key=?', (key,)).fetchone()
            cursor = state['cursor']
            query = 'SELECT c.*,i.body FROM event_catalog c JOIN inbound i USING(seq) WHERE c.key=?' + (' AND c.seq>?' if cursor is not None else '') + ' ORDER BY c.seq LIMIT ?'
            args = (key, cursor, limit) if cursor is not None else (key, limit)
            rows = db.execute(query, args).fetchall()
            projection, delivered, uncertain, conflict = state['projection'], state['delivered'], state['uncertain'], state['conflict']
            representative = state['representative']
            resumed = state['resume_state']
            for row in rows:
                if raw_hash(row['body']) != row['raw_hash']:
                    self.effects.barrier(db, row['seq'], None, row['wire_id'], row['body'], 'Raw body changed during seed')
                    conflict = 1
                    cursor = row['seq']
                    continue
                alias = decode(row['body'])
                current = self.proof(db, row['peer'], alias)
                current = {**current, 'sender': state['sender']}
                if state['sealed']:
                    try:
                        proposal = self.projection(row['peer'], alias, current)
                    except EventConflict:
                        proposal = None
                    if row['legacy'] and row['delivery'] == 'uncertain':
                        db.execute('INSERT OR IGNORE INTO event_legacy_attempts VALUES(?,?,?,?,?)',
                                   (row['seq'], key, row['wire_id'], row['raw_hash'], current['thread']))
                    if row['legacy'] and resumed not in ('native_delivered', 'wait_delivered', 'legacy_suppressed'):
                        if row['delivery'] == 'delivered':
                            resumed = 'legacy_suppressed'
                        elif row['delivery'] == 'uncertain':
                            resumed = 'uncertain'
                    if proposal != state['projection'] or current['state'] == 'conflict' or row['conflict']:
                        if row['legacy'] and resumed not in ('native_delivered', 'wait_delivered', 'legacy_suppressed'):
                            resumed = 'conflict'
                        db.execute('UPDATE event_catalog SET conflict=? WHERE seq=?', ('Conflicting proposal after canonical seed', row['seq']))
                        self.note(db, {'seq': row['seq'], 'id': row['wire_id'], 'body': row['body']}, 'Conflicting proposal after canonical seed')
                    cursor = row['seq']
                    continue
                try:
                    candidate = self.projection(row['peer'], alias, current)
                except EventConflict:
                    candidate, conflict = None, 1
                if current['state'] == 'conflict' or row['conflict'] or row['delivery'] == 'rejected':
                    conflict = 1
                if projection is None:
                    projection = candidate
                elif candidate != projection:
                    conflict = 1
                representative = min(representative, row['seq']) if representative is not None else row['seq']
                delivered |= row['delivery'] == 'delivered'
                uncertain |= row['delivery'] == 'uncertain'
                if row['legacy'] and row['delivery'] == 'uncertain':
                    db.execute('INSERT OR IGNORE INTO event_legacy_attempts VALUES(?,?,?,?,?)',
                               (row['seq'], key, row['wire_id'], row['raw_hash'], current['thread']))
                cursor = row['seq']
            db.execute('UPDATE event_seed_state SET cursor=?,projection=?,delivered=?,uncertain=?,conflict=?,representative=? WHERE key=?',
                       (cursor, projection, delivered, uncertain, conflict, representative, key))
            if state['sealed']:
                db.execute('UPDATE event_seed_state SET resume_state=? WHERE key=?', (resumed, key))
            more = db.execute('SELECT 1 FROM event_catalog WHERE key=? AND seq>? LIMIT 1', (key, cursor)).fetchone()
            if more:
                return {'processed': len(rows), 'complete': False, 'state': 'seeding'}
            if state['sealed']:
                db.execute('UPDATE event_effects SET seed_complete=1,state=?,revision=revision+1 WHERE key=?', (resumed, key))
                return {'processed': len(rows), 'complete': True, 'state': resumed}
            outcome = ('conflict' if conflict else 'legacy_suppressed' if delivered else
                       'uncertain' if uncertain else 'deferred' if proof['state'] != 'confirmed' else
                       'receipt_available' if not event['text'] else 'pending')
            thread = proof['thread'] if uncertain or delivered else state['thread_snapshot']
            db.execute('UPDATE event_effects SET projection=?,representative=?,state=?,seed_complete=1,thread=?,revision=revision+1,outcome=? WHERE key=?',
                       (projection or '', representative, outcome, thread,
                        encode({'legacy_delivered_unproven': bool(delivered), 'legacy_uncertain': bool(uncertain)}), key))
            db.execute('UPDATE event_seed_state SET sealed=1,resume_state=? WHERE key=?', (outcome, key))
            db.execute("INSERT INTO event_publications VALUES(?,?,?,?,?) ON CONFLICT(seq)DO NOTHING",
                       (representative, first['wire_id'], 'final', key, event['target']))
            return {'processed': len(rows), 'complete': True, 'state': outcome}

    def guard(self, db, effect):
        """Pure DB correlation, called INSIDE the shared claim/promotion transaction."""
        row = db.execute('SELECT c.*,i.body FROM event_catalog c JOIN inbound i USING(seq) WHERE c.seq=?', (effect['representative'],)).fetchone()
        if not row or raw_hash(row['body']) != row['raw_hash']:
            raise EventConflict('Canonical representative raw identity changed')
        event = decode(row['body'])
        proof = self.proof(db, row['peer'], event)
        seeded = db.execute('SELECT * FROM event_seed_state WHERE key=?', (effect['key'],)).fetchone()
        if proof['state'] == 'conflict' or not seeded or seeded['signature'] != digest(proof['binding']):
            raise EventConflict('Canonical original identity changed')
        if proof['state'] != 'confirmed':
            raise EventDeferred('Original Native acceptance is unresolved')
        self.projection(row['peer'], event, proof)
        frozen = self.projection(row['peer'], event, {**proof, 'sender': seeded['sender']})
        if frozen != effect['projection'] or event['target'] != effect['agent']:
            raise EventConflict('Canonical payload or recipient changed')
        return event, proof

    @staticmethod
    def route_shape(route):
        if not isinstance(route, dict) or set(route) != {'physical_peer', 'origin', 'destination'}:
            raise EventConflict('Invalid verified event route')
        for value in route.values():
            name(value)
        return encode(route)

    def cached_route(self, peer, task):
        rows = self.store.rows('SELECT r.* FROM event_original_routes r JOIN operations o ON o.id=r.task AND o.peer=? WHERE r.peer=? AND r.task=?',
                               ('outbound:' + peer, peer, task))
        return decode(rows[0]['route']) if rows else None

    def recover_original(self, peer, event, route):
        """Reuse only earlier independent route/acceptance evidence, never event text."""
        encoded_route = self.route_shape(route)
        with self.store.transaction() as db:
            saved = db.execute('SELECT * FROM event_original_routes WHERE peer=? AND task=?', (peer, event['task_id'])).fetchone()
            original = db.execute('SELECT * FROM operations WHERE peer=? AND id=?', ('outbound:' + peer, event['task_id'])).fetchone()
            if not saved or not original or saved['route'] != encoded_route:
                raise EventDeferred('No exact cached original route')
            signature = digest([peer, original['hash'], original['request']])
            if saved['signature'] != signature or raw_hash(original['request']) != original['hash']:
                raise EventConflict('Cached original request binding differs')
            request = decode(original['request']); receipt = decode(original['result']) if original['result'] else {}
            try:
                if confirm_event_turn(request, original['state'], receipt, event):
                    return receipt
                saved_receipt = decode(saved['receipt'])
                accepted = confirm_event_turn(request, 'delivered', saved_receipt, event)
            except BridgeError as error:
                raise EventConflict(str(error)) from error
            if not accepted:
                raise EventDeferred('Cached original acceptance is unresolved')
            db.execute("UPDATE operations SET state='delivered',result=? WHERE id=?", (saved['receipt'], event['task_id']))
            return saved_receipt

    def admit(self, peer, event, *, cursor=None, route=None):
        """Persist an alias/cursor atomically; result is persistence, NOT Native acceptance."""
        name(peer)
        try:
            validate_event(event)
        except BridgeError as error:
            raise EventConflict(str(error)) from error
        if cursor is not None and (type(cursor) is not int or not 0 < cursor <= 2**63-1):
            raise EventConflict('Invalid event cursor')
        encoded_route = self.route_shape(route) if route is not None else None
        body = encode(event)
        with self.store.transaction() as db:
            proof = self.proof(db, peer, event)
            def refused(reason):
                db.execute('INSERT OR IGNORE INTO event_admission_conflicts VALUES(?,?,?,?,?)',
                           (peer, event['task_id'], event['id'], raw_hash(body), reason))
                if cursor is not None:
                    db.execute('INSERT INTO cursors VALUES(?,?) ON CONFLICT(peer)DO UPDATE SET value=max(value,excluded.value)', (peer, cursor))
                return {'state': 'rejected', 'event_id': event['id'], 'reason': reason}
            if proof['state'] == 'conflict':
                return refused('Contradictory original identity')
            if proof['state'] != 'confirmed':
                raise EventDeferred('Waiting for original Native acceptance')
            try:
                self.projection(peer, event, proof)
            except EventConflict as error:
                return refused(str(error))
            prior = db.execute('SELECT * FROM inbound WHERE id=?', (event['id'],)).fetchone()
            if prior and (prior['peer'], prior['body']) != (peer, body):
                return refused('Wire ID body or peer collision')
            if not prior:
                collision = db.execute('SELECT 1 FROM operations WHERE id=?', (event['id'],)).fetchone()
                collision = collision or db.execute('SELECT 1 FROM event_native_ids WHERE id=?', (event['id'],)).fetchone()
                if db.execute("SELECT 1 FROM sqlite_master WHERE name='mail_messages'").fetchone():
                    collision = collision or db.execute('SELECT 1 FROM mail_messages WHERE id=?', (event['id'],)).fetchone()
                if collision:
                    return refused('Cross-purpose wire ID collision')
            if encoded_route is not None:
                original = db.execute('SELECT * FROM operations WHERE id=? AND peer=?', (event['task_id'], 'outbound:' + peer)).fetchone()
                signature = digest([peer, original['hash'], original['request']])
                saved = db.execute('SELECT * FROM event_original_routes WHERE peer=? AND task=?', (peer, event['task_id'])).fetchone()
                if saved and (saved['signature'], saved['route']) != (signature, encoded_route):
                    return refused('Original authenticated route changed')
                db.execute('INSERT OR IGNORE INTO event_original_routes VALUES(?,?,?,?,?)',
                           (peer, event['task_id'], signature, encoded_route, original['result']))
            if event['kind'] == 'final':
                key = canonical_key(peer, event['task_id'])
                seeded = db.execute('SELECT * FROM event_seed_state WHERE key=? AND sealed=1', (key,)).fetchone()
                if seeded:
                    candidate = self.projection(peer, event, {**proof, 'sender': seeded['sender']})
                    if seeded['signature'] != digest(proof['binding']) or candidate != seeded['projection']:
                        return refused('Conflicting final proposal')
            was_complete = self.complete(db)
            created = prior is None
            if not prior:
                db.execute('INSERT INTO inbound(id,peer,body)VALUES(?,?,?)', (event['id'], peer, body))
                prior = db.execute('SELECT * FROM inbound WHERE id=?', (event['id'],)).fetchone()
            self.index_row(db, prior, legacy=not created)
            if was_complete:
                db.execute('UPDATE event_catalog_meta SET watermark=? WHERE id=1', (db.execute('SELECT max(seq) FROM inbound').fetchone()[0],))
            if cursor is not None:
                db.execute('INSERT INTO cursors VALUES(?,?) ON CONFLICT(peer)DO UPDATE SET value=max(value,excluded.value)', (peer, cursor))
            return {'state': 'persisted', 'event_id': event['id'], 'seq': prior['seq']}

    def next_publication(self, agent, after):
        """Read only. Consumer must still win/recheck the shared effect claim."""
        with self.store.transaction() as db:
            if not self.complete(db):
                raise EventDeferred('History catalog has an unresolved tail')
            row = db.execute('SELECT p.*,i.body FROM event_publications p JOIN inbound i USING(seq) WHERE p.target=? AND p.seq>? ORDER BY p.seq LIMIT 1', (agent, after)).fetchone()
            if row is None:
                return None
            if db.execute('SELECT 1 FROM event_history_barriers WHERE seq<=? LIMIT 1', (row['seq'],)).fetchone():
                raise EventDeferred('Unknown earlier recipient holds the wait cursor')
            entry = db.execute('SELECT * FROM event_catalog WHERE seq=?', (row['seq'],)).fetchone()
            if raw_hash(row['body']) != entry['raw_hash']:
                raise EventConflict('Publication body changed')
            event = decode(row['body'])
            proof = self.proof(db, entry['peer'], event)
            if proof['state'] == 'conflict' or entry['conflict']:
                raise EventConflict('Publication has contradictory original proof')
            if proof['state'] != 'confirmed':
                raise EventDeferred('Original Native acceptance is unresolved')
            self.projection(entry['peer'], event, proof)
            result = dict(row)
            if row['kind'] == 'final':
                seeded = db.execute('SELECT signature FROM event_seed_state WHERE key=?', (row['key'],)).fetchone()
                if not seeded or seeded['signature'] != digest(proof['binding']):
                    raise EventConflict('Publication original authority changed')
                effect = db.execute('SELECT * FROM event_effects WHERE key=?', (row['key'],)).fetchone()
                if not effect or not effect['seed_complete']:
                    raise EventDeferred('Canonical final seed is incomplete')
                self.effects.checked(db, row['key'], effect['revision'])
                if effect['state'] == 'uncertain':
                    raise EventDeferred('Uncertain final attempt holds this cursor')
                result['revision'] = effect['revision']
            return result

    def publish_final(self, task_id, thread, status, text):
        """Freeze first exact final and finalize task in one bridge transaction."""
        from csl_bridge_store import contains_findings
        if contains_findings(text):
            text = 'Result text withheld by partial secret detection. Review selected evidence locally; completion alone is not validation.'
        with self.store.transaction() as db:
            task = db.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone()
            if not task or not task['turn']:
                raise EventDeferred('No independently accepted task turn')
            wire_id = event_id(task_id + '/' + task['turn'] + '/final')
            event = {'id': wire_id, 'task_id': task_id, 'source': task['target'], 'target': task['source'],
                     'kind': 'final', 'text': text, 'turn_id': task['turn'], 'status': status, 'session': thread}
            validate_event(event)
            original = db.execute('SELECT * FROM operations WHERE id=? AND peer=?', (task_id, task['peer'])).fetchone()
            if not original:
                raise EventDeferred('Original task receipt is missing')
            request = decode(original['request']); receipt = decode(original['result']) if original['result'] else {}
            if raw_hash(original['request']) != original['hash']:
                raise EventConflict('Original task fingerprint differs')
            try:
                confirmed = confirm_event_turn(request, original['state'], receipt, event)
            except BridgeError as error:
                raise EventConflict(str(error)) from error
            if not confirmed:
                raise EventDeferred('Original task Native receipt is unconfirmed')
            registered = db.execute('SELECT thread FROM agents WHERE name=?', (task['target'],)).fetchone()
            if not registered or registered[0] != thread or receipt.get('thread_id', thread) != thread:
                raise EventConflict('Native completion thread binding differs')
            body = encode(event)
            old = db.execute('SELECT peer,body FROM outbound WHERE id=?', (wire_id,)).fetchone()
            if old and (old['peer'], old['body']) != (task['peer'], body):
                db.execute('INSERT OR IGNORE INTO event_final_conflicts VALUES(?,?,?,?)',
                           (task_id, wire_id, raw_hash(body), 'Final narrative/status changed'))
                return {'state': 'conflict', 'event_id': wire_id}
            if not old and task['status'] not in ('running', 'pending'):
                raise EventDeferred('Terminal legacy task has no durable final publication')
            db.execute('INSERT OR IGNORE INTO outbound(id,peer,body)VALUES(?,?,?)', (wire_id, task['peer'], body))
            db.execute('UPDATE tasks SET status=? WHERE id=?', (status, task_id))
            return {'state': 'published', 'event_id': wire_id}

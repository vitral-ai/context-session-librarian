"""Shared event delivery: commit one claim, invoke outside SQLite, reconcile only."""
from csl_bridge_event_history import EventHistory, canonical_key
from csl_bridge_event_state import EventConflict, EventDeferred, SETTLED, digest
from csl_bridge_store import decode, encode, identifier, name
from csl_bridge_rpc import RuntimeRejected, RuntimeUnavailable


class EventDelivery:
    def __init__(self, store):
        self.store = store
        self.history = EventHistory(store)
        self.effects = self.history.effects
        with store.transaction() as db:
            db.execute('CREATE TABLE IF NOT EXISTS event_delivery_jobs(key TEXT PRIMARY KEY,seq INTEGER NOT NULL,wire_id TEXT NOT NULL,peer TEXT NOT NULL,kind TEXT NOT NULL,dirty INTEGER NOT NULL DEFAULT 1)')
            db.execute('CREATE INDEX IF NOT EXISTS event_delivery_work ON event_delivery_jobs(dirty,seq)')
            db.execute('CREATE TABLE IF NOT EXISTS event_delivery_meta(id INTEGER PRIMARY KEY CHECK(id=1),watermark INTEGER,after_seq INTEGER NOT NULL DEFAULT 0,horizon INTEGER NOT NULL DEFAULT 0)')
            db.execute('INSERT OR IGNORE INTO event_delivery_meta(id)VALUES(1)')
            db.execute('CREATE TABLE IF NOT EXISTS event_seen_wires(peer TEXT NOT NULL,wire_id TEXT NOT NULL,raw_hash TEXT NOT NULL,PRIMARY KEY(peer,wire_id))')
            db.execute('CREATE TABLE IF NOT EXISTS event_progress_effects(wire_id TEXT PRIMARY KEY,thread TEXT,number INTEGER NOT NULL DEFAULT 0,state TEXT NOT NULL,outcome TEXT)')

    @staticmethod
    def job_key(row):
        return row['key'] if row['kind'] == 'final' else 'progress:' + row['wire_id']

    def book(self, db, row, *, wake=False):
        if row['kind'] not in ('progress', 'final'):
            return
        key = self.job_key(row)
        db.execute('INSERT INTO event_delivery_jobs(key,seq,wire_id,peer,kind)VALUES(?,?,?,?,?) ON CONFLICT(key)DO UPDATE SET seq=min(seq,excluded.seq),wire_id=CASE WHEN excluded.seq<seq THEN excluded.wire_id ELSE wire_id END',
                   (key, row['seq'], row['wire_id'], row['peer'], row['kind']))
        if wake:
            db.execute('UPDATE event_delivery_jobs SET dirty=1 WHERE key=?', (key,))

    def housekeeping(self, limit=32):
        """Only bounded local catalog/job work; returns candidates, performs no I/O."""
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ValueError('Event work batch must contain 1..32 candidates')
        self.history.catalog_step()
        with self.store.transaction() as db:
            metadata = db.execute('SELECT * FROM event_delivery_meta WHERE id=1').fetchone()
            query = 'SELECT * FROM event_catalog' + (' WHERE seq>?' if metadata['watermark'] is not None else '') + ' ORDER BY seq LIMIT 128'
            args = (metadata['watermark'],) if metadata['watermark'] is not None else ()
            rows = db.execute(query, args).fetchall()
            for row in rows:
                self.book(db, row)
                if row['kind'] == 'final':
                    effect = db.execute('SELECT seed_complete,state FROM event_effects WHERE key=?', (row['key'],)).fetchone()
                    if effect and (not effect['seed_complete'] or effect['state'] not in (*SETTLED, 'receipt_available', 'conflict', 'rejected')):
                        db.execute('UPDATE event_delivery_jobs SET dirty=1 WHERE key=?', (row['key'],))
            if rows:
                db.execute('UPDATE event_delivery_meta SET watermark=? WHERE id=1', (rows[-1]['seq'],))
            maximum = db.execute('SELECT coalesce(max(seq),0) FROM event_delivery_jobs').fetchone()[0]
            after, horizon = metadata['after_seq'], metadata['horizon'] or maximum
            candidates = db.execute('SELECT * FROM event_delivery_jobs WHERE dirty=1 AND seq>? AND seq<=? ORDER BY seq LIMIT ?', (after, horizon, limit)).fetchall()
            if len(candidates) < limit:
                after, horizon = 0, maximum
            else:
                after = candidates[-1]['seq']
            db.execute('UPDATE event_delivery_meta SET after_seq=?,horizon=? WHERE id=1', (after, horizon))
            return [dict(row) for row in candidates]

    def accept(self, peer, event, *, cursor=None, route=None):
        from csl_bridge_engine import validate_event
        from csl_bridge_store import BridgeError
        from csl_bridge_event_history import raw_hash
        try:
            validate_event(event)
        except BridgeError as error:
            raise EventConflict(str(error)) from error
        name(peer)
        if cursor is not None and (type(cursor) is not int or not 0 < cursor <= 2**63-1):
            raise EventConflict('Invalid event cursor')
        if route is not None:
            self.history.route_shape(route)
        fingerprint = raw_hash(encode(event))
        collision = False
        with self.store.transaction() as db:
            terminal = db.execute('SELECT 1 FROM event_admission_conflicts WHERE peer=? AND wire_id=? LIMIT 1', (peer, event['id'])).fetchone()
            if not terminal:
                known = db.execute('SELECT raw_hash FROM event_seen_wires WHERE peer=? AND wire_id=?', (peer, event['id'])).fetchone()
                if known and known[0] != fingerprint:
                    collision = True
                    db.execute('INSERT OR IGNORE INTO event_admission_conflicts VALUES(?,?,?,?,?)',
                               (peer, event['task_id'], event['id'], fingerprint, 'Deferred wire ID body changed'))
                    if cursor is not None:
                        db.execute('INSERT INTO cursors VALUES(?,?) ON CONFLICT(peer)DO UPDATE SET value=max(value,excluded.value)', (peer, cursor))
                elif not known:
                    proof = self.history.proof(db, peer, event)
                    if proof['binding'] is not None:
                        db.execute('INSERT OR IGNORE INTO event_seen_wires VALUES(?,?,?)', (peer, event['id'], fingerprint))
        if collision:
            raise EventConflict('Deferred wire ID body changed')
        result = self.history.admit(peer, event, cursor=cursor, route=route, include_created=True)
        if result['state'] == 'rejected':
            raise EventConflict(result['reason'])
        with self.store.transaction() as db:
            row = db.execute('SELECT * FROM event_catalog WHERE seq=?', (result['seq'],)).fetchone()
            self.book(db, row, wake=True)
        return result

    @staticmethod
    def response(effect, wire_id):
        state = effect['state']
        return {'state': 'delivered' if state in ('native_delivered', 'wait_delivered') else 'persisted',
                'delivery': state, 'native_accepted': state == 'native_delivered', 'event_id': wire_id}

    def receipt(self, peer, wire_id):
        """Current per-home event outcome; read-only, never drives Native recovery."""
        name(peer); identifier(wire_id)
        with self.store.transaction() as db:
            row = db.execute('SELECT c.*,i.body FROM event_catalog c JOIN inbound i USING(seq) WHERE c.peer=? AND c.wire_id=?', (peer, wire_id)).fetchone()
            if not row:
                refused = db.execute("SELECT reason FROM event_wire_decisions WHERE peer=? AND wire_id=? AND state='rejected'", (peer, wire_id)).fetchone()
                return {'state': 'rejected', 'result': {'state': 'rejected', 'event_id': wire_id,
                        'native_accepted': False, 'reason': refused[0]}} if refused else None
            from csl_bridge_event_history import raw_hash
            if raw_hash(row['body']) != row['raw_hash']:
                raise EventConflict('Event receipt raw identity changed')
            if row['kind'] == 'final':
                effect = db.execute('SELECT * FROM event_effects WHERE key=?', (row['key'],)).fetchone()
                seed = db.execute('SELECT cursor FROM event_seed_state WHERE key=?', (row['key'],)).fetchone()
                validated = seed and seed[0] is not None and seed[0] >= row['seq']
            else:
                effect = db.execute('SELECT * FROM event_progress_effects WHERE wire_id=?', (wire_id,)).fetchone()
                validated = effect is not None
            state = ('conflict' if row['conflict'] else effect['state'] if effect and (validated or effect['state'] in ('conflict', 'rejected')) else 'seeding')
            result = self.response({'state': state}, wire_id)
            if state in ('conflict', 'rejected'):
                result['state'] = 'rejected'
            if state == 'native_delivered' and effect['outcome']:
                result['native_receipt'] = decode(effect['outcome'])
            return {'state': result['state'], 'result': result}

    def final(self, peer, task, wire_id, *, available, send, find):
        key = canonical_key(peer, task)
        self.history.catalog_step()
        self.history.seed_step(key)
        with self.store.transaction() as db:
            effect = db.execute('SELECT * FROM event_effects WHERE key=?', (key,)).fetchone()
            if effect is None:
                raise EventDeferred('Known original is unavailable before initial seed')
            self.effects.checked(db, key, effect['revision'])
            event, proof = self.history.guard(db, effect)
            if effect['state'] in (*SETTLED, 'receipt_available'):
                db.execute('UPDATE event_delivery_jobs SET dirty=0 WHERE key=?', (key,))
                return self.response(effect, wire_id)
            saved = dict(effect)
        if saved['state'] == 'uncertain':
            self.recover_final(saved, find)
            effect = self.store.rows('SELECT * FROM event_effects WHERE key=?', (key,))[0]
            return self.response(effect, wire_id)
        if saved['state'] != 'pending':
            raise EventDeferred('Canonical final is not claimable')
        if not available(event, saved['thread']):
            return {**self.response(saved, wire_id), 'availability': 'client_or_turn_unavailable'}
        try:
            with self.store.transaction() as db:
                claim = self.effects.claim_native(key, saved['revision'], transaction=db, guard=self.history.guard)
                if claim is None:
                    effect = db.execute('SELECT * FROM event_effects WHERE key=?', (key,)).fetchone()
                    return self.response(effect, wire_id)
                db.execute("UPDATE inbound SET delivery='uncertain' WHERE seq=?", (saved['representative'],))
        except EventDeferred:
            current = self.store.rows('SELECT * FROM event_effects WHERE key=?', (key,))[0]
            if current['state'] == 'conflict':
                raise EventConflict('Canonical identity changed during claim')
            return self.response(current, wire_id)
        # Claim is durable. Never call this send path for an uncertain retry.
        definite = False
        try:
            result = send(claim, event)
            definite = isinstance(result, dict) and result.get('state') == 'mail_ready_for_client'
        except RuntimeRejected:
            result, definite = {'state': 'rejected', 'request_id': claim['client_id']}, True
        except (RuntimeUnavailable, OSError, TimeoutError):
            result = {'state': 'uncertain', 'request_id': claim['client_id']}
        with self.store.transaction() as db:
            state = self.effects.finish_native(claim, result, definitely_unaccepted=definite,
                                              transaction=db, guard=self.history.guard)
            raw = 'delivered' if state == 'native_delivered' else 'pending' if state == 'pending' else 'uncertain'
            db.execute('UPDATE inbound SET delivery=? WHERE seq=?', (raw, saved['representative']))
            if state == 'native_delivered':
                db.execute('UPDATE event_delivery_jobs SET dirty=0 WHERE key=?', (key,))
            effect = db.execute('SELECT * FROM event_effects WHERE key=?', (key,)).fetchone()
            return self.response(effect, wire_id)

    def recover_final(self, effect, find):
        """No new effect; lookup returns only exact, unique typed user-message IDs."""
        if effect['attempt'] and effect['client_id'] and effect['thread']:
            matches = find(effect['thread'], [effect['client_id']])
            turn = matches.get(effect['client_id'])
            if not turn:
                return
            claim = {'key': effect['key'], 'number': effect['attempt'], 'client_id': effect['client_id'],
                     'thread': effect['thread'], 'revision': effect['revision']}
            result = {'state': 'delivered', 'request_id': effect['client_id'], 'thread_id': effect['thread'], 'turn_id': turn}
            with self.store.transaction() as db:
                self.effects.finish_native(claim, result, transaction=db, guard=self.history.guard)
                db.execute("UPDATE inbound SET delivery='delivered' WHERE seq=?", (effect['representative'],))
                db.execute('UPDATE event_delivery_jobs SET dirty=0 WHERE key=?', (effect['key'],))
            return
        # Unknown historical thread never becomes today's registered thread.
        with self.store.transaction() as db:
            metadata_key = 'legacy:' + effect['key']
            scan = db.execute('SELECT * FROM event_recovery_scans WHERE consumer=?', (metadata_key,)).fetchone()
            maximum = db.execute('SELECT coalesce(max(seq),0) FROM event_legacy_attempts WHERE key=?', (effect['key'],)).fetchone()[0]
            after, horizon = (scan['after_seq'], scan['horizon']) if scan else (0, maximum)
            rows = db.execute('SELECT * FROM event_legacy_attempts WHERE key=? AND seq>? AND seq<=? ORDER BY seq LIMIT 32', (effect['key'], after, horizon)).fetchall()
            next_after, next_horizon = (rows[-1]['seq'], horizon) if len(rows) == 32 else (0, maximum)
            db.execute('INSERT INTO event_recovery_scans VALUES(?,?,?) ON CONFLICT(consumer)DO UPDATE SET after_seq=excluded.after_seq,horizon=excluded.horizon', (metadata_key, next_after, next_horizon))
        by_thread = {}
        for row in rows:
            if row['thread']:
                by_thread.setdefault(row['thread'], []).append(dict(row))
        proved = []
        for thread, aliases in by_thread.items():
            matches = find(thread, [row['wire_id'] for row in aliases])
            proved.extend((row, matches[row['wire_id']]) for row in aliases if row['wire_id'] in matches)
        if not proved:
            return
        with self.store.transaction() as db:
            current = self.effects.checked(db, effect['key'], effect['revision'])
            _, proof = self.history.guard(db, current)
            if current['state'] != 'uncertain':
                return
            seeded = db.execute('SELECT sender FROM event_seed_state WHERE key=?', (effect['key'],)).fetchone()
            from csl_bridge_event_history import raw_hash
            for observed, accepted in proved:
                recorded = db.execute('SELECT a.*,c.peer,c.key AS catalog_key,c.kind,c.raw_hash AS catalog_hash,i.body FROM event_legacy_attempts a JOIN event_catalog c USING(seq) JOIN inbound i USING(seq) WHERE a.seq=?', (observed['seq'],)).fetchone()
                if (not recorded or any(recorded[field] != observed[field] for field in ('key', 'wire_id', 'raw_hash', 'thread')) or
                        recorded['catalog_key'] != current['key'] or recorded['kind'] != 'final' or
                        recorded['catalog_hash'] != observed['raw_hash'] or raw_hash(recorded['body']) != observed['raw_hash']):
                    raise EventConflict('Historical matched alias identity changed during lookup')
                alias = decode(recorded['body'])
                projection = self.history.projection(recorded['peer'], alias, {**proof, 'sender': seeded['sender']})
                if projection != current['projection']:
                    raise EventConflict('Historical matched alias differs from canonical final')
                identifier(accepted)
            row, turn = min(proved, key=lambda item: item[0]['seq'])
            # Legacy acceptance is independently observed, not a new claim. All
            # legacy wire IDs stay in their table; no historical exactly-once claim.
            result = {'state': 'delivered', 'request_id': row['wire_id'], 'thread_id': row['thread'],
                      'turn_id': turn, 'legacy': True, 'historical_duplicate_possible': True,
                      'observed_alias_acceptances': [{'wire_id': old['wire_id'], 'thread_id': old['thread'], 'turn_id': accepted} for old, accepted in proved]}
            db.execute("UPDATE event_effects SET state='native_delivered',outcome=?,revision=revision+1 WHERE key=?", (encode(result), effect['key']))
            db.execute("UPDATE inbound SET delivery='delivered' WHERE seq=?", (effect['representative'],))
            db.execute('UPDATE event_delivery_jobs SET dirty=0 WHERE key=?', (effect['key'],))

    def progress_guard(self, db, wire_id):
        if not self.history.complete(db):
            raise EventDeferred('History catalog has an unresolved tail')
        row = db.execute('SELECT c.*,i.body,i.delivery AS current_delivery FROM event_catalog c JOIN inbound i USING(seq) WHERE c.wire_id=?', (wire_id,)).fetchone()
        if not row or row['kind'] != 'progress' or row['conflict']:
            raise EventConflict('Unknown or conflicted progress identity')
        from csl_bridge_event_history import raw_hash
        if raw_hash(row['body']) != row['raw_hash']:
            raise EventConflict('Progress body changed')
        if db.execute('SELECT 1 FROM event_history_barriers WHERE peer IS NULL OR peer=? LIMIT 1', (row['peer'],)).fetchone():
            raise EventDeferred('Unresolved historical progress scope')
        event = decode(row['body'])
        key = 'progress:' + wire_id
        effect = db.execute('SELECT state FROM event_progress_effects WHERE wire_id=?', (wire_id,)).fetchone()
        saved = db.execute('SELECT proof FROM event_accepted_originals WHERE key=?', (key,)).fetchone()
        retained = decode(saved[0]) if saved else None
        proof = retained if effect and effect['state'] in SETTLED and retained else self.history.proof(db, row['peer'], event)
        if proof['state'] == 'conflict':
            raise EventConflict('Progress original identity differs')
        if proof['state'] != 'confirmed':
            raise EventDeferred('Original Native acceptance is unresolved')
        known = db.execute('SELECT signature FROM event_original_bindings WHERE key=?', (key,)).fetchone()
        if known and known[0] != digest(proof['binding']):
            raise EventConflict('Progress original authority changed')
        if retained and retained['sender'] and proof['sender'] and retained['sender'] != proof['sender']:
            raise EventConflict('Known progress sender binding changed')
        self.history.projection(row['peer'], event, proof)
        return row, event, proof

    @staticmethod
    def remember_progress(db, row, proof):
        key = 'progress:' + row['wire_id']
        db.execute('INSERT OR IGNORE INTO event_original_bindings VALUES(?,?)', (key, digest(proof['binding'])))
        db.execute('INSERT OR IGNORE INTO event_accepted_originals VALUES(?,?)', (key, encode(proof)))

    def progress(self, wire_id, *, available, send, find):
        self.history.catalog_step()
        with self.store.transaction() as db:
            row, event, proof = self.progress_guard(db, wire_id)
            self.remember_progress(db, row, proof)
            effect = db.execute('SELECT * FROM event_progress_effects WHERE wire_id=?', (wire_id,)).fetchone()
            if not effect:
                registered = db.execute('SELECT thread FROM agents WHERE name=?', (event['target'],)).fetchone()
                attempted = db.execute('SELECT thread FROM event_legacy_attempts WHERE key=? AND wire_id=?', ('progress:' + wire_id, wire_id)).fetchone()
                thread = (attempted[0] if attempted else None) if row['current_delivery'] == 'uncertain' else (proof['thread'] or (registered[0] if registered else None))
                state = ('legacy_suppressed' if row['current_delivery'] == 'delivered' else
                         'conflict' if row['current_delivery'] == 'rejected' else
                         'uncertain' if row['current_delivery'] == 'uncertain' else
                         'receipt_available' if not event['text'] else 'pending')
                db.execute('INSERT INTO event_progress_effects(wire_id,thread,state)VALUES(?,?,?)', (wire_id, thread, state))
                effect = db.execute('SELECT * FROM event_progress_effects WHERE wire_id=?', (wire_id,)).fetchone()
            if effect['state'] in (*SETTLED, 'receipt_available'):
                db.execute('UPDATE event_delivery_jobs SET dirty=0 WHERE key=?', ('progress:' + wire_id,))
                return self.response(effect, wire_id)
            if effect['state'] == 'conflict':
                raise EventConflict('Progress has a prior rejected identity')
            saved = dict(effect)
        if saved['state'] == 'uncertain':
            if saved['thread']:
                matches = find(saved['thread'], [wire_id])
                if matches.get(wire_id):
                    result = {'state': 'delivered', 'request_id': wire_id, 'thread_id': saved['thread'], 'turn_id': matches[wire_id]}
                    with self.store.transaction() as db:
                        self.finish_progress(db, saved, result)
            effect = self.store.rows('SELECT * FROM event_progress_effects WHERE wire_id=?', (wire_id,))[0]
            return self.response(effect, wire_id)
        if not available(event, saved['thread']):
            return {**self.response(saved, wire_id), 'availability': 'client_or_turn_unavailable'}
        with self.store.transaction() as db:
            row, event, proof = self.progress_guard(db, wire_id)
            current = db.execute('SELECT * FROM event_progress_effects WHERE wire_id=?', (wire_id,)).fetchone()
            if current['state'] != 'pending' or row['current_delivery'] != 'pending':
                raise EventDeferred('Another progress claimant won')
            registered = db.execute('SELECT thread FROM agents WHERE name=?', (event['target'],)).fetchone()
            if not registered or registered[0] != saved['thread']:
                raise EventDeferred('Progress recipient registration changed')
            self.effects.reserve_id(db, wire_id, 'progress', row['raw_hash'], saved['thread'])
            number = current['number'] + 1
            db.execute("UPDATE event_progress_effects SET state='uncertain',number=? WHERE wire_id=?", (number, wire_id))
            db.execute("UPDATE inbound SET delivery='uncertain' WHERE id=?", (wire_id,))
            db.execute('INSERT INTO event_attempts VALUES(?,?,?,?,?,?,NULL)', ('progress:' + wire_id, number, 'native', saved['thread'], wire_id, 'uncertain'))
            claim = {'wire_id': wire_id, 'client_id': wire_id, 'thread': saved['thread'], 'number': number}
        definite = False
        try:
            result = send(claim, event)
            definite = isinstance(result, dict) and result.get('state') == 'mail_ready_for_client'
        except RuntimeRejected:
            result, definite = {'state': 'rejected', 'request_id': wire_id}, True
        except (RuntimeUnavailable, OSError, TimeoutError):
            result = {'state': 'uncertain', 'request_id': wire_id}
        with self.store.transaction() as db:
            self.finish_progress(db, claim, result, definite=definite)
            effect = db.execute('SELECT * FROM event_progress_effects WHERE wire_id=?', (wire_id,)).fetchone()
            return self.response(effect, wire_id)

    def finish_progress(self, db, claim, result, *, definite=False):
        wire_id = claim['wire_id']
        row, _, _ = self.progress_guard(db, wire_id)
        effect = db.execute('SELECT * FROM event_progress_effects WHERE wire_id=?', (wire_id,)).fetchone()
        if (effect['state'] != 'uncertain' or effect['thread'] != claim['thread'] or effect['number'] != claim['number']):
            raise EventDeferred('Progress attempt changed before settlement')
        state = 'uncertain'
        if (isinstance(result, dict) and result.get('state') == 'delivered' and
                result.get('request_id') == wire_id and result.get('thread_id') == claim['thread'] and result.get('turn_id')):
            identifier(result['turn_id'])
            state = 'native_delivered'
        elif definite and isinstance(result, dict) and result.get('state') in ('rejected', 'mail_ready_for_client'):
            state = 'pending'
        db.execute('UPDATE event_progress_effects SET state=?,outcome=? WHERE wire_id=?', (state, encode(result), wire_id))
        db.execute('UPDATE event_attempts SET state=?,outcome=? WHERE key=? AND number=?', (state, encode(result), 'progress:' + wire_id, claim['number']))
        raw = 'delivered' if state == 'native_delivered' else 'pending' if state == 'pending' else 'uncertain'
        db.execute('UPDATE inbound SET delivery=? WHERE id=?', (raw, wire_id))
        if state == 'native_delivered':
            db.execute('UPDATE event_delivery_jobs SET dirty=0 WHERE key=?', ('progress:' + wire_id,))

    def observe(self, agent, after):
        jobs = self.housekeeping()
        if self.history.complete(self.store.db):
            for job in jobs:
                if job['kind'] == 'final':
                    self.history.seed_step(job['key'])
        row = None
        for _ in range(32):
            try:
                row = self.history.next_publication(agent, after, progress_guard=self.progress_guard)
                break
            except EventConflict as error:
                if not hasattr(error, 'event_seq'):
                    raise
                with self.store.transaction() as db:
                    # A terminal seed may precede any publication. Persist its
                    # invisible representative as well as the raw refusal.
                    db.execute('INSERT INTO event_publications(seq,wire_id,kind,key,target,visible) SELECT c.seq,c.wire_id,c.kind,c.key,?,0 FROM event_catalog c WHERE c.seq=? ON CONFLICT(seq)DO UPDATE SET visible=0', (agent, error.event_seq))
                    db.execute("UPDATE inbound SET delivery='rejected' WHERE seq=?", (error.event_seq,))
        if row is None:
            return None
        if row['kind'] == 'progress':
            with self.store.transaction() as db:
                progress, event, proof = self.progress_guard(db, row['wire_id'])
                self.remember_progress(db, progress, proof)
                effect = db.execute('SELECT * FROM event_progress_effects WHERE wire_id=?', (row['wire_id'],)).fetchone()
                if progress['current_delivery'] == 'uncertain' or (effect and effect['state'] == 'uncertain'):
                    raise EventDeferred('An uncertain progress attempt holds this cursor')
                if progress['current_delivery'] == 'rejected':
                    raise EventConflict('Rejected progress holds this cursor')
                if progress['current_delivery'] == 'pending':
                    db.execute("INSERT INTO event_progress_effects(wire_id,thread,state)VALUES(?,?,'wait_delivered') ON CONFLICT(wire_id)DO UPDATE SET state='wait_delivered'", (row['wire_id'], proof['thread']))
                    db.execute("UPDATE inbound SET delivery='delivered' WHERE id=?", (row['wire_id'],))
                db.execute('UPDATE event_delivery_jobs SET dirty=0 WHERE key=?', ('progress:' + row['wire_id'],))
            return {'cursor': row['seq'], 'event': event}
        with self.store.transaction() as db:
            self.effects.observe_wait(row['key'], row['revision'], transaction=db, guard=self.history.guard)
            db.execute("UPDATE inbound SET delivery='delivered' WHERE seq=?", (row['seq'],))
            db.execute('UPDATE event_delivery_jobs SET dirty=0 WHERE key=?', (row['key'],))
        return {'cursor': row['seq'], 'event': decode(row['body'])}

"""Local history preparation: real stores, no consumer/Native activation."""
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import threading
import itertools
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid

from csl_bridge_event_history import EventHistory, canonical_key, raw_hash
from csl_bridge_event_state import EventConflict, EventDeferred
from csl_bridge_store import Store, encode, decode


def uid():
    return str(uuid.uuid4())


class EventHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name) / 'bridge'
        self.store = Store(self.home, create=True); self.addCleanup(self.store.close)
        self.local_thread, self.remote_thread = uid(), uid()
        with self.store.transaction() as db:
            db.execute('INSERT INTO agents VALUES(?,?)', ('coordinator', self.local_thread))
        self._history = None

    @property
    def history(self):
        if self._history is None:
            self._history = EventHistory(self.store)
        return self._history

    def task(self, *, peer='remote', known_thread=False, confirmed=True):
        request = {'version': 1, 'op': 'followup', 'id': uid(), 'source': 'coordinator',
                   'target': 'worker', 'text': 'Synthetic original'}
        if known_thread:
            request['session'] = self.local_thread
        turn = uid()
        receipt = {'state': 'delivered', 'request_id': request['id'],
                   'turn_id': turn, 'thread_id': self.remote_thread}
        self.store.reserve('outbound:' + peer, request)
        self.store.operation(request['id'], 'delivered' if confirmed else 'uncertain', receipt if confirmed else {})
        event = {'id': uid(), 'task_id': request['id'], 'source': 'worker', 'target': 'coordinator',
                 'kind': 'final', 'status': 'completed', 'text': 'Synthetic final', 'turn_id': turn}
        return request, receipt, event

    def alias(self, event, *, peer='remote', delivery='pending', legacy=True):
        event = dict(event, id=uid())
        with self.store.transaction() as db:
            cursor = db.execute('INSERT INTO inbound(id,peer,body,delivery)VALUES(?,?,?,?)',
                                (event['id'], peer, encode(event), delivery))
            seq = cursor.lastrowid
            if not legacy:
                row = db.execute('SELECT * FROM inbound WHERE seq=?', (seq,)).fetchone()
                self.history.index_row(db, row, legacy=False)
        return event, seq

    def catalog(self):
        for _ in range(50):
            result = self.history.catalog_step()
            if result['complete']:
                return result
        self.fail('Synthetic bounded catalog did not finish')

    def seed(self, event, peer='remote'):
        self.catalog()
        key = canonical_key(peer, event['task_id'])
        for _ in range(50):
            result = self.history.seed_step(key)
            if result['complete']:
                return key, result
        self.fail('Synthetic bounded seed did not finish')

    def effect(self, key):
        return self.store.rows('SELECT * FROM event_effects WHERE key=?', (key,))[0]

    def test_unseeded_final_holds_later_progress_across_reopen(self):
        _, _, event = self.task()
        _, seq = self.alias(event)
        self.alias(dict(event, kind='progress', status='progress'))
        self.catalog()
        for _ in range(2):
            self._history = EventHistory(self.store)
            with self.assertRaises(EventDeferred):
                self.history.next_publication('coordinator', 0)
        self.seed(event)
        self.assertEqual(self.history.next_publication('coordinator', 0)['seq'], seq)

    def test_partial_final_holds_later_seeded_task_and_duplicate_aliases(self):
        _, _, first = self.task()
        for _ in range(65):
            self.alias(first)
        _, _, later = self.task(); self.alias(later)
        self.catalog(); key = canonical_key('remote', first['task_id'])
        self.assertFalse(self.history.seed_step(key)['complete'])
        self.seed(later)
        self._history = EventHistory(self.store)
        with self.assertRaises(EventDeferred):
            self.history.next_publication('coordinator', 0)
        self.assertEqual(self.history.seed_step(key)['processed'], 1)
        publication = self.history.next_publication('coordinator', 0)
        self.assertEqual(publication['seq'], 1)
        self.history.effects.observe_wait(key, publication['revision'], guard=self.history.guard)
        # Aliases are not extra publications after the representative is consumed.
        self.assertEqual(self.history.next_publication('coordinator', 1)['seq'], 66)

    def test_known_authority_never_changes_before_or_during_seed(self):
        for partial, field in itertools.product((False, True), ('text', 'session', 'hash')):
            with self.subTest(partial=partial, field=field):
                request, _, event = self.task(known_thread=True)
                for _ in range(65 if partial else 1):
                    self.alias(event)
                self.catalog(); key = canonical_key('remote', event['task_id'])
                if partial:
                    self.history.seed_step(key)
                before = self.store.rows('SELECT * FROM event_seed_state WHERE key=?', (key,))
                changed = dict(request, **({field: 'Other body' if field == 'text' else uid()} if field != 'hash' else {}))
                body = encode(changed)
                with self.store.transaction() as db:
                    db.execute('UPDATE operations SET request=?,hash=? WHERE id=?',
                               (body, raw_hash(body) if field != 'hash' else '0' * 64, request['id']))
                self._history = EventHistory(self.store)
                result = self.history.seed_step(key)
                self.assertEqual(result['state'], 'conflict')
                if before:
                    after = self.store.rows('SELECT * FROM event_seed_state WHERE key=?', (key,))[0]
                    for name in ('signature', 'cursor', 'projection', 'thread_snapshot'):
                        self.assertEqual(after[name], before[0][name])
                with self.assertRaises((EventConflict, EventDeferred)):
                    self.history.effects.claim_native(key, self.effect(key)['revision'], guard=self.history.guard)
                # A later repair cannot silently erase the known contradiction.
                with self.store.transaction() as db:
                    db.execute('UPDATE operations SET request=?,hash=? WHERE id=?',
                               (encode(request), raw_hash(encode(request)), request['id']))
                self.assertEqual(self.history.seed_step(key)['state'], 'conflict')

    def test_catalog_remembers_observed_known_mutation_after_original_repair(self):
        request, _, event = self.task(known_thread=True)
        self.alias(event); self.catalog()
        changed = dict(request, text='Different stored original')
        with self.store.transaction() as db:
            db.execute('UPDATE operations SET request=?,hash=? WHERE id=?',
                       (encode(changed), raw_hash(encode(changed)), request['id']))
        self.alias(event); self.catalog()
        with self.store.transaction() as db:
            db.execute('UPDATE operations SET request=?,hash=? WHERE id=?',
                       (encode(request), raw_hash(encode(request)), request['id']))
        _, result = self.seed(event)
        self.assertEqual(result['state'], 'conflict')
        self.assertTrue(self.store.rows('SELECT * FROM event_history_conflicts'))

    def test_absent_original_resolves_without_upgrading_legacy_uncertainty(self):
        for delivery, partial in itertools.product(('pending', 'uncertain', 'delivered'), (False, True)):
            with self.subTest(delivery=delivery, partial=partial):
                request, receipt, event = self.task(known_thread=True)
                with self.store.transaction() as db:
                    db.execute('DELETE FROM operations WHERE id=?', (request['id'],))
                for _ in range(65 if partial else 1):
                    self.alias(event, delivery=delivery)
                self.catalog(); key = canonical_key('remote', event['task_id'])
                first = self.history.seed_step(key)
                if not partial:
                    self.assertEqual(first['state'], {'pending': 'deferred', 'uncertain': 'uncertain', 'delivered': 'legacy_suppressed'}[delivery])
                self.assertIsNone(self.store.rows('SELECT signature FROM event_seed_state WHERE key=?', (key,))[0]['signature'])
                attempts = self.store.rows('SELECT * FROM event_legacy_attempts WHERE key=?', (key,))
                self.store.reserve('outbound:remote', request)
                self.store.operation(request['id'], 'delivered', receipt)
                # Also cover a new cataloged alias acquiring the first known binding.
                self.alias(event); self.catalog()
                self._history = EventHistory(self.store)
                _, result = self.seed(event)
                self.assertEqual(result['state'], {'pending': 'pending', 'uncertain': 'uncertain', 'delivered': 'legacy_suppressed'}[delivery])
                self.assertEqual(self.store.rows('SELECT * FROM event_legacy_attempts WHERE key=?', (key,)), attempts)
                if delivery == 'uncertain':
                    self.assertIsNone(self.effect(key)['thread'])
                    self.assertTrue(all(row['thread'] is None for row in attempts))
                elif delivery == 'pending':
                    claim = self.history.effects.claim_native(key, self.effect(key)['revision'], guard=self.history.guard)
                    self.assertEqual(claim['thread'], self.local_thread)
                else:
                    self.assertIsNone(self.history.effects.claim_native(key, self.effect(key)['revision'], guard=self.history.guard))

    def test_settled_reads_use_retained_proof_without_original_lookup(self):
        from unittest.mock import patch
        for mode in ('wait', 'native', 'legacy'):
            with self.subTest(mode=mode):
                request, _, event = self.task(known_thread=True)
                self.alias(event, delivery='delivered' if mode == 'legacy' else 'pending')
                key, _ = self.seed(event)
                if mode == 'wait':
                    self.history.effects.observe_wait(key, self.effect(key)['revision'], guard=self.history.guard)
                elif mode == 'native':
                    claim = self.history.effects.claim_native(key, self.effect(key)['revision'], guard=self.history.guard)
                    self.history.effects.finish_native(claim, {'state': 'delivered', 'request_id': claim['client_id'],
                        'thread_id': claim['thread'], 'turn_id': uid()}, guard=self.history.guard)
                before = dict(self.effect(key))
                attempts = self.store.rows('SELECT * FROM event_attempts WHERE key=?', (key,))
                with self.store.transaction() as db:
                    db.execute('DELETE FROM operations WHERE id=?', (request['id'],))
                self._history = EventHistory(self.store)
                with patch.object(self.history, 'proof', side_effect=AssertionError('Settled history must not re-prove originals')):
                    self.assertEqual(self.history.next_publication('coordinator', before['representative'] - 1)['key'], key)
                    self.assertTrue(self.history.effects.observe_wait(key, before['revision'], guard=self.history.guard)['history_read'])
                    self.assertIsNone(self.history.effects.claim_native(key, before['revision'], guard=self.history.guard))
                self.assertEqual(dict(self.effect(key)), before)
                self.assertEqual(self.store.rows('SELECT * FROM event_attempts WHERE key=?', (key,)), attempts)
                with self.store.transaction() as db:
                    db.execute('UPDATE inbound SET body=? WHERE seq=?', (encode(dict(event, text='changed raw')), before['representative']))
                with self.assertRaises(EventConflict):
                    self.history.next_publication('coordinator', before['representative'] - 1)

    def test_settled_outcome_survives_contradictory_new_proposal(self):
        _, _, event = self.task(); self.alias(event)
        key, _ = self.seed(event)
        self.history.effects.observe_wait(key, self.effect(key)['revision'], guard=self.history.guard)
        before = dict(self.effect(key))
        self.assertEqual(self.history.admit('remote', dict(event, id=uid(), text='Contradictory final'))['state'], 'rejected')
        self.assertEqual(dict(self.effect(key)), before)
        self.assertEqual(self.history.next_publication('coordinator', 0)['key'], key)

    def test_terminal_refusal_replays_and_never_accepts_changed_wire(self):
        for change in ('turn', 'body', 'task'):
            with self.subTest(change=change):
                _, _, good = self.task()
                bad = dict(good, turn_id=uid())
                first = self.history.admit('remote', bad, cursor=10)
                self.assertEqual(first['state'], 'rejected')
                self.assertEqual(self.history.admit('remote', bad, cursor=11), first)
                corrected = dict(good)
                if change == 'body':
                    corrected['text'] = 'Changed valid narrative'
                if change == 'task':
                    _, _, another = self.task(); corrected = dict(another, id=good['id'])
                self._history = EventHistory(self.store)
                self.assertEqual(self.history.admit('remote', corrected, cursor=12)['state'], 'rejected')
                self.assertEqual(self.store.rows('SELECT * FROM inbound WHERE id=?', (good['id'],)), [])
                self.assertEqual(self.store.rows('SELECT value FROM cursors WHERE peer=?', ('remote',))[0]['value'], 12)
                conflicts = self.store.rows('SELECT * FROM event_admission_conflicts WHERE wire_id=?', (good['id'],))
                self.assertEqual(len(conflicts), 2)
                self.assertNotEqual(conflicts[0]['raw_hash'], conflicts[1]['raw_hash'])

    def test_terminal_refusal_and_cursor_roll_back_together(self):
        _, _, event = self.task()
        with self.store.transaction() as db:
            db.execute("CREATE TRIGGER reject_cursor BEFORE INSERT ON cursors BEGIN SELECT RAISE(ABORT,'synthetic cursor failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.history.admit('remote', dict(event, turn_id=uid()), cursor=3)
        self.assertEqual(self.store.rows('SELECT * FROM event_admission_conflicts'), [])
        with self.store.transaction() as db:
            db.execute('DROP TRIGGER reject_cursor')
        self.assertEqual(self.history.admit('remote', event, cursor=3)['state'], 'persisted')

    def test_initialization_opens_existing_home_without_rewriting_originals(self):
        _, _, event = self.task(); self.alias(event)
        before = self.store.rows('SELECT * FROM inbound')
        EventHistory(self.store); EventHistory(self.store)
        self.assertEqual(self.store.rows('SELECT * FROM inbound'), before)

    def test_catalog_is_128_bounded_and_records_complete_watermark(self):
        _, _, event = self.task()
        for _ in range(130):
            self.alias(event)
        first = self.history.catalog_step()
        self.assertEqual(first, {'processed': 128, 'complete': False})
        with self.assertRaises(EventDeferred):
            self.history.seed_step(canonical_key('remote', event['task_id']))
        self.assertEqual(self.history.catalog_step(), {'processed': 2, 'complete': True})

    def test_complete_seed_sees_delivered_alias_beyond_first_128_rows(self):
        _, _, event = self.task()
        for _ in range(149):
            self.alias(event)
        self.alias(event, delivery='delivered')
        self.catalog(); key = canonical_key('remote', event['task_id'])
        for _ in range(2):
            result = self.history.seed_step(key)
            self.assertEqual(result['processed'], 64)
            self.assertFalse(result['complete'])
            with self.assertRaises(EventDeferred):
                self.history.effects.claim_native(key, self.effect(key)['revision'])
        self.assertEqual(self.history.seed_step(key)['state'], 'legacy_suppressed')
        self.assertEqual(self.effect(key)['representative'], 1)

    def test_legacy_pending_uncertain_delivered_cross_product(self):
        for left, right in itertools.product(('pending', 'uncertain', 'delivered'), repeat=2):
            with self.subTest(left=left, right=right):
                _, _, event = self.task()
                self.alias(event, delivery=left); self.alias(event, delivery=right)
                key, result = self.seed(event)
                expected = 'legacy_suppressed' if 'delivered' in (left, right) else 'uncertain' if 'uncertain' in (left, right) else 'pending'
                self.assertEqual(result['state'], expected)
                self.assertEqual(len(self.store.rows("SELECT * FROM event_publications WHERE key=? AND kind='final'", (key,))), 1)

    def test_unknown_legacy_thread_is_preserved_without_current_registration_guess(self):
        _, _, event = self.task()
        first, _ = self.alias(event, delivery='uncertain'); second, _ = self.alias(event, delivery='uncertain')
        key, result = self.seed(event)
        self.assertEqual(result['state'], 'uncertain')
        attempts = self.store.rows('SELECT wire_id,thread FROM event_legacy_attempts WHERE key=? ORDER BY seq', (key,))
        self.assertEqual(attempts, [{'wire_id': first['id'], 'thread': None}, {'wire_id': second['id'], 'thread': None}])
        self.assertIsNone(self.effect(key)['thread'])
        self.assertEqual(self.history.effects.recovery_batch('synthetic'), [])
        with self.assertRaises(EventDeferred):
            self.history.effects.claim_native(key, self.effect(key)['revision'])

    def test_known_legacy_thread_comes_from_owned_original_source_session(self):
        _, _, event = self.task(known_thread=True)
        self.alias(event, delivery='uncertain')
        key, _ = self.seed(event)
        with self.store.transaction() as db:
            db.execute('UPDATE agents SET thread=?', (uid(),))
        self.assertEqual(self.effect(key)['thread'], self.local_thread)
        self.assertEqual(self.store.rows('SELECT thread FROM event_legacy_attempts'), [{'thread': self.local_thread}])

    def test_missing_receipt_defers_then_recovers_without_resend(self):
        request, receipt, event = self.task(confirmed=False)
        self.alias(event)
        key, result = self.seed(event)
        self.assertEqual(result['state'], 'deferred')
        with self.assertRaises(EventDeferred):
            self.history.next_publication('coordinator', 0)
        self.store.operation(request['id'], 'delivered', receipt)
        self.assertEqual(self.history.seed_step(key)['state'], 'pending')
        self.assertEqual(decode(self.history.next_publication('coordinator', 0)['body'])['task_id'], request['id'])

    def test_conflicting_historical_text_status_turn_and_actor_select_no_winner(self):
        for changes in ({'text': 'Different'}, {'status': 'failed'}, {'turn_id': uid()}, {'source': 'someone-else'}):
            with self.subTest(changes=changes):
                _, _, event = self.task()
                self.alias(event); self.alias(dict(event, **changes))
                key, result = self.seed(event)
                self.assertEqual(result['state'], 'conflict')
                with self.assertRaises(EventConflict):
                    self.history.effects.claim_native(key, self.effect(key)['revision'])

    def test_optional_sender_header_alias_requires_independent_original_thread(self):
        _, _, event = self.task()
        self.alias(event); self.alias(dict(event, session=self.remote_thread))
        key, result = self.seed(event)
        self.assertEqual(result['state'], 'pending')
        self.assertEqual(len(self.store.rows('SELECT * FROM event_publications WHERE key=?', (key,))), 1)
        _, _, other = self.task()
        self.alias(other); self.alias(dict(other, session=uid()))
        self.assertEqual(self.seed(other)[1]['state'], 'conflict')

    def test_unkeyable_malformed_row_blocks_scoped_claim_and_later_wait_gap(self):
        _, _, affected = self.task(peer='affected'); self.alias(affected, peer='affected')
        _, _, other = self.task(peer='other')
        with self.store.transaction() as db:
            db.execute('INSERT INTO inbound(id,peer,body)VALUES(?,?,?)', (uid(), 'affected', '{bad'))
        self.alias(other, peer='other')
        affected_key, _ = self.seed(affected, 'affected'); other_key, _ = self.seed(other, 'other')
        with self.assertRaises(EventDeferred):
            self.history.effects.claim_native(affected_key, self.effect(affected_key)['revision'])
        self.assertIsNotNone(self.history.effects.claim_native(other_key, self.effect(other_key)['revision']))
        with self.assertRaises(EventDeferred):
            self.history.next_publication('coordinator', 1)
        self.assertEqual(self.store.rows('SELECT peer FROM event_history_barriers'), [{'peer': 'affected'}])

    def test_unknown_scope_creates_home_barrier(self):
        with self.store.transaction() as db:
            db.execute('INSERT INTO inbound(id,peer,body)VALUES(?,?,?)', (uid(), b'unknown', b'\xff'))
        _, _, event = self.task(); self.alias(event)
        key, _ = self.seed(event)
        self.assertEqual(self.store.rows('SELECT peer FROM event_history_barriers'), [{'peer': None}])
        with self.assertRaises(EventDeferred):
            self.history.effects.claim_native(key, self.effect(key)['revision'])

    def test_catalog_barrier_and_watermark_rollback_together(self):
        with self.store.transaction() as db:
            db.execute('INSERT INTO inbound(id,peer,body)VALUES(?,?,?)', (uid(), 'remote', '{bad'))
        history = self.history
        with self.store.transaction() as db:
            db.execute("CREATE TRIGGER fail_barrier BEFORE INSERT ON event_history_barriers BEGIN SELECT RAISE(ABORT,'synthetic failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            history.catalog_step()
        self.assertEqual(self.store.rows('SELECT * FROM event_catalog'), [])
        self.assertIsNone(self.store.rows('SELECT watermark FROM event_catalog_meta')[0]['watermark'])
        with self.store.transaction() as db:
            db.execute('DROP TRIGGER fail_barrier')
        self.assertTrue(history.catalog_step()['complete'])
        self.assertEqual(len(self.store.rows('SELECT * FROM event_history_barriers')), 1)

    def test_partial_seed_survives_reopen_and_catalog_tail_drift(self):
        _, _, event = self.task()
        for _ in range(70):
            self.alias(event)
        self.catalog(); key = canonical_key('remote', event['task_id'])
        self.assertFalse(self.history.seed_step(key)['complete'])
        self.alias(event, delivery='uncertain')
        with self.assertRaises(EventDeferred):
            self.history.seed_step(key)
        self.catalog()
        with closing(Store(self.home)) as store:
            history = EventHistory(store)
            self.assertEqual(history.seed_step(key)['state'], 'uncertain')
        self.assertEqual(len(self.store.rows('SELECT * FROM event_legacy_attempts')), 1)

    def test_alias_publication_skips_duplicate_sequences_but_keeps_progress(self):
        _, _, event = self.task()
        first, first_seq = self.alias(event)
        progress, progress_seq = self.alias(dict(event, kind='progress', status='progress', text='Progress'))
        self.alias(event)
        key, _ = self.seed(event)
        publication = self.history.next_publication('coordinator', 0)
        self.assertEqual(publication['seq'], first_seq)
        self.history.effects.observe_wait(key, publication['revision'])
        self.assertEqual(self.history.next_publication('coordinator', first_seq)['seq'], progress_seq)
        self.assertIsNone(self.history.next_publication('coordinator', progress_seq))
        self.assertEqual(self.history.next_publication('coordinator', 0)['wire_id'], first['id'])
        self.assertEqual(len(self.store.rows('SELECT * FROM event_attempts')), 1)

    def test_fresh_alias_never_reopens_already_claimed_uncertainty(self):
        _, _, event = self.task(); self.alias(event)
        key, _ = self.seed(event)
        claim = self.history.effects.claim_native(key, self.effect(key)['revision'])
        self.alias(event, legacy=False)
        self.catalog()
        self.assertEqual(self.history.seed_step(key)['state'], 'uncertain')
        self.assertEqual(self.effect(key)['client_id'], claim['client_id'])
        with self.assertRaises(EventDeferred):
            self.history.effects.claim_native(key, self.effect(key)['revision'])

    def test_new_conflicting_proposal_preserves_valid_canonical_body(self):
        _, _, event = self.task(); self.alias(event)
        key, _ = self.seed(event)
        projection = self.effect(key)['projection']
        changed, _ = self.alias(dict(event, text='Conflicting new proposal'), legacy=False)
        self.catalog(); self.history.seed_step(key)
        self.assertEqual(self.effect(key)['projection'], projection)
        self.assertEqual(self.effect(key)['state'], 'pending')
        self.assertEqual(len(self.store.rows('SELECT * FROM event_history_conflicts WHERE wire_id=?', (changed['id'],))), 1)

    def test_cross_kind_legacy_id_is_quarantined(self):
        request, _, event = self.task()
        event['id'] = request['id']
        with self.store.transaction() as db:
            db.execute('INSERT INTO inbound(id,peer,body)VALUES(?,?,?)', (event['id'], 'remote', encode(event)))
        self.assertEqual(self.seed(event)[1]['state'], 'conflict')

    def publisher_task(self):
        request = {'version': 1, 'op': 'followup', 'id': uid(), 'source': 'delegator',
                   'target': 'coordinator', 'text': 'Synthetic work'}
        turn = uid()
        self.store.reserve('remote', request)
        self.store.operation(request['id'], 'delivered', {'state': 'delivered', 'request_id': request['id'],
            'turn_id': turn, 'thread_id': self.local_thread})
        with self.store.transaction() as db:
            db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?)',
                       (request['id'], 'remote', 'delegator', 'coordinator', turn, 'running'))
        return request

    def test_first_exact_final_is_frozen_and_later_callback_recovery_conflicts_persist(self):
        request = self.publisher_task()
        first = self.history.publish_final(request['id'], self.local_thread, 'completed', 'First narrative')
        self.assertEqual(first['state'], 'published')
        self.assertEqual(self.history.publish_final(request['id'], self.local_thread, 'completed', 'Joined later narrative')['state'], 'conflict')
        self.assertEqual(self.history.publish_final(request['id'], self.local_thread, 'failed', 'First narrative')['state'], 'conflict')
        self.assertEqual(self.history.publish_final(request['id'], self.local_thread, 'completed', 'First narrative'), first)
        self.assertEqual(decode(self.store.rows('SELECT body FROM outbound')[0]['body'])['text'], 'First narrative')
        self.assertEqual(self.store.rows('SELECT status FROM tasks'), [{'status': 'completed'}])
        self.assertEqual(len(self.store.rows('SELECT * FROM event_final_conflicts')), 2)

    def test_final_publication_and_task_finalize_are_one_transaction(self):
        request = self.publisher_task(); history = self.history
        with self.store.transaction() as db:
            db.execute("CREATE TRIGGER fail_finalize BEFORE UPDATE ON tasks BEGIN SELECT RAISE(ABORT,'synthetic finalize failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            history.publish_final(request['id'], self.local_thread, 'completed', 'Synthetic complete')
        self.assertEqual(self.store.rows('SELECT * FROM outbound'), [])
        self.assertEqual(self.store.rows('SELECT status FROM tasks'), [{'status': 'running'}])

    def test_final_empty_failed_interrupted_remain_typed_receipts(self):
        for status in ('completed', 'failed', 'interrupted'):
            request = self.publisher_task()
            self.history.publish_final(request['id'], self.local_thread, status, '')
            row = self.store.rows('SELECT body FROM outbound ORDER BY seq DESC LIMIT 1')[0]
            self.assertEqual((decode(row['body'])['text'], decode(row['body'])['status']), ('', status))

    def test_concurrent_catalog_and_seed_batches_keep_complete_prefix(self):
        _, _, event = self.task()
        for _ in range(150):
            self.alias(event)
        history = self.history
        start = threading.Barrier(2, timeout=5)
        def catalog(_):
            with closing(Store(self.home)) as store:
                instance = EventHistory(store)
                start.wait()
                return instance.catalog_step()
        with ThreadPoolExecutor(max_workers=2) as pool:
            result = list(pool.map(catalog, range(2)))
        self.assertEqual(sorted(r['processed'] for r in result), [22, 128])
        self.assertEqual(len(self.store.rows('SELECT * FROM event_catalog')), 150)
        key = canonical_key('remote', event['task_id'])
        start = threading.Barrier(2, timeout=5)
        def seed(_):
            with closing(Store(self.home)) as store:
                instance = EventHistory(store)
                start.wait()
                return instance.seed_step(key)
        with ThreadPoolExecutor(max_workers=2) as pool:
            result = list(pool.map(seed, range(2)))
        self.assertEqual([r['processed'] for r in result], [64, 64])
        self.assertFalse(self.effect(key)['seed_complete'])
        self.assertEqual(history.seed_step(key), {'processed': 22, 'complete': True, 'state': 'pending'})

    def test_seed_failure_rolls_back_cursor_uncertain_evidence_and_visibility(self):
        _, _, event = self.task()
        for _ in range(99):
            self.alias(event)
        self.alias(event, delivery='uncertain')
        self.catalog(); key = canonical_key('remote', event['task_id'])
        self.history.seed_step(key)
        cursor = self.store.rows('SELECT cursor FROM event_seed_state')[0]['cursor']
        attempts = self.store.rows('SELECT * FROM event_legacy_attempts')
        with self.store.transaction() as db:
            db.execute("CREATE TRIGGER fail_seed BEFORE UPDATE ON event_effects BEGIN SELECT RAISE(ABORT,'synthetic seed failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.history.seed_step(key)
        self.assertEqual(self.store.rows('SELECT cursor FROM event_seed_state')[0]['cursor'], cursor)
        self.assertFalse(self.effect(key)['seed_complete'])
        self.assertEqual(self.store.rows('SELECT * FROM event_publications'), [])
        self.assertEqual(self.store.rows('SELECT * FROM event_legacy_attempts'), attempts)
        with self.store.transaction() as db:
            db.execute('DROP TRIGGER fail_seed')
        self.assertEqual(self.history.seed_step(key)['state'], 'uncertain')

    def test_actual_disk_full_cannot_advance_catalog_or_create_visibility(self):
        _, _, event = self.task()
        for _ in range(128):
            self.alias(event)
        history = self.history
        pages = self.store.db.execute('PRAGMA page_count').fetchone()[0]
        self.store.db.execute('PRAGMA max_page_count=' + str(pages))
        with self.assertRaises(sqlite3.OperationalError):
            history.catalog_step()
        self.assertEqual(self.store.rows('SELECT * FROM event_catalog'), [])
        self.assertIsNone(self.store.rows('SELECT watermark FROM event_catalog_meta')[0]['watermark'])
        self.assertEqual(self.store.rows('SELECT * FROM event_publications'), [])
        self.store.db.execute('PRAGMA max_page_count=' + str(pages + 10000))
        self.assertTrue(history.catalog_step()['complete'])

    def test_new_alias_after_wait_delivery_remains_one_claim(self):
        _, _, event = self.task(); self.alias(event)
        key, _ = self.seed(event)
        self.history.effects.observe_wait(key, self.effect(key)['revision'])
        self.alias(event, legacy=False)
        self.catalog(); self.history.seed_step(key)
        self.assertEqual(self.effect(key)['state'], 'wait_delivered')
        publication = self.history.next_publication('coordinator', 0)
        self.assertTrue(self.history.effects.observe_wait(key, publication['revision'])['history_read'])
        self.assertEqual(len(self.store.rows('SELECT * FROM event_attempts')), 1)

    def test_changed_owned_original_quarantines_without_rewriting_canonical_body(self):
        request, _, event = self.task(); self.alias(event)
        key, _ = self.seed(event)
        projection = self.effect(key)['projection']
        changed = dict(request, text='Different owned original')
        with self.store.transaction() as db:
            db.execute('UPDATE operations SET request=?,hash=? WHERE id=?', (encode(changed), raw_hash(encode(changed)), request['id']))
        with self.assertRaises(EventConflict):
            self.history.next_publication('coordinator', 0)
        self.assertEqual(self.history.seed_step(key)['state'], 'conflict')
        self.assertEqual(self.effect(key)['projection'], projection)
        with self.assertRaises(EventConflict):
            self.history.effects.claim_native(key, self.effect(key)['revision'])

    def test_invalid_body_after_catalog_creates_durable_home_barrier(self):
        _, _, event = self.task(); alias, seq = self.alias(event)
        self.catalog()
        with self.store.transaction() as db:
            db.execute('UPDATE inbound SET body=? WHERE seq=?', ('{broken', seq))
        result = self.history.seed_step(canonical_key('remote', event['task_id']))
        self.assertEqual(result['state'], 'conflict')
        self.assertEqual(self.store.rows('SELECT peer FROM event_history_barriers'), [{'peer': None}])
        self.assertEqual(self.store.rows('SELECT raw_hash FROM event_catalog')[0]['raw_hash'], raw_hash(encode(alias)))

    def test_recovered_original_does_not_bless_wrong_sender_header(self):
        request, receipt, event = self.task(confirmed=False)
        event['session'] = uid()
        self.alias(event)
        key, _ = self.seed(event)
        self.store.operation(request['id'], 'delivered', receipt)
        self.assertEqual(self.history.seed_step(key)['state'], 'conflict')
        with self.assertRaises(EventConflict):
            self.history.effects.claim_native(key, self.effect(key)['revision'])

    def test_wrong_publisher_receipt_is_permanent_and_unknown_is_deferred(self):
        request = self.publisher_task()
        self.store.operation(request['id'], 'delivered', {'state': 'delivered', 'turn_id': uid()})
        with self.assertRaises(EventConflict):
            self.history.publish_final(request['id'], self.local_thread, 'completed', 'Synthetic result')
        self.store.operation(request['id'], 'uncertain', {})
        with self.assertRaises(EventDeferred):
            self.history.publish_final(request['id'], self.local_thread, 'completed', 'Synthetic result')
        self.assertEqual(self.store.rows('SELECT * FROM outbound'), [])

    def test_atomic_admit_records_alias_and_watch_cursor_with_no_native_claim(self):
        _, _, event = self.task()
        result = self.history.admit('remote', event, cursor=7)
        self.assertEqual(result['state'], 'persisted')
        self.assertEqual(self.store.rows('SELECT * FROM cursors'), [{'peer': 'remote', 'value': 7}])
        self.assertEqual(self.store.rows('SELECT legacy FROM event_catalog'), [{'legacy': 0}])
        self.assertEqual(self.store.rows('SELECT * FROM event_attempts'), [])
        self.assertEqual(self.history.admit('remote', event, cursor=8)['seq'], result['seq'])
        self.assertEqual(len(self.store.rows('SELECT * FROM inbound')), 1)

    def test_wire_collision_refusal_is_durable_and_does_not_rewrite_original(self):
        _, _, event = self.task()
        self.history.admit('remote', event, cursor=1)
        changed = dict(event, text='Changed same wire ID')
        result = self.history.admit('remote', changed, cursor=2)
        self.assertEqual(result['state'], 'rejected')
        self.assertEqual(decode(self.store.rows('SELECT body FROM inbound')[0]['body']), event)
        self.assertEqual(len(self.store.rows('SELECT * FROM event_admission_conflicts')), 1)
        self.assertEqual(self.store.rows('SELECT value FROM cursors'), [{'value': 2}])

    def test_unknown_original_admit_does_not_consume_cursor(self):
        _, _, event = self.task(confirmed=False)
        with self.assertRaises(EventDeferred):
            self.history.admit('remote', event, cursor=5)
        self.assertEqual(self.store.rows('SELECT * FROM cursors'), [])
        self.assertEqual(self.store.rows('SELECT * FROM inbound'), [])

    def test_retransmitted_unindexed_legacy_uncertainty_retains_actual_wire_id(self):
        _, _, event = self.task(known_thread=True)
        alias, _ = self.alias(event, delivery='uncertain')
        self.history.admit('remote', alias)
        key, _ = self.seed(event)
        self.assertEqual(self.store.rows('SELECT wire_id,thread FROM event_legacy_attempts WHERE key=?', (key,)),
                         [{'wire_id': alias['id'], 'thread': self.local_thread}])
        self.assertEqual(self.effect(key)['state'], 'uncertain')

    def test_cached_original_route_recovers_only_its_independent_native_receipt(self):
        request, receipt, event = self.task()
        route = {'physical_peer': 'hub', 'origin': 'remote', 'destination': 'remote'}
        self.history.admit('remote', event, route=route)
        self.store.operation(request['id'], 'uncertain', {})
        with self.assertRaises(EventDeferred):
            self.history.recover_original('remote', event, dict(route, origin='other'))
        recovered = self.history.recover_original('remote', event, route)
        self.assertEqual(recovered, receipt)
        self.assertEqual(decode(self.store.rows('SELECT result FROM operations')[0]['result']), receipt)
        wrong = dict(event, turn_id=uid())
        with self.assertRaises(EventConflict):
            self.history.recover_original('remote', wrong, route)

    def test_changed_route_refused_without_replacing_first_positive_provenance(self):
        _, _, event = self.task()
        route = {'physical_peer': 'hub', 'origin': 'remote', 'destination': 'remote'}
        self.history.admit('remote', event, route=route)
        alias = dict(event, id=uid())
        result = self.history.admit('remote', alias, route=dict(route, physical_peer='elsewhere'))
        self.assertEqual(result['state'], 'rejected')
        self.assertEqual(self.history.cached_route('remote', event['task_id']), route)
        self.assertEqual(len(self.store.rows('SELECT * FROM inbound')), 1)

    def test_claim_rechecks_original_proof_inside_exact_home_transaction(self):
        request, receipt, event = self.task(); self.alias(event)
        key, _ = self.seed(event)
        self.store.operation(request['id'], 'uncertain', {})
        with self.assertRaises(EventDeferred):
            self.history.effects.claim_native(key, self.effect(key)['revision'], guard=self.history.guard)
        self.assertEqual(self.effect(key)['state'], 'pending')
        self.store.operation(request['id'], 'delivered', receipt)
        with self.store.transaction() as db:
            claim = self.history.effects.claim_native(key, self.effect(key)['revision'], transaction=db, guard=self.history.guard)
        self.assertEqual(claim['thread'], self.local_thread)

    def test_foreign_home_transaction_cannot_grant_a_claim(self):
        _, _, event = self.task(); self.alias(event)
        key, _ = self.seed(event)
        with closing(Store(Path(self.temp.name) / 'other', create=True)) as other:
            with other.transaction() as db, self.assertRaises(EventConflict):
                self.history.effects.claim_native(key, self.effect(key)['revision'], transaction=db)
        self.assertEqual(self.effect(key)['state'], 'pending')

    def test_unindexed_home_tail_prevents_kernel_claim_even_before_seed_call(self):
        _, _, event = self.task(); self.alias(event)
        key, _ = self.seed(event)
        self.alias(event)
        with self.assertRaises(EventDeferred):
            self.history.effects.claim_native(key, self.effect(key)['revision'])
        self.assertEqual(self.effect(key)['state'], 'pending')

    def test_legacy_wire_cannot_borrow_another_canonical_native_acceptance_id(self):
        _, _, first = self.task(); self.alias(first)
        first_key, _ = self.seed(first)
        claim = self.history.effects.claim_native(first_key, self.effect(first_key)['revision'])
        _, _, second = self.task()
        second['id'] = claim['client_id']
        with self.store.transaction() as db:
            db.execute("INSERT INTO inbound(id,peer,body,delivery)VALUES(?,?,?,'uncertain')", (second['id'], 'remote', encode(second)))
        self.assertEqual(self.seed(second)[1]['state'], 'conflict')

    def test_outer_admission_failure_rolls_back_claim_and_native_id_reservation(self):
        _, _, event = self.task(); self.alias(event)
        key, _ = self.seed(event)
        with self.assertRaises(RuntimeError):
            with self.store.transaction() as db:
                self.history.effects.claim_native(key, self.effect(key)['revision'], transaction=db, guard=self.history.guard)
                raise RuntimeError('Synthetic outer admission failure before Native')
        self.assertEqual(self.effect(key)['state'], 'pending')
        self.assertEqual(self.store.rows('SELECT * FROM event_attempts'), [])
        self.assertEqual(self.store.rows('SELECT * FROM event_native_ids'), [])

    def test_unknown_schema_version_cannot_seed_or_claim(self):
        _, _, event = self.task(); self.alias(event)
        key, _ = self.seed(event)
        with self.store.transaction() as db:
            db.execute('UPDATE event_catalog_meta SET version=2')
        with self.assertRaises(EventDeferred):
            EventHistory(self.store)
        with self.assertRaises(EventDeferred):
            self.history.effects.claim_native(key, self.effect(key)['revision'])
        self.assertEqual(self.effect(key)['state'], 'pending')

    def test_unavailable_cached_receipt_does_not_grant_native_acceptance(self):
        request, _, event = self.task()
        route = {'physical_peer': 'hub', 'origin': 'remote', 'destination': 'remote'}
        self.history.admit('remote', event, route=route)
        self.store.operation(request['id'], 'uncertain', {})
        with self.store.transaction() as db:
            db.execute('UPDATE event_original_routes SET receipt=?', (encode({'state': 'queued'}),))
        with self.assertRaises(EventDeferred):
            self.history.recover_original('remote', event, route)
        self.assertEqual(self.store.rows('SELECT state FROM operations'), [{'state': 'uncertain'}])

    def test_cached_route_does_not_outlive_changed_original_request_identity(self):
        request, _, event = self.task()
        route = {'physical_peer': 'hub', 'origin': 'remote', 'destination': 'remote'}
        self.history.admit('remote', event, route=route)
        changed = dict(request, text='Different original')
        with self.store.transaction() as db:
            db.execute('UPDATE operations SET request=?,hash=?', (encode(changed), raw_hash(encode(changed))))
        self.assertIsNone(self.history.cached_route('remote', event['task_id']))
        with self.assertRaises(EventConflict):
            self.history.recover_original('remote', event, route)

    def test_native_and_wait_claim_guards_rollback_with_original_proof_failure(self):
        request, _, event = self.task(); self.alias(event)
        key, _ = self.seed(event)
        self.store.operation(request['id'], 'uncertain', {})
        with self.assertRaises(EventDeferred):
            self.history.effects.observe_wait(key, self.effect(key)['revision'], guard=self.history.guard)
        self.assertEqual(self.effect(key)['state'], 'pending')
        self.assertEqual(self.store.rows('SELECT * FROM event_attempts'), [])

    def test_known_other_peer_is_refused_but_missing_original_stays_deferred(self):
        _, _, event = self.task(peer='actual')
        result = self.history.admit('different', event)
        self.assertEqual(result['state'], 'rejected')
        missing = dict(event, id=uid(), task_id=uid())
        with self.assertRaises(EventDeferred):
            self.history.admit('different', missing)
        self.assertEqual(self.store.rows('SELECT * FROM inbound'), [])

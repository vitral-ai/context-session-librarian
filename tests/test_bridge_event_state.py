"""Inert shared-store primitives, real private SQLite; no consumer activation."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
import uuid
from unittest.mock import patch

from csl_bridge_event_state import EventState, EventConflict, EventDeferred, digest
from csl_bridge_store import Store, encode


def uid():
    return str(uuid.uuid4())


class EventStateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name) / 'bridge'
        self.store = Store(self.home, create=True)
        self.addCleanup(self.store.close)
        self.thread = uid()
        with self.store.transaction() as db:
            db.execute('INSERT INTO agents VALUES(?,?)', ('recipient', self.thread))
        self.events = EventState(self.store)

    def seeded(self, *, peer='remote', state='pending', complete=True):
        # Trusted precondition fixture, NOT a claim that legacy admission/seeding
        # is implemented. The follow-up consumer integration must establish it.
        key = uid()
        with self.store.transaction() as db:
            db.execute('INSERT INTO event_effects(key,peer,task,projection,representative,state,seed_complete,agent,thread)VALUES(?,?,?,?,?,?,?,?,?)',
                       (key, peer, uid(), digest({'synthetic': key}), 1, state, int(complete), 'recipient', self.thread))
        return key

    def row(self, key):
        return self.store.rows('SELECT * FROM event_effects WHERE key=?', (key,))[0]

    def accepted(self, claim):
        return {'state': 'delivered', 'request_id': claim['client_id'],
                'thread_id': claim['thread'], 'turn_id': uid()}

    def test_initialization_is_additive_and_idempotent_on_existing_home(self):
        other = Path(self.temp.name) / 'old-home'
        with closing(Store(other, create=True)) as store:
            self.assertEqual(store.rows("SELECT name FROM sqlite_master WHERE name='event_effects'"), [{'name': 'event_effects'}])
        with closing(Store(other)) as store:
            EventState(store); EventState(store)
            self.assertEqual(len(store.rows("SELECT name FROM sqlite_master WHERE name='event_effects'")), 1)

    def test_real_sqlite_concurrent_native_claim_has_one_winner(self):
        key = self.seeded()
        start = threading.Barrier(8)
        def claim(_):
            with closing(Store(self.home)) as store:
                events = EventState(store)
                start.wait()
                try:
                    return events.claim_native(key, 0)
                except EventDeferred:
                    return None
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(claim, range(8)))
        self.assertEqual(sum(c is not None for c in claims), 1)
        self.assertEqual(self.row(key)['state'], 'uncertain')
        self.assertEqual(len(self.store.rows('SELECT * FROM event_attempts')), 1)

    def test_native_and_wait_compete_on_same_claim(self):
        for first in ('native', 'wait'):
            with self.subTest(first=first):
                key = self.seeded()
                if first == 'native':
                    self.events.claim_native(key, 0)
                    with self.assertRaises(EventDeferred):
                        self.events.observe_wait(key, self.row(key)['revision'])
                else:
                    self.events.observe_wait(key, 0)
                    self.assertIsNone(self.events.claim_native(key, self.row(key)['revision']))
                attempts = self.store.rows('SELECT mode FROM event_attempts WHERE key=?', (key,))
                self.assertEqual(attempts, [{'mode': first}])

    def test_wait_observation_commits_atomically_and_old_cursor_is_history_read(self):
        key = self.seeded()
        result = self.events.observe_wait(key, 0)
        self.assertFalse(result['history_read'])
        self.assertEqual(self.row(key)['state'], 'wait_delivered')
        self.assertEqual(self.store.rows('SELECT state FROM event_attempts WHERE key=?', (key,)), [{'state': 'wait_delivered'}])
        self.assertTrue(self.events.observe_wait(key, self.row(key)['revision'])['history_read'])
        self.assertEqual(len(self.store.rows('SELECT * FROM event_attempts WHERE key=?', (key,))), 1)

    def test_settled_native_state_suppresses_new_claim_at_any_age(self):
        key = self.seeded()
        claim = self.events.claim_native(key, 0)
        self.assertEqual(self.events.finish_native(claim, self.accepted(claim)), 'native_delivered')
        with closing(Store(self.home)) as store:
            events = EventState(store)
            self.assertIsNone(events.claim_native(key, self.row(key)['revision']))
            self.assertTrue(events.observe_wait(key, self.row(key)['revision'])['history_read'])

    def test_ambiguous_or_client_only_result_is_not_native_delivery(self):
        for result in (None, {'state': 'mail_ready_for_client'}, {'state': 'delivered'}, {'state': 'queued'}):
            with self.subTest(result=result):
                key = self.seeded(); claim = self.events.claim_native(key, 0)
                self.assertEqual(self.events.finish_native(claim, result), 'uncertain')
                with self.assertRaises(EventDeferred):
                    self.events.claim_native(key, self.row(key)['revision'])

    def test_definitive_nonacceptance_can_retry_with_same_id_and_retained_attempt(self):
        key = self.seeded(); claim = self.events.claim_native(key, 0)
        self.assertEqual(self.events.finish_native(claim, {'state': 'rejected'}, definitely_unaccepted=True), 'pending')
        retry = self.events.claim_native(key, self.row(key)['revision'])
        self.assertEqual(retry['client_id'], claim['client_id'])
        self.assertEqual(retry['number'], 2)
        self.assertEqual(len(self.store.rows('SELECT * FROM event_attempts WHERE key=?', (key,))), 2)

    def test_attempt_is_bound_to_recorded_thread_after_registration_changes(self):
        key = self.seeded(); claim = self.events.claim_native(key, 0)
        with self.store.transaction() as db:
            db.execute('UPDATE agents SET thread=?', (uid(),))
        # Evidence in a NEW thread cannot settle the old attempt.
        result = dict(self.accepted(claim), thread_id=uid())
        self.assertEqual(self.events.finish_native(claim, result), 'uncertain')
        self.assertEqual(self.row(key)['thread'], self.thread)
        other = self.seeded()
        with self.assertRaises(EventDeferred):
            self.events.claim_native(other, 0)

    def test_incomplete_seed_and_malformed_peer_home_barriers_prevent_claims(self):
        incomplete = self.seeded(complete=False)
        with self.assertRaises(EventDeferred):
            self.events.claim_native(incomplete, 0)
        affected = self.seeded(peer='affected'); unaffected = self.seeded(peer='unaffected')
        with self.store.transaction() as db:
            self.events.barrier(db, 1, 'affected', 'bad-id', '{broken', 'unparseable')
        with self.assertRaises(EventDeferred):
            self.events.claim_native(affected, 0)
        self.assertIsNotNone(self.events.claim_native(unaffected, 0))
        home_blocked = self.seeded(peer='elsewhere')
        with self.store.transaction() as db:
            self.events.barrier(db, 2, None, 'unknown', '[]', 'scope unknown')
        with self.assertRaises(EventDeferred):
            self.events.observe_wait(home_blocked, 0)
        with self.store.transaction() as db:
            with self.assertRaises(EventConflict):
                self.events.barrier(db, 2, 'invented-narrow-scope', 'unknown', '[]', 'scope unknown')

    def test_native_id_purposes_serialize_with_operation_reservation(self):
        value = uid()
        request = {'id': value, 'version': 1, 'op': 'followup', 'source': 'remote',
                   'target': 'recipient', 'text': 'Synthetic ordinary work'}
        with self.store.transaction() as db:
            self.events.reserve_id(db, value, 'operation', digest(request), self.thread)
            self.store.reserve('remote', request, transaction=db)
        key = self.seeded()
        with patch('csl_bridge_event_state.uuid.uuid4', return_value=uuid.UUID(value)):
            with self.assertRaises(EventConflict):
                self.events.claim_native(key, 0)
        self.assertEqual(self.row(key)['state'], 'pending')
        other = self.seeded(); final = self.events.claim_native(other, 0)
        with self.assertRaises(EventConflict), self.store.transaction() as db:
            self.events.reserve_id(db, final['client_id'], 'operation', digest(request), self.thread)
        self.assertEqual(len(self.store.rows('SELECT * FROM operations')), 1)

    def test_id_collisions_with_legacy_mail_and_progress_are_refused(self):
        from csl_bridge_mailbox import schema
        schema(self.store)
        for kind in ('progress', 'mail'):
            with self.subTest(kind=kind):
                value = uid()
                with self.store.transaction() as db:
                    if kind == 'progress':
                        db.execute('INSERT INTO inbound(id,peer,body)VALUES(?,?,?)', (value, 'remote', encode({'kind': 'progress'})))
                    else:
                        db.execute('INSERT INTO mail_messages(id,direction,peer,source,target,fingerprint,envelope,persisted_at)VALUES(?,?,?,?,?,?,?,?)',
                                   (value, 'in', 'remote', 'remote', 'recipient', 'fingerprint', '{}', 'synthetic'))
                key = self.seeded()
                with patch('csl_bridge_event_state.uuid.uuid4', return_value=uuid.UUID(value)):
                    with self.assertRaises(EventConflict):
                        self.events.claim_native(key, 0)
                self.assertEqual(self.row(key)['state'], 'pending')

    def test_failed_attempt_write_rolls_back_claim_and_reservation(self):
        key = self.seeded()
        with self.store.transaction() as db:
            db.execute("CREATE TRIGGER fail_attempt BEFORE INSERT ON event_attempts BEGIN SELECT RAISE(ABORT,'synthetic write failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.events.claim_native(key, 0)
        self.assertEqual(self.row(key)['state'], 'pending')
        self.assertEqual(self.store.rows('SELECT * FROM event_native_ids'), [])

    def test_failed_success_write_preserves_uncertainty(self):
        key = self.seeded(); claim = self.events.claim_native(key, 0)
        with self.store.transaction() as db:
            db.execute("CREATE TRIGGER fail_success BEFORE UPDATE ON event_effects BEGIN SELECT RAISE(ABORT,'synthetic commit failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.events.finish_native(claim, self.accepted(claim))
        self.assertEqual(self.row(key)['state'], 'uncertain')
        self.assertEqual(self.store.rows('SELECT state FROM event_attempts'), [{'state': 'uncertain'}])

    def test_busy_database_does_not_create_claim(self):
        key = self.seeded()
        with closing(Store(self.home)) as other:
            other.db.execute('PRAGMA busy_timeout=1')
            events = EventState(other)
            with self.store.transaction():
                with self.assertRaises(sqlite3.OperationalError):
                    events.claim_native(key, 0)
        self.assertEqual(self.row(key)['state'], 'pending')

    def test_recovery_rotates_past_32_stuck_keys_and_wraps_under_arrivals(self):
        keys = [self.seeded() for _ in range(40)]
        for key in keys:
            self.events.claim_native(key, 0)
        first = self.events.recovery_batch('synthetic')
        self.assertEqual([row['key'] for row in first], keys[:32])
        extra = self.seeded(); self.events.claim_native(extra, 0)
        second = self.events.recovery_batch('synthetic')
        self.assertEqual([row['key'] for row in second], keys[32:])
        third = self.events.recovery_batch('synthetic')
        self.assertEqual([row['key'] for row in third], keys[:32])
        fourth = self.events.recovery_batch('synthetic')
        self.assertEqual([row['key'] for row in fourth], keys[32:] + [extra])

    def test_recovery_excludes_settled_empty_deferred_and_wait_work(self):
        for state in ('native_delivered', 'wait_delivered', 'receipt_available', 'deferred', 'seeding', 'legacy_suppressed'):
            self.seeded(state=state)
        self.events.observe_wait(self.seeded(), 0)
        self.assertEqual(self.events.recovery_batch('synthetic'), [])

    def test_actual_wait_native_race_commits_one_claim(self):
        for _ in range(8):
            key = self.seeded()
            start = threading.Barrier(2)
            def compete(mode):
                with closing(Store(self.home)) as store:
                    events = EventState(store)
                    start.wait()
                    try:
                        return events.claim_native(key, 0) if mode == 'native' else events.observe_wait(key, 0)
                    except EventDeferred:
                        return None
            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(compete, ('native', 'wait')))
            self.assertEqual(len(self.store.rows('SELECT * FROM event_attempts WHERE key=?', (key,))), 1)
            self.assertIn(self.row(key)['state'], ('uncertain', 'wait_delivered'))

    def test_empty_receipt_observation_is_not_native_or_narrative_acceptance(self):
        key = self.seeded(state='receipt_available')
        result = self.events.observe_wait(key, 0)
        self.assertFalse(result['narrative_accepted'])
        self.assertEqual(self.row(key)['receipt_observed'], 1)
        self.assertEqual(self.row(key)['state'], 'receipt_available')
        self.assertEqual(self.store.rows('SELECT * FROM event_attempts'), [])
        with self.assertRaises(EventDeferred):
            self.events.claim_native(key, 0)

    def test_unkeyable_blob_and_invalid_scope_create_home_barrier(self):
        key = self.seeded()
        with self.store.transaction() as db:
            self.events.barrier(db, 1, 'invalid/path', 'bad-id', b'\xff', 'not text')
        self.assertIsNone(self.store.rows('SELECT peer FROM event_history_barriers')[0]['peer'])
        with self.assertRaises(EventDeferred):
            self.events.claim_native(key, 0)

    def test_final_wire_id_is_not_a_progress_native_purpose(self):
        value = uid()
        body = {'kind': 'final'}
        with self.store.transaction() as db:
            db.execute('INSERT INTO inbound(id,peer,body)VALUES(?,?,?)', (value, 'remote', encode(body)))
        with self.assertRaises(EventConflict), self.store.transaction() as db:
            self.events.reserve_id(db, value, 'progress', digest(body), self.thread)

    def test_reserving_id_does_not_create_optional_mail_schema(self):
        with self.store.transaction() as db:
            self.events.reserve_id(db, uid(), 'operation', 'synthetic binding', self.thread)
        self.assertEqual(self.store.rows("SELECT name FROM sqlite_master WHERE name='mail_messages'"), [])

    def test_recovery_excludes_newly_barred_or_incompletely_seeded_attempts(self):
        affected = self.seeded(peer='affected'); other = self.seeded(peer='other')
        incomplete = self.seeded(peer='incomplete')
        for key in (affected, other, incomplete):
            self.events.claim_native(key, 0)
        with self.store.transaction() as db:
            self.events.barrier(db, 1, 'affected', None, '{bad', 'unknown earlier effect')
            db.execute('UPDATE event_effects SET seed_complete=0 WHERE key=?', (incomplete,))
        self.assertEqual([row['key'] for row in self.events.recovery_batch('synthetic')], [other])

    def test_recovery_limit_bounds_candidates_before_barrier_filter(self):
        blocked = [self.seeded(peer='barred') for _ in range(32)]
        eligible = self.seeded(peer='eligible')
        for key in blocked + [eligible]:
            self.events.claim_native(key, 0)
        with self.store.transaction() as db:
            self.events.barrier(db, 1, 'barred', 'unknown', '{bad', 'unresolved scope')
        self.assertEqual(self.events.recovery_batch('synthetic'), [])
        cursor = self.store.rows('SELECT after_seq FROM event_recovery_scans')[0]['after_seq']
        self.assertEqual(cursor, self.row(blocked[-1])['seq'])
        self.assertEqual([row['key'] for row in self.events.recovery_batch('synthetic')], [eligible])

    def test_blob_historical_native_id_refuses_without_attribute_error(self):
        value = uid()
        with self.store.transaction() as db:
            db.execute('INSERT INTO inbound(id,peer,body)VALUES(?,?,?)', (value, 'remote', b'\xff'))
        key = self.seeded()
        with patch('csl_bridge_event_state.uuid.uuid4', return_value=uuid.UUID(value)):
            with self.assertRaises(EventConflict):
                self.events.claim_native(key, 0)
        self.assertEqual(self.row(key)['state'], 'pending')

    def test_distinct_canonical_tasks_cannot_share_one_native_client_id(self):
        first = self.seeded(); second = self.seeded()
        with self.store.transaction() as db:
            # Even an identical supplied projection cannot erase the actual key
            # and task identity from the final Native-ID purpose binding.
            db.execute('UPDATE event_effects SET projection=? WHERE key=?', (self.row(first)['projection'], second))
        claim = self.events.claim_native(first, 0)
        with patch('csl_bridge_event_state.uuid.uuid4', return_value=uuid.UUID(claim['client_id'])):
            with self.assertRaises(EventConflict):
                self.events.claim_native(second, 0)
        self.assertEqual(self.row(second)['state'], 'pending')

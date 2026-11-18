"""Client-only waits and actual publisher adapters; private SQLite/fake RPC only."""
from contextlib import closing
import sqlite3
import threading
import time
import unittest
from unittest.mock import patch

import test_bridge as bridge_fixture
import test_bridge_event_delivery as delivery_fixture
import test_light_stream as light_fixture
from csl_bridge_engine import Connector, publish_task_completion, retry_publications
from csl_bridge_event_history import EventHistory, canonical_key
from csl_bridge_event_state import EventDeferred
from csl_bridge_store import Store, encode, decode


class WaitLivenessTests(unittest.TestCase):
    def setUp(self):
        self.fixture = delivery_fixture.DeliveryTests()
        self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        self.store = self.fixture.store
        with self.store.transaction() as db:
            db.executemany('INSERT INTO config VALUES(?,?)', [('identity', encode('synthetic')), ('runtime_socket', 'null')])

    def insert(self, events, delivery='pending'):
        with self.store.transaction() as db:
            db.executemany('INSERT INTO inbound(id,peer,body,delivery)VALUES(?,?,?,?)',
                [(e['id'], 'peer', encode(e), delivery) for e in events])

    def wait(self, timeout=.5):
        connector = Connector(self.fixture.home); self.addCleanup(connector.close)
        self.assertTrue(connector.client_only)
        start = time.monotonic()
        result = connector.local({'local_op': 'wait', 'agent': 'recipient', 'after': 0, 'timeout': timeout})
        self.assertLess(time.monotonic() - start, timeout + .3)
        self.assertEqual(self.fixture.native.effects, [])
        return result, connector

    def test_catalog_300_progress_rows_in_first_client_only_wait(self):
        event = self.fixture.event(kind='progress', known_thread=True)
        self.insert([dict(event, id=delivery_fixture.uid()) for _ in range(300)])
        result, _ = self.wait()
        self.assertEqual(len(result['events']), 1)
        self.assertEqual(self.store.rows('SELECT watermark FROM event_catalog_meta')[0]['watermark'], 300)

    def test_130_alias_seed_in_first_client_only_wait(self):
        event = self.fixture.event(known_thread=True)
        self.insert([dict(event, id=delivery_fixture.uid()) for _ in range(130)])
        with patch.object(EventHistory, 'seed_step', autospec=True, side_effect=EventHistory.seed_step) as seeds:
            result, _ = self.wait()
        self.assertEqual(len(result['events']), 1)
        self.assertGreaterEqual(seeds.call_count, 3)
        self.assertTrue(all(call.kwargs.get('limit', 64) <= 64 for call in seeds.call_args_list))

    def test_64_permanent_refusals_before_good_progress(self):
        refused = [dict(self.fixture.event(known_thread=True), turn_id=delivery_fixture.uid()) for _ in range(64)]
        good = self.fixture.event(kind='progress', known_thread=True)
        self.insert([*refused, good])
        result, _ = self.wait()
        self.assertEqual(result['events'][0]['event'], good)
        self.assertEqual(self.store.rows("SELECT count(*) AS n FROM inbound WHERE delivery='rejected'")[0]['n'], 64)

    def test_recipient_gap_gets_seed_slot_past_31_unrelated_jobs(self):
        unrelated = [dict(self.fixture.event(known_thread=True), target='other-recipient') for _ in range(40)]
        good = self.fixture.event(known_thread=True)
        self.insert([*unrelated, good])
        result, _ = self.wait()
        self.assertEqual(result['events'][0]['event'], good)

    def test_unknown_scope_retains_home_barrier_and_does_not_skip(self):
        good = self.fixture.event(kind='progress', known_thread=True)
        with self.store.transaction() as db:
            db.execute('INSERT INTO inbound(id,peer,body)VALUES(?,?,?)', (delivery_fixture.uid(), 'peer', '{}'))
        self.insert([good])
        result, _ = self.wait(.06)
        self.assertEqual(result['events'], [])
        self.assertTrue(self.store.rows('SELECT * FROM event_history_barriers'))
        self.assertEqual(self.store.rows('SELECT delivery FROM inbound WHERE id=?', (good['id'],)), [{'delivery': 'pending'}])

    def test_missing_original_holds_cursor_without_busy_polling(self):
        event = self.fixture.event(known_thread=True)
        self.insert([event])
        with self.store.transaction() as db:
            db.execute('DELETE FROM operations WHERE id=?', (event['task_id'],))
        from csl_bridge_event_delivery import EventDelivery
        with patch.object(EventDelivery, 'observe', autospec=True, side_effect=EventDelivery.observe) as observe:
            result, _ = self.wait(.06)
        self.assertEqual(result['events'], [])
        self.assertLessEqual(observe.call_count, 3)
        self.assertEqual(self.store.rows('SELECT delivery FROM inbound'), [{'delivery': 'pending'}])

    def test_uncertain_progress_holds_cursor_without_busy_polling(self):
        self.insert([self.fixture.event(kind='progress', known_thread=True)], 'uncertain')
        from csl_bridge_event_delivery import EventDelivery
        with patch.object(EventDelivery, 'observe', autospec=True, side_effect=EventDelivery.observe) as observe:
            result, _ = self.wait(.06)
        self.assertEqual(result['events'], [])
        self.assertLessEqual(observe.call_count, 3)


class PublisherLivenessTests(unittest.TestCase):
    def setUp(self):
        self.fixture = bridge_fixture.BridgeTests()
        self.fixture.setUp(); self.addCleanup(self.fixture.tearDown)
        self.connector = self.fixture.connector; self.store = self.connector.store

    def pair(self, corruption):
        bad, good = self.fixture.request(), self.fixture.request()
        a = self.connector.incoming('alice', bad); b = self.connector.incoming('alice', good)
        self.assertEqual(a['turn_id'], b['turn_id'])
        saved = self.store.rows('SELECT * FROM operations WHERE id=?', (bad['id'],))[0]
        with self.store.transaction() as db:
            if corruption == 'missing': db.execute('DELETE FROM operations WHERE id=?', (bad['id'],))
            elif corruption == 'hash': db.execute('UPDATE operations SET hash=? WHERE id=?', ('0' * 64, bad['id']))
            elif corruption == 'receipt': db.execute('UPDATE operations SET result=? WHERE id=?', ('[]', bad['id']))
            elif corruption == 'blob': db.execute('UPDATE operations SET request=? WHERE id=?', (b'bad', bad['id']))
            elif corruption == 'thread':
                reply = decode(saved['result']); reply['thread_id'] = delivery_fixture.uid()
                db.execute('UPDATE operations SET result=? WHERE id=?', (encode(reply), bad['id']))
        return bad, good, a['turn_id'], saved

    def assert_sibling(self, bad, good, state):
        self.assertEqual(self.store.rows('SELECT status FROM tasks WHERE id=?', (good['id'],)), [{'status': 'completed'}])
        self.assertEqual(len(self.store.rows('SELECT * FROM outbound')), 1)
        self.assertEqual(self.store.rows('SELECT state FROM event_publication_holds WHERE task=?', (bad['id'],)), [{'state': state}])
        self.assertEqual(self.store.rows('SELECT status FROM tasks WHERE id=?', (bad['id'],)), [{'status': 'running'}])

    def test_callback_isolates_known_corruptions_and_missing_original(self):
        # Separate real homes avoid sibling state leaking between subcases.
        for corruption in ('hash', 'receipt', 'blob', 'thread', 'missing'):
            with self.subTest(corruption=corruption):
                bad, good, turn, _ = self.pair(corruption)
                self.connector.answers[(self.fixture.thread, turn)] = 'Synthetic final'
                self.connector.completed('worker', turn, 'completed')
                self.assert_sibling(bad, good, 'deferred' if corruption == 'missing' else 'conflict')
                with self.store.transaction() as db:
                    db.execute('DELETE FROM outbound'); db.execute('DELETE FROM tasks'); db.execute('DELETE FROM operations'); db.execute('DELETE FROM event_publication_holds')

    def test_runtime_loop_survives_bad_task_and_publishes_sibling(self):
        bad, good, turn, _ = self.pair('hash')
        self.connector.factory = lambda _: self.fixture.runtime
        self.connector.answers[(self.fixture.thread, turn)] = 'Synthetic final'
        self.fixture.runtime.events.put({'method': 'turn/completed', 'params': {'threadId': self.fixture.thread, 'turn': {'id': turn, 'status': 'completed'}}})
        errors = []
        def run():
            try: self.connector.runtime_loop()
            except BaseException as error: errors.append(error)
        thread = threading.Thread(target=run); thread.start()
        try:
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and not self.store.rows('SELECT * FROM outbound'): time.sleep(.01)
            self.assertTrue(thread.is_alive()); self.assertEqual(errors, [])
            self.assert_sibling(bad, good, 'conflict')
        finally:
            self.connector.stop.set(); thread.join(3)
        self.assertFalse(thread.is_alive())

    def test_conflict_is_parked_with_one_evidence_row_even_after_repair(self):
        bad, good, turn, saved = self.pair('hash')
        publish_task_completion(self.store, 'worker', self.fixture.thread, turn, 'completed', 'Synthetic final')
        evidence = self.store.rows('SELECT * FROM event_publication_holds')
        with self.store.transaction() as db: db.execute('UPDATE operations SET hash=? WHERE id=?', (saved['hash'], bad['id']))
        calls = len(self.fixture.runtime.calls)
        self.connector.completed('worker', turn, 'completed'); self.connector.reconcile(False)
        self.assertEqual(len(self.fixture.runtime.calls), calls)
        self.assertEqual(self.store.rows('SELECT * FROM event_publication_holds'), evidence)
        self.assert_sibling(bad, good, 'conflict')

    def test_deferred_proposal_recovers_metadata_only_after_original_restored(self):
        bad, good, turn, saved = self.pair('missing')
        publish_task_completion(self.store, 'worker', self.fixture.thread, turn, 'completed', 'Synthetic final')
        self.assert_sibling(bad, good, 'deferred')
        with self.store.transaction() as db:
            db.execute('INSERT INTO operations(id,peer,hash,request,state,result)VALUES(?,?,?,?,?,?)', tuple(saved[k] for k in ('id','peer','hash','request','state','result')))
        calls = len(self.fixture.runtime.calls)
        retry_publications(self.store)
        self.assertEqual(len(self.fixture.runtime.calls), calls)
        self.assertEqual(len(self.store.rows('SELECT * FROM outbound')), 2)
        self.assertEqual(self.store.rows('SELECT state FROM event_publication_holds'), [{'state': 'published'}])

    def test_deferred_unknown_is_paced_without_native_or_repeated_evidence_writes(self):
        bad, good, turn, _ = self.pair('missing')
        publish_task_completion(self.store, 'worker', self.fixture.thread, turn, 'completed', 'Synthetic final')
        evidence = self.store.rows('SELECT * FROM event_publication_holds')
        calls = len(self.fixture.runtime.calls)
        for _ in range(3): self.connector.reconcile(False)
        self.assertEqual(self.store.rows('SELECT * FROM event_publication_holds'), evidence)
        self.assertEqual(len(self.fixture.runtime.calls), calls)
        self.assert_sibling(bad, good, 'deferred')

    def test_cached_completion_refuses_changed_task_turn(self):
        bad, good, turn, saved = self.pair('missing')
        publish_task_completion(self.store, 'worker', self.fixture.thread, turn, 'completed', 'Synthetic final')
        with self.store.transaction() as db:
            db.execute('INSERT INTO operations(id,peer,hash,request,state,result)VALUES(?,?,?,?,?,?)', tuple(saved[k] for k in ('id','peer','hash','request','state','result')))
            db.execute('UPDATE tasks SET turn=? WHERE id=?', (delivery_fixture.uid(), bad['id']))
        retry_publications(self.store)
        self.assert_sibling(bad, good, 'conflict')
        evidence = self.store.rows('SELECT * FROM event_publication_holds')[0]
        self.assertEqual(evidence['first_reason'], 'Original task receipt is missing')
        self.assertNotEqual(evidence['first_evidence'], evidence['evidence'])

    def test_hold_write_failure_does_not_finalize_task_or_swallow_store_error(self):
        bad, good, turn, _ = self.pair('hash')
        with self.store.transaction() as db:
            db.execute("CREATE TRIGGER fail_hold BEFORE INSERT ON event_publication_holds BEGIN SELECT RAISE(ABORT,'synthetic failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            publish_task_completion(self.store, 'worker', self.fixture.thread, turn, 'completed', 'Synthetic final')
        self.assertEqual(self.store.rows('SELECT * FROM event_publication_holds'), [])
        self.assertEqual(self.store.rows('SELECT * FROM outbound'), [])
        self.assertEqual({r['status'] for r in self.store.rows('SELECT status FROM tasks')}, {'running'})
        with self.store.transaction() as db: db.execute('DROP TRIGGER fail_hold')
        publish_task_completion(self.store, 'worker', self.fixture.thread, turn, 'completed', 'Synthetic final')
        self.assert_sibling(bad, good, 'conflict')

    def test_retry_rotation_reaches_task_33_without_native_history_reads(self):
        saved = []
        for _ in range(33):
            request = self.fixture.request(); receipt = self.connector.incoming('alice', request)
            row = self.store.rows('SELECT * FROM operations WHERE id=?', (request['id'],))[0]; saved.append(row)
            with self.store.transaction() as db: db.execute('DELETE FROM operations WHERE id=?', (request['id'],))
        publish_task_completion(self.store, 'worker', self.fixture.thread, receipt['turn_id'], 'completed', 'Synthetic final')
        last = saved[-1]
        with self.store.transaction() as db:
            db.execute('INSERT INTO operations(id,peer,hash,request,state,result)VALUES(?,?,?,?,?,?)', tuple(last[k] for k in ('id','peer','hash','request','state','result')))
        before = len(self.fixture.runtime.calls)
        retry_publications(self.store); self.assertEqual(self.store.rows('SELECT * FROM outbound'), [])
        retry_publications(self.store); self.assertEqual(len(self.store.rows('SELECT * FROM outbound')), 1)
        self.assertEqual(len(self.fixture.runtime.calls), before)

    def test_malformed_completion_turn_never_broadens_task_scope(self):
        _, _, _, _ = self.pair('hash')
        from csl_bridge_store import BridgeError
        for turn in (None, '', b'bad', 'not-a-uuid'):
            with self.subTest(turn=turn):
                with self.assertRaises(BridgeError):
                    publish_task_completion(self.store, 'worker', self.fixture.thread, turn, 'completed', 'Synthetic final')
                with self.assertRaises(BridgeError): self.connector.completed('worker', turn, 'completed')
        self.assertEqual(self.store.rows('SELECT * FROM event_publication_holds'), [])
        self.assertEqual(self.store.rows('SELECT * FROM outbound'), [])
        self.assertEqual({r['status'] for r in self.store.rows('SELECT status FROM tasks')}, {'running'})

    def test_schema_gate_is_not_converted_to_task_hold(self):
        _, _, turn, _ = self.pair('hash')
        with self.store.transaction() as db: db.execute('UPDATE event_catalog_meta SET version=2')
        with self.assertRaises(EventDeferred):
            publish_task_completion(self.store, 'worker', self.fixture.thread, turn, 'completed', 'Synthetic final')
        self.assertEqual(self.store.rows('SELECT * FROM event_publication_holds'), [])
        self.assertEqual(self.store.rows('SELECT * FROM outbound'), [])

    def test_failed_final_commit_keeps_cached_task_unfinalized(self):
        bad, good, turn, saved = self.pair('missing')
        publish_task_completion(self.store, 'worker', self.fixture.thread, turn, 'completed', 'Synthetic final')
        with self.store.transaction() as db:
            db.execute('INSERT INTO operations(id,peer,hash,request,state,result)VALUES(?,?,?,?,?,?)', tuple(saved[k] for k in ('id','peer','hash','request','state','result')))
            db.execute("CREATE TRIGGER fail_finalize BEFORE UPDATE OF status ON tasks BEGIN SELECT RAISE(ABORT,'synthetic failure'); END")
        with self.assertRaises(sqlite3.IntegrityError): retry_publications(self.store)
        self.assert_sibling(bad, good, 'deferred')
        with self.store.transaction() as db: db.execute('DROP TRIGGER fail_finalize')
        retry_publications(self.store)
        self.assertEqual(len(self.store.rows('SELECT * FROM outbound')), 2)


class LightPublisherLivenessTests(unittest.TestCase):
    def test_bad_task_does_not_block_sibling_or_other_home(self):
        fixture = light_fixture.StreamTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        requests = []
        for _ in range(2):
            request = {'version': 1, 'op': 'followup', 'id': delivery_fixture.uid(), 'source': 'coordinator', 'target': 'worker', 'text': 'Synthetic completion'}
            fixture.guest.dispatch('hub', {'type': 'request', 'request': request, 'origin': 'hub', 'destination': 'env-a'}); requests.append(request)
        home, _ = fixture.guest.agent('worker')
        with closing(Store(home)) as store:
            with store.transaction() as db: db.execute('UPDATE operations SET hash=? WHERE id=?', ('0' * 64, requests[0]['id']))
            fixture.endpoints['env-a'].history[0]['status'] = 'completed'
            # A missing home is an independent home failure, not a batch failure.
            fixture.guest.config['bridges'] = {'missing': {'home': str(home) + '-missing', 'agents': []}, **fixture.guest.config['bridges']}
            fixture.guest.finish_pending()
            self.assertEqual(len(store.rows('SELECT * FROM outbound')), 1)
            self.assertEqual(store.rows('SELECT state FROM event_publication_holds'), [{'state': 'conflict'}])

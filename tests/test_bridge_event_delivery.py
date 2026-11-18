"""Actual shared delivery store with synthetic, bounded Native callbacks."""
from contextlib import closing
from pathlib import Path
import tempfile
import unittest
import uuid

from csl_bridge_event_delivery import EventDelivery
from csl_bridge_event_history import canonical_key
from csl_bridge_event_state import EventConflict, EventDeferred
from csl_bridge_rpc import RuntimeUnavailable
from csl_bridge_store import Store, encode, decode


def uid():
    return str(uuid.uuid4())


class SyntheticNative:
    def __init__(self):
        self.turns = {}; self.effects = []; self.reads = []; self.lose_ack = False
        self.online = True

    def available(self, event, thread):
        return self.online

    def send(self, claim, event):
        turn = uid(); self.effects.append((claim['thread'], claim['client_id'], dict(event)))
        self.turns.setdefault(claim['thread'], []).insert(0, {'id': turn, 'items': [
            {'type': 'userMessage', 'clientId': claim['client_id']}]})
        if self.lose_ack:
            self.lose_ack = False
            raise RuntimeUnavailable('Synthetic ACK lost after actual effect')
        return {'state': 'delivered', 'request_id': claim['client_id'], 'thread_id': claim['thread'], 'turn_id': turn}

    def find(self, thread, ids):
        self.reads.append((thread, list(ids)))
        found = {}
        for turn in self.turns.get(thread, [])[:20]:
            for item in turn['items']:
                if item.get('type') == 'userMessage' and item.get('clientId') in ids:
                    found.setdefault(item['clientId'], set()).add(turn['id'])
        return {key: next(iter(turns)) for key, turns in found.items() if len(turns) == 1}

    def age(self, thread, count=21):
        for _ in range(count):
            self.turns.setdefault(thread, []).insert(0, {'id': uid(), 'items': []})


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)/'bridge'
        self.store = Store(self.home, create=True); self.addCleanup(self.store.close)
        self.thread, self.remote = uid(), uid()
        with self.store.transaction() as db:
            db.execute('INSERT INTO agents VALUES(?,?)', ('recipient', self.thread))
        self.delivery = EventDelivery(self.store); self.native = SyntheticNative()

    def event(self, *, kind='final', known_thread=False, confirmed=True, text='Synthetic final'):
        request = {'version': 1, 'op': 'followup', 'id': uid(), 'source': 'recipient', 'target': 'worker', 'text': 'Synthetic original'}
        if known_thread:
            request['session'] = self.thread
        turn = uid(); self.store.reserve('outbound:peer', request)
        self.store.operation(request['id'], 'delivered' if confirmed else 'uncertain',
            {'state': 'delivered', 'request_id': request['id'], 'turn_id': turn, 'thread_id': self.remote} if confirmed else {})
        return {'id': uid(), 'task_id': request['id'], 'source': 'worker', 'target': 'recipient',
                'kind': kind, 'status': 'completed' if kind == 'final' else 'progress', 'text': text, 'turn_id': turn}

    def run_event(self, event, *, delivery=None):
        delivery = delivery or self.delivery
        delivery.accept('peer', event)
        if event['kind'] == 'progress':
            return delivery.progress(event['id'], available=self.native.available, send=self.native.send, find=self.native.find)
        return delivery.final('peer', event['task_id'], event['id'], available=self.native.available, send=self.native.send, find=self.native.find)

    def test_fallback_refusal_is_sticky_with_atomic_cursor_progress(self):
        event = self.event()
        with self.assertRaises(EventConflict):
            self.delivery.accept('peer', dict(event, turn_id=uid()), cursor=1)
        with self.assertRaises(EventConflict):
            self.delivery.accept('peer', event, cursor=2)
        self.assertEqual(self.store.rows('SELECT * FROM inbound'), [])
        self.assertEqual(self.store.rows('SELECT value FROM cursors')[0]['value'], 2)
        self.assertEqual(self.native.effects, [])

    def test_terminal_unpublished_final_does_not_hide_later_valid_progress(self):
        invalid = self.event(); valid = self.event(kind='progress')
        invalid = dict(invalid, turn_id=uid())
        with self.store.transaction() as db:
            for event in (invalid, valid):
                db.execute('INSERT INTO inbound(id,peer,body)VALUES(?,?,?)', (event['id'], 'peer', encode(event)))
        result = self.delivery.observe('recipient', 0)
        self.assertEqual(result['event'], valid)
        self.assertEqual(self.store.rows('SELECT delivery FROM inbound ORDER BY seq'), [{'delivery': 'rejected'}, {'delivery': 'delivered'}])
        self.assertEqual(self.native.effects, [])

    def test_legacy_matched_alias_identity_is_rechecked_after_history_io(self):
        for changed in ('body', 'thread'):
            with self.subTest(changed=changed):
                event = self.event(known_thread=True)
                matched = dict(event, id=uid())
                with self.store.transaction() as db:
                    for alias in (event, matched):
                        db.execute("INSERT INTO inbound(id,peer,body,delivery)VALUES(?,?,?,'uncertain')", (alias['id'], 'peer', encode(alias)))
                turn = uid()
                def find(thread, ids):
                    self.assertIn(matched['id'], ids)
                    with self.store.transaction() as db:
                        if changed == 'body':
                            db.execute('UPDATE inbound SET body=? WHERE id=?', (encode(dict(matched, text='Changed during Native lookup')), matched['id']))
                        else:
                            db.execute('UPDATE event_legacy_attempts SET thread=? WHERE wire_id=?', (uid(), matched['id']))
                    return {matched['id']: turn}
                self.delivery.accept('peer', event)
                with self.assertRaises((EventConflict, EventDeferred)):
                    self.delivery.final('peer', event['task_id'], event['id'], available=self.native.available, send=self.native.send, find=find)
                key = canonical_key('peer', event['task_id'])
                self.assertEqual(self.store.rows('SELECT state FROM event_effects WHERE key=?', (key,))[0]['state'], 'uncertain')
                self.assertEqual(self.native.effects, [])

    def test_event_scheduling_and_backoff_are_distinct_between_bridge_homes(self):
        from csl_light import Node
        import threading
        from types import SimpleNamespace
        submitted, due = [], []
        node = Node.__new__(Node); node.exec_lock = threading.RLock(); node.executing = set()
        node.config = {'links': {'hub': {}}}
        node.journal = SimpleNamespace(retry_due=lambda peer, token: due.append((peer, token)) or True)
        node.pool = SimpleNamespace(submit=lambda *args: submitted.append(args))
        job = {'peer': 'hub', 'key': 'same-key-in-separate-homes'}
        node.schedule_event(self.home, job)
        node.schedule_event(self.home.parent / 'other-home', job)
        self.assertEqual(len(submitted), 2)
        self.assertEqual(len(set(due)), 2)
        self.assertEqual({peer for peer, token in node.executing}, {'hub'})

    def test_routed_event_jobs_share_physical_peer_worker_cap(self):
        from csl_light import Node
        import threading
        from types import SimpleNamespace
        submitted = []
        node = Node.__new__(Node)
        node.exec_lock = threading.RLock(); node.executing = set()
        aliases = ['alias-' + str(n) for n in range(5)]
        node.config = {'links': dict.fromkeys(['hub', *aliases], {})}
        node.journal = SimpleNamespace(retry_due=lambda peer, token: True)
        node.pool = SimpleNamespace(submit=lambda *args: submitted.append(args))
        for n, alias in enumerate(aliases):
            node.schedule_event(self.home, {'peer': alias, 'transport_peer': 'hub', 'key': str(n)})
        self.assertEqual(len(submitted), 4)
        self.assertEqual({peer for peer, token in node.executing}, {'hub'})

    def test_known_original_missing_before_initial_seed_is_a_paced_deferral(self):
        event = self.event(known_thread=True)
        self.delivery.accept('peer', event)
        with self.store.transaction() as db:
            db.execute('DELETE FROM operations WHERE id=?', (event['task_id'],))
        with self.assertRaises(EventDeferred):
            self.delivery.final('peer', event['task_id'], event['id'], available=self.native.available, send=self.native.send, find=self.native.find)
        self.assertEqual(self.native.effects, [])
        self.assertEqual(self.native.reads, [])

    def test_settled_progress_replays_without_original_or_native_lookup(self):
        from unittest.mock import patch
        for mode in ('native', 'wait'):
            with self.subTest(mode=mode):
                event = self.event(kind='progress')
                self.native.online = mode == 'native'
                self.run_event(event)
                if mode == 'wait':
                    self.delivery.observe('recipient', 0)
                before = len(self.native.effects), len(self.native.reads)
                with self.store.transaction() as db:
                    db.execute('DELETE FROM operations WHERE id=?', (event['task_id'],))
                with patch.object(self.delivery.history, 'proof', side_effect=AssertionError('Settled progress re-proved original')):
                    self.assertEqual(self.run_event(event)['delivery'], 'native_delivered' if mode == 'native' else 'wait_delivered')
                    self.assertEqual(self.delivery.observe('recipient', 0)['event'], event)
                self.assertEqual((len(self.native.effects), len(self.native.reads)), before)
                # Hide this observed entry for the next loop's deliberate cursor.
                with self.store.transaction() as db:
                    db.execute('UPDATE event_publications SET visible=0 WHERE wire_id=?', (event['id'],))

    def test_progress_unknown_old_attempt_thread_is_not_restored_retroactively(self):
        event = self.event(kind='progress', known_thread=True)
        original = self.store.rows('SELECT * FROM operations WHERE id=?', (event['task_id'],))[0]
        with self.store.transaction() as db:
            db.execute('DELETE FROM operations WHERE id=?', (event['task_id'],))
            db.execute("INSERT INTO inbound(id,peer,body,delivery)VALUES(?,?,?,'uncertain')", (event['id'], 'peer', encode(event)))
        self.delivery.housekeeping()
        self.store.reserve('outbound:peer', decode(original['request']))
        self.store.operation(event['task_id'], 'delivered', decode(original['result']))
        self.assertEqual(self.run_event(event)['delivery'], 'uncertain')
        self.assertEqual(self.native.effects, [])
        self.assertEqual(self.native.reads, [])
        self.assertIsNone(self.store.rows('SELECT thread FROM event_progress_effects WHERE wire_id=?', (event['id'],))[0]['thread'])

    def test_progress_catalog_hash_contradiction_survives_repair(self):
        event = self.event(kind='progress', known_thread=True)
        original = self.store.rows('SELECT * FROM operations WHERE id=?', (event['task_id'],))[0]
        with self.store.transaction() as db:
            db.execute("INSERT INTO inbound(id,peer,body)VALUES(?,?,?)", (event['id'], 'peer', encode(event)))
            db.execute('UPDATE operations SET hash=? WHERE id=?', ('0' * 64, event['task_id']))
        self.delivery.housekeeping()
        with self.store.transaction() as db:
            db.execute('UPDATE operations SET hash=? WHERE id=?', (original['hash'], event['task_id']))
        with self.assertRaises(EventConflict):
            self.run_event(event)
        self.assertEqual(self.native.effects, [])

    def test_pending_final_refuses_changed_original_sender_binding(self):
        event = self.event(known_thread=True)
        self.delivery.accept('peer', event); self.delivery.history.catalog_step()
        key = canonical_key('peer', event['task_id']); self.delivery.history.seed_step(key)
        original = self.store.rows('SELECT result FROM operations WHERE id=?', (event['task_id'],))[0]
        self.store.operation(event['task_id'], 'delivered', dict(decode(original['result']), thread_id=uid()))
        with self.assertRaises(EventConflict):
            self.delivery.final('peer', event['task_id'], event['id'], available=self.native.available, send=self.native.send, find=self.native.find)
        self.assertEqual(self.native.effects, [])
        self.assertEqual(self.native.reads, [])

    def test_all_native_callbacks_run_outside_bridge_transactions(self):
        event = self.event(); self.delivery.accept('peer', event)
        def check():
            self.assertFalse(self.store.db.in_transaction)
        def available(body, thread):
            check(); return self.native.available(body, thread)
        def send(claim, body):
            check(); return self.native.send(claim, body)
        def find(thread, ids):
            check(); return self.native.find(thread, ids)
        self.native.lose_ack = True
        self.delivery.final('peer', event['task_id'], event['id'], available=available, send=send, find=find)
        self.assertTrue(self.delivery.final('peer', event['task_id'], event['id'], available=available, send=send, find=find)['native_accepted'])
        self.assertEqual(len(self.native.effects), 1)

    def test_failed_post_native_commit_recovers_evidence_without_reinjection(self):
        import sqlite3
        event = self.event(); self.delivery.accept('peer', event)
        def send(claim, body):
            result = self.native.send(claim, body)
            with self.store.transaction() as db:
                db.execute("CREATE TRIGGER fail_native_receipt BEFORE UPDATE ON event_effects WHEN NEW.state='native_delivered' BEGIN SELECT RAISE(ABORT,'synthetic receipt write failure'); END")
            return result
        with self.assertRaises(sqlite3.IntegrityError):
            self.delivery.final('peer', event['task_id'], event['id'], available=self.native.available, send=send, find=self.native.find)
        self.assertEqual(self.store.rows('SELECT state FROM event_effects')[0]['state'], 'uncertain')
        with self.store.transaction() as db:
            db.execute('DROP TRIGGER fail_native_receipt')
        self.assertTrue(self.run_event(event)['native_accepted'])
        self.assertEqual(len(self.native.effects), 1)
        self.assertEqual(len(self.store.rows('SELECT * FROM event_attempts')), 1)

    def test_wait_winner_prevents_native_effect_from_an_older_pending_snapshot(self):
        from concurrent.futures import ThreadPoolExecutor
        import threading
        event = self.event(); self.delivery.accept('peer', event)
        key = canonical_key('peer', event['task_id'])
        self.delivery.history.catalog_step(); self.delivery.history.seed_step(key)
        available_seen, release = threading.Event(), threading.Event()
        def deliver():
            with closing(Store(self.home)) as store:
                other = EventDelivery(store)
                def available(body, thread):
                    available_seen.set()
                    self.assertTrue(release.wait(5))
                    return True
                return other.final('peer', event['task_id'], event['id'], available=available, send=self.native.send, find=self.native.find)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(deliver)
            try:
                self.assertTrue(available_seen.wait(5))
                self.assertEqual(self.delivery.observe('recipient', 0)['event'], event)
            finally:
                release.set()
            self.assertEqual(future.result(timeout=5)['delivery'], 'wait_delivered')
        self.assertEqual(self.native.effects, [])
        self.assertEqual(self.store.rows('SELECT mode FROM event_attempts'), [{'mode': 'wait'}])

    def test_parallel_alias_can_reconcile_while_original_native_ack_is_pending(self):
        from concurrent.futures import ThreadPoolExecutor
        import threading
        event = self.event(); self.delivery.accept('peer', event)
        sent, release = threading.Event(), threading.Event()
        def deliver():
            with closing(Store(self.home)) as store:
                other = EventDelivery(store)
                def send(claim, body):
                    result = self.native.send(claim, body); sent.set()
                    self.assertTrue(release.wait(5))
                    return result
                try:
                    return other.final('peer', event['task_id'], event['id'], available=self.native.available, send=send, find=self.native.find)
                except EventDeferred:
                    return {'delivery': 'uncertain'}
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(deliver)
            try:
                self.assertTrue(sent.wait(5))
                self.assertTrue(self.run_event(dict(event, id=uid()))['native_accepted'])
            finally:
                release.set()
            future.result(timeout=5)
        self.assertTrue(self.run_event(event)['native_accepted'])
        self.assertEqual(len(self.native.effects), 1)
        self.assertEqual(len(self.store.rows('SELECT * FROM event_attempts')), 1)

    def test_process_exit_at_claim_input_and_receipt_boundaries_never_reinjects(self):
        import json
        import subprocess
        import sys
        script = r"""
import json,os,sys,uuid
sys.path.insert(0,sys.argv[1])
from csl_bridge_store import Store
from csl_bridge_event_delivery import EventDelivery
source,home,event_file,effects_file,phase=sys.argv[1:]
store=Store(home);delivery=EventDelivery(store)
event=json.load(open(event_file))
def available(event,thread):
    assert not store.db.in_transaction
    if phase=='before_claim':os._exit(30)
    return True
def send(claim,event):
    assert not store.db.in_transaction
    if phase=='after_claim':os._exit(31)
    result={'state':'delivered','request_id':claim['client_id'],'thread_id':claim['thread'],'turn_id':str(uuid.uuid4())}
    with open(effects_file,'a') as file:
        file.write(json.dumps(result)+'\n');file.flush();os.fsync(file.fileno())
    if phase=='after_input':os._exit(32)
    return result
delivery.accept('peer',event)
result=delivery.final('peer',event['task_id'],event['id'],available=available,send=send,find=lambda thread,ids:{})
assert phase=='after_delivered' and result['native_accepted']
os._exit(33)
"""
        for phase, exit_code in (('before_claim', 30), ('after_claim', 31), ('after_input', 32), ('after_delivered', 33)):
            with self.subTest(phase=phase):
                event = self.event(known_thread=True)
                self.delivery.accept('peer', event)
                event_file = Path(self.temp.name) / (phase + '-event.json')
                effects_file = Path(self.temp.name) / (phase + '-effects.jsonl')
                event_file.write_text(encode(event))
                child = subprocess.run([sys.executable, '-c', script, str(Path(__file__).resolve().parents[1]), str(self.home), str(event_file), str(effects_file), phase], capture_output=True, timeout=10)
                self.assertEqual((child.returncode, child.stderr), (exit_code, b''))
                key = canonical_key('peer', event['task_id'])
                state = self.store.rows('SELECT state FROM event_effects WHERE key=?', (key,))[0]['state']
                self.assertEqual(state, 'pending' if phase == 'before_claim' else 'native_delivered' if phase == 'after_delivered' else 'uncertain')
                receipts = [json.loads(line) for line in effects_file.read_text().splitlines()] if effects_file.exists() else []
                self.assertEqual(len(receipts), int(phase in ('after_input', 'after_delivered')))
                for receipt in receipts:
                    self.native.turns.setdefault(receipt['thread_id'], []).insert(0, {'id': receipt['turn_id'], 'items': [{'type': 'userMessage', 'clientId': receipt['request_id']}]})
                before_effects, before_reads = len(self.native.effects), len(self.native.reads)
                if phase == 'after_delivered':
                    with self.store.transaction() as db:
                        db.execute('DELETE FROM operations WHERE id=?', (event['task_id'],))
                with closing(Store(self.home)) as store:
                    result = self.run_event(event, delivery=EventDelivery(store))
                    replay = self.run_event(event, delivery=EventDelivery(store))
                self.assertEqual(result['delivery'], 'uncertain' if phase == 'after_claim' else 'native_delivered')
                self.assertEqual(replay['delivery'], result['delivery'])
                self.assertEqual(len(self.native.effects) - before_effects, int(phase == 'before_claim'))
                if phase == 'after_delivered':
                    self.assertEqual(len(self.native.reads), before_reads)
                self.assertEqual(len(self.store.rows('SELECT * FROM event_attempts WHERE key=?', (key,))), 1)

    def test_receipt_reads_current_canonical_state_without_driving_native(self):
        event = self.event(); self.native.lose_ack = True
        self.run_event(event)
        before = len(self.native.effects), len(self.native.reads)
        pending = self.delivery.receipt('peer', event['id'])
        self.assertEqual(pending['result']['delivery'], 'uncertain')
        self.assertFalse(pending['result']['native_accepted'])
        self.assertEqual((len(self.native.effects), len(self.native.reads)), before)
        self.assertIsNone(self.delivery.receipt('other', event['id']))
        self.run_event(event)
        after = len(self.native.effects), len(self.native.reads)
        accepted = self.delivery.receipt('peer', event['id'])
        self.assertTrue(accepted['result']['native_accepted'])
        receipt = accepted['result']['native_receipt']
        self.assertEqual(receipt['request_id'], self.native.effects[0][1])
        self.assertNotEqual(receipt['request_id'], event['id'])
        self.assertEqual(receipt['thread_id'], self.thread)
        self.assertEqual((len(self.native.effects), len(self.native.reads)), after)

    def test_receipt_never_borrows_a_settled_outcome_for_an_unseeded_alias(self):
        event = self.event(); self.run_event(event)
        bad = dict(event, id=uid(), text='Conflicting unclassified legacy alias')
        with self.store.transaction() as db:
            db.execute('INSERT INTO inbound(id,peer,body)VALUES(?,?,?)', (bad['id'], 'peer', encode(bad)))
        self.delivery.history.catalog_step()
        self.assertTrue(self.delivery.receipt('peer', event['id'])['result']['native_accepted'])
        self.assertFalse(self.delivery.receipt('peer', bad['id'])['result']['native_accepted'])
        self.delivery.history.seed_step(canonical_key('peer', event['task_id']))
        self.assertEqual(self.delivery.receipt('peer', bad['id'])['state'], 'rejected')
        self.assertTrue(self.delivery.receipt('peer', event['id'])['result']['native_accepted'])
        self.assertEqual(len(self.native.effects), 1)

    def test_two_ids_same_final_are_one_claim_and_one_actual_effect(self):
        event = self.event()
        self.assertEqual(self.run_event(event)['state'], 'delivered')
        alias = dict(event, id=uid())
        self.assertEqual(self.run_event(alias)['state'], 'delivered')
        self.assertEqual(len(self.native.effects), 1)
        self.assertNotEqual(self.native.effects[0][1], event['id'])
        self.assertEqual(self.native.effects[0][2], event)
        self.assertEqual(self.native.reads, [])

    def test_delivered_same_and_new_ids_replay_after_restart_and_history_age(self):
        event = self.event(); self.run_event(event); self.native.age(self.thread)
        with closing(Store(self.home)) as store:
            reopened = EventDelivery(store)
            self.assertEqual(self.run_event(event, delivery=reopened)['state'], 'delivered')
            self.assertEqual(self.run_event(dict(event, id=uid()), delivery=reopened)['state'], 'delivered')
        self.assertEqual(len(self.native.effects), 1)
        self.assertEqual(self.native.reads, [])

    def test_uncertain_after_lost_ack_does_not_reinject_beyond_history_window(self):
        event = self.event(); self.native.lose_ack = True
        self.assertEqual(self.run_event(event)['delivery'], 'uncertain')
        self.native.age(self.thread)
        with closing(Store(self.home)) as store:
            result = self.run_event(dict(event, id=uid()), delivery=EventDelivery(store))
        self.assertEqual(result['delivery'], 'uncertain')
        self.assertEqual(len(self.native.effects), 1)
        self.assertEqual(self.native.reads[0][0], self.thread)

    def test_uncertain_recovery_uses_recorded_client_id_not_wire_alias(self):
        event = self.event(); self.native.lose_ack = True
        self.run_event(event)
        result = self.run_event(dict(event, id=uid()))
        self.assertTrue(result['native_accepted'])
        self.assertEqual(len(self.native.effects), 1)
        self.assertEqual(self.native.reads[0][1], [self.native.effects[0][1]])

    def test_client_only_persistence_and_empty_receipt_do_not_fabricate_acceptance(self):
        self.native.online = False
        event = self.event()
        result = self.run_event(event)
        self.assertEqual(result['state'], 'persisted')
        self.assertFalse(result['native_accepted'])
        empty = self.event(text='')
        self.assertEqual(self.run_event(empty)['delivery'], 'receipt_available')
        self.assertEqual(self.native.effects, [])
        self.assertEqual(self.native.reads, [])

    def test_unknown_original_cannot_admit_or_invoke(self):
        event = self.event(confirmed=False)
        with self.assertRaises(EventDeferred):
            self.run_event(event)
        self.assertEqual(self.native.effects, [])
        self.assertEqual(self.store.rows('SELECT * FROM inbound'), [])

    def test_old_uncertain_without_thread_never_uses_current_registration(self):
        event = self.event()
        with self.store.transaction() as db:
            db.execute("INSERT INTO inbound(id,peer,body,delivery)VALUES(?,?,?,'uncertain')", (event['id'], 'peer', encode(event)))
        result = self.run_event(event)
        self.assertEqual(result['delivery'], 'uncertain')
        self.assertEqual(self.native.effects, [])
        self.assertEqual(self.native.reads, [])

    def test_legacy_known_thread_looks_up_actual_wire_id_after_reregistration(self):
        event = self.event(known_thread=True)
        old_thread = self.thread
        with self.store.transaction() as db:
            db.execute("INSERT INTO inbound(id,peer,body,delivery)VALUES(?,?,?,'uncertain')", (event['id'], 'peer', encode(event)))
            db.execute('UPDATE agents SET thread=?', (uid(),))
        self.native.turns[old_thread] = [{'id': uid(), 'items': [{'type': 'userMessage', 'clientId': event['id']}]}]
        result = self.run_event(event)
        self.assertTrue(result['native_accepted'])
        self.assertEqual(self.native.reads, [(old_thread, [event['id']])])
        self.assertEqual(self.native.effects, [])

    def test_ambiguous_same_id_under_multiple_turns_stays_uncertain(self):
        event = self.event(); self.native.lose_ack = True; self.run_event(event)
        client_id = self.native.effects[0][1]
        self.native.turns[self.thread].insert(0, {'id': uid(), 'items': [{'type': 'userMessage', 'clientId': client_id}]})
        self.assertEqual(self.run_event(event)['delivery'], 'uncertain')
        self.assertEqual(len(self.native.effects), 1)

    def test_ordinary_operation_client_id_cannot_be_borrowed_by_final(self):
        event = self.event()
        original = {'version': 1, 'op': 'followup', 'id': event['id'], 'source': 'worker', 'target': 'recipient', 'text': 'Different ordinary message'}
        self.store.reserve('peer', original)
        with self.assertRaises(EventConflict):
            self.run_event(event)
        self.assertEqual(self.native.effects, [])

    def test_wait_and_native_use_same_claim_with_deliberate_old_cursor_history(self):
        event = self.event(); self.native.online = False; self.run_event(event)
        first = self.delivery.observe('recipient', 0)
        self.assertEqual(first['event'], event)
        self.native.online = True
        self.assertEqual(self.run_event(dict(event, id=uid()))['delivery'], 'wait_delivered')
        self.assertEqual(self.delivery.observe('recipient', 0), first)
        self.assertEqual(self.native.effects, [])
        self.assertEqual(len(self.store.rows('SELECT * FROM event_attempts')), 1)

    def test_unresolved_native_claim_blocks_wait_cursor(self):
        event = self.event(); self.native.lose_ack = True; self.run_event(event)
        self.native.age(self.thread)
        with self.assertRaises(EventDeferred):
            self.delivery.observe('recipient', 0)
        self.assertEqual(len(self.native.effects), 1)

    def test_progress_replays_without_reinjecting_at_any_age(self):
        event = self.event(kind='progress'); self.run_event(event); self.native.age(self.thread)
        self.assertEqual(self.run_event(event)['delivery'], 'native_delivered')
        self.assertEqual(len(self.native.effects), 1)
        self.assertEqual(self.native.reads, [])

    def test_progress_uncertainty_is_evidence_only(self):
        event = self.event(kind='progress'); self.native.lose_ack = True; self.run_event(event)
        self.native.age(self.thread)
        self.assertEqual(self.run_event(event)['delivery'], 'uncertain')
        self.assertEqual(len(self.native.effects), 1)
        with self.assertRaises(EventDeferred):
            self.delivery.observe('recipient', 0)

    def test_progress_wait_observation_prevents_later_native_effect(self):
        event = self.event(kind='progress'); self.native.online = False; self.run_event(event)
        self.assertEqual(self.delivery.observe('recipient', 0)['event'], event)
        self.native.online = True
        self.assertEqual(self.run_event(event)['delivery'], 'wait_delivered')
        self.assertEqual(self.native.effects, [])

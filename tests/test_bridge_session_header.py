"""Sender session headers: optional field, per-peer opt-in, no notice in injected text."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from csl_bridge_engine import Connector
from csl_bridge_rpc import RuntimeUnavailable
from csl_bridge_store import NOTICE, BridgeError, Store, decode, encode, message_header, validate
from test_bridge import FakeRuntime

ROOT = Path(__file__).resolve().parents[1]
LEGACY_FIELDS = {'version', 'op', 'id', 'source', 'target', 'text'}


def uid():
    return str(uuid.uuid4())


def unavailable(_path):
    raise RuntimeUnavailable('Synthetic test owns the runtime')


def make_home(home, identity, runtime_socket, agents, peers):
    store = Store(home, create=True)
    with store.transaction() as db:
        db.executemany('INSERT INTO config VALUES(?,?)', [('identity', encode(identity)),
            ('runtime_socket', encode(runtime_socket))])
        db.executemany('INSERT INTO agents VALUES(?,?)', list(agents.items()))
        db.executemany('INSERT INTO peers VALUES(?,?)', [(n, encode(c)) for n, c in peers.items()])
    store.close()


class SessionHeaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.thread = uid()
        ops = ['followup', 'send_message', 'status', 'interrupt', 'watch', 'receipt']
        make_home(self.root / 'bridge', 'bob', str(self.root / 'never-personal.sock'), {'worker': self.thread}, {
            'alice': {'transport': 'lab', 'home': str(self.root / 'alice'), 'allow_agents': ['worker'], 'allow_ops': ops},
            'carol': {'transport': 'lab', 'home': str(self.root / 'carol'), 'allow_agents': ['worker'], 'allow_ops': ops,
                      'send_session': True}})
        self.connector = Connector(self.root / 'bridge')
        self.runtime = FakeRuntime(self.thread)
        self.connector.runtime = self.runtime

    def tearDown(self):
        self.connector.close()
        self.temp.cleanup()

    def request(self, op='followup', **kwargs):
        return {'version': 1, 'op': op, 'id': uid(), 'source': 'coordinator', 'target': 'worker',
                'text': 'Synthetic fictional review', **kwargs}

    def injected(self):
        return [params['input'][0]['text'] for method, params in self.runtime.calls
                if method in ('turn/start', 'turn/steer')]

    def test_header_helper_shapes(self):
        session, task = uid(), uid()
        self.assertEqual(message_header('fable-next', task, session),
                         '[Message from=fable-next; session=' + session + '; task=' + task + ']\n')
        self.assertEqual(message_header('fable-next', task), '[Message from=fable-next; task=' + task + ']\n')

    def test_validation_accepts_optional_session_and_still_rejects_unknown_fields(self):
        for op in ('followup', 'send_message'):
            validate(self.request(op))
            validate(self.request(op, session=uid()))
        invalid = [self.request(session='not-a-uuid'), self.request(session=uid().upper()),
                   self.request(session=7), self.request(session=None), self.request(session=''),
                   self.request(sender='alice'), self.request(session=uid(), sender='alice'),
                   {'version': 1, 'op': 'status', 'id': uid(), 'target': 'worker', 'session': uid()},
                   {'version': 1, 'op': 'receipt', 'id': uid(), 'request_id': uid(), 'session': uid()}]
        for request in invalid:
            with self.subTest(request=request), self.assertRaises(BridgeError):
                validate(request)
        missing = self.request(session=uid())
        del missing['text']
        with self.assertRaises(BridgeError):
            validate(missing)

    def test_followup_frames_sender_session_and_task_without_notice(self):
        session = uid()
        request = self.request(session=session, text='Please review the fixture')
        self.connector.incoming('alice', request)
        self.assertEqual(self.injected(), ['[Message from=coordinator; session=' + session + '; task=' +
                                           request['id'] + ']\nPlease review the fixture'])
        message = self.request('send_message', session=session, text='Clarification')
        self.connector.incoming('alice', message)
        self.assertEqual(self.injected()[-1], '[Message from=coordinator; session=' + session + '; task=' +
                         message['id'] + ']\nClarification')
        self.assertFalse(any(NOTICE in text for text in self.injected()))

    def test_old_peer_without_session_is_framed_without_inventing_one(self):
        request = self.request(text='Legacy peer text')
        self.connector.incoming('alice', request)
        self.assertEqual(self.injected(), ['[Message from=coordinator; task=' + request['id'] + ']\nLegacy peer text'])
        self.assertNotIn('session=', self.injected()[0])
        self.assertNotIn(NOTICE, self.injected()[0])

    def test_pending_message_keeps_session_when_flushed_later(self):
        session = uid()
        message = self.request('send_message', session=session, text='Held while idle')
        self.assertEqual(self.connector.incoming('alice', message)['state'], 'pending')
        self.connector.incoming('alice', self.request())
        self.connector.flush('worker')
        self.assertIn('[Message from=coordinator; session=' + session + '; task=' + message['id'] + ']\nHeld while idle',
                      self.injected())

    def outgoing_event(self, peer='alice', **extra):
        request = {'version': 1, 'op': 'followup', 'id': uid(), 'source': 'worker', 'target': 'coordinator',
                   'text': 'Synthetic delegated task'}
        self.connector.store.reserve('outbound:' + peer, request)
        turn = uid()
        self.connector.store.operation(request['id'], 'delivered', {'turn_id': turn})
        return {'seq': 1, 'event': {'id': uid(), 'task_id': request['id'], 'source': 'coordinator',
            'target': 'worker', 'kind': 'final', 'text': 'Synthetic final evidence',
            'turn_id': turn, 'status': 'completed', **extra}}

    def test_events_steered_into_running_turn_use_same_header(self):
        self.connector.incoming('alice', self.request())
        session = uid()
        with_session = self.outgoing_event(session=session)
        self.connector.accept_event('alice', with_session)
        legacy = self.outgoing_event()
        self.connector.accept_event('alice', {**legacy, 'seq': 2})
        texts = self.injected()[1:]
        self.assertEqual(texts, ['[Message from=coordinator; session=' + session + '; task=' +
                                 with_session['event']['task_id'] + ']\nSynthetic final evidence',
                                 '[Message from=coordinator; task=' + legacy['event']['task_id'] +
                                 ']\nSynthetic final evidence'])
        self.assertFalse(any(NOTICE in text for text in self.injected()))

    def test_event_session_is_validated_and_unknown_event_fields_rejected(self):
        for extra in ({'session': 'bad'}, {'session': 12}, {'sender': 'x'}, {'session': uid(), 'sender': 'x'}):
            with self.subTest(extra=extra), self.assertRaises(BridgeError):
                self.connector.accept_event('alice', self.outgoing_event(**extra))
        self.assertFalse(self.connector.store.rows('SELECT * FROM inbound'))

    def sent_by_local_call(self, peer, **extra):
        request = self.request(source='worker', target='coordinator', **extra)
        with patch('csl_bridge_engine.exchange', return_value={'state': 'delivered', 'request_id': request['id']}) as send:
            self.connector.local({'local_op': 'call', 'peer': peer, 'request': request})
        return send.call_args.args[2]

    def test_session_is_sent_only_to_opted_in_peers(self):
        legacy = self.sent_by_local_call('alice')
        self.assertEqual(set(legacy), LEGACY_FIELDS)
        opted = self.sent_by_local_call('carol')
        self.assertEqual(opted['session'], self.thread)
        validate(opted)
        message = self.request('send_message', source='worker', target='coordinator')
        with patch('csl_bridge_engine.exchange', return_value={'state': 'delivered'}) as send:
            self.connector.local({'local_op': 'call', 'peer': 'carol', 'request': message})
        self.assertEqual(send.call_args.args[2]['session'], self.thread)
        status = {'version': 1, 'op': 'status', 'id': uid(), 'target': 'coordinator'}
        with patch('csl_bridge_engine.exchange', return_value={'status': 'idle'}) as send:
            self.connector.local({'local_op': 'call', 'peer': 'carol', 'request': status})
        self.assertNotIn('session', send.call_args.args[2])

    def test_caller_cannot_choose_or_force_session(self):
        with patch('csl_bridge_engine.exchange') as send, self.assertRaises(BridgeError):
            self.connector.local({'local_op': 'call', 'peer': 'carol',
                                  'request': self.request(source='worker', target='coordinator', session=uid())})
        send.assert_not_called()
        # A matching claim is still not forwarded to a peer that has not opted in.
        self.assertEqual(set(self.sent_by_local_call('alice', session=self.thread)), LEGACY_FIELDS)

    def test_published_final_and_progress_events_follow_peer_opt_in(self):
        for peer in ('alice', 'carol'):
            task = self.request()
            receipt = self.connector.incoming(peer, task)
            progress = uid()
            self.connector.local({'local_op': 'progress', 'source': 'worker', 'task_id': task['id'],
                                  'id': progress, 'text': 'Synthetic progress'})
            self.connector.answers[(self.thread, receipt['turn_id'])] = 'Synthetic final'
            self.connector.completed('worker', receipt['turn_id'], 'completed')
            self.runtime.turn = None
            bodies = [decode(r['body']) for r in self.connector.store.rows('SELECT body FROM outbound WHERE peer=?', (peer,))]
            self.assertEqual(len(bodies), 2)
            for body in bodies:
                with self.subTest(peer=peer, kind=body['kind']):
                    if peer == 'carol':
                        self.assertEqual(body['session'], self.thread)
                    else:
                        self.assertNotIn('session', body)


class LabRoundTripTests(unittest.TestCase):
    def test_two_homes_deliver_registered_sender_session_end_to_end(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sender_thread, worker_thread = uid(), uid()
            ops = ['followup', 'send_message', 'status', 'receipt', 'watch']
            make_home(root / 'bob', 'bob-host', str(root / 'never.sock'), {'worker': worker_thread},
                      {'alice-host': {'transport': 'receive-only', 'allow_agents': ['worker'], 'allow_ops': ops}})
            make_home(root / 'alice', 'alice-host', None, {'fable-next': sender_thread},
                      {'bob-host': {'transport': 'lab', 'home': str(root / 'bob'), 'allow_agents': ['fable-next'],
                                    'allow_ops': ['status'], 'send_session': True}})
            receiver = Connector(root / 'bob', runtime_factory=unavailable)
            runtime = FakeRuntime(worker_thread)
            receiver.runtime = runtime
            sender = Connector(root / 'alice')
            try:
                receiver.start()
                request = {'version': 1, 'op': 'followup', 'id': uid(), 'source': 'fable-next', 'target': 'worker',
                           'text': 'Round trip'}
                result = sender.local({'local_op': 'call', 'peer': 'bob-host', 'request': request})
                self.assertEqual(result['state'], 'delivered')
                texts = [p['input'][0]['text'] for m, p in runtime.calls if m in ('turn/start', 'turn/steer')]
                self.assertEqual(texts, ['[Message from=fable-next; session=' + sender_thread + '; task=' +
                                         request['id'] + ']\nRound trip'])
            finally:
                sender.close()
                receiver.close()


class PeerSessionCliTests(unittest.TestCase):
    def csl(self, home, *args):
        return subprocess.run([sys.executable, str(ROOT / 'csl'), 'bridge', '--home', str(home), *args],
                              capture_output=True, text=True)

    def peer_config(self, home, peer):
        store = Store(home)
        try:
            return store.peers()[peer]
        finally:
            store.close()

    def test_send_session_is_off_by_default_and_toggled_offline_per_peer(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp) / 'home'
            for args in (['init', '--id', 'origin', '--client-only'], ['agent', 'fable-next', '--thread', uid()],
                         ['peer', 'plain', '--lab-home', tmp, '--allow-agent', 'fable-next', '--allow-op', 'followup'],
                         ['peer', 'modern', '--lab-home', tmp, '--allow-agent', 'fable-next', '--allow-op', 'followup',
                          '--send-session']):
                self.assertEqual(self.csl(home, *args).returncode, 0, args)
            self.assertNotIn('send_session', self.peer_config(home, 'plain'))
            self.assertIs(self.peer_config(home, 'modern')['send_session'], True)
            self.assertEqual(self.csl(home, 'peer-session', 'plain', '--enable').returncode, 0)
            self.assertIs(self.peer_config(home, 'plain')['send_session'], True)
            self.assertEqual(self.csl(home, 'peer-session', 'modern', '--disable').returncode, 0)
            self.assertNotIn('send_session', self.peer_config(home, 'modern'))
            self.assertEqual(self.peer_config(home, 'modern')['allow_ops'], ['followup'])
            self.assertNotEqual(self.csl(home, 'peer-session', 'missing', '--enable').returncode, 0)
            self.assertNotEqual(self.csl(home, 'peer-session', 'plain').returncode, 0)


if __name__ == '__main__':
    unittest.main()

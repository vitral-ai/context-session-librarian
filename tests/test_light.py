import importlib.util, json, os, socket, sys, tempfile, threading, time, unittest, uuid
from pathlib import Path
from unittest.mock import patch
P = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(P))
import csl_light as light
from csl_bridge_store import Store, encode

class Tests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.root.chmod(448)
        self.home = self.root / 'bridge'
        store = Store(self.home, create=True)
        self.thread = str(uuid.uuid4())
        store.db.execute('INSERT INTO agents VALUES(?,?)', ('worker', self.thread))
        store.db.execute('INSERT INTO config VALUES(?,?)', ('runtime_socket', 'null'))
        store.close()
        self.cfg = {'node': 'env-a', 'home': str(self.root / 'light'), 'csl_source': str(P), 'default_agent': 'worker', 'bridges': {'env-a': {'home': str(self.home), 'agents': ['worker']}}, 'upstream': 'hub', 'links': {'hub': {'agents': []}}}
        self.node = light.Node(self.cfg)
        self.addCleanup(self.node.pool.shutdown, False)

    def req(self):
        return {'version': 1, 'op': 'followup', 'id': str(uuid.uuid4()), 'source': 'coordinator', 'target': 'worker', 'text': 'synthetic only', 'session': str(uuid.uuid4())}

    def test_replay_one_id_one_durable_row_and_payload_collision_refused(self):
        r = self.req()
        one = self.node.journal.put('in', 'hub', r['id'], r)
        two = self.node.journal.put('in', 'hub', r['id'], r)
        self.assertEqual(one['hash'], two['hash'])
        with self.node.journal.db() as db:
            self.assertEqual(db.execute('select count(*) from rpc').fetchone()[0], 1)
        r['text'] = 'changed'
        with self.assertRaises(ValueError):
            self.node.journal.put('in', 'hub', r['id'], r)

    def test_crash_restart_recovers_pending_and_cached_ack(self):
        r = self.req()
        j = self.node.journal
        j.put('out', 'hub', r['id'], r)
        new = light.Journal(j.path)
        self.assertEqual(len(new.pending('out')), 1)
        new.finish('out', 'hub', r['id'], {'ok': True, 'data': {'state': 'delivered'}})
        again = light.Journal(j.path)
        self.assertEqual(again.pending('out'), [])
        self.assertEqual(json.loads(again.get('out', 'hub', r['id'])['result'])['data']['state'], 'delivered')

    def test_mail_mirror_is_idempotent_in_existing_csl_inbox(self):
        r = {'version': 1, 'op': 'mail', 'message_version': 1, 'action': 'send', 'id': str(uuid.uuid4()), 'source': 'coordinator', 'target': 'worker', 'envelope': {'kind': 'message', 'body': 'synthetic', 'in_reply_to': None, 'grant_ref': None, 'created_at': '2026-11-17T00:00:00Z', 'expires_at': None}}
        self.node.mirror(str(self.home), 'light-router', r)
        self.node.mirror(str(self.home), 'light-router', r)
        with light.sql(self.home / 'bridge.sqlite') as db:
            self.assertEqual(db.execute('SELECT count(*) FROM mail_messages').fetchone()[0], 1)

    def test_mail_flush_reconciles_lost_send_response_without_resending(self):
        from csl_bridge_engine import Connector
        from csl_bridge_store import BridgeError
        from unittest.mock import Mock
        sender_home = self.root / 'sender'
        store = Store(sender_home, create=True)
        with store.transaction() as db:
            db.executemany('INSERT INTO config VALUES(?,?)',
                           [('identity', encode('coordinator')), ('runtime_socket', encode(None))])
            db.execute('INSERT INTO agents VALUES(?,?)', ('coordinator', str(uuid.uuid4())))
            db.execute('INSERT INTO peers VALUES(?,?)', ('env-a', encode({
                'transport': 'lab', 'home': str(self.home), 'allow_agents': ['coordinator'],
                'allow_sources': ['worker'], 'allow_ops': ['mail']})))
        store.close()
        runtime = Mock(side_effect=AssertionError('Mail must not invoke native tasks'))
        sender = Connector(sender_home, runtime_factory=runtime)
        self.addCleanup(sender.close)
        message_id = str(uuid.uuid4())
        sender.local({'local_op': 'mail_send', 'agent': 'coordinator', 'peer': 'env-a',
                      'target': 'worker', 'id': message_id, 'body': 'Synthetic lost-response test'})
        actions = []

        def exchange(config, origin, request):
            actions.append(request['action'])
            result = self.node.mirror(str(self.home), origin, request)
            if request['action'] == 'send':
                raise BridgeError('Synthetic lost response after durable receiver commit')
            return result

        with patch('csl_bridge_transport.exchange', side_effect=exchange):
            sender.mailbox.flush('env-a')
            self.assertEqual(sender.store.rows('SELECT state FROM operations WHERE id=?',
                                              (message_id,))[0]['state'], 'uncertain')
            sender.mailbox.flush('env-a')
            sender.mailbox.flush('env-a')
        operation = sender.store.rows('SELECT state,result FROM operations WHERE id=?', (message_id,))[0]
        self.assertEqual(operation['state'], 'delivered')
        self.assertEqual(json.loads(operation['result'])['message_id'], message_id)
        self.assertEqual(actions, ['send', 'receipt'])
        self.assertIsNotNone(sender.store.rows('SELECT persisted_at FROM mail_messages WHERE id=?',
                                               (message_id,))[0]['persisted_at'])
        with light.sql(self.home / 'bridge.sqlite') as db:
            self.assertEqual(db.execute('SELECT count(*) FROM mail_messages WHERE id=?',
                                        (message_id,)).fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT count(*) FROM tasks').fetchone()[0], 0)
        self.assertEqual(sender.store.rows('SELECT count(*) AS n FROM tasks')[0]['n'], 0)
        runtime.assert_not_called()

    def test_authenticated_env_cannot_claim_coordinator(self):
        self.node.config['node'] = 'hub'
        self.node.is_router = True
        self.node.config['links'] = {'env-a': {'agents': ['worker']}}
        r = self.req()
        with self.assertRaises(ValueError):
            self.node.authenticate('env-a', {'id': r['id'], 'request': r})

    def test_route_name_uuid_and_agent_collision(self):
        self.node.journal.route('reviewer', 'hub', '00000000-0000-4000-8000-000000000005')
        self.assertEqual(self.node.journal.resolve('00000000-0000-4000-8000-000000000005')['agent'], 'reviewer')
        with self.assertRaises(ValueError):
            self.node.journal.route('reviewer', 'other', '00000000-0000-4000-8000-000000000006')
        with self.assertRaises(ValueError):
            self.node.journal.resolve('unknown')

    def test_channel_ack_loss_replay_returns_cached_result_without_native_duplicate(self):
        r = self.req()
        frame = {'type': 'request', 'id': r['id'], 'request': r, 'origin': 'host-bridge', 'destination': 'env-a', 'source_session': r['session']}
        self.node.journal.put('in', 'hub', r['id'], frame)
        calls = []
        self.node.dispatch = lambda *args: calls.append(args) or {'state': 'delivered', 'request_id': r['id']}
        self.node.deliver('hub', r['id'])
        row = self.node.journal.put('in', 'hub', r['id'], frame)
        self.assertEqual(row['state'], 'done')
        self.assertEqual(len(calls), 1)

    def test_partial_writes_preserve_complete_frame(self):

        class Writer:

            def __init__(self):
                self.data = b''

            def write(self, value):
                n = min(3, len(value))
                self.data += bytes(value[:n])
                return n

            def flush(self):
                pass
        writer = Writer()
        channel = light.Channel(self.node, 'hub', None, writer, lambda: None)
        channel.send({'type': 'ping'})
        until = time.monotonic() + 1
        while not writer.data.endswith(b'\n') and time.monotonic() < until:
            time.sleep(.01)
        self.assertEqual(json.loads(writer.data), {'type': 'ping'})
        channel.close()

    def test_native_crash_after_accept_reconciles_client_id_before_retry(self):
        r = self.req()
        accepted = []

        class RPC:
            connected = True

            def call(inner, method, params):
                if method == 'thread/read':
                    return {'thread': {'id': self.thread}}
                if method == 'thread/turns/list':
                    return {'data': [{'id': 'turn1', 'items': [{'type': 'userMessage', 'clientId': r['id']}]}] if accepted else []}
                if method == 'turn/start':
                    accepted.append(params['clientUserMessageId'])
                    raise OSError('synthetic ACK loss after native accepted')
        self.node.native.client = lambda home: RPC()
        with self.assertRaises(OSError):
            self.node.native.inject('worker', r)
        result = self.node.native.inject('worker', r)
        self.assertTrue(result['reconciled'])
        self.assertEqual(accepted, [r['id']])

    def test_reconcile_keeps_newest_active_turn_when_older_page_has_stale_inprogress(self):
        request=self.req();steered=[]
        class RPC:
            def call(inner,method,params):
                if method=='thread/read':return {'thread':{'id':self.thread}}
                if method=='thread/turns/list':
                    if 'cursor'not in params:return {'data':[{'id':'current','status':'inProgress','items':[]}],'nextCursor':'older'}
                    return {'data':[{'id':'historical','status':'inProgress','items':[]}]}
                if method=='turn/steer':
                    steered.append(params['expectedTurnId']);return {'turnId':params['expectedTurnId']}
                raise AssertionError(method)
        self.node.native.client=lambda home:RPC()
        self.assertEqual(self.node.native.inject('worker',request,request['session'])['turn_id'],'current')
        self.assertEqual(steered,['current'])

    def test_disconnect_reconnect_replays_pending_over_real_socketpair(self):
        r = self.req()
        frame = {'type': 'request', 'id': r['id'], 'request': r, 'origin': 'host-bridge', 'destination': 'env-a'}
        self.node.journal.put('out', 'hub', r['id'], frame)

        def connect():
            left, right = socket.socketpair()
            right.settimeout(3)
            channel = light.Channel(self.node, 'hub', left.makefile('rb'), left.makefile('wb', buffering=0), lambda: left.shutdown(socket.SHUT_RDWR))
            thread = threading.Thread(target=channel.run, daemon=True)
            thread.start()
            return (left, right, channel, thread, right.makefile('rb'))
        left, right, c, t, reader = connect()
        while light.read_line(reader).get('type') != 'request':
            pass
        c.close()
        t.join(2)
        reader.close()
        right.close()
        left.close()
        self.assertEqual(len(self.node.journal.pending('out')), 1)
        left, right, c, t, reader = connect()
        while light.read_line(reader).get('type') != 'request':
            pass
        right.sendall((light.wire({'type': 'response', 'id': r['id'], 'result': {'ok': True, 'data': {'state': 'delivered'}}}) + '\n').encode())
        limit = time.monotonic() + 2
        while self.node.journal.pending('out') and time.monotonic() < limit:
            time.sleep(0.01)
        self.assertEqual(self.node.journal.pending('out'), [])
        c.close()
        t.join(2)
        reader.close()
        right.close()
        left.close()

    def test_delete_key_revokes_an_existing_forced_stream(self):
        import subprocess
        home = self.root / 'attach'
        home.mkdir()
        key = home / 'public-key'
        key.write_text('synthetic-public-key')
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(str(home / 'light.sock'))
        listener.listen(1)
        listener.settimeout(3)
        child = subprocess.Popen([sys.executable, '-I', '-B', str(P / 'csl_light.py'), '--home', str(home), 'attach', '--peer', 'hub', '--key-file', str(key)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        connection, _ = listener.accept()
        self.assertEqual(light.read_line(connection.makefile('rb'))['action'], 'attach')
        key.unlink()
        self.assertEqual(child.wait(timeout=3), 0)
        for stream in (child.stdin, child.stdout, child.stderr):
            stream.close()
        connection.close()
        listener.close()

    def test_guest_native_path_uses_current_envelope_and_keeps_legacy_task_tracking(self):
        r = self.req()
        frame = {'type': 'request', 'id': r['id'], 'request': r, 'origin': 'host-bridge', 'source_session': r['session']}
        result = {'state': 'delivered', 'request_id': r['id'], 'turn_id': str(uuid.uuid4()), 'thread_id': self.thread}
        accepted = []
        self.node.native.client = lambda home: object()
        self.node.native.inject = lambda target, request, session: accepted.append(session) or result
        self.assertEqual(self.node.dispatch('hub', frame), result)
        with light.sql(self.home / 'bridge.sqlite') as db:
            raw = db.execute('SELECT request FROM operations WHERE id=?', (r['id'],)).fetchone()[0]
            self.assertEqual(json.loads(raw)['session'],r['session'])
            self.assertEqual(db.execute('SELECT turn,status FROM tasks WHERE id=?', (r['id'],)).fetchone(), (result['turn_id'], 'running'))
        self.assertEqual(accepted, [r['session']])
        self.assertEqual(frame['request']['session'], r['session'])

    def test_native_render_is_exact_registered_sender_session_envelope(self):
        r = self.req()
        seen = []

        class RPC:

            def call(inner, method, params):
                if method == 'thread/read':
                    return {}
                if method == 'thread/turns/list':
                    return {'data': []}
                seen.append(params['input'][0]['text'])
                return {'turn': {'id': 'synthetic-turn'}}
        self.node.native.client = lambda home: RPC()
        self.node.native.inject('worker', r, r['session'])
        self.assertTrue(seen[0].startswith('CSL from coordinator (' + r['id'][:8] + '):\n'))

    def test_polling_closes_database_descriptors(self):
        before = len(list(Path('/proc/self/fd').iterdir()))
        for _ in range(400):
            self.node.agents()
            self.node.journal.pending('out')
            self.node.bridge_config(str(self.home))
        self.assertLessEqual(len(list(Path('/proc/self/fd').iterdir())), before + 3)

    def test_inbox_keeps_messages_visible_despite_background_query_traffic(self):
        import csl_light_cli
        (self.node.home / 'config.json').write_text(json.dumps(self.cfg))
        r = self.req()
        frame = {'request': r, 'source_session': r['session']}
        self.node.journal.put('in', 'hub', r['id'], frame)
        for _ in range(220):
            i = str(uuid.uuid4())
            self.node.journal.put('in', 'hub', i, {'request': {'op': 'watch', 'id': i}})
        result = csl_light_cli.inbox(self.node.home)
        self.assertEqual([m['id'] for m in result['messages']], [r['id']])
        self.assertEqual(csl_light_cli.inbox(self.node.home, after=result['cursor'])['messages'], [])

    def test_enrollment_requires_started_brief_and_registered_thread(self):
        import enroll_light
        registered = {'second-worker': self.thread}
        marker = {'agent': 'second-worker', 'thread': self.thread}
        self.assertEqual(enroll_light.check_brief('second-worker', registered, marker), self.thread)
        for bad in ({}, {'agent': 'second-worker', 'thread': str(uuid.uuid4())}):
            with self.assertRaises(ValueError):
                enroll_light.check_brief('second-worker', registered, bad)
        with self.assertRaises(ValueError):
            enroll_light.check_brief('second-worker', registered, marker, str(uuid.uuid4()))

    def test_cli_same_id_reuses_original_mail_timestamp_and_rejects_changed_body(self):
        import csl_light_cli
        (self.node.home / 'config.json').write_text(json.dumps(self.cfg))
        r = {'version': 1, 'op': 'mail', 'message_version': 1, 'action': 'send', 'id': str(uuid.uuid4()), 'source': 'worker', 'target': 'worker', 'envelope': {'kind': 'message', 'body': 'same content', 'in_reply_to': None, 'grant_ref': None, 'created_at': '2026-11-17T00:00:00Z', 'expires_at': None}}
        frame = {'request': r, 'origin': 'env-a'}
        self.node.journal.put('out', 'hub', r['id'], frame)
        self.node.journal.finish('out', 'hub', r['id'], {'ok': True, 'data': {'state': 'persisted'}})
        cached = csl_light_cli.cached_request(self.node.home, self.thread, None, r['id'], 'same content', 'send')
        self.assertEqual(cached['frame']['request']['envelope']['created_at'], '2026-11-17T00:00:00Z')
        self.assertEqual(cached['result']['data']['state'], 'persisted')
        with self.assertRaises(ValueError):
            csl_light_cli.cached_request(self.node.home, 'worker', None, r['id'], 'changed', 'send')

    def test_outbound_helper_persists_message_when_daemon_socket_is_absent(self):
        (self.node.home / 'config.json').write_text(json.dumps(self.cfg))
        r = self.req()
        r['source'] = 'worker'
        r['target'] = 'coordinator'
        result = light.local(self.node.home, {'action': 'exchange', 'peer': 'hub', 'origin': 'env-a', 'request': r}, timeout=0.05)
        self.assertEqual(result['data']['state'], 'queued')
        row = self.node.journal.get('out', 'hub', r['id'])
        self.assertIsNotNone(row)
        self.assertEqual(json.loads(row['payload'])['request'], r)
if __name__ == '__main__':
    unittest.main(verbosity=2)

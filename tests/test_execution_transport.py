"""Synthetic transport, never a native authorization or positive import test."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from csl_backup_core import canonical
import csl_execution as execution
import csl_execution_transport as transport


def request_value(root, *, bound=2_000_000_000):
    boot, now = execution.linux_clock()
    return dict(schema_version=1, operation='import', execution_id='re_'+'1'*64,
                import_key='ri_'+'2'*64, provider='codex', source_id='synthetic',
                package_size=7, package_sha256=hashlib.sha256(b'package').hexdigest(),
                owner='1'*24, host_id='ses_fixture',
                assignment_id='33333333-3333-4333-8333-333333333333', generation='1',
                boot_id=boot, cwd=str(root), cwd_device=str(root.stat().st_dev),
                cwd_inode=str(root.stat().st_ino), authority=dict(boot_id=boot,
                    start_boottime_ns=str(now), end_boottime_ns=str(now+bound)))


def frame(value):
    raw = canonical(value)
    return struct.pack('!I', len(raw)) + raw


def recv(sock):
    def exact(n):
        raw = b''
        while len(raw) < n:
            part = sock.recv(n-len(raw))
            if not part:
                raise EOFError()
            raw += part
        return raw
    n = struct.unpack('!I', exact(4))[0]
    if n > transport.CONTROL_LIMIT:
        raise ValueError('fixture control too large')
    return json.loads(exact(n))


@unittest.skipUnless(sys.platform == 'linux', 'private transport is Linux-only')
class TransportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path.home(), prefix='.fd9-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.req = execution.request(canonical(request_value(self.root)))
        self.parent, child = socket.socketpair()
        self.addCleanup(self.parent.close)
        self.parent.settimeout(3)
        # Unit-only: same-process socketpair peer is the actual creating PID.
        # The separate Go/Python fixture below exercises the real getppid check.
        with patch.object(os, 'getppid', return_value=os.getpid()):
            self.channel = transport.Channel(child.detach(), self.req,
                parent_pid=os.getpid(), parent_uid=os.getuid())
        self.addCleanup(self.channel.close)

    def hello(self, **changes):
        value = dict(type='hello', request_sha256=self.req.commitment, authority=self.req.value['authority'])
        value.update(changes)
        self.parent.sendall(frame(value))

    def server(self, fn):
        errors = []
        def run():
            try:
                fn()
            except BaseException as exc:
                errors.append(exc)
        thread = threading.Thread(target=run)
        thread.start()
        def join():
            thread.join(4)
            self.assertFalse(thread.is_alive(), 'fixture server stuck')
            self.assertEqual(errors, [])
        self.addCleanup(join)
        return thread

    def ack(self, message, **changes):
        value = dict(type='ok', seq=message['seq'], authority=self.req.value['authority'])
        value.update(changes)
        self.parent.sendall(frame(value))

    def admitted(self):
        self.hello()
        self.channel.admit()

    def test_exact_handshake_check_and_sticky_denial(self):
        self.admitted()
        def parent():
            message = recv(self.parent)
            self.assertEqual(message, dict(type='check', seq='1', phase='capture'))
            self.ack(message)
            recv(self.parent)
            self.parent.sendall(frame(dict(type='deny', reason='assignment_revoked')))
        self.server(parent)
        self.channel.check()
        with self.assertRaises(transport.Denied) as denied:
            self.channel.check()
        self.assertEqual(denied.exception.reason, 'assignment_revoked')
        with self.assertRaises(transport.Denied) as second:
            self.channel.check()
        self.assertIs(second.exception, denied.exception)

    def test_wrong_peer_closes_consumed_endpoint(self):
        a, b = socket.socketpair()
        with a:
            fd = b.detach()
            with self.assertRaisesRegex(transport.TransportError, 'control_peer'):
                transport.Channel(fd, self.req, parent_pid=os.getpid()+10000, parent_uid=os.getuid())
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_wrong_peer_uid_refuses(self):
        a,b=socket.socketpair()
        with a,patch.object(os,'getppid',return_value=os.getpid()):
            with self.assertRaisesRegex(transport.TransportError,'control_peer'):
                transport.Channel(b.detach(),self.req,parent_pid=os.getpid(),parent_uid=os.getuid()+1)

    def test_cancellation_is_sticky(self):
        self.admitted()
        cancelled=KeyboardInterrupt('fixture cancellation')
        def clock():
            raise cancelled
        self.channel.clock=clock
        for unused in range(2):
            with self.assertRaises(KeyboardInterrupt) as error:
                self.channel.check('effect')
            self.assertIs(error.exception,cancelled)

    def test_wrong_hello_never_admits(self):
        self.hello(request_sha256='0'*64)
        with self.assertRaisesRegex(transport.TransportError, 'hello_mismatch'):
            self.channel.admit()
        self.assertFalse(self.channel.admitted)

    def test_replayed_hello_fences_next_effect(self):
        self.admitted()
        self.hello()
        with self.assertRaisesRegex(transport.TransportError, 'unsolicited'):
            self.channel.check('effect')

    def test_all_control_frame_cuts_refuse(self):
        raw = frame(dict(type='hello', request_sha256=self.req.commitment, authority=self.req.value['authority']))
        # Every byte boundary, not just one cut in the header.
        for cut in range(len(raw)):
            with self.subTest(cut=cut):
                a, b = socket.socketpair()
                with a, patch.object(os, 'getppid', return_value=os.getpid()):
                    with transport.Channel(b.detach(), self.req, parent_pid=os.getpid(), parent_uid=os.getuid()) as ch:
                        a.sendall(raw[:cut])
                        a.shutdown(socket.SHUT_WR)
                        with self.assertRaisesRegex(transport.TransportError, 'parent_eof'):
                            ch.admit()

    def test_oversize_refuses_header_before_payload(self):
        self.parent.sendall(struct.pack('!I', transport.CONTROL_LIMIT+1))
        with self.assertRaisesRegex(transport.TransportError, 'control_limit'):
            self.channel.admit()

    def test_not_socket_is_closed_before_any_io(self):
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        with self.assertRaises(OSError):
            transport.Channel(fd,self.req,parent_pid=os.getpid(),parent_uid=os.getuid())
        with self.assertRaises(OSError):
            os.fstat(fd)

    def test_control_write_pressure_cannot_extend_original_bound(self):
        a, b = socket.socketpair()
        req = execution.request(canonical(request_value(self.root,bound=80_000_000)))
        with a, patch.object(os,'getppid',return_value=os.getpid()):
            with transport.Channel(b.detach(),req,parent_pid=os.getpid(),parent_uid=os.getuid()) as ch:
                a.sendall(frame(dict(type='hello',request_sha256=req.commitment,authority=req.value['authority'])))
                ch.admit()
                ch.sock.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,4096)
                while True:
                    try:
                        ch.sock.send(b'x'*4096)
                    except BlockingIOError:
                        break
                start=time.monotonic()
                with self.assertRaisesRegex(transport.TransportError,'original_bound_lost'):
                    ch.check('effect')
                self.assertLess(time.monotonic()-start,1)

    def test_duplicate_key_invalid_control(self):
        raw = b'{"type":"hello","type":"hello"}'
        self.parent.sendall(struct.pack('!I', len(raw))+raw)
        with self.assertRaisesRegex(transport.TransportError, 'control_invalid'):
            self.channel.admit()

    def test_sequence_bound_and_schema_mismatch(self):
        for change in [dict(seq='2'), dict(extra=1), dict(authority={})]:
            with self.subTest(change=change):
                # Each refusal is terminal, so each uses a fresh channel.
                a, b = socket.socketpair()
                with a, patch.object(os, 'getppid', return_value=os.getpid()):
                    with transport.Channel(b.detach(), self.req, parent_pid=os.getpid(), parent_uid=os.getuid()) as ch:
                        a.sendall(frame(dict(type='hello',request_sha256=self.req.commitment,authority=self.req.value['authority'])))
                        ch.admit()
                        def parent():
                            message = recv(a)
                            value = dict(type='ok',seq=message['seq'],authority=self.req.value['authority'])
                            value.update(change)
                            a.sendall(frame(value))
                        t = threading.Thread(target=parent)
                        t.start()
                        try:
                            with self.assertRaisesRegex(transport.TransportError, 'check_mismatch'):
                                ch.check()
                        finally:
                            t.join(3)
                            self.assertFalse(t.is_alive())

    def test_partial_queued_denial_immediately_fences(self):
        self.admitted()
        self.parent.sendall(b'\0')
        with self.assertRaisesRegex(transport.TransportError, 'partial'):
            self.channel.check('effect')

    def test_denial_reason_beats_expiry_if_already_observed(self):
        self.admitted()
        self.parent.sendall(frame(dict(type='deny',reason='revoked')))
        self.channel.clock = lambda: (self.req.value['boot_id'], self.channel.end)
        with self.assertRaises(transport.Denied):
            self.channel.check('effect')

    def test_boot_backwards_and_equality_refuse(self):
        self.admitted()
        self.channel.clock = lambda: ('different-boot', self.channel.last)
        with self.assertRaisesRegex(transport.TransportError, 'original_bound_lost'):
            self.channel.check()
        for clock in [lambda: (self.req.value['boot_id'], self.channel.last-1),
                      lambda: (self.req.value['boot_id'], self.channel.end)]:
            self.channel.clock = clock
            with self.assertRaisesRegex(transport.TransportError, 'original_bound_lost'):
                self.channel._local()

    def test_prehello_silence_uses_original_deadline(self):
        a, b = socket.socketpair()
        req = execution.request(canonical(request_value(self.root,bound=80_000_000)))
        start = time.monotonic()
        with a, patch.object(os, 'getppid', return_value=os.getpid()):
            with transport.Channel(b.detach(),req,parent_pid=os.getpid(),parent_uid=os.getuid()) as ch:
                with self.assertRaisesRegex(transport.TransportError,'original_bound_lost'):
                    ch.admit()
        self.assertLess(time.monotonic()-start, 1)


class GoPythonTransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sys.platform != 'linux' or not shutil.which('go'):
            if os.environ.get('CSL_REQUIRE_EXECUTION_PROCESS_TESTS') == '1':
                raise AssertionError('Linux and Go required for execution process fixture')
            raise unittest.SkipTest('requires Linux and Go for actual process fixture')
        cls.tmp = tempfile.TemporaryDirectory(dir=Path.home(), prefix='.gp-')
        cls.root = Path(cls.tmp.name)
        cls.binary = cls.root/'parent'
        cls.fixture = Path(__file__).parent/'fixtures'/'execution_fd9'
        build = subprocess.run(['go','build','-o',str(cls.binary),str(cls.fixture/'parent.go')],capture_output=True,text=True,timeout=90)
        if build.returncode:
            cls.tmp.cleanup()
            raise AssertionError(build.stderr)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def run_pair(self, scenario, cwd_suffix=None):
        with tempfile.TemporaryDirectory(dir=self.root,prefix='case-') as root:
            if cwd_suffix is not None:
                path=Path(root)/cwd_suffix
                path.mkdir(mode=0o700)
                root=str(path)
            result = subprocess.run([str(self.binary),sys.executable,str(self.fixture/'child.py'),
                str(Path(__file__).resolve().parents[1]),root,scenario],
                capture_output=True,text=True,timeout=12,
                env=dict(PATH=os.environ['PATH'],HOME=root,TMPDIR=root,PYTHONDONTWRITEBYTECODE='1'))
            self.assertEqual(result.returncode,0,result.stdout+'\n'+result.stderr)
            return json.loads(result.stdout)

    def test_real_go_python_fd_table_capture_and_request_result(self):
        result = self.run_pair('success')
        self.assertEqual(result['status'],'captured')
        self.assertEqual(result['fds'],list(range(3,10)))
        self.assertTrue(result['sealed'])
        self.assertTrue(result['input_offset_zero'])

    def test_private_publisher_result_and_parent_death_before_link(self):
        import hashlib
        import io
        import zipfile
        sid='11111111-1111-4111-8111-111111111111'
        body=(json.dumps({'type':'session_meta','payload':{'id':sid,'cwd':'/synthetic'}})+'\n'+
              json.dumps({'type':'response_item','payload':{'type':'message','role':'user','content':[]}})+'\n').encode()
        manifest=dict(format='codex-session-librarian',version=1,mode='raw',sessions=[dict(id=sid,provider='codex',title='synthetic',file='sessions/'+sid+'.jsonl',bytes=len(body),sha256=hashlib.sha256(body).hexdigest())])
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w') as archive:
            archive.writestr('manifest.json',json.dumps(manifest))
            archive.writestr('sessions/'+sid+'.jsonl',body)
        for scenario in ('publisher','death-publisher-effect'):
            with self.subTest(scenario=scenario),tempfile.TemporaryDirectory(dir=self.root,prefix='publish-') as path:
                root=Path(path);root.chmod(0o700)
                (root/'publisher.zip').write_bytes(stream.getvalue())
                result=subprocess.run([str(self.binary),sys.executable,str(self.fixture/'publisher.py'),
                    str(Path(__file__).resolve().parents[1]),path,scenario],capture_output=True,text=True,timeout=12,
                    env=dict(PATH=os.environ['PATH'],HOME=path,TMPDIR=path,PYTHONDONTWRITEBYTECODE='1'))
                self.assertEqual(result.returncode,0,result.stdout+'\n'+result.stderr)
                value=json.loads(result.stdout)
                if scenario=='publisher':
                    self.assertEqual(value['validation'],'native_unverified')
                    self.assertEqual(value['state'],'imported')
                    self.assertNotEqual(value['imported_native_id'],sid)
                    self.assertEqual(len(list((root/'codex').rglob('*.jsonl'))),1)
                    self.assertEqual(value,json.loads((root/'report.json').read_bytes()))
                else:
                    self.assertEqual(value,{'reaped':True,'signal':9,'effect':False})
                    self.assertEqual(list((root/'codex').rglob('*.jsonl')),[])
                    row=json.loads(next(root.glob('ri_*.json')).read_bytes())
                    self.assertEqual(row['state'],'publishing')
                    self.assertIsNone(row['receipt'])

    def test_real_result_backpressure_and_request_truncation(self):
        for scenario in ['result-pressure','request-pressure','request-truncated','request-header-truncated','request-trailing','request-mismatch','request-oversize','deny','close','wrong-seq','wrong-bound','partial-deny','prehello-silence','capture-tamper']:
            with self.subTest(scenario=scenario):
                result = self.run_pair(scenario)
                self.assertEqual(result['status'],'refused')
                expected={'deny':'execution_parent_denied:fixture_revoked',
                          'close':'execution_parent_eof',
                          'wrong-seq':'execution_check_mismatch',
                          'wrong-bound':'execution_check_mismatch',
                          'capture-tamper':'execution_package_changed',
                          'request-oversize':'execution_request_limit',
                          'request-truncated':'execution_request_truncated',
                          'request-header-truncated':'execution_request_truncated',
                          'request-trailing':'execution_request_trailing',
                          'request-mismatch':'execution_request_mismatch'}
                if scenario in expected:
                    self.assertEqual(result['error'],expected[scenario])

    def test_parent_death_before_hello_and_before_effect_kills_reaps(self):
        for scenario in ['death-prehello','death-effect']:
            with self.subTest(scenario=scenario):
                result = self.run_pair(scenario)
                self.assertTrue(result['reaped'])
                self.assertEqual(result['signal'],9)
                self.assertFalse(result['effect'])

    def test_parent_original_bound_supervises_stalled_child_bootstrap(self):
        result=self.run_pair('setup-stall')
        self.assertEqual(result,dict(status='refused',error='parent_original_bound_lost',parent_killed=True))

    def test_shared_exact_canonical_request_preimages(self):
        path=self.fixture/'request-canonical-v1.json'
        rows=json.loads(path.read_text())
        for row in rows:
            with self.subTest(case=row['name']):
                req=execution.request(canonical(row['request']))
                self.assertEqual(req.encoded.hex(),row['canonical_hex'])
                self.assertEqual(req.commitment,row['sha256'])
                pretty=json.dumps(row['request'],ensure_ascii=True,indent=2).encode()
                self.assertNotEqual(pretty,req.encoded)
                self.assertEqual(execution.request(pretty),req)
        result=subprocess.run([str(self.binary),'canonical-fixtures',str(path)],capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads(result.stdout),[row['sha256'] for row in rows])

    def test_real_go_python_all_accepted_cwd_characters(self):
        rows=json.loads((self.fixture/'request-canonical-v1.json').read_text())
        for row in rows:
            with self.subTest(case=row['name']):
                self.assertEqual(self.run_pair('success',row['cwd_suffix'])['status'],'captured')


@unittest.skipUnless(sys.platform=='linux','private transport is Linux-only')
class LateGuardTests(unittest.TestCase):
    def probe(self,operation,loss,clock='valid'):
        repo=Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(dir=Path.home(),prefix='.late-') as name:
            result=subprocess.run([sys.executable,'-I','-B',str(repo/'tests/fixtures/execution_fd9/late_guard.py'),str(repo),name,operation,loss,clock],capture_output=True,text=True,timeout=8)
            self.assertEqual(result.returncode,0,result.stdout+'\n'+result.stderr)
            row=json.loads(result.stdout)
            self.assertEqual(row['emitted_hex'],'',row)
            self.assertEqual(row['control_hex'],'',row)
            self.assertFalse(row['control_reset'],row)
            self.assertEqual(row['child']['memfd_creates'],0,row)
            expected={'deny':'execution_parent_denied:assignment_revoked','partial':'execution_control_unsolicited_or_partial','eof':'execution_parent_eof'}
            self.assertEqual(row['child']['error'],expected[loss],row)
            self.assertTrue(row['reaped'])

    def test_loss_during_last_clock_fences_real_result_and_guard_effects(self):
        for operation in ('result','guard','capture'):
            for loss in ('deny','partial','eof'):
                with self.subTest(operation=operation,loss=loss):self.probe(operation,loss)

    def test_queued_known_denial_beats_late_clock_expiry_or_failure(self):
        for clock in ('expired','failed'):
            with self.subTest(clock=clock):self.probe('result','deny',clock)

    def test_framing_and_selector_returns_repeat_final_control_fence(self):
        for operation in ('send','receive','wait'):
            for loss in ('deny','partial','eof'):
                with self.subTest(operation=operation,loss=loss):self.probe(operation,loss)

    def test_expected_denial_completed_during_clock_loss_keeps_its_reason(self):
        for clock in ('expired','failed'):
            with self.subTest(clock=clock):self.probe('receive-partial','deny',clock)

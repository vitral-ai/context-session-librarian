"""Owned helper/clock controls only; no account, provider or collector."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from csl_workload_observation_agent import Agent, AgentProcess
from csl_workload_observation_driver import AcceptanceDriver
from csl_workload_identity import canonical

ROOT = str(Path(__file__).resolve().parents[1])
PROBE = ('import json,socket,sys;sys.path.insert(0,sys.argv[1]);'
         'from csl_workload_runner import parent_death;'
         's=socket.socket(fileno=int(sys.argv[2]));s.recv(4096);parent_death(int(sys.argv[3]));'
         's.sendall(b"{\\"armed\\":true}");'
         '\nwhile s.recv(4096): s.sendall(b"{\\"pong\\":true}")')


class LifetimeTests(unittest.TestCase):
    def probe(self, cancel=False):
        armed, release = threading.Event(), threading.Event()
        instances = []
        initialize = AgentProcess.__init__
        def command(process, fd):
            return [sys.executable, '-B', '-S', '-c', PROBE, ROOT, str(fd), str(os.getpid())]
        def held(process, payload):
            initialize(process, payload); instances.append(process)
            end = time.monotonic()+3
            while time.monotonic() < end:
                if any(v.get('armed') is True for v in process.poll()): armed.set(); break
                time.sleep(.001)
            if not armed.is_set(): process.close(); raise AssertionError('owned helper did not arm')
            if not release.wait(3): process.close(); raise AssertionError('fixture barrier not released')
        with patch.object(AgentProcess, 'command', command), patch.object(AgentProcess, '__init__', held):
            agent = Agent({})
            try:
                self.assertTrue(armed.wait(3))
                self.assertIsNone(instances[0].process.poll())
                self.assertIsNone(agent.instance)
                if cancel:
                    started = time.monotonic(); agent.close()
                    self.assertLess(time.monotonic()-started, .5)
                release.set()
                end = time.monotonic()+2
                while agent.instance is None and time.monotonic() < end: time.sleep(.001)
                self.assertIsNotNone(agent.instance)
                if not cancel:
                    agent.send({'op': 'probe'})
                    pong = False
                    while time.monotonic() < end:
                        try:
                            if any(v.get('pong') is True for v in agent.poll()): pong = True; break
                        except (OSError, ValueError): break
                        time.sleep(.001)
                    print(json.dumps({'fixture': 'arm_before_launcher_return', 'armed': True,
                        'parent_alive': os.getpid() > 0, 'helper_return': agent.process.poll(),
                        'owner_thread_alive': agent.launcher.is_alive()}))
                    self.assertTrue(pong, 'armed helper lost while main parent alive')
                    self.assertTrue(agent.launcher.is_alive(), 'creating thread must own helper lifetime')
                    self.assertIsNone(agent.process.poll())
            finally:
                release.set(); agent.close(); agent.launcher.join(2)
                for process in instances: process.process.wait(timeout=2)
            self.assertFalse(agent.launcher.is_alive())
            self.assertEqual(instances[0].process.returncode, -9)

    def test_arm_before_launcher_return_preserves_healthy_helper(self): self.probe()
    def test_cancel_before_launcher_return_remains_nonblocking_and_reaped(self): self.probe(cancel=True)

    def test_true_parent_process_death_still_kills_and_reaps_helper(self):
        # Subreaper ownership is confined to this separate synthetic control.
        parent = ('import json,os,socket,sys,time;sys.path.insert(0,sys.argv[1]);'
            'from unittest.mock import patch;from csl_workload_observation_agent import Agent,AgentProcess;'
            'probe=sys.argv[2];original=AgentProcess.__init__;'
            '\ndef command(self,fd): return [sys.executable,"-B","-S","-c",probe,sys.argv[1],str(fd),str(os.getpid())]\n'
            'def initialized(self,payload):\n'
            ' original(self,payload);end=time.monotonic()+3\n'
            ' while time.monotonic()<end:\n'
            '  if any(v.get("armed") is True for v in self.poll()): return\n'
            '  time.sleep(.001)\n'
            ' raise RuntimeError("helper did not arm")\n'
            'with patch.object(AgentProcess,"command",command),patch.object(AgentProcess,"__init__",initialized):\n'
            ' a=Agent({});end=time.monotonic()+3\n'
            ' while a.instance is None and time.monotonic()<end: time.sleep(.001)\n'
            ' assert a.instance is not None and a.process.poll() is None and a.launcher.is_alive()\n'
            ' print(a.process.pid,flush=True);sys.stdin.readline();os._exit(0)')
        control = ('import ctypes,json,os,signal,subprocess,sys,time;'
            'assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0;'
            'p=subprocess.Popen([sys.executable,"-B","-S","-c",sys.argv[1],sys.argv[2],sys.argv[3]],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);'
            'pid=None;status=None;'
            '\ntry:\n'
            ' pid=int(p.stdout.readline());p.stdin.write("exit\\n");p.stdin.flush();p.wait(timeout=3);end=time.monotonic()+3\n'
            ' while time.monotonic()<end:\n'
            '  child,value=os.waitpid(pid,os.WNOHANG)\n'
            '  if child: status=value;break\n'
            '  time.sleep(.001)\n'
            ' print(json.dumps({"parent_exit":p.returncode,"helper_signal":os.WTERMSIG(status) if status is not None and os.WIFSIGNALED(status) else None}))\n'
            'finally:\n'
            ' if p.poll() is None: p.kill();p.wait(timeout=2)\n'
            ' if pid and status is None:\n'
            '  try: os.kill(pid,signal.SIGKILL);os.waitpid(pid,0)\n'
            '  except ProcessLookupError: pass\n')
        result = subprocess.run([sys.executable, '-B', '-S', '-c', control, parent, ROOT, PROBE],
            capture_output=True, text=True, timeout=12, env={'PATH': os.defpath, 'PYTHONDONTWRITEBYTECODE': '1'})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {'parent_exit': 0, 'helper_signal': 9})


class AcquisitionDeadlineTests(unittest.TestCase):
    def setUp(self):
        self.now = 19.9
        self.ids = [str(uuid.uuid4()) for _ in range(5)]
        self.driver = AcceptanceDriver(source='1'*64, scope='2'*64,
            account_id='3'*24, device_id='ses_fixture', session=self.ids[0])
        d = self.driver
        d.valid = d.started = True
        d.expected_worker, d.expected_seal, d.deadline = self.ids[1], self.ids[2], 20
        plan = {'snapshot': self.ids[0], 'revision': self.ids[3], 'generation': self.ids[4],
                'file_key': 'main', 'length': 1, 'sha256': '4'*64, 'last_seq': 1}
        d.expected_plan = plan
        self.ticket = str(uuid.uuid4())
        self.result = {'observation': {kind: {'canary_present': False, 'matched_identity': plan[key]}
                        for kind, key in zip(('snapshot', 'content', 'view'), ('snapshot', 'revision', 'generation'))},
                      'observation_plan': plan, 'observation_run': d.run, 'observation_seal': d.expected_seal,
                      'observation_context': hashlib.sha256(canonical(d.context)).hexdigest()}
        self.receipt = {'sequence': d.expected_worker, 'scope': d.context['scope'], 'ok': True, 'pending': 0,
                        'synced': 1, 'manifest': [{}], 'delivery_plan': plan, 'observation_pending': self.ticket,
                        **{k: self.result[k] for k in ('observation_run', 'observation_seal', 'observation_context')}}

    def poll(self, jump=False):
        self.driver.finals[self.ticket] = self.result
        if jump: self.now = 20.1

    def test_acquisition_crossing_original_worker_deadline_is_none(self):
        with patch.object(self.driver, 'poll', side_effect=lambda: self.poll(True)), \
             patch('csl_workload_observation_driver.boot_clock', side_effect=lambda: self.now, create=True):
            self.driver.consume(self.receipt, self.driver.expected_worker, complete=True, now=19.9)
        self.assertIsNone(self.driver.result)

    def test_publication_crossing_deadline_is_none_after_valid_candidate(self):
        candidate = self.driver._candidate
        def crossing(*args, **kwargs):
            value = candidate(*args, **kwargs); self.now = 20.1; return value
        with patch.object(self.driver, 'poll', side_effect=self.poll), \
             patch.object(self.driver, '_candidate', side_effect=crossing), \
             patch('csl_workload_observation_driver.boot_clock', side_effect=lambda: self.now):
            self.driver.consume(self.receipt, self.driver.expected_worker, complete=True, now=19.9)
        self.assertIsNone(self.driver.result)

    def test_timely_acquisition_and_publication_remain_valid(self):
        with patch.object(self.driver, 'poll', side_effect=self.poll), \
             patch('csl_workload_observation_driver.boot_clock', side_effect=lambda: self.now):
            self.driver.consume(self.receipt, self.driver.expected_worker, complete=True, now=19.9)
        self.assertEqual(self.driver.result, self.result['observation'])


if __name__ == '__main__': unittest.main()

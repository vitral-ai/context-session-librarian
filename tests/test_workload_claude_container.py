"""Synthetic gate/install fixtures for the Claude sole-producer overlay; not a native/image receipt."""
import base64
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest.mock import Mock
from unittest.mock import patch

from csl_sync_state import SyncError

ROOT = Path(__file__).resolve().parents[1]
OVERLAY = ROOT/'packaging/claude-sole-producer'
SID = '0b9f6b2a-1c3d-4e5f-8a7b-9c0d1e2f3a4b'
E = {'VITRAL_COLLECTOR_EXPECT_SESSION': SID}
PIN = ['--session-id', SID]


def module(name):
    spec = importlib.util.spec_from_file_location('claude_overlay_'+name, OVERLAY/(name+'.py'))
    result = importlib.util.module_from_spec(spec); spec.loader.exec_module(result)
    return result


def elf():
    value = bytearray(64); value[:6] = b'\x7fELF\x02\x01'; value[18:20] = b'\x3e\x00'
    return bytes(value)


class InstallTests(unittest.TestCase):
    def setUp(self):
        self.install = module('install')
        self.tmp = tempfile.TemporaryDirectory(prefix='claude-install-')
        self.addCleanup(self.tmp.cleanup); self.root = Path(self.tmp.name)

    def archive(self, members):
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode='w:gz') as archive:
            for name, body in members:
                info = tarfile.TarInfo(name); info.size = len(body); info.mode = 0o755
                archive.addfile(info, io.BytesIO(body))
        return data.getvalue()

    def row(self, raw, members):
        return {'url': 'https://example.invalid/x.tgz', 'size': len(raw),
                'sha256': hashlib.sha256(raw).hexdigest(),
                'integrity': 'sha512-'+base64.b64encode(hashlib.sha512(raw).digest()).decode(),
                'members': {name: len(body) for name, body in members},
                'install': {'package/claude': 'bin/claude', 'package/LICENSE.md': 'licenses/LICENSE.md'}}

    def run_install(self, raw, row, name='out'):
        spool = tempfile.TemporaryFile(); self.addCleanup(spool.close)
        spool.write(raw)
        self.install.check(spool, row)
        output = self.root/name; output.mkdir()
        self.install.unpack(spool, row, output)
        return output

    def test_pinned_inputs_match_profile_version(self):
        row = json.loads((OVERLAY/'inputs.json').read_text())
        self.assertIn('claude-code-linux-x64-2.1.308.tgz', row['url'])
        self.assertEqual(row['version'], '2.1.308 (Claude Code)')
        profiles = json.loads((ROOT/'packaging/workload-collector/profiles.json').read_text())
        self.assertEqual(profiles['claude'], 'claude-2.1.308-text-v1')

    def test_closed_inventory_installs_only_selected_files(self):
        members = [('package/claude', elf()), ('package/LICENSE.md', b'license'), ('package/package.json', b'{}')]
        raw = self.archive(members)
        output = self.run_install(raw, self.row(raw, members))
        self.assertEqual(sorted(str(p.relative_to(output)) for p in output.rglob('*') if p.is_file()),
                         ['bin/claude', 'licenses/LICENSE.md', 'native-manifest.json'])
        self.assertEqual(oct((output/'bin/claude').stat().st_mode & 0o777), '0o555')
        manifest = json.loads((output/'native-manifest.json').read_text())
        self.assertEqual(manifest['files']['bin/claude'], hashlib.sha256(elf()).hexdigest())

    def test_refusals(self):
        members = [('package/claude', elf()), ('package/LICENSE.md', b'license')]
        raw = self.archive(members)
        bad = self.row(raw, members); bad['sha256'] = '0'*64
        with self.assertRaises(ValueError): self.run_install(raw, bad, 'a')
        bad = self.row(raw, members); bad['integrity'] = 'sha512-AAAA'
        with self.assertRaises(ValueError): self.run_install(raw, bad, 'b')
        extra = members + [('package/unlisted', b'x')]
        raw2 = self.archive(extra); row2 = self.row(raw2, members); row2.update(size=len(raw2),
            sha256=hashlib.sha256(raw2).hexdigest(), integrity='sha512-'+base64.b64encode(hashlib.sha512(raw2).digest()).decode())
        with self.assertRaises(ValueError): self.run_install(raw2, row2, 'c')
        wrong = [('package/claude', b'#!/bin/sh\n'+b'x'*60), ('package/LICENSE.md', b'license')]
        raw3 = self.archive(wrong)
        with self.assertRaises(ValueError): self.run_install(raw3, self.row(raw3, wrong), 'd')
        missing = self.row(raw, members); missing['members']['package/README.md'] = 1
        with self.assertRaises(ValueError): self.run_install(raw, missing, 'e')

    def test_recipe_pins_parent_and_closed_context(self):
        recipe = (OVERLAY/'Containerfile').read_text()
        self.assertIn('a8/collector@sha256:07a751363f3b5f2eb931a1c2c62d8825916e5e76277d503f00c85baa68127aef', recipe)
        self.assertTrue(recipe.endswith('ENTRYPOINT ["/usr/local/bin/csl-claude-prepare"]\nCMD []\n'))
        ignore = (OVERLAY/'Containerfile.dockerignore').read_text().splitlines()
        self.assertEqual(ignore[0], '**')
        for name in ('install.py', 'inputs.json', 'prepare.py', 'version_check.py', 'csl-claude-prepare'):
            self.assertIn('!packaging/claude-sole-producer/'+name, ignore)


class GateTests(unittest.TestCase):
    def setUp(self):
        self.gate = module('prepare')
        self.tmp = tempfile.TemporaryDirectory(prefix='claude-gate-')
        self.addCleanup(self.tmp.cleanup); self.root = Path(self.tmp.name)

    def test_literal_argument_bounds_and_native_exec_refusal_before_effect(self):
        for argv in ([], ['--'], ['--prepare-seconds', '0', '--', 'x'],
                     ['--prepare-seconds', '01', '--', 'x'], ['--prepare-seconds', '1801', '--', 'x'],
                     ['--', '\x00'], ['--', 'x'*65537]):
            with self.assertRaises(self.gate.Refused): self.gate.arguments(argv)
        values = ['space value', ';no shell', '$(no exec)', '']
        self.assertEqual(self.gate.arguments(['--prepare-seconds', '10', '--', *values]), (10, values))
        calls = []
        def execute(*args): calls.append(args); raise RuntimeError('fixture exec boundary')
        with self.assertRaisesRegex(RuntimeError, 'fixture exec boundary'):
            self.gate.native_exec(['10.0', '1:2:1:2', '--', *values, *PIN], E, now=lambda: 1,
                                 validator=lambda env: ((1, 2), (1, 2)), execute=execute)
        self.assertEqual(calls, [(self.gate.CLAUDE, [self.gate.CLAUDE, *values, *PIN], E)])
        for deadline in ('0', 'nan', 'inf', '-1', '2000'):
            with self.assertRaises((self.gate.Refused, ValueError)):
                self.gate.native_exec([deadline, '1:2:1:2', '--', 'x', *PIN], E, now=lambda: 1,
                                     validator=lambda env: ((1, 2), (1, 2)), execute=execute)
        self.assertEqual(len(calls), 1)
        clock = [1]
        def late(_env):
            clock[0] = 11
            return ((1, 2), (1, 2))
        with self.assertRaises(self.gate.Refused):
            self.gate.native_exec(['10.0', '1:2:1:2', '--', 'x', *PIN], E, now=lambda: clock[0], validator=late, execute=execute)
        self.assertEqual(len(calls), 1)

    def test_session_binding_requires_exactly_the_owner_pinned_fresh_session(self):
        self.assertEqual(self.gate.session_binding(['-p', 'x', *PIN], E), SID)
        other = '1b9f6b2a-1c3d-4e5f-8a7b-9c0d1e2f3a4b'
        for values, env in ((['-p', 'x'], E), ([*PIN], {}), (['--session-id', other], E),
                            ([*PIN, *PIN], E), ([*PIN, '--resume', SID], E), ([*PIN, '-c'], E),
                            ([*PIN, '--fork-session'], E), (['--session-id='+SID], E),
                            ([*PIN, '--resume='+SID], E), (['--session-id'], E)):
            with self.subTest(values=values), self.assertRaises(self.gate.Refused):
                self.gate.session_binding(values, env)
        execute = Mock()
        with self.assertRaisesRegex(self.gate.Refused, 'session binding'):
            self.gate.native_exec(['10.0', '1:2:1:2', '--', '-p', 'x'], E, now=lambda: 1,
                                 validator=lambda _: ((1, 2), (1, 2)), execute=execute)
        execute.assert_not_called()

    def test_pre_exec_rejects_root_replacement_before_native_command(self):
        execute = Mock()
        with self.assertRaisesRegex(self.gate.Refused, 'mount changed'):
            self.gate.native_exec(['10.0', '1:2:1:2', '--', 'literal', *PIN], E,
                                 now=lambda: 1, validator=lambda _: ((1, 3), (1, 3)),
                                 execute=execute)
        execute.assert_not_called()

    def child(self, *, late=False, cancel_on_handoff=False, idle=False):
        ready, handed = self.root/'ready', self.root/'handed'
        code = '''import importlib.util,json,os,signal,sys
from pathlib import Path
s=importlib.util.spec_from_file_location('gate',sys.argv[1]);g=importlib.util.module_from_spec(s);s.loader.exec_module(g)
ready,handed=map(Path,sys.argv[2:4]); count=[0]; now=[1.0]
def validate(env):
 count[0]+=1
 ready.write_text('ready')
 if sys.argv[4]=='late' and count[0]==2: now[0]=20.0
 return ((1, 2), (1, 2))
def run(argv):
 if sys.argv[5]=='cancel': os.kill(os.getpid(),signal.SIGTERM)
 handed.write_text(json.dumps(argv)); return 0
try: result=g.prepare(['--prepare-seconds','1' if sys.argv[4]=='idle' else '10','--','space value',';no shell','$(no exec)','--session-id','0b9f6b2a-1c3d-4e5f-8a7b-9c0d1e2f3a4b'],{'VITRAL_COLLECTOR_EXPECT_SESSION':'0b9f6b2a-1c3d-4e5f-8a7b-9c0d1e2f3a4b'},now=g.clock if sys.argv[4]=='idle' else lambda:now[0],validator=validate,run=run)
except g.Cancelled as e: result=128+e.signum
except g.Refused: result=2
os._exit(result)
'''
        process = subprocess.Popen([sys.executable, '-B', '-E', '-s', '-S', '-c', code,
            str(OVERLAY/'prepare.py'), str(ready), str(handed), 'idle' if idle else 'late' if late else 'normal',
            'cancel' if cancel_on_handoff else 'normal'],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={'PATH': os.defpath})
        self.addCleanup(lambda: self.cleanup_child(process))
        until = time.monotonic()+3
        while not ready.exists() and process.poll() is None and time.monotonic()<until: time.sleep(.01)
        self.assertTrue(ready.exists())
        return process, handed

    @staticmethod
    def cleanup_child(process):
        if process.poll() is None: process.kill()
        process.communicate(timeout=3)

    def test_real_signal_one_handoff_preserves_literals_and_ignores_duplicate_usr1(self):
        process, handed = self.child()
        process.send_signal(signal.SIGSTOP)
        process.send_signal(signal.SIGUSR1); process.send_signal(signal.SIGUSR1)
        process.send_signal(signal.SIGCONT)
        output, errors = process.communicate(timeout=3)
        self.assertEqual(process.returncode, 0, errors); self.assertEqual(output+errors, b'')
        argv = json.loads(handed.read_text())
        self.assertEqual(argv, ['--', self.gate.PYTHON, '-E', '-s', '-S', self.gate.SCRIPT,
                               '--native-exec', '11.0', '1:2:1:2', '--', 'space value', ';no shell', '$(no exec)', *PIN])

    def test_term_before_usr1_and_coqueued_term_win_without_handoff(self):
        for coqueued, signum in ((False, signal.SIGTERM), (True, signal.SIGTERM),
                                (False, signal.SIGINT), (True, signal.SIGINT)):
            with self.subTest(coqueued=coqueued, signum=signum):
                for name in ('ready', 'handed'): (self.root/name).unlink(missing_ok=True)
                process, handed = self.child()
                process.send_signal(signal.SIGSTOP)
                if coqueued: process.send_signal(signal.SIGUSR1)
                process.send_signal(signum); process.send_signal(signal.SIGCONT)
                output, errors = process.communicate(timeout=3)
                self.assertEqual(process.returncode, 128+signum, errors)
                self.assertFalse(handed.exists()); self.assertEqual(output+errors, b'')

    def test_real_boottime_idle_gate_expires_without_start(self):
        process, handed = self.child(idle=True)
        output, errors = process.communicate(timeout=3)
        self.assertEqual(process.returncode, 2, errors)
        self.assertFalse(handed.exists()); self.assertEqual(output+errors, b'')

    def test_layout_contract_with_synthetic_mount_facts_and_closed_inputs(self):
        # This exercises contract decisions, not a qualified Linux bind mount.
        home = self.root/'home'; native = home/'.claude'; source = native/'projects'; state = self.root/'state'
        home.mkdir(mode=0o700); native.mkdir(mode=0o700); source.mkdir(); state.mkdir()
        env = {'HOME': str(home), 'CLAUDE_CONFIG_DIR': str(native),
            'VITRAL_COLLECTOR_ORIGIN': 'https://identity.example.test',
            'VITRAL_COLLECTOR_PROVIDER': 'claude', 'VITRAL_COLLECTOR_PROFILE': 'claude-2.1.308-text-v1',
            'VITRAL_COLLECTOR_TRANSCRIPT_ROOT': str(source), 'VITRAL_COLLECTOR_STATE': str(state),
            'VITRAL_COLLECTOR_REDACTION': 'required', 'VITRAL_COLLECTOR_EXPECT_SESSION': SID}
        env.update({'VITRAL_LOCATION_'+name.upper(): value for name, value in {
            'label': 'fixture', 'provider': 'kubernetes', 'cluster': 'kind', 'namespace': 'test',
            'workload': 'fixture', 'replica': 'one'}.items()})
        with patch.multiple(self.gate, HOME=home, NATIVE=native, TRANSCRIPTS=source, STATE=state), \
                patch.object(self.gate.os.path, 'ismount', return_value=False):
            with patch.object(self.gate, 'readonly', side_effect=[False, False, False, True, False]): self.gate.validate(env)
            for flags in ([False, False, True], [False, False, False, False],
                          [False, False, False, True, True], [True]):
                with patch.object(self.gate, 'readonly', side_effect=flags), self.assertRaises(self.gate.Refused):
                    self.gate.validate(env)
            (source/'existing.jsonl').write_bytes(b'previous source')
            with patch.object(self.gate, 'readonly', side_effect=[False, False, False, True, False]), \
                    self.assertRaises(self.gate.Refused): self.gate.validate(env)
            (source/'existing.jsonl').unlink()
            home.chmod(0o755)
            with self.assertRaises(self.gate.Refused): self.gate.validate(env)
            home.chmod(0o700)
            for key, value in (('HOME', '/other-home'), ('VITRAL_COLLECTOR_PROVIDER', 'codex'),
                               ('VITRAL_COLLECTOR_PROFILE', 'auto')):
                with self.subTest(key=key), self.assertRaises((self.gate.Refused, SyncError)):
                    self.gate.validate(env | {key: value})

    def test_deadline_jump_and_term_at_runner_handoff_never_start(self):
        for late, cancelled, code in ((True, False, 2), (False, True, 143)):
            with self.subTest(late=late):
                for name in ('ready', 'handed'): (self.root/name).unlink(missing_ok=True)
                process, handed = self.child(late=late, cancel_on_handoff=cancelled)
                process.send_signal(signal.SIGUSR1)
                output, errors = process.communicate(timeout=3)
                self.assertEqual(process.returncode, code, errors)
                self.assertFalse(handed.exists()); self.assertEqual(output+errors, b'')

    def test_public_gate_refuses_non_pid1_silently_without_a_test_override(self):
        result = subprocess.run([sys.executable, '-B', '-E', '-s', '-S', str(OVERLAY/'prepare.py'),
                                 '--', 'not-a-model-turn'], capture_output=True, timeout=3, env={'PATH': os.defpath})
        self.assertEqual(result.returncode, 2); self.assertEqual(result.stdout+result.stderr, b'')



if __name__ == '__main__': unittest.main()

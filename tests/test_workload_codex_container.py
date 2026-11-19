"""Owned synthetic archive/signal fixtures, not an actual native/image receipt."""
import gzip
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import runpy
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest.mock import patch

from csl_sync_state import SyncError

ROOT = Path(__file__).resolve().parents[1]
OVERLAY = ROOT/'packaging/codex-sole-producer'


def module(name):
    spec = importlib.util.spec_from_file_location('codex_overlay_'+name, OVERLAY/(name+'.py'))
    result = importlib.util.module_from_spec(spec); spec.loader.exec_module(result)
    return result


def elf():
    value = bytearray(64); value[:6] = b'\x7fELF\x02\x01'; value[18:20] = b'\x3e\x00'
    return bytes(value)


class InputsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='codex-overlay-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.inputs = self.root/'inputs'; self.inputs.mkdir()
        self.output = self.root/'output'; self.install = module('install')

    def archive(self, entries=None, *, fmt=tarfile.USTAR_FORMAT):
        entries = [('codex', elf(), tarfile.REGTYPE)] if entries is None else entries
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode='w', format=fmt) as archive:
            for name, content, kind in entries:
                member = tarfile.TarInfo(name); member.type = kind
                member.size = len(content) if kind == tarfile.REGTYPE else 0
                member.mode = 0o755
                if kind in (tarfile.LNKTYPE, tarfile.SYMTYPE): member.linkname = 'codex'
                archive.addfile(member, io.BytesIO(content) if member.isfile() else None)
        data = gzip.compress(buffer.getvalue())
        (self.inputs/'native.tar.gz').write_bytes(data)
        (self.inputs/'LICENSE').write_bytes(b'public synthetic license')
        return {'version': 1, 'source': 'synthetic-not-upstream', 'base_manifest': 'sha256:'+'0'*64,
            'archives': [{'name': 'native.tar.gz', 'size': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
                          'files': {'codex': 'bin/codex'}, 'directories': []}],
            'licenses': [{'name': 'LICENSE', 'target': 'licenses/codex/LICENSE',
                          'sha256': hashlib.sha256(b'public synthetic license').hexdigest()}]}

    def test_complete_inventory_elf_and_license_assemble_without_execution(self):
        spec = self.archive()
        self.install.install(self.inputs, self.output, spec)
        manifest = json.loads((self.output/'native-manifest.json').read_bytes())
        self.assertEqual(set(manifest['sha256']), {'bin/codex', 'licenses/codex/LICENSE', 'codex-package.json'})
        for name, digest in manifest['sha256'].items():
            path = self.output/name
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), digest)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o555 if name == 'bin/codex' else 0o444)
        self.assertEqual((self.output/'bin/codex').read_bytes(), elf())
        with self.assertRaises(ValueError): self.install.install(self.inputs, self.output, spec)

    def test_all_digests_checked_before_unpack_or_output_creation(self):
        spec = self.archive(); spec['licenses'][0]['sha256'] = '0'*64
        with patch.object(self.install, 'unpack') as unpack, self.assertRaises(ValueError):
            self.install.install(self.inputs, self.output, spec)
        unpack.assert_not_called(); self.assertFalse(self.output.exists())
        self.assertEqual(sorted(path.name for path in self.root.iterdir()), ['inputs'])

    def test_wrong_size_and_extra_inputs_fail_without_replacing_anything(self):
        spec = self.archive(); original = (self.inputs/'native.tar.gz').read_bytes()
        spec['archives'][0]['size'] += 1
        with self.assertRaises(ValueError): self.install.install(self.inputs, self.output, spec)
        self.assertFalse(self.output.exists())
        spec['archives'][0]['size'] -= 1
        (self.inputs/'extra.sqlite').write_bytes(b'not a source')
        with self.assertRaises(ValueError):
            with self.install.verified_inputs(self.inputs, spec): pass
        with self.assertRaises(ValueError): self.install.install(self.inputs, self.output, spec)
        self.assertEqual((self.inputs/'native.tar.gz').read_bytes(), original)

    def test_special_symlink_and_hardlinked_inputs_refuse_promptly_and_preserve(self):
        for kind in ('fifo', 'symlink', 'hardlink'):
            with self.subTest(kind=kind):
                spec = self.archive(); target = self.inputs/'native.tar.gz'
                original = target.read_bytes(); target.unlink()
                sibling = self.root/'original'; sibling.write_bytes(original)
                if kind == 'fifo': os.mkfifo(target, 0o600)
                elif kind == 'symlink': target.symlink_to(sibling)
                else: os.link(sibling, target)
                try:
                    code = '''import importlib.util,json,sys
from pathlib import Path
s=importlib.util.spec_from_file_location('installer',sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
try: m.install(Path(sys.argv[2]),Path(sys.argv[3]),json.load(sys.stdin))
except (ValueError,OSError): sys.exit(2)
'''
                    result = subprocess.run([sys.executable, '-B', '-E', '-s', '-S', '-c', code,
                        str(OVERLAY/'install.py'), str(self.inputs), str(self.output)],
                        input=json.dumps(spec), text=True, capture_output=True, timeout=3)
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertEqual(result.stdout+result.stderr, '')
                    self.assertEqual(sibling.read_bytes(), original); self.assertTrue(target.is_symlink() or target.exists())
                    self.assertFalse(self.output.exists())
                finally: target.unlink(); sibling.unlink()

    def test_closed_member_types_paths_duplicates_and_missing_files_refuse(self):
        cases = [[], [('codex', elf(), tarfile.REGTYPE), ('codex', elf(), tarfile.REGTYPE)],
                 [('codex', elf(), tarfile.REGTYPE), ('extra', b'public', tarfile.REGTYPE)]]
        cases += [[(name, elf(), tarfile.REGTYPE)] for name in ('../codex', '/codex', 'dir/codex')]
        cases += [[('codex', b'', kind)] for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE,
                                                    tarfile.FIFOTYPE, tarfile.CHRTYPE, tarfile.DIRTYPE, tarfile.CONTTYPE)]
        for entries in cases:
            with self.subTest(entries=[row[0] for row in entries]):
                spec = self.archive(entries)
                with self.assertRaises((ValueError, tarfile.TarError)): self.install.install(self.inputs, self.output, spec)
                self.assertFalse(self.output.exists())
                self.assertFalse(list(self.root.glob('codex-assembly-*')))

    def test_decompression_metadata_output_and_elf_bounds(self):
        for blob in (b'#!/bin/sh\nexit 0', b'\x7fELF\x01\x01'+b'x'*50):
            spec = self.archive([('codex', blob, tarfile.REGTYPE)])
            with self.assertRaises(ValueError): self.install.install(self.inputs, self.output, spec)
        spec = self.archive()
        for limit, amount in (('TAR_LIMIT', 1024), ('OUTPUT_LIMIT', 16)):
            with patch.object(self.install, limit, amount), self.assertRaises(ValueError):
                self.install.install(self.inputs, self.output, spec)
            self.assertFalse(self.output.exists())
        spec = self.archive([('x'*70000, elf(), tarfile.REGTYPE)], fmt=tarfile.PAX_FORMAT)
        with self.assertRaises(ValueError): self.install.install(self.inputs, self.output, spec)
        self.assertFalse(self.output.exists())

    def test_lock_is_closed_to_pinned_native_helpers_rg_and_licenses(self):
        lock = json.loads((OVERLAY/'inputs.json').read_bytes())
        self.assertEqual(lock['source'], 'd32063f96c2dff1dc96af839b3952abb6acfeda4')
        self.assertEqual(lock['base_manifest'], 'sha256:8691a8fdc0ea6e00ebeb2e2a8fc56f9f607b718dd45555ffd38c8753cd8119e1')
        self.assertEqual(len(lock['archives']), 5); self.assertEqual(len(lock['licenses']), 3)
        targets = {target for row in lock['archives'] for target in row['files'].values() if target}
        self.assertEqual(targets, {'bin/codex', 'bin/codex-code-mode-host', 'bin/codex-exec-server',
            'codex-resources/bwrap', 'codex-path/rg', 'licenses/ripgrep/COPYING',
            'licenses/ripgrep/LICENSE-MIT', 'licenses/ripgrep/UNLICENSE'})
        for row in lock['archives']+lock['licenses']:
            self.assertRegex(row['sha256'], '^[0-9a-f]{64}$')
            self.assertTrue(row['url'].startswith(('https://github.com/', 'https://raw.githubusercontent.com/')))
        recipe = (OVERLAY/'Containerfile').read_text()
        self.assertEqual([line for line in recipe.splitlines() if line.startswith('FROM ')],
                         ['FROM reviewed_collector AS install', 'FROM reviewed_collector'])
        self.assertIn('RUN --network=none python3 -B -I', recipe)
        self.assertIn('assert v=="codex-cli 0.169.0"', recipe)
        self.assertIn('install -d -m 0700 -o 10001 -g 10001 /agent-private', recipe)
        self.assertNotRegex(recipe, 'curl|wget|pip install|apt-get|FROM .*:latest|USER 0:0\\nENTRYPOINT')
        self.assertTrue(recipe.endswith('ENTRYPOINT ["/usr/local/bin/csl-codex-prepare"]\nCMD []\n'))
        includes = {line[1:] for line in (OVERLAY/'Containerfile.dockerignore').read_text().splitlines() if line.startswith('!')}
        self.assertEqual(includes, {'packaging', 'packaging/codex-sole-producer'} |
            {'packaging/codex-sole-producer/'+name for name in ('Containerfile', 'Containerfile.dockerignore',
             'inputs.json', 'install.py', 'prepare.py', 'csl-codex-prepare')})
        probe = runpy.run_path(str(ROOT/'scripts/check_workload_build_context.py'))
        self.assertEqual(set(probe['CODEX_ALLOWED']), includes-{'packaging', 'packaging/codex-sole-producer'})
        self.assertEqual(len(probe['CODEX_DENIED']), 7)
        policy = (OVERLAY/'Containerfile.dockerignore').read_text().splitlines()
        for parent in ('packaging', 'packaging/codex-sole-producer'):
            self.assertEqual(policy[policy.index('!'+parent)+1], parent+'/**')


class BuildTests(unittest.TestCase):
    setUp = InputsTests.setUp
    archive = InputsTests.archive

    def fixture(self, *, architecture='amd64'):
        specification = self.archive()
        layout = self.root/'layout'; (layout/'blobs/sha256').mkdir(parents=True)
        def store(value):
            data = json.dumps(value, sort_keys=True).encode()
            digest = hashlib.sha256(data).hexdigest()
            (layout/'blobs/sha256'/digest).write_bytes(data)
            return {'digest': 'sha256:'+digest, 'size': len(data)}
        config = store({'os': 'linux', 'architecture': architecture})
        manifest = store({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                          'config': config, 'layers': []})
        specification['base_manifest'] = manifest['digest']
        public_code = self.root/'public-code'; public_code.mkdir()
        (public_code/'inputs.json').write_text(json.dumps(specification))
        (public_code/'install.py').write_bytes((OVERLAY/'install.py').read_bytes())
        evidence = self.root/'evidence'; evidence.mkdir(mode=0o700)
        return layout, public_code, evidence/'image.tar', manifest

    def test_fixed_named_descriptor_platform_and_no_push_or_shell(self):
        layout, code, output, manifest = self.fixture()
        builder = module('build')
        with patch.object(builder, 'HERE', code): argv = builder.command(layout, self.inputs, output, 'a'*40)
        self.assertEqual(argv[:4], ['docker', 'buildx', 'build', '--platform'])
        self.assertIn('reviewed_collector=oci-layout://'+str(layout)+'@'+manifest['digest'], argv)
        self.assertIn('org.opencontainers.image.revision='+'a'*40, argv)
        self.assertIn('type=oci,dest='+str(output), argv)
        self.assertNotIn('--push', argv); self.assertNotIn('--load', argv)
        self.assertFalse(output.exists()); self.assertFalse(output.with_suffix('.metadata.json').exists())

    def test_corrupt_descriptor_and_wrong_platform_refuse(self):
        layout, code, output, manifest = self.fixture()
        builder = module('build'); path = layout/'blobs/sha256'/manifest['digest'][7:]
        path.write_bytes(path.read_bytes()+b' ')
        with patch.object(builder, 'HERE', code), self.assertRaises(ValueError):
            builder.command(layout, self.inputs, output, 'a'*40)
        with self.assertRaises(ValueError): builder.blob(layout, '../wrong')
        # A separate validly hashed synthetic ARM base still fails the platform gate.
        for child in self.root.iterdir():
            if child.name in ('layout', 'public-code', 'evidence'):
                import shutil
                shutil.rmtree(child)
        layout, code, output, _ = self.fixture(architecture='arm64')
        with patch.object(builder, 'HERE', code), self.assertRaises(ValueError):
            builder.command(layout, self.inputs, output, 'a'*40)

    def test_existing_outputs_and_unknown_public_inputs_refuse_before_build(self):
        layout, code, output, _ = self.fixture(); builder = module('build')
        with patch.object(builder, 'HERE', code):
            with self.assertRaises(ValueError): builder.command(layout, self.inputs, output, 'main')
            with self.assertRaises(ValueError):
                builder.command(layout, self.inputs, output.with_name('image,compression=zstd.tar'), 'a'*40)
            with patch.object(builder, 'ROOT', self.root), self.assertRaises(ValueError):
                builder.command(layout, self.inputs, output, 'a'*40)
            output.write_bytes(b'keep existing artifact')
            with self.assertRaises(ValueError): builder.command(layout, self.inputs, output, 'a'*40)
            self.assertEqual(output.read_bytes(), b'keep existing artifact'); output.unlink()
            metadata = output.with_suffix('.metadata.json'); metadata.write_bytes(b'keep existing metadata')
            with self.assertRaises(ValueError): builder.command(layout, self.inputs, output, 'a'*40)
            self.assertEqual(metadata.read_bytes(), b'keep existing metadata'); metadata.unlink()
            (self.inputs/'unlisted.env').write_bytes(b'public synthetic canary')
            with self.assertRaises(ValueError): builder.command(layout, self.inputs, output, 'a'*40)
            self.assertFalse(output.exists())


class GateTests(unittest.TestCase):
    def setUp(self):
        self.gate = module('prepare')
        self.tmp = tempfile.TemporaryDirectory(prefix='codex-gate-')
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
            self.gate.native_exec(['10.0', '--', *values], {}, now=lambda: 1,
                                 validator=lambda env: None, execute=execute)
        self.assertEqual(calls, [(self.gate.CODEX, [self.gate.CODEX, 'exec', *values], {})])
        for deadline in ('0', 'nan', 'inf', '-1', '2000'):
            with self.assertRaises((self.gate.Refused, ValueError)):
                self.gate.native_exec([deadline, '--', 'x'], {}, now=lambda: 1,
                                     validator=lambda env: None, execute=execute)
        self.assertEqual(len(calls), 1)
        clock = [1]
        def late(_env): clock[0] = 11
        with self.assertRaises(self.gate.Refused):
            self.gate.native_exec(['10.0', '--', 'x'], {}, now=lambda: clock[0], validator=late, execute=execute)
        self.assertEqual(len(calls), 1)

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
def run(argv):
 if sys.argv[5]=='cancel': os.kill(os.getpid(),signal.SIGTERM)
 handed.write_text(json.dumps(argv)); return 0
try: result=g.prepare(['--prepare-seconds','1' if sys.argv[4]=='idle' else '10','--','space value',';no shell','$(no exec)'],{},now=g.clock if sys.argv[4]=='idle' else lambda:now[0],validator=validate,run=run)
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
                               '--native-exec', '11.0', '--', 'space value', ';no shell', '$(no exec)'])

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
        home = self.root/'home'; native = home/'.codex'; source = native/'sessions'; state = self.root/'state'
        home.mkdir(mode=0o700); native.mkdir(mode=0o700); source.mkdir(); state.mkdir()
        env = {'HOME': str(home), 'CODEX_HOME': str(native),
            'VITRAL_COLLECTOR_ORIGIN': 'https://identity.example.test',
            'VITRAL_COLLECTOR_PROVIDER': 'codex', 'VITRAL_COLLECTOR_PROFILE': 'codex-0.169.0-text-v1',
            'VITRAL_COLLECTOR_TRANSCRIPT_ROOT': str(source), 'VITRAL_COLLECTOR_STATE': str(state),
            'VITRAL_COLLECTOR_REDACTION': 'required'}
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
            for key, value in (('HOME', '/other-home'), ('VITRAL_COLLECTOR_PROVIDER', 'claude'),
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

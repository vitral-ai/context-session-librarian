"""Source-assembly tests, distinct from the hosted OCI build/run smoke."""
import ast
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import test_workload_packaging as chart_fixture

ROOT = Path(__file__).resolve().parents[1]
IMAGE = ROOT/'packaging/collector-image'


def module(name):
    spec = importlib.util.spec_from_file_location(name, IMAGE/(name+'.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


class WorkloadImageTests(unittest.TestCase):
    def test_inventory_is_exact_static_workload_import_closure(self):
        expected, pending = set(), ['csl_workload_runner']
        while pending:
            name = pending.pop()
            if name in expected: continue
            expected.add(name)
            for node in ast.walk(ast.parse((ROOT/(name+'.py')).read_bytes())):
                imports = ([item.name for item in node.names] if isinstance(node, ast.Import)
                           else [node.module or ''] if isinstance(node, ast.ImportFrom) else [])
                pending.extend(item for item in imports if item.startswith('csl_'))
        value = json.loads((IMAGE/'runtime.json').read_bytes())
        self.assertEqual(set(value['modules']), {name+'.py' for name in expected})
        self.assertEqual(value['data'], ['packaging/workload-collector/profiles.json', 'LICENSE'])
        for forbidden in ('csl_sync.py', 'csl_sync_auth.py', 'codex_sessions.py', 'csl_bridge.py'):
            self.assertNotIn(forbidden, value['modules'])

    def test_assembled_runtime_imports_without_checkout_or_site_packages(self):
        with tempfile.TemporaryDirectory(prefix='workload-image-') as temporary:
            output = Path(temporary)/'runtime'
            module('assemble').assemble(ROOT, output)
            value = json.loads((output/'runtime-manifest.json').read_bytes())
            actual = {str(path.relative_to(output)) for path in output.rglob('*') if path.is_file()}
            self.assertEqual(actual, set(value['sha256']) | {'runtime-manifest.json'})
            for name, digest in value['sha256'].items():
                self.assertEqual(hashlib.sha256((output/name).read_bytes()).hexdigest(), digest)
                self.assertEqual((output/name).stat().st_mode & 0o777, 0o444)
            code = ('import sys, importlib, json; sys.path.insert(0, sys.argv[1]); '
                    'm=json.load(open(sys.argv[1]+"/runtime-manifest.json")); '
                    '[importlib.import_module(n[:-3]) for n in m["sha256"] if n.endswith(".py")]; '
                    'from csl_workload_config import PROVIDER_PROFILES; '
                    'assert set(PROVIDER_PROFILES)=={"codex","claude","gemini"}')
            result = subprocess.run([sys.executable, '-E', '-s', '-S', '-c', code, str(output)],
                cwd=temporary, env={'PATH': os.defpath, 'PYTHONDONTWRITEBYTECODE': '1',
                                    'PYTHONPATH': '/not-an-approved-checkout'},
                capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr.decode())

    def test_context_and_copy_inputs_are_exact_no_checkout_glob(self):
        value = json.loads((IMAGE/'runtime.json').read_bytes())
        expected = set(value['modules'] + value['data'])
        lines = (IMAGE/'Containerfile').read_text().replace('\\\n', ' ').splitlines()
        source = set()
        for line in lines:
            if line.startswith('COPY ') and ' /source/' in line:
                source.update(line.split()[1:-1])
        self.assertEqual(source, expected | {'packaging/collector-image/runtime.json'})
        ignores = (IMAGE/'Containerfile.dockerignore').read_text().splitlines()
        self.assertEqual(ignores[0], '**')
        includes = {line[1:] for line in ignores if line.startswith('!')}
        allowed = expected | {'packaging', 'packaging/collector-image', 'packaging/workload-collector'}
        allowed.update('packaging/collector-image/'+name for name in (
            'Containerfile', 'Containerfile.dockerignore', 'runtime.json', 'assemble.py',
            'fetch-openssl.py', 'check-runtime.py', 'csl-workload-collector'))
        self.assertEqual(includes, allowed)
        self.assertFalse(any('*' in name for name in includes))
        for directory in ('packaging/collector-image', 'packaging/workload-collector'):
            index = ignores.index('!'+directory)
            self.assertEqual(ignores[index+1], directory+'/**')

    @unittest.skipUnless(chart_fixture.HELM, 'Helm required for real rendered image configuration')
    def test_rendered_both_modes_and_profiles_reach_assembled_image_parser(self):
        with tempfile.TemporaryDirectory(prefix='image-rendered-abi-') as temporary:
            root = Path(temporary)
            output = root/'runtime'
            module('assemble').assemble(ROOT, output)
            for name in ('transcripts', 'state'): (root/name).mkdir()
            (root/'ca.crt').touch()
            for mode in ('sidecar', 'daemonSet'):
                options = ({'daemonSet.narrowSameOwnerExport': 'true',
                            'daemonSet.existingClaim': 'approved-transcripts'} if mode == 'daemonSet' else {})
                for provider, profile in chart_fixture.PROFILES.items():
                    for spool, flush in ((None, None), ('536870912', '1')):
                        with self.subTest(mode=mode, provider=provider, spool=spool):
                            values = {'transcripts.provider': provider, 'transcripts.profile': profile}
                            if spool is not None:
                                values.update({'collector.spoolLimitBytes': spool, 'collector.flushSeconds': flush})
                            rendered = chart_fixture.WorkloadChartTests().render(mode=mode, **options, **values)
                            self.assertEqual(rendered.returncode, 0, rendered.stderr)
                            # The existing stdlib chart fixture's bounded env-row
                            # projection; mounted/downward-API values become owned
                            # synthetic paths/IDs, never real workload credentials.
                            rows = re.findall(r'^ +?- name: (VITRAL_[A-Z_]+)\n +value: (.+)$',
                                              rendered.stdout, re.MULTILINE)
                            env = {key: json.loads(value) if value.startswith('"') else value
                                   for key, value in rows}
                            env.update(VITRAL_LOCATION_NAMESPACE='test', VITRAL_LOCATION_REPLICA='fixture',
                                       VITRAL_COLLECTOR_TRANSCRIPT_ROOT=str(root/'transcripts'),
                                       VITRAL_COLLECTOR_STATE=str(root/'state'),
                                       VITRAL_COLLECTOR_TRUST_CA=str(root/'ca.crt'))
                            code = ('import sys,json; sys.path.insert(0,sys.argv[1]); '
                                    'from csl_workload_config import Configuration; '
                                    'c=Configuration.from_environment(json.load(sys.stdin)); '
                                    'print(json.dumps([c.provider,c.profile,c.spool_bytes,c.flush_seconds,c.ca_file]))')
                            result = subprocess.run([sys.executable, '-E', '-s', '-S', '-c', code, str(output)],
                                input=json.dumps(env), text=True, capture_output=True, cwd=temporary, timeout=10)
                            self.assertEqual(result.returncode, 0, result.stderr)
                            self.assertEqual(json.loads(result.stdout),
                                [provider, profile, int(spool or '134217728'), int(flush or '20'), str(root/'ca.crt')])

    def test_context_probe_covers_old_admissions_and_distinguishes_infrastructure_failure(self):
        probe = runpy.run_path(str(ROOT/'scripts/check_workload_build_context.py'))
        current = (IMAGE/'Containerfile.dockerignore').read_text()
        previous = probe['old_filter'](current)
        self.assertEqual(previous.splitlines(), [line for line in current.splitlines()
                         if line not in probe['EXCLUSIONS']])
        self.assertEqual(len(probe['REGRESSION']), 4)
        self.assertEqual(len(set(probe['ALLOWED']+probe['REGRESSION']+probe['SIBLINGS'])), 13)
        with self.assertRaises(ValueError): probe['old_filter'](previous)
        candidate = probe['REGRESSION'][0]
        with tempfile.TemporaryDirectory(prefix='context-verdict-') as temporary:
            target = Path(temporary)/'sentinel'
            accepted = subprocess.CompletedProcess([], 0, stdout=b'')
            missing = subprocess.CompletedProcess([], 1, stdout=(candidate+': not found').encode())
            infra = subprocess.CompletedProcess([], 1, stdout=b'cannot connect to Docker daemon')
            verify = probe['verify_result']
            verify(missing, candidate, False, target, b'public control')
            for result, allowed in ((infra, False), (accepted, False), (accepted, True)):
                with self.subTest(allowed=allowed, code=result.returncode), self.assertRaises(RuntimeError):
                    verify(result, candidate, allowed, target, b'public control')
            target.write_bytes(b'public control')
            verify(accepted, candidate, True, target, b'public control')
            with self.assertRaises(RuntimeError): verify(missing, candidate, False, target, b'public control')
            with self.assertRaises(RuntimeError): verify(accepted, candidate, True, target, b'wrong bytes')

    def test_fixed_exec_nonroot_pins_and_no_inherited_default_command(self):
        launcher = (IMAGE/'csl-workload-collector').read_text()
        self.assertEqual(launcher, '#!/bin/sh\nexec /usr/local/bin/python3 -E -s -S '
                         '/opt/vitral/collector/csl_workload_runner.py "$@"\n')
        recipe = (IMAGE/'Containerfile').read_text()
        bases = re.findall(r'^FROM (\S+)', recipe, re.M)
        self.assertEqual(len(bases), 3)
        for base in bases:
            self.assertRegex(base, r'^docker.io/library/python:3\.12\.16-(?:slim-)?bookworm@sha256:[0-9a-f]{64}$')
        self.assertIn('no-shared no-module no-tests no-zlib', recipe)
        self.assertIn('COPY --from=helper /opt/helper/bin/openssl /usr/bin/openssl', recipe)
        self.assertIn('USER 10001:10001\nRUN python3 -E -s -S', recipe)
        self.assertTrue(recipe.endswith('ENTRYPOINT ["/usr/local/bin/csl-workload-collector"]\nCMD []\n'))
        self.assertNotRegex(recipe, r'(?mi)^\s*(EXPOSE|VOLUME|ADD)\b|pip install|apt-get|COPY \. ')

    def test_wrong_helper_digest_refuses_before_extraction_or_disk_write(self):
        fetch = module('fetch-openssl')
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'not the pinned archive'
        opener = MagicMock()
        opener.open.return_value = response
        with patch.object(fetch.urllib.request, 'build_opener', return_value=opener), \
                patch.object(fetch.tarfile, 'open') as extract, \
                patch.object(fetch.Path, 'write_bytes') as write:
            with self.assertRaisesRegex(ValueError, 'digest mismatch'): fetch.fetch()
        response.__enter__.return_value.read.assert_called_once_with(64*1024*1024+1)
        extract.assert_not_called()
        write.assert_not_called()


if __name__ == '__main__': unittest.main()

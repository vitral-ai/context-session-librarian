from copy import deepcopy
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
import sys
import unittest

import yaml

from check_rendered_abi import check


SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'tests'))
sys.path.insert(0, str(SOURCE))
import test_workload_packaging as packaging_fixture

SCRIPT = (SOURCE / 'packaging/workload-collector/writer-supervisor.py').read_bytes()
SUPERVISOR = 'fixture-collector-writer-' + hashlib.sha256(b'default:fixture:' + SCRIPT).hexdigest()[:12]

PINS = {'collector_image': 'test/csl@sha256:' + '1' * 64,
        'writer_image': 'test/writer@sha256:' + '1' * 64,
        'origin': 'https://vitral.fixture.test', 'provider': 'codex', 'ca_configmap': 'test-ca',
        'writer_argv': ['15', '--', 'python3'], 'supervisor_name': SUPERVISOR,
        'supervisor_sha256': hashlib.sha256(SCRIPT).hexdigest(), 'grace_seconds': 40, 'namespace': 'default',
        'location': {'provider': 'kubernetes', 'cluster': 'test:cluster',
                     'label': 'Fixture', 'workload': 'local/workload'}}


def local_only_manifests(text):
    manifests = list(yaml.safe_load_all(text))
    pod = next(row for row in manifests if row['kind'] == 'Deployment')['spec']['template']['spec']
    for container in pod['initContainers'] + pod['containers']:
        container['imagePullPolicy'] = 'Never'
    return manifests


class RenderedABI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        result = packaging_fixture.WorkloadChartTests().render()
        result.check_returncode()
        cls.manifests = local_only_manifests(result.stdout)

    def attempt(self, edit):
        manifests = deepcopy(self.manifests)
        pod = next(row for row in manifests if row['kind'] == 'Deployment')['spec']['template']['spec']
        edit(pod)
        return check(manifests, **PINS)

    def test_rendered_inputs_track_review_pins(self):
        self.assertEqual(check(self.manifests, **PINS), [])
        unreviewed = PINS | {'collector_image': 'test/csl@sha256:' + '2' * 64}
        self.assertEqual(check(self.manifests, **unreviewed), ['collector_image_pin'])

    def test_reject_writer_state_mount_and_host_volume(self):
        def edit(pod):
            pod['containers'][0]['volumeMounts'].append({'name': 'collector-state'})
            pod['volumes'].append({'name': 'foreign', 'hostPath': {'path': '/var/lib'}})
        self.assertEqual(self.attempt(edit), ['fixture_mount_boundaries', 'fixture_volume_boundaries'])

    def test_reject_wrong_profile_and_ambient_trust(self):
        def edit(pod):
            collector = pod['initContainers'][0]
            for entry in collector['env']:
                if entry['name'] == 'VITRAL_COLLECTOR_PROFILE':
                    entry['value'] = 'auto'
            for volume in pod['volumes']:
                if volume['name'] == 'collector-trust':
                    volume['configMap']['name'] = 'wrong'
        self.assertEqual(self.attempt(edit), ['collector_abi', 'explicit_fixture_ca'])

    def test_reject_scientific_numeric_and_wrong_budgets(self):
        for value in ('1.34217728e+08', 134217728, '134217729'):
            with self.subTest(value=value):
                def edit(pod):
                    env = pod['initContainers'][0]['env']
                    next(entry for entry in env if
                         entry['name'] == 'VITRAL_COLLECTOR_SPOOL_LIMIT_BYTES')['value'] = value
                self.assertEqual(self.attempt(edit), ['collector_budget_abi'])

    def test_reject_changed_execution_inputs(self):
        def collector(pod):
            return pod['initContainers'][0]

        cases = [
            ('writer_supervisor', lambda p: p['containers'][0].update(
                args=['120', '--', 'sh', '-c', 'echo unreviewed'])),
            ('supervisor_volume', lambda p: p['volumes'][0]['configMap'].update(name='foreign')),
            ('fixture_mount_boundaries', lambda p: p['containers'][0]['volumeMounts'][1].update(
                mountPath='/elsewhere', readOnly=False)),
            ('collector_process_boundary', lambda p: p.update(hostPID=True, hostIPC=True, hostNetwork=True)),
            ('container_execution_boundary', lambda p: collector(p)['securityContext'].update(
                privileged=True, runAsUser=0, capabilities={'add': ['SYS_ADMIN']})),
            ('container_execution_boundary', lambda p: collector(p).update(
                envFrom=[{'secretRef': {'name': 'ambient'}}])),
            ('container_execution_boundary', lambda p: collector(p).update(args=['--', 'foreign'])),
            ('collector_abi', lambda p: collector(p)['env'].append(
                {'name': 'AMBIENT', 'valueFrom': {'secretKeyRef': {'name': 'ambient', 'key': 'key'}}})),
            ('container_execution_boundary', lambda p: collector(p).update(imagePullPolicy='Always')),
            ('collector_resources', lambda p: collector(p).pop('resources')),
            ('drain_grace', lambda p: p.update(terminationGracePeriodSeconds=1)),
            ('unreviewed_pod_fields', lambda p: p.update(serviceAccountName='alternate')),
        ]
        for label, edit in cases:
            with self.subTest(label=label):
                self.assertIn(label, self.attempt(edit))

    def test_reject_changed_supervisor_bytes_or_mutability(self):
        for edit in ({'data': {'supervisor.py': 'print(1)'}}, {'immutable': False}):
            with self.subTest(edit=edit):
                manifests = deepcopy(self.manifests)
                next(row for row in manifests if row['kind'] == 'ConfigMap').update(edit)
                self.assertIn('supervisor_configmap', check(manifests, **PINS))

    def test_reviewed_literal_writer_argv_remains_data(self):
        result = packaging_fixture.WorkloadChartTests().render(
            string_settings={'writer.args[0]': 'space and $(literal); --'})
        result.check_returncode()
        self.assertEqual(check(local_only_manifests(result.stdout),
                               **(PINS | {'writer_argv': ['15', '--', 'python3',
                                                          'space and $(literal); --']})), [])

    def test_cli_binds_namespace_local_references(self):
        cases = (
            (None, None, None, 0),
            ('default', 'default', 'default', 0),
            ('default', None, None, 0),
            (None, 'default', None, 0),
            ('foreign', 'default', None, 1),
            ('default', 'foreign', None, 1),
            ('foreign', 'foreign', None, 1),
            (None, None, 'foreign', 1),
            ('', None, None, 1),
        )
        with tempfile.TemporaryDirectory() as home:
            input_file = Path(home) / 'render.json'
            pin_file = Path(home) / 'pins.json'
            pin_file.write_text(json.dumps(PINS), encoding='utf-8')
            for config_ns, deployment_ns, template_ns, expected_exit in cases:
                with self.subTest(config=config_ns, deployment=deployment_ns, pod=template_ns):
                    manifests = deepcopy(self.manifests)
                    config = next(row for row in manifests if row['kind'] == 'ConfigMap')
                    deployment = next(row for row in manifests if row['kind'] == 'Deployment')
                    for row, namespace in ((config, config_ns), (deployment, deployment_ns),
                                           (deployment['spec']['template'], template_ns)):
                        if namespace is not None:
                            row['metadata']['namespace'] = namespace
                    input_file.write_text(json.dumps(manifests), encoding='utf-8')
                    result = subprocess.run(
                        [sys.executable, str(Path(__file__).with_name('check_rendered_abi.py')),
                         str(input_file), str(pin_file)],
                        capture_output=True, text=True, check=False, timeout=10)
                    self.assertEqual(result.returncode, expected_exit, result.stderr)
                    self.assertEqual(json.loads(result.stdout)['failures'],
                                     ['fixture_namespace'] if expected_exit else [])

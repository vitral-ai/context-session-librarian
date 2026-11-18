from copy import deepcopy
from pathlib import Path
import sys
import unittest

import yaml

from check_rendered_abi import check


SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'tests'))
sys.path.insert(0, str(SOURCE))
import test_workload_packaging as packaging_fixture

PINS = {'collector_image': 'test/csl@sha256:' + '1' * 64,
        'writer_image': 'test/writer@sha256:' + '1' * 64,
        'origin': 'https://vitral.fixture.test', 'provider': 'codex', 'ca_configmap': 'test-ca'}


class RenderedABI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        result = packaging_fixture.WorkloadChartTests().render()
        result.check_returncode()
        cls.manifests = list(yaml.safe_load_all(result.stdout))

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

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from compose_enrollment import compose
import test_check_rendered_abi as baseline_fixture


PINS = baseline_fixture.PINS


class EnrollmentComposition(unittest.TestCase):
    helper_path = (Path(__file__).resolve().parents[2]
                   / 'scripts/fixtures/workload-replica/enroll.py')
    helper = helper_path.read_bytes()

    @classmethod
    def setUpClass(cls):
        baseline_fixture.RenderedABI.setUpClass()
        cls.baseline = baseline_fixture.RenderedABI.manifests

    def test_private_exec_overlay_preserves_baseline_writer_and_pins(self):
        original = deepcopy(self.baseline)
        result = compose(self.baseline, PINS, self.helper, 90)
        self.assertEqual(self.baseline, original)
        self.assertEqual(result['exec']['argv'], [
            '/usr/local/bin/python3', '-E', '-s', '-S', '/fixture/enroll.py',
            '--runtime', '/opt/vitral/collector', '--consent-fd', '1', '--budget-seconds', '90'])
        self.assertIs(result['exec']['tty'], False)
        deployment = next(row for row in result['manifests'] if row['kind'] == 'Deployment')
        pod = deployment['spec']['template']['spec']
        self.assertNotIn('fixture-enrollment', [mount['name']
                         for mount in pod['containers'][0]['volumeMounts']])
        self.assertEqual(pod['initContainers'][0]['volumeMounts'][-1],
                         {'name': 'fixture-enrollment', 'mountPath': '/fixture', 'readOnly': True})
        config = result['manifests'][-1]
        self.assertIs(config['immutable'], True)
        self.assertTrue(all(row['metadata']['namespace'] == PINS['namespace']
                            for row in result['manifests']))
        self.assertEqual(pod['volumes'][-1]['configMap']['name'], config['metadata']['name'])

    def test_refuse_wrong_hash_foreign_namespace_and_unbounded_budget(self):
        foreign = deepcopy(self.baseline)
        foreign[0]['metadata']['namespace'] = 'foreign'
        for manifests, helper, budget in ((self.baseline, self.helper + b'\n', 90),
                                          (foreign, self.helper, 90),
                                          (self.baseline, self.helper, 121),
                                          (self.baseline, self.helper, True)):
            with self.subTest(budget=budget):
                with self.assertRaises(ValueError):
                    compose(manifests, PINS, helper, budget)

    def test_cli_verifies_exact_overlay_and_rejects_mutations(self):
        expected = compose(self.baseline, PINS, self.helper, 90)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            render, pins, candidate = (root / name for name in ('render', 'pins', 'candidate'))
            render.write_text(json.dumps(self.baseline), encoding='utf-8')
            pins.write_text(json.dumps(PINS), encoding='utf-8')
            argv = [sys.executable, str(Path(__file__).with_name('compose_enrollment.py')),
                    str(render), str(pins), str(self.helper_path), '90', str(candidate)]
            def change_exec(plan):
                plan['exec']['argv'] = ['sh']

            def add_tty(plan):
                plan['exec']['tty'] = True

            def change_helper(plan):
                plan['manifests'][-1]['binaryData']['enroll.py'] = 'Cg=='

            def change_mount(plan):
                deployment = next(row for row in plan['manifests'] if row['kind'] == 'Deployment')
                pod = deployment['spec']['template']['spec']
                pod['initContainers'][0]['volumeMounts'][-1]['readOnly'] = False

            for edit in (None, change_exec, add_tty, change_helper, change_mount):
                with self.subTest(edit=edit):
                    plan = deepcopy(expected)
                    if edit is not None:
                        edit(plan)
                    candidate.write_text(json.dumps(plan), encoding='utf-8')
                    result = subprocess.run(argv, capture_output=True, check=False, timeout=10)
                    self.assertEqual(result.returncode, int(edit is not None), result.stderr)

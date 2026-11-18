"""Compose the reviewed baseline with one fixture-only enrollment script."""

from base64 import b64encode
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys

from check_rendered_abi import check


HELPER_SHA256 = 'e5c29e2fa11d048ebb5444d3a9ddaa1b29bada2c2d745b42561fa8c77ddd6981'


def compose(manifests, pins, helper, budget_seconds):
    failures = check(manifests, **pins)
    if failures:
        raise ValueError('baseline fixture refused')
    if (not helper or len(helper) > 64 * 1024
            or hashlib.sha256(helper).hexdigest() != HELPER_SHA256):
        raise ValueError('enrollment helper digest mismatch')
    if type(budget_seconds) is not int or not 1 <= budget_seconds <= 120:
        raise ValueError('invalid exec budget')

    objects = deepcopy(manifests)
    deployment = next(row for row in objects if row['kind'] == 'Deployment')
    namespace = pins['namespace']
    identity = ':'.join((namespace, deployment['metadata']['name'], HELPER_SHA256))
    helper_name = 'fixture-enroll-' + hashlib.sha256(identity.encode()).hexdigest()[:32]
    if helper_name in (pins['supervisor_name'], pins['ca_configmap']):
        raise ValueError('enrollment object name collision')
    for row in objects:
        row['metadata']['namespace'] = namespace
    pod = deployment['spec']['template']['spec']
    collector = pod['initContainers'][0]
    collector['volumeMounts'].append(
        {'name': 'fixture-enrollment', 'mountPath': '/fixture', 'readOnly': True})
    pod['volumes'].append({'name': 'fixture-enrollment', 'configMap': {
        'name': helper_name, 'defaultMode': 0o444,
        'items': [{'key': 'enroll.py', 'path': 'enroll.py'}]}})
    objects.append({'apiVersion': 'v1', 'kind': 'ConfigMap',
                    'metadata': {'namespace': namespace, 'name': helper_name},
                    'immutable': True, 'binaryData': {'enroll.py': b64encode(helper).decode('ascii')}})
    argv = ['/usr/local/bin/python3', '-E', '-s', '-S', '/fixture/enroll.py',
            '--runtime', '/opt/vitral/collector', '--consent-fd', '1',
            '--budget-seconds', str(budget_seconds)]
    return {'manifests': objects, 'exec': {'namespace': namespace, 'container': 'collector',
                                          'tty': False, 'argv': argv}}


def main():
    try:
        render, pins, helper, budget, *candidate = sys.argv[1:]
        if len(candidate) > 1:
            raise ValueError('unexpected arguments')
        with Path(helper).open('rb') as source:
            helper_bytes = source.read(64 * 1024 + 1)
        result = compose(json.loads(Path(render).read_text(encoding='utf-8')),
                         json.loads(Path(pins).read_text(encoding='utf-8')),
                         helper_bytes, int(budget))
        if candidate:
            submitted = json.loads(Path(candidate[0]).read_text(encoding='utf-8'))
            if json.dumps(submitted, sort_keys=True) != json.dumps(result, sort_keys=True):
                raise ValueError('overlay differs from reviewed composition')
    except (OSError, KeyError, TypeError, ValueError):
        raise SystemExit('enrollment composition refused') from None
    print(json.dumps(result) if not candidate else '{"verified":true}')


if __name__ == '__main__':
    main()

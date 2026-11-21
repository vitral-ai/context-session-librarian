"""Check rendered fixture inputs against review pins, without starting a pod."""
import hashlib
import json
import re


COLLECTOR = '/usr/local/bin/csl-workload-collector'
PROFILES = {
    'codex': 'codex-0.169.0-text-v1',
    'claude': 'claude-2.1.308-text-v1',
}
DIGEST_REF = re.compile(r'.+@sha256:[0-9a-f]{64}$')


def by_name(rows):
    result = {row['name']: row for row in rows}
    if len(result) != len(rows):
        raise ValueError('duplicate_name')
    return result


def check(manifests, *, collector_image, writer_image, origin, provider, ca_configmap,
          writer_argv, supervisor_name, supervisor_sha256, namespace, location, grace_seconds,
          flush_seconds=20, spool_limit_bytes=134217728):
    """Return bounded assertion labels. Pins are trusted inputs from review."""
    if (type(flush_seconds) is not int or not 1 <= flush_seconds <= 20
            or type(spool_limit_bytes) is not int or not 1 <= spool_limit_bytes <= 512 * 1024**2
            or type(grace_seconds) is not int or not 30 <= grace_seconds <= 300):
        return ['invalid_reviewed_budgets']
    kinds = {row['kind']: row for row in manifests}
    if len(kinds) != len(manifests) or set(kinds) != {'ConfigMap', 'Deployment'}:
        return ['unexpected_fixture_resources']
    if (type(namespace) is not str
            or not re.fullmatch(r'[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?', namespace)):
        return ['invalid_reviewed_namespace']
    # Apply must use the reviewed namespace for resources that omit it.
    if any(row.get('metadata', {}).get('namespace', namespace) != namespace
           for row in kinds.values()):
        return ['fixture_namespace']
    template = kinds['Deployment']['spec']['template']
    if template.get('metadata', {}).get('namespace', namespace) != namespace:
        return ['fixture_namespace']
    pod = template['spec']
    pod_fields = {'automountServiceAccountToken', 'securityContext',
                  'terminationGracePeriodSeconds', 'initContainers', 'containers', 'volumes'}
    if set(pod) - pod_fields - {'hostPID', 'hostIPC', 'hostNetwork', 'shareProcessNamespace'}:
        return ['unreviewed_pod_fields']
    init = by_name(pod['initContainers'])
    writers = by_name(pod['containers'])
    if set(init) != {'collector'} or set(writers) != {'writer'}:
        return ['unexpected_fixture_containers']
    collector, writer = init['collector'], writers['writer']
    failures = set()
    for name, container, pin in (('collector', collector, collector_image),
                                 ('writer', writer, writer_image)):
        if not DIGEST_REF.fullmatch(pin) or container['image'] != pin:
            failures.add(name + '_image_pin')
    if (collector['command'] != [COLLECTOR] or collector['restartPolicy'] != 'Always'
            or pod.get('automountServiceAccountToken') is not False
            or any(pod.get(field, False) is not False for field in
                   ('hostPID', 'hostIPC', 'hostNetwork', 'shareProcessNamespace'))):
        failures.add('collector_process_boundary')
    security = {'allowPrivilegeEscalation': False, 'runAsNonRoot': True,
                'capabilities': {'drop': ['ALL']}}
    pod_security = {'runAsUser': 10001, 'runAsGroup': 10001, 'fsGroup': 10001,
                    'seccompProfile': {'type': 'RuntimeDefault'}}
    container_fields = {'name', 'image', 'command', 'securityContext', 'volumeMounts',
                        'imagePullPolicy'}
    collector_security = security | {'readOnlyRootFilesystem': True}
    if (set(collector) - container_fields - {'restartPolicy', 'env', 'resources'}
            or set(writer) - container_fields - {'args'}
            or any(json.dumps(item.get('securityContext'), sort_keys=True) !=
                   json.dumps(expected_security, sort_keys=True)
                   for item, expected_security in ((collector, collector_security),
                                                   (writer, security)))
            or pod.get('securityContext') != pod_security
            or any(item.get('imagePullPolicy') != 'Never' for item in (collector, writer))):
        failures.add('container_execution_boundary')
    if (not isinstance(writer_argv, list) or len(writer_argv) < 3
            or any(type(arg) is not str for arg in writer_argv)
            or not re.fullmatch(r'0|[1-9][0-9]?|1[01][0-9]|120', writer_argv[0])
            or writer_argv[1] != '--' or not writer_argv[2]
            or writer.get('args') != writer_argv
            or writer.get('command') != ['python3', '/vitral-writer/supervisor.py']):
        failures.add('writer_supervisor')
    else:
        grace = pod.get('terminationGracePeriodSeconds')
        if (type(grace) is not int or grace < max(30, int(writer_argv[0]) + flush_seconds + 5)
                or grace != grace_seconds):
            failures.add('drain_grace')
    if collector.get('resources') != {'requests': {'cpu': '50m', 'memory': '64Mi'},
                                      'limits': {'memory': '256Mi'}}:
        failures.add('collector_resources')
    config = kinds['ConfigMap']
    script = config.get('data', {}).get('supervisor.py')
    if (config.get('metadata', {}).get('name') != supervisor_name
            or config.get('immutable') is not True or 'binaryData' in config
            or set(config.get('data', {})) != {'supervisor.py'}
            or type(script) is not str
            or hashlib.sha256(script.encode()).hexdigest() != supervisor_sha256):
        failures.add('supervisor_configmap')
    mounts = by_name(collector['volumeMounts'])
    writer_mounts = by_name(writer['volumeMounts'])
    if (set(mounts) != {'transcript', 'collector-state', 'collector-trust'}
            or mounts.get('transcript') != {'name': 'transcript', 'mountPath': '/transcripts',
                                             'readOnly': True}
            or mounts.get('collector-state') != {'name': 'collector-state',
                                                  'mountPath': '/collector-state'}
            or writer_mounts != {'transcript': {'name': 'transcript', 'mountPath': '/transcripts'},
                                 'writer-supervisor': {'name': 'writer-supervisor',
                                                       'mountPath': '/vitral-writer',
                                                       'readOnly': True}}):
        failures.add('fixture_mount_boundaries')
    volumes = by_name(pod['volumes'])
    if (set(volumes) != {'transcript', 'collector-state', 'collector-trust', 'writer-supervisor'}
            or volumes.get('collector-state') != {'name': 'collector-state',
                                                  'emptyDir': {'sizeLimit': '256Mi'}}
            or volumes.get('transcript') != {'name': 'transcript',
                                             'emptyDir': {'sizeLimit': '512Mi'}}):
        failures.add('fixture_volume_boundaries')
    if volumes.get('writer-supervisor') != {
        'name': 'writer-supervisor', 'configMap': {
            'name': supervisor_name, 'items': [{'key': 'supervisor.py', 'path': 'supervisor.py'}]}
    }:
        failures.add('supervisor_volume')
    trust = volumes.get('collector-trust', {}).get('configMap', {})
    if (trust != {'name': ca_configmap, 'items': [{'key': 'ca.crt', 'path': 'ca.crt'}]}
            or mounts.get('collector-trust') != {'name': 'collector-trust',
                                                 'mountPath': '/collector-trust', 'readOnly': True}):
        failures.add('explicit_fixture_ca')
    env = by_name(collector['env'])
    expected = {
        'VITRAL_COLLECTOR_ORIGIN': origin,
        'VITRAL_COLLECTOR_PROVIDER': provider,
        'VITRAL_COLLECTOR_PROFILE': PROFILES.get(provider),
        'VITRAL_COLLECTOR_STATE': '/collector-state',
        'VITRAL_COLLECTOR_TRANSCRIPT_ROOT': '/transcripts',
        'VITRAL_COLLECTOR_REDACTION': 'required',
        'VITRAL_COLLECTOR_CLIENT_ID': 'vitral-workload-collector',
        'VITRAL_COLLECTOR_TRUST_CA': '/collector-trust/ca.crt',
    }
    expected.update({'VITRAL_LOCATION_' + name.upper(): location[name]
                     for name in ('label', 'provider', 'cluster', 'workload')})
    budgets = {'VITRAL_COLLECTOR_FLUSH_SECONDS': flush_seconds,
               'VITRAL_COLLECTOR_SPOOL_LIMIT_BYTES': spool_limit_bytes}
    for name, budget in budgets.items():
        if env.get(name) != {'name': name, 'value': str(budget)}:
            failures.add('collector_budget_abi')
    fields = {'VITRAL_LOCATION_REPLICA': 'metadata.uid',
              'VITRAL_LOCATION_NAMESPACE': 'metadata.namespace'}
    if (set(env) != set(expected) | set(budgets) | set(fields)
            or any(type(value) is not str or env.get(name) != {'name': name, 'value': value}
                   for name, value in expected.items())):
        failures.add('collector_abi')
    if any(env.get(name) != {'name': name, 'valueFrom': {'fieldRef': {'fieldPath': path}}}
           for name, path in fields.items()):
        failures.add('replica_provenance')
    return sorted(failures)


if __name__ == '__main__':
    from pathlib import Path
    import sys

    try:
        manifests = json.loads(Path(sys.argv[1]).read_text())
        pins = json.loads(Path(sys.argv[2]).read_text())
        failures = check(manifests, **pins)
    except (IndexError, KeyError, OSError, TypeError, ValueError):
        failures = ['invalid_rendered_abi_inputs']
    print(json.dumps({'classification': 'rendered-abi', 'failures': failures}))
    raise SystemExit(bool(failures))

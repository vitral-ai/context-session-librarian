"""Check rendered fixture inputs against review pins, without starting a pod."""
import re


COLLECTOR = '/usr/local/bin/csl-workload-collector'
PROFILES = {
    'codex': 'codex-0.169.0-text-v1',
    'claude': 'claude-2.1.308-text-v1',
    'gemini': 'gemini-cli-0.68.0-jsonl-text-v1',
}
DIGEST_REF = re.compile(r'.+@sha256:[0-9a-f]{64}$')


def by_name(rows):
    result = {row['name']: row for row in rows}
    if len(result) != len(rows):
        raise ValueError('duplicate_name')
    return result


def check(manifests, *, collector_image, writer_image, origin, provider, ca_configmap,
          flush_seconds=20, spool_limit_bytes=134217728):
    """Return bounded assertion labels. Pins are trusted inputs from review."""
    kinds = {row['kind']: row for row in manifests}
    if len(kinds) != len(manifests) or set(kinds) != {'ConfigMap', 'Deployment'}:
        return ['unexpected_fixture_resources']
    pod = kinds['Deployment']['spec']['template']['spec']
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
            or pod.get('shareProcessNamespace', False) is not False):
        failures.add('collector_process_boundary')
    if not writer.get('args') or writer.get('command') != ['python3', '/vitral-writer/supervisor.py']:
        failures.add('writer_supervisor')
    mounts = by_name(collector['volumeMounts'])
    writer_mounts = by_name(writer['volumeMounts'])
    if (set(mounts) != {'transcript', 'collector-state', 'collector-trust'}
            or mounts.get('transcript') != {'name': 'transcript', 'mountPath': '/transcripts',
                                             'readOnly': True}
            or mounts.get('collector-state') != {'name': 'collector-state',
                                                  'mountPath': '/collector-state'}
            or set(writer_mounts) != {'transcript', 'writer-supervisor'}):
        failures.add('fixture_mount_boundaries')
    volumes = by_name(pod['volumes'])
    if (set(volumes) != {'transcript', 'collector-state', 'collector-trust', 'writer-supervisor'}
            or not all(set(volumes.get(name, {})) == {'name', 'emptyDir'}
                       for name in ('transcript', 'collector-state'))):
        failures.add('fixture_volume_boundaries')
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
    if any(value is None or env.get(name, {}).get('value') != value for name, value in expected.items()):
        failures.add('collector_abi')
    for name, budget in (('VITRAL_COLLECTOR_FLUSH_SECONDS', flush_seconds),
                         ('VITRAL_COLLECTOR_SPOOL_LIMIT_BYTES', spool_limit_bytes)):
        if env.get(name, {}).get('value') != str(budget):
            failures.add('collector_budget_abi')
    if env.get('VITRAL_LOCATION_REPLICA', {}).get('valueFrom') != {
            'fieldRef': {'fieldPath': 'metadata.uid'}}:
        failures.add('replica_provenance')
    return sorted(failures)


if __name__ == '__main__':
    import json
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

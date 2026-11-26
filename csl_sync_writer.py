"""Explicit same-native-ID writer handoff; journals never contain credentials."""
from contextlib import contextmanager
import hashlib
import json
import re
import urllib.parse
import uuid

from csl_sync_state import SyncError, exclusive, private_json, read_private_json


def fence_path(provider, identifier):
    from csl_sync_providers import provider_home
    if provider not in ('codex', 'claude'):
        raise SyncError('writer_unsupported_source')
    return provider_home(provider) / '.csl-writers' / (hashlib.sha256(native_id(identifier).encode('utf-8')).hexdigest() + '.json')


def read_fence(provider, identifier):
    """Read while holding native_lock. Missing is legacy; malformed fails closed."""
    try:
        value = read_private_json(fence_path(provider, identifier))
    except (SyncError, OSError):
        raise SyncError('invalid_writer_fence') from None
    if value is not None and (value.get('schema_version') != 1 or value.get('provider') != provider or
            value.get('native_session_id') != identifier or value.get('state') not in
            ('active', 'pending', 'publishing', 'releasing', 'released')):
        raise SyncError('invalid_writer_fence')
    return value


def save_fence(provider, identifier, status, binding):
    """Shared launcher fence, updated under native_lock before a remote mutation."""
    value = dict(schema_version=1, provider=provider, native_session_id=identifier, state=status,
                 binding=binding)
    private_json(fence_path(provider, identifier), value, durable=True)


def profile_binding(state):
    return dict(profile=str(state.root.resolve()), device_id=state.get('device_id'),
                endpoint=state.get('settings', {}).get('endpoint'))


def check_fence(state, provider, identifier, *, uploading=False):
    value = read_fence(provider, identifier)
    if value is not None:
        if value.get('binding') != profile_binding(state):
            raise SyncError('writer_fenced')
        if uploading and value['state'] != 'active':
            raise SyncError('writer_fenced')
    return value


def native_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', value):
        raise SyncError('invalid_native_id')
    return value


@contextmanager
def native_lock(provider, identifier):
    from csl_sync_providers import provider_home
    if provider not in ('codex', 'claude'):
        raise SyncError('writer_unsupported_source')
    key = hashlib.sha256(native_id(identifier).encode()).hexdigest()
    with exclusive(provider_home(provider) / '.csl-writers', key + '.lock'):
        yield


def write_headers(state, provider, identifier, registry=None):
    fence = check_fence(state, provider, identifier, uploading=True)
    record = state.session(provider, identifier).get('writer')
    if record is None:
        if fence is not None:
            raise SyncError('writer_fenced')
        return {}
    if (record.get('state') != 'active' or record.get('device_id') != state.get('device_id') or
            record.get('endpoint') != state.get('settings', {}).get('endpoint')):
        raise SyncError('writer_fenced')
    generation = record.get('generation')
    if type(generation) is not int or generation <= 0:
        raise SyncError('writer_fenced')
    if registry is None or Writer(state, registry).attest(record['device_id']) != record.get('account_id'):
        raise SyncError('writer_device_required')
    return {'X-Vitral-Session-Writer': str(generation)}


class Writer:
    def __init__(self, state, registry):
        self.state, self.registry = state, registry

    def attest(self, expected_device):
        from csl_sync_http import checked_url
        settings = self.state.get('settings', {})
        if settings.get('kind') != 'local' or not self.state.get('registered'):
            raise SyncError('writer_device_required')
        if expected_device != self.state.get('device_id'):
            raise SyncError('writer_device_required')
        if not isinstance(settings.get('token_endpoint'), str):
            raise SyncError('writer_device_required')
        token_url = urllib.parse.urlsplit(checked_url(settings['token_endpoint']))
        endpoint = urllib.parse.urlunsplit((token_url.scheme, token_url.netloc, '/session-registry/authorize', '', ''))
        result = self.registry.http.request(endpoint, 'POST', data=b'{"action":"write"}',
            headers={'Authorization': 'Bearer ' + self.registry.tokens.access_token(), 'Content-Type': 'application/json'}, retry=False)
        if (result.get('kind') != 'collector' or result.get('device_id') != expected_device or
                not isinstance(result.get('account_id'), str) or not result['account_id']):
            raise SyncError('writer_device_required')
        return result['account_id']

    def released(self, registry_id, provider, identifier, snapshot, generation):
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', registry_id or ''):
            raise SyncError('invalid_registry_session_id')
        if type(generation) is not int or generation < 0:
            raise SyncError('writer_fenced')
        row = self.registry.call('GET', '/sessions/' + registry_id)
        if (row.get('id') != registry_id or row.get('provider') != provider or row.get('native_id') != identifier or
                row.get('snapshot_id') != snapshot):
            raise SyncError('writer_snapshot_changed')
        owner = row.get('writer', {})
        if owner.get('state') != 'released' or owner.get('generation') != generation:
            raise SyncError('writer_in_use')
        if row.get('sync', {}).get('complete') is not True or row.get('package', {}).get('available') is not True:
            raise SyncError('source_incomplete')
        return row

    def acquire(self, provider, identifier, *, registry_id, snapshot, generation, request_id,
                expected_device, package_sha256, account_id, destinations):
        native_id(identifier)
        native_id(registry_id)
        if (not isinstance(snapshot, str) or not snapshot or type(generation) is not int or generation < 0 or
                not isinstance(account_id, str) or not account_id or not destinations):
            raise SyncError('invalid_writer_binding')
        try:
            if str(uuid.UUID(request_id)) != request_id: raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise SyncError('invalid_writer_request_id') from None
        binding = dict(endpoint=self.state.get('settings', {})['endpoint'], registry_id=registry_id,
            snapshot_id=snapshot, previous_generation=generation, request_id=request_id,
            device_id=expected_device, account_id=account_id, package_sha256=package_sha256,
            destinations=destinations)
        local = self.state.session(provider, identifier)
        previous = local.get('writer')
        check_fence(self.state, provider, identifier)
        if previous and any(previous.get(key) != value for key, value in binding.items()):
            raise SyncError('writer_request_conflict')
        if previous and previous.get('state') in ('publishing', 'active'):
            return previous
        # Persist the exact request BEFORE acquire. On ambiguity, only that request can resume.
        local['writer'] = dict(binding, state='pending')
        save_fence(provider, identifier, 'pending', profile_binding(self.state))
        self.state.save_session(provider, identifier, local)
        response = self.registry.call('POST', '/sessions/' + registry_id + '/writer/acquire',
            body=dict(request_id=request_id, expected_snapshot_id=snapshot, expected_generation=generation), key=request_id)
        if (response.get('device_id') != expected_device or type(response.get('generation')) is not int or
                response['generation'] != generation + 1 or response.get('state') != 'active'):
            raise SyncError('invalid_writer_response')
        local['writer'] = dict(binding, state='publishing', generation=response['generation'])
        self.state.save_session(provider, identifier, local)
        save_fence(provider, identifier, 'publishing', profile_binding(self.state))
        return local['writer']

    def published(self, provider, identifier):
        local = self.state.session(provider, identifier)
        record = local.get('writer', {})
        if record.get('state') not in ('publishing', 'active'):
            raise SyncError('writer_fenced')
        record['state'] = 'active'
        self.state.save_session(provider, identifier, local)
        save_fence(provider, identifier, 'active', profile_binding(self.state))

    def release(self, provider, identifier, *, registry_id, snapshot, generation, expected_device):
        """Caller holds native_lock and has positively observed the managed end."""
        native_id(registry_id)
        if type(generation) is not int or generation < 0:
            raise SyncError('invalid_writer_generation')
        account = self.attest(expected_device)
        check_fence(self.state, provider, identifier)
        local = self.state.session(provider, identifier)
        previous = local.get('writer')
        if generation and (not previous or previous.get('generation') != generation or
                           previous.get('registry_id') != registry_id or previous.get('account_id') != account):
            raise SyncError('writer_fenced')
        if previous and (previous.get('snapshot_id') != snapshot and previous.get('state') in ('releasing','released')):
            raise SyncError('writer_request_conflict')
        row = self.registry.call('GET', '/sessions/' + registry_id)
        if (row.get('id') != registry_id or row.get('provider') != provider or
                row.get('native_id') != identifier or row.get('snapshot_id') != snapshot):
            raise SyncError('writer_snapshot_changed')
        owner = row.get('writer', {})
        if owner.get('generation') != generation or owner.get('device_id') != expected_device:
            raise SyncError('writer_fenced')
        if row.get('sync', {}).get('complete') is not True or row.get('package', {}).get('available') is not True:
            raise SyncError('writer_snapshot_incomplete')
        if owner.get('state') == 'released':
            if not previous or previous.get('state') not in ('releasing','released'):
                raise SyncError('writer_fenced')
        elif owner.get('state') not in ('legacy', 'active'):
            raise SyncError('writer_fenced')
        record = dict(previous or {}, endpoint=self.state.get('settings', {})['endpoint'],
                      registry_id=registry_id, snapshot_id=snapshot, generation=generation,
                      account_id=account, device_id=expected_device, state='releasing')
        # A lost response leaves both collector and managed launcher fenced.
        save_fence(provider, identifier, 'releasing', profile_binding(self.state))
        local['writer'] = record
        self.state.save_session(provider, identifier, local)
        if owner.get('state') != 'released':
            self.registry.call('POST', '/sessions/' + registry_id + '/writer/release',
                               body=dict(generation=generation, expected_snapshot_id=snapshot))
        self.released(registry_id, provider, identifier, snapshot, generation)
        record['state'] = 'released'
        self.state.save_session(provider, identifier, local)
        save_fence(provider, identifier, 'released', profile_binding(self.state))
        return dict(state='released', generation=generation)



def require_stopped(provider, identifier):
    """Only the trusted host's positive process-end receipt authorizes a handoff."""
    import subprocess
    import tempfile
    from datetime import datetime
    try:
        # Spool stdout: even a broken local CLI cannot grow collector memory.
        with tempfile.TemporaryFile() as output:
            result = subprocess.run(['vitral-host', 'keeper-receipt', '--provider', provider,
                                     '--native-id', identifier], stdout=output,
                                    stderr=subprocess.DEVNULL, timeout=10, check=False)
            output.seek(0)
            raw = output.read(16385)
        if result.returncode or len(raw) > 16384:
            raise ValueError()
        value = json.loads(raw)
        process, end = value.get('process'), value.get('end')
        if (value.get('schema_version') != 1 or value.get('provider') != provider or
                value.get('native_session_id') != identifier or
                not isinstance(value.get('keeper_session_id'), str) or not value['keeper_session_id'] or
                not isinstance(process, dict) or type(process.get('pid')) is not int or process['pid'] <= 0 or
                not isinstance(process.get('start_identity'), str) or not 0 < len(process['start_identity']) <= 512 or
                not isinstance(end, dict) or type(end.get('exit_status')) is not int or
                not 0 <= end['exit_status'] <= 255):
            raise ValueError()
        ended = datetime.fromisoformat(end['exited_at'].replace('Z', '+00:00'))
        if ended.utcoffset() is None or ended.utcoffset().total_seconds() != 0:
            raise ValueError()
        return value
    except (OSError, ValueError, TypeError, KeyError, AttributeError, subprocess.TimeoutExpired):
        raise SyncError('writer_unsupported_source') from None


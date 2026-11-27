"""Owner-local typed adapter. Browser payloads never select paths or commands."""
import argparse
from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import uuid

from csl_sync_state import State, SyncError, exclusive, read_private_json, private_json
from csl_sync_fs import open_read, private_file
from csl_sync_writer import Writer, native_lock, check_fence, require_stopped, save_fence, writer_incarnation


def canonical_uuid(value):
    return isinstance(value, str) and str(uuid.UUID(value)) == value


def validate(value):
    required = {'schema_version', 'action', 'request_id', 'provider', 'native_id',
                'registry_session_id', 'expected_snapshot_id', 'expected_generation', 'expected_host_id'}
    if not isinstance(value, dict) or not required <= value.keys() or value.keys() - required - {'intent', 'expected_device_id', 'target_id'}:
        raise SyncError('invalid_resume_request')
    try:
        if (type(value['schema_version']) is not int or value['schema_version'] != 1 or
                value['action'] not in ('check', 'release', 'import', 'run') or
                value['provider'] not in ('codex', 'claude') or
                any(not canonical_uuid(value[k]) for k in ('request_id', 'native_id', 'expected_snapshot_id')) or
                not re.fullmatch(r'ss_[0-9a-f]{64}', value['registry_session_id']) or
                not re.fullmatch(r'host-[0-9a-f]{16}', value['expected_host_id']) or
                type(value['expected_generation']) is not int or not 0 <= value['expected_generation'] < 2**53):
            raise ValueError()
        check = value['action'] == 'check'
        if check:
            if value.get('intent') not in ('source', 'destination', 'in_place'): raise ValueError()
            if value['intent'] in ('destination', 'in_place') and 'target_id' not in value: raise ValueError()
        elif 'intent' in value: raise ValueError()
        if 'expected_device_id' in value:
            if not canonical_uuid(value['expected_device_id']): raise ValueError()
        elif not (check and value['intent'] == 'destination'): raise ValueError()
        if 'target_id' in value and not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', value['target_id']): raise ValueError()
        if value['action'] in ('import', 'run') and 'target_id' not in value: raise ValueError()
        if (value['action'] == 'release' or check and value['intent'] == 'source') and 'target_id' in value: raise ValueError()
    except (ValueError, TypeError, AttributeError):
        raise SyncError('invalid_resume_request') from None
    return value


def private_text(path, limit):
    with os.fdopen(open_read(path), 'rb') as stream:
        if not private_file(path, os.fstat(stream.fileno())): raise SyncError('unsafe_local_binding')
        raw = stream.read(limit + 1)
    if len(raw) > limit: raise SyncError('unsafe_local_binding')
    return raw.decode('utf-8')


def absolute(value):
    if not isinstance(value, str) or not Path(value).is_absolute(): raise SyncError('unsafe_local_binding')
    return Path(value)


class Local:
    def __init__(self, request):
        if sys.platform != 'linux': raise SyncError('managed_platform_unsupported')
        self.request = request
        self.host_home = Path(os.environ.get('VITRAL_TUNNEL_HOME', str(Path.home() / '.vitral-tunnel')))
        if not self.host_home.is_absolute(): raise SyncError('unsafe_local_binding')
        host_info = self.host_home.lstat()
        if self.host_home.is_symlink() or host_info.st_uid != os.getuid() or host_info.st_mode & 0o077:
            raise SyncError('unsafe_local_binding')
        if private_text(self.host_home / 'api.HOST_ID', 128).strip() != request['expected_host_id']:
            raise SyncError('host_binding_changed')
        binding = read_private_json(self.host_home / 'collector_setup.json')
        if not binding or not {'home', 'runtime', 'source'} <= binding.keys():
            raise SyncError('personal_collector_required')
        self.profile, runtime, source = (absolute(binding[k]) for k in ('home', 'runtime', 'source'))
        if source.resolve() != Path(__file__).resolve().parent or not runtime.is_file():
            raise SyncError('collector_binding_changed')
        if not (self.profile / 'state.sqlite').is_file(): raise SyncError('sign_in_required')
        # The Connect installation has a fixed signed host path; guided local
        # installations retain their host executable in the enrolled home.
        self.host = self.host_home / 'vitral-host'
        if not self.host.is_file():
            self.host = Path('/opt/vitral-connect/bin/vitral-host')
        if not self.host.is_file(): raise SyncError('managed_host_required')
        self.target = None
        if request.get('target_id'):
            directory = self.host_home / 'managed-resume-targets'
            info = directory.stat()
            if info.st_uid != os.getuid() or info.st_mode & 0o077: raise SyncError('unsafe_local_binding')
            target = read_private_json(directory / (request['target_id'] + '.json'))
            if not target or set(target) != {'schema_version', 'provider', 'cwd', 'provider_home', 'executable'} or target['schema_version'] != 1 or target['provider'] != request['provider']:
                raise SyncError('invalid_resume_target')
            from csl_sync_providers import provider_home
            try:
                cwd, home, executable = (absolute(target[k]).resolve(strict=True) for k in ('cwd', 'provider_home', 'executable'))
            except (OSError, RuntimeError):
                raise SyncError('invalid_resume_target') from None
            if not cwd.is_dir() or home.resolve() != provider_home(request['provider']).resolve() or not executable.is_file():
                raise SyncError('invalid_resume_target')
            self.target = dict(target, cwd=str(cwd), provider_home=str(home), executable=str(executable))

    def run_host(self, admission):
        q = self.request
        payload = {k: q[k] for k in ('schema_version', 'request_id', 'provider', 'native_id', 'target_id', 'expected_host_id')}
        payload.update(admission)
        with tempfile.TemporaryFile() as output:
            completed = subprocess.run([str(self.host), 'keeper-managed-resume', '--json-stdin'],
                input=json.dumps(payload).encode(), stdout=output, stderr=subprocess.DEVNULL, timeout=30)
            output.seek(0); raw = output.read(4097)
        if completed.returncode or len(raw) > 4096: raise SyncError('managed_launch_refused')
        try:
            value = json.loads(raw)
            if (set(value) != {'schema_version', 'request_id', 'provider', 'native_session_id', 'state', 'keeper_session_id', 'process'} or
                    value['schema_version'] != 1 or value['request_id'] != q['request_id'] or value['provider'] != q['provider'] or
                    value['native_session_id'] != q['native_id'] or value['state'] != 'launched' or
                    not isinstance(value['keeper_session_id'], str) or not value['keeper_session_id'].startswith('live-') or
                    not canonical_uuid(value['keeper_session_id'][5:]) or
                    set(value['process']) != {'pid', 'start_identity'} or type(value['process']['pid']) is not int or value['process']['pid'] <= 0 or
                    not isinstance(value['process']['start_identity'], str) or not 0 < len(value['process']['start_identity']) <= 512):
                raise ValueError()
        except (ValueError, TypeError, KeyError): raise SyncError('invalid_managed_launch_result') from None
        return value


def execute(q):
    from csl_sync import attest_installation, pull
    from csl_sync_auth import Tokens
    from csl_sync_http import Registry
    from csl_sync_providers import _resolve, ProviderError
    local = Local(q)
    state = State(local.profile)
    try:
        identity = attest_installation(state)
        if q.get('expected_device_id', identity['device_id']) != identity['device_id']: raise SyncError('writer_device_required')
        derived = 'ss_' + hashlib.sha256(json.dumps([identity['account_id'], q['provider'], q['native_id']], separators=(',', ':')).encode()).hexdigest()
        if derived != q['registry_session_id']: raise SyncError('writer_owner_mismatch')
        settings = state.get('settings', {})
        registry = Registry(settings['endpoint'], Tokens(state.root, settings, allow_inflight_replay=False))
        writer = Writer(state, registry)
        remote = registry.call('GET', '/sessions/' + derived)
        if any(remote.get(k) != v for k, v in [('id', derived), ('provider', q['provider']), ('native_id', q['native_id']), ('snapshot_id', q['expected_snapshot_id'])]):
            raise SyncError('writer_snapshot_changed')
        base = dict(schema_version=1, request_id=q['request_id'], action=q['action'],
                    provider=q['provider'], native_session_id=q['native_id'], registry_session_id=derived,
                    host_id=q['expected_host_id'], **identity)
        if local.target: base['target_id'] = q['target_id']
        if q['action'] != 'check':
            # Bind retries to the owner-local target and complete typed payload
            # before any remote mutation or provider start.
            binding = dict(request=q, identity=identity, target=local.target, endpoint=settings['endpoint'],
                           host=str(local.host), profile=str(state.root.resolve()))
            journal = state.root / 'resume-actions' / (q['request_id'] + '-' + q['action'] + '.json')
            with exclusive(state.root):
                previous = read_private_json(journal)
                if previous is not None and previous != binding: raise SyncError('writer_request_conflict')
                if previous is None: private_json(journal, binding, durable=True)
        if q['action'] in ('check', 'release', 'run'):
            with exclusive(state.root), native_lock(q['provider'], q['native_id']):
                if q['action'] == 'run' or q.get('intent') == 'in_place':
                    fence = check_fence(state, q['provider'], q['native_id'], uploading=True)
                    record = state.session(q['provider'], q['native_id']).get('writer', {})
                    if (not fence or fence['state'] != 'active' or record.get('registry_id') != derived or
                            record.get('account_id') != identity['account_id'] or record.get('device_id') != identity['device_id'] or
                            record.get('state') != 'active' or record.get('generation') != q['expected_generation'] or
                            remote.get('writer', {}).get('state') != 'active' or remote['writer'].get('device_id') != identity['device_id'] or
                            remote['writer'].get('generation') != q['expected_generation']): raise SyncError('writer_fenced')
                    _resolve(q['provider'], q['native_id'], None)
                    incarnation = writer_incarnation(record)
                    # Older active fences did not carry the writer generation.
                    # Upgrade only after matching SQLite, attestation and registry
                    # while holding the same lock used by host admission.
                    if fence.get('writer') is None and q['action'] == 'run':
                        save_fence(q['provider'], q['native_id'], 'active', fence['binding'], writer=incarnation)
                    elif fence.get('writer') is not None and fence['writer'] != incarnation:
                        raise SyncError('writer_fenced')
                    admission = dict(expected_target=dict(local.target), expected_writer=incarnation,
                                     expected_binding=dict(fence['binding']))
                elif q['action'] == 'release' or q.get('intent') == 'source':
                    require_stopped(q['provider'], q['native_id'], host=str(local.host))
                    owner = remote.get('writer', {})
                    if (owner.get('generation') != q['expected_generation'] or
                            owner.get('device_id') != identity['device_id'] or owner.get('state') not in ('legacy', 'active', 'released')):
                        raise SyncError('writer_fenced')
                    if remote.get('sync', {}).get('complete') is not True: raise SyncError('source_incomplete')
                    if q['action'] == 'release':
                        from csl_sync_engine import Collector, Providers
                        from csl_sync_providers import _metadata
                        record = state.session(q['provider'], q['native_id']).get('writer', {})
                        if record.get('state') not in ('releasing', 'released'):
                            root, path = _resolve(q['provider'], q['native_id'], None)
                            Collector(state, registry, Providers())._sync_session(q['provider'], q['native_id'],
                                _metadata(q['provider'], path, root, 0))
                        writer.release(q['provider'], q['native_id'], registry_id=derived,
                            snapshot=q['expected_snapshot_id'], generation=q['expected_generation'], expected_device=identity['device_id'])
                else:
                    if not local.target: raise SyncError('resume_target_required')
                    # Read-only destination suitability is needed before the
                    # source releases. Only import requires released state.
                    owner = remote.get('writer', {})
                    if (owner.get('state') not in ('legacy', 'active', 'released') or
                            owner.get('generation') != q['expected_generation']):
                        raise SyncError('writer_fenced')
                    if remote.get('sync', {}).get('complete') is not True or remote.get('package', {}).get('available') is not True:
                        raise SyncError('source_incomplete')
                    try: _resolve(q['provider'], q['native_id'], None)
                    except ProviderError as error:
                        if error.code != 'session_not_found': raise
                    else: require_stopped(q['provider'], q['native_id'], host=str(local.host))
            if q['action'] == 'run': return dict(base, **{k:v for k,v in local.run_host(admission).items() if k not in base})
            return dict(base, state='released' if q['action'] == 'release' else 'eligible')
        if not local.target: raise SyncError('resume_target_required')
        from csl_resume import import_same_id
        with tempfile.TemporaryDirectory(prefix='resume-action-', dir=state.root) as directory:
            package = pull(registry, derived, Path(directory))
            args = SimpleNamespace(sync_home=state.root, bundle=Path(package['path']), cwd=Path(local.target['cwd']),
                open=False, list=False, name=None, target=None, leaf=None, repair_interrupted=False, history_policy='active',
                provider=q['provider'], registry_session_id=derived, expected_snapshot_id=q['expected_snapshot_id'],
                expected_generation=q['expected_generation'], request_id=q['request_id'], expected_device_id=identity['device_id'],
                receipt_host=str(local.host))
            with redirect_stdout(io.StringIO()): result = import_same_id(args, None)
            if result != [q['native_id']]: raise SyncError('invalid_resume_result')
        return dict(base, state='imported', generation=q['expected_generation'] + 1)
    finally: state.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--json-stdin', action='store_true', required=True)
    parser.parse_args()
    try:
        raw = sys.stdin.buffer.read(4097)
        if len(raw) > 4096: raise SyncError('invalid_resume_request')
        result = execute(validate(json.loads(raw)))
        print(json.dumps(result, separators=(',', ':')))
    except SyncError as error:
        print(json.dumps({'error':error.code, 'retryable':error.retryable}), file=sys.stderr)
        return 75 if error.retryable or error.code == 'collector_busy' else 1
    except (OSError, ValueError, TypeError, KeyError, subprocess.TimeoutExpired):
        print('{"error":"resume_action_failed"}', file=sys.stderr)
        return 1
    return 0

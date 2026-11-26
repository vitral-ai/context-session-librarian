"""Explicit raw same-ID import, separate from the managed collector runtime."""
import hashlib
import json
import os
import sys
from pathlib import Path
from csl_sync_state import SyncError, exclusive
from csl_sync_writer import Writer, native_id, native_lock, require_stopped, check_fence


def same_bytes(path, digest):
    from csl_sync_fs import open_read
    try:
        with os.fdopen(open_read(path), 'rb') as source:
            return hashlib.file_digest(source, 'sha256').hexdigest() == digest
    except OSError:
        raise SyncError('native_destination_conflict') from None


def safe_destination(path, root):
    from csl_sync_fs import reparse
    try:
        parts = path.relative_to(root).parts
        current = root
        for part in parts:
            current = current / part
            if reparse(current):
                raise ValueError()
    except ValueError:
        raise SyncError('native_destination_conflict') from None


def verify_native_content(path, provider, identifier):
    from csl_canonical import strict_json
    found = False
    with path.open('rb') as source:
        while line := source.readline(8*1024*1024 + 1):
            if len(line) > 8*1024*1024 or not line.endswith(b'\n'):
                raise SyncError('invalid_native_primary_content')
            if not line.strip(): continue
            try:
                row = strict_json(line)
                if not isinstance(row, dict): raise ValueError()
                if provider == 'codex' and row.get('type') == 'session_meta':
                    if row.get('payload', {}).get('id') != identifier: raise ValueError()
                    found = True
                if provider == 'claude':
                    if row.get('sessionId') not in (None, identifier): raise ValueError()
                    if row.get('type') in ('user', 'assistant'):
                        if (not isinstance(row.get('message'), dict) or
                                row['message'].get('role') != row['type'] or not isinstance(row.get('uuid'), str)):
                            raise ValueError()
                        found = True
            except (ValueError, UnicodeError, TypeError, AttributeError):
                raise SyncError('invalid_native_primary_content') from None
    if not found: raise SyncError('invalid_native_primary_content')


def validate_arguments(args):
    import uuid
    for key in ('registry_session_id', 'expected_snapshot_id', 'expected_device_id'):
        native_id(getattr(args, key, None))
    generation = getattr(args, 'expected_generation', None)
    if type(generation) is not int or not 0 <= generation < 2**63 - 1:
        raise SyncError('invalid_writer_generation')
    try:
        if str(uuid.UUID(args.request_id)) != args.request_id: raise ValueError()
    except (ValueError, TypeError, AttributeError):
        raise SyncError('invalid_writer_request_id') from None


def import_same_id(args, csl):
    """Validate and stage first; acquire once; preserve exact raw bytes and native ID."""
    import tempfile
    import zipfile
    from csl_sync_auth import Tokens
    from csl_sync_http import Registry
    from csl_sync_state import State
    from csl_provider_bundle import validate
    from csl_transfer import publish_session_file
    from csl_sync_providers import provider_home, _resolve, ProviderError
    import csl_claude
    validate_arguments(args)
    # Raw CLI launches do not reserve a native ID under the managed launcher lock.
    if args.open:
        raise SyncError('writer_managed_launch_required')
    state = State(args.sync_home)
    try:
        settings = state.get('settings', {})
        registry = Registry(settings['endpoint'], Tokens(state.root, settings))
        writer = Writer(state, registry)
        with exclusive(state.root), zipfile.ZipFile(args.bundle.expanduser()) as archive:
            # Read only the bounded identity hint, then validate all members while locked.
            try:
                manifest_info = archive.getinfo('manifest.json')
            except KeyError:
                raise SyncError('invalid_native_manifest') from None
            if manifest_info.file_size > 1024*1024:
                raise SyncError('oversized_native_manifest')
            manifest = json.loads(archive.read('manifest.json'))
            if (not isinstance(manifest, dict) or not isinstance(manifest.get('sessions'), list) or
                    len(manifest['sessions']) != 1 or not isinstance(manifest['sessions'][0], dict)):
                raise SyncError('writer_requires_single_session')
            entry = manifest['sessions'][0]
            provider, identifier = entry.get('provider'), native_id(entry.get('id'))
            if provider not in ('codex', 'claude') or getattr(args, 'provider', None) not in (None, provider):
                raise SyncError('writer_unsupported_source')
            if ('codex_history' in entry or args.list or args.name or getattr(args, 'target', None) or
                    getattr(args, 'leaf', None) or getattr(args, 'repair_interrupted', False) or
                    getattr(args, 'history_policy', 'active') != 'active'):
                raise SyncError('writer_unsupported_source')
            cwd = args.cwd.expanduser().resolve()
            if not cwd.is_dir(): raise SyncError('native_cwd_missing')
            with native_lock(provider, identifier):
                manifest = validate(archive)
                fence = check_fence(state, provider, identifier)
                record = state.session(provider, identifier).get('writer', {})
                try:
                    _, existing = _resolve(provider, identifier, None)
                except ProviderError as error:
                    if error.code != 'session_not_found': raise
                    existing = None
                unpublished_retry = (record.get('request_id') == args.request_id and
                                     record.get('state') in ('pending', 'publishing', 'active') and fence and
                                     fence['state'] in ('pending', 'publishing'))
                if existing is not None and not unpublished_retry:
                    require_stopped(provider, identifier)
                account = writer.attest(args.expected_device_id)
                remote = registry.call('GET', '/sessions/' + args.registry_session_id)
                matching_retry = (record.get('request_id') == args.request_id and
                    remote.get('writer', {}).get('device_id') == args.expected_device_id and
                    remote.get('writer', {}).get('generation') == args.expected_generation + 1 and
                    remote.get('writer', {}).get('state') == 'active')
                if not matching_retry:
                    writer.released(args.registry_session_id, provider, identifier,
                                    args.expected_snapshot_id, args.expected_generation)
                if (remote.get('id') != args.registry_session_id or remote.get('provider') != provider or
                        remote.get('native_id') != identifier or remote.get('snapshot_id') != args.expected_snapshot_id or
                        remote.get('sync', {}).get('complete') is not True):
                    raise SyncError('writer_snapshot_changed')
                # Bind a caller-provided ZIP to the authenticated owner-scoped package.
                with tempfile.TemporaryDirectory(prefix='resume-', dir=state.root) as directory:
                    directory = Path(directory)
                    package = directory / 'registry.zip'
                    digest = hashlib.sha256()
                    class Sink:
                        def __init__(self, stream): self.stream = stream
                        def write(self, value): digest.update(value); return self.stream.write(value)
                    with package.open('xb') as output:
                        os.chmod(package, 0o600)
                        registry.download('/sessions/' + args.registry_session_id + '/package', Sink(output), 4*1024**3)
                    with zipfile.ZipFile(package) as verified:
                        actual = validate(verified)
                        # ZIP timestamps/compression need not match; the verified manifest/member digests must.
                        if actual != manifest: raise SyncError('writer_package_mismatch')
                    package_hash = hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
                    target = existing
                    if target is None:
                        if provider == 'claude': target = csl_claude.target_path(identifier, cwd)
                        else:
                            from datetime import datetime, timezone
                            stamp = datetime.fromisoformat(manifest['created_at'].replace('Z', '+00:00'))
                            if stamp.utcoffset() is None: raise SyncError('invalid_native_timestamp')
                            stamp = stamp.astimezone(timezone.utc).strftime('%Y-%m-%dT%H-%M-%S')
                            target = provider_home(provider) / 'sessions' / stamp[:4] / stamp[5:7] / stamp[8:10] / ('rollout-' + stamp + '-' + identifier + '.jsonl')
                    companion = target.with_suffix('') if provider == 'claude' else target.parent / identifier
                    plans = []
                    for i, item in enumerate(entry['files']):
                        primary = item['file'] == entry['primary']
                        destination = target if primary else companion / item['relative_path'].removeprefix('session/')
                        safe_destination(destination, provider_home(provider))
                        local = directory / str(i)
                        h = hashlib.sha256()
                        with archive.open(item['file']) as source, local.open('xb') as output:
                            os.chmod(local, 0o600)
                            while chunk := source.read(1024*1024):
                                h.update(chunk); output.write(chunk)
                        if h.hexdigest() != item['sha256']: raise SyncError('native_checksum_mismatch')
                        if primary: verify_native_content(local, provider, identifier)
                        if destination.exists() or destination.is_symlink():
                            if not same_bytes(destination, item['sha256']): raise SyncError('native_destination_conflict')
                        plans.append((primary, local, destination, item['sha256']))
                    writer.acquire(provider, identifier, registry_id=args.registry_session_id,
                        snapshot=args.expected_snapshot_id, generation=args.expected_generation,
                        request_id=args.request_id, expected_device=args.expected_device_id,
                        package_sha256=package_hash, account_id=account,
                        destinations=[str(destination) for _, _, destination, _ in plans])
                    # No rollback deletion: after acquisition a partial publication stays pending for exact retry.
                    for _, local, destination, expected_digest in sorted(plans):
                        safe_destination(destination, provider_home(provider))
                        if not destination.exists(): publish_session_file(local, destination)
                        if not same_bytes(destination, expected_digest): raise SyncError('native_destination_conflict')
                        from csl_sync_fs import flush_directory
                        flush_directory(destination.parent)
                    writer.published(provider, identifier)
                    state.select(provider, identifier, True)
                print('resumed: ' + identifier + '; writer generation ' + str(args.expected_generation + 1))
                print('Resume through Vitral Connect using the explicit ' + provider + ' native ID ' + identifier + '.')
        return [identifier]
    finally:
        state.close()

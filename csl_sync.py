"""Explicit opt-in sync CLI. No startup side effects, daemon or provider parsing."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sqlite3
import stat
import sys
import tempfile
import time
import urllib.parse

from csl_sync_auth import Tokens, discover
from csl_sync_engine import Collector, PROVIDERS, Providers
from csl_sync_fs import WINDOWS, protect_fd, protect_path, reparse, replace
from csl_sync_http import HTTP, Registry, checked_url
from csl_sync_state import State, SyncError, exclusive, now, private_json
from csl_sync_watch import pass_lock

WARNING = ('Raw sync uploads the complete transcript bytes and supported companion files, '
           'including any secrets they contain. It does not scrub or encrypt them for the registry. '
           'Only enable it for a registry and account you trust. Nothing syncs until you sign in and run sync.')



def recovery_action(state, settings, authentication):
    reason = state.get('last_error')
    auth_state = authentication.get('state')
    if auth_state == 'inflight': reason = 'refresh_outcome_unknown'
    elif auth_state in ('revoked', 'signed_out'): reason = 'sign_in_required'
    if reason not in ('refresh_outcome_unknown', 'sign_in_required', 'credential_revoked'):
        return None
    keys = ('endpoint', 'discovery_url', 'client_id')
    if not all(isinstance(settings.get(key), str) and settings[key] for key in keys):
        return {'reason': reason, 'command': None, 'argv': None}
    argv = ['csl', 'sync', '--home', str(state.root), 'sign-in']
    for key in keys + ('scope', 'audience', 'label', 'kind', 'environment'):
        value = settings.get(key)
        if value:
            option = 'environment-name' if key == 'environment' else key.replace('_', '-')
            argv.extend(['--' + option, str(value)])
    if settings.get('manual_browser'):
        argv.append('--manual-browser')
    if type(settings.get('callback_port')) is int:
        argv.extend(['--callback-port', str(settings['callback_port'])])
    if type(settings.get('refresh_replay_grace_seconds')) is int:
        argv.extend(['--refresh-replay-grace-seconds', str(settings['refresh_replay_grace_seconds'])])
    instructions = None
    if 'manual_browser' not in settings or 'callback_port' not in settings:
        instructions = 'On a headless host, add --manual-browser --callback-port PORT and forward that loopback port to your browser.'
    return dict(reason=reason, argv=argv, instructions=instructions,
                command=subprocess.list2cmdline(argv) if os.name == 'nt' else shlex.join(argv))


def status_snapshot(state):
    settings = state.get('settings', {})
    result = state.status()
    result['authentication'] = Tokens(state.root, settings).status()
    result['enabled'] = bool(settings.get('enabled'))
    result.update(schema_version=1, paired=bool(result['authentication']['signed_in'] and state.get('registered', False)),
                  policy=state.get('selection_policy', {'selection': 'legacy', 'configured_at': None, 'limit': None, 'folders': []}))
    try:
        with exclusive(state.root): running = False
    except SyncError as error:
        if error.code != 'collector_busy': raise
        running = True
    busy = running
    running = busy and state.get('lock_action') in ('run', 'watch')
    result['collector'] = {'running': running, 'busy': busy}
    from csl_sync_watch import watch_pid
    owner_pid = watch_pid(state) if busy else None
    if owner_pid is not None:
        result['collector'].update(running=True, watch_pid=owner_pid)
    rows = result['sessions']
    included = [row for row in rows if row['selected']]
    result['totals'] = dict(selected=len(included), synced=sum(row['status'] == 'synced' for row in included),
                            pending=sum(row['status'] != 'synced' for row in included),
                            excluded=state.db.execute('SELECT COUNT(*) FROM selections WHERE included=0').fetchone()[0],
                            synced_bytes=sum(row['synced_bytes'] for row in included),
                            planned_bytes=sum(row['planned_bytes'] for row in included))
    recovery = recovery_action(state, settings, result['authentication'])
    if recovery:
        result['recovery'] = recovery
        result['paired'] = False
    return result


def print_status(result, *, verbose=False):
    totals = result['totals']
    running = result['collector']['running']
    print(f"Collector {'running' if running else 'stopped'}; {'paired' if result['paired'] else 'not paired'}; policy {result['policy']['selection']}.")
    print(f"{totals['synced']} synced, {totals['pending']} pending, {totals['excluded']} excluded; {totals['synced_bytes']}/{totals['planned_bytes']} bytes uploaded.")
    if result.get('last_error'): print('Last error: ' + result['last_error'])
    if result.get('last_error') == 'workload_text_format_refused':
        print('Upload refused: unsupported workload transcript schema or version; update the managed collector. Source files are unchanged.')
    recovery = result.get('recovery')
    if recovery:
        print('Sign in again: ' + (recovery['command'] or 'csl sync sign-in --help'))
        if recovery.get('instructions'): print(recovery['instructions'])
    if verbose:
        for row in result['sessions']:
            print(f"{row['provider']} {row['native_id']} {row['status']} {row['last_error'] or ''}")
    if running:
        print('You can close this status view; the collector continues running.')
    else:
        print('This view does not start uploads; start your configured collector service to sync.')


def configure_choice(state, selection=None, *, limit=None, folders=()):
    from csl_sync_policy import configure
    if selection is None:
        if not sys.stdin.isatty():
            raise SyncError('selection_required')
        print('Upload sessions: 1) New only [default]  2) Last N and new  3) Everything  4) By folder', file=sys.stderr)
        def answer(prompt):
            try: return input(prompt).strip()
            except EOFError: raise SyncError('selection_cancelled') from None
        choice = answer('Choice [1]: ') or '1'
        selection = {'1': 'new', '2': 'last', '3': 'all', '4': 'folders'}.get(choice)
        if selection is None: raise SyncError('invalid_selection_policy')
        if selection == 'last':
            try: limit = int(answer('How many recent sessions? [30]: ') or '30')
            except ValueError: raise SyncError('invalid_selection_limit') from None
        elif selection == 'folders':
            paths = []
            while True:
                value = answer('Folder (empty to finish): ')
                if not value: break
                paths.append(value)
            folders = paths
    rows = []
    if selection == 'last':
        sources = Providers(state.root)
        for provider in PROVIDERS:
            rows.extend({**row, 'provider': provider} for row in sources.list_sessions(provider))
    return configure(state, selection, limit=limit, folders=folders, rows=rows)


def command(args, csl):
    state = None
    try:
        if args.sync_action == 'attest-installation' and not (Path(args.sync_home).expanduser() / 'state.sqlite').is_file():
            raise SyncError('sign_in_required')
        state = State(args.sync_home)
        action = args.sync_action
        if action == 'attest-installation':
            print(json.dumps(attest_installation(state), separators=(',', ':')))
            return
        if action == 'status':
            while True:
                result = status_snapshot(state)
                if args.json:
                    print(json.dumps(result, separators=(',', ':')), flush=True)
                else:
                    print_status(result, verbose=args.verbose)
                    sys.stdout.flush()
                if not getattr(args, 'follow', False): return
                time.sleep(args.interval)
        if action == 'watch':
            settings = state.get('settings', {})
            if not settings.get('enabled') or not Tokens(state.root, settings).status()['signed_in']:
                raise SyncError('sign_in_required')
            from csl_sync_cache import CachedProviders
            from csl_sync_watch import watch
            import csl_sync_providers
            watch(args, state, CachedProviders(csl_sync_providers, state.root))
            return
        if action == 'pull':
            # Read-only download with this device's own sign-in. It must not take the
            # collector lock, or it would fail on every host where sync watch runs;
            # token refresh serializes on its own lock.
            settings = state.get('settings', {})
            if not settings.get('enabled'):
                raise SyncError('sign_in_required')
            tokens = Tokens(state.root, settings)
            if not tokens.status()['signed_in']:
                raise SyncError('sign_in_required')
            print(json.dumps(pull(Registry(settings['endpoint'], tokens), args.session_id,
                                  args.out_dir or state.root / 'pulled'), separators=(',', ':')))
            return
        with pass_lock(state.root):
            state.set('lock_action', action)
            if action == 'configure':
                policy = configure_choice(state, args.selection, limit=args.limit, folders=args.folder or [])
                print(json.dumps({'policy': policy, 'remote_deleted': False}))
                return
            if action in ('include', 'exclude'):
                state.select(args.provider, args.native_id, action == 'include')
                print(json.dumps({'provider': args.provider, 'included': action == 'include', 'remote_deleted': False}))
                return
            if action == 'sign-in':
                print(WARNING, file=sys.stderr)
                # Preserve installed collectors' legacy selection; new installs anchor once.
                if state.get('selection_policy') is None and not state.get('settings'):
                    configure_choice(state, None if sys.stdin.isatty() else 'new')
                state.set('registered', False)
                settings = {'endpoint': checked_url(args.endpoint), 'client_id': args.client_id,
                            'discovery_url': checked_url(args.discovery_url), 'scope': args.scope,
                            'audience': args.audience, 'refresh_replay_grace_seconds': args.refresh_replay_grace_seconds,
                            'label': args.label, 'kind': args.kind, 'environment': args.environment_name,
                            'enabled': False, 'manual_browser': bool(getattr(args, 'manual_browser', False)),
                            'callback_port': getattr(args, 'callback_port', 0)}
                state.set('settings', settings)
                settings.update(discover(settings['discovery_url']))
                requested_grace = args.refresh_replay_grace_seconds
                advertised_grace = settings.get('refresh_replay_grace_seconds')
                settings['refresh_replay_grace_seconds'] = (
                    min(requested_grace, advertised_grace) if requested_grace is not None and advertised_grace is not None
                    else requested_grace if requested_grace is not None else advertised_grace or 0)
                state.set('settings', settings)
                options = {'device_id': state.get('device_id'), 'device_label': args.label,
                           'callback_port': getattr(args, 'callback_port', 0),
                           'timeout': getattr(args, 'sign_in_timeout', 180)}
                authorization_file = state.root / 'authorization.json'
                if getattr(args, 'manual_browser', False):
                    def manual_browser(url):
                        private_json(authorization_file, {'authorization_url': url})
                        print('Open authorization.json in the selected private sync state directory with your browser; waiting for the loopback callback.', file=sys.stderr)
                        return True
                    options['browser'] = manual_browser
                try:
                    result = Tokens(state.root, settings).sign_in(**options)
                finally:
                    if getattr(args, 'manual_browser', False): authorization_file.unlink(missing_ok=True)
                state.reset_checkpoints()
                settings.update(enabled=True, enabled_at=now())
                state.set('settings', settings)
                state.set('last_error', None)
                print(json.dumps({'authentication': result, 'enabled': True, 'sync_started': False}))
                return
            settings = state.get('settings', {})
            if action == 'sign-out':
                Tokens(state.root, settings).revoke_locally()
                settings['enabled'] = False
                state.set('settings', settings)
                print(json.dumps({'signed_in': False, 'remote_revocation': False, 'sync_enabled': False}))
                return
            if not settings.get('enabled'):
                raise SyncError('sign_in_required')
            tokens = Tokens(state.root, settings)
            if not tokens.status()['signed_in']:
                raise SyncError('sign_in_required')
            registry = Registry(settings['endpoint'], tokens)
            if action == 'register-device':
                # Registration needs no provider discovery or transcript reads.
                result = Collector(state, registry, None).register()
                print(json.dumps({'device_id': state.get('device_id'), 'registered': True}))
                return
            if action == 'release-writer':
                from csl_sync_writer import Writer, native_id, native_lock, require_stopped
                from csl_sync_providers import _resolve, _metadata
                native_id(args.native_id)
                native_id(args.registry_session_id)
                native_id(args.expected_snapshot_id)
                native_id(args.expected_device_id)
                if not 0 <= args.expected_generation < 2**63 - 1:
                    raise SyncError('invalid_writer_generation')
                with native_lock(args.provider, args.native_id):
                    require_stopped(args.provider, args.native_id)
                    writer = Writer(state, registry)
                    writer.attest(args.expected_device_id)
                    local = state.session(args.provider, args.native_id).get('writer', {})
                    if local.get('state') not in ('releasing', 'released'):
                        root, path = _resolve(args.provider, args.native_id, None)
                        metadata = _metadata(args.provider, path, root, 0)
                        # Keep the same native lock through flush and remote release.
                        Collector(state, registry, Providers())._sync_session(args.provider, args.native_id, metadata)
                    result = writer.release(args.provider, args.native_id,
                        registry_id=args.registry_session_id, snapshot=args.expected_snapshot_id,
                        generation=args.expected_generation, expected_device=args.expected_device_id)
                print(json.dumps(result))
                return
            providers = Providers(state.root)
            collector = Collector(state, registry, providers)
            result = collector.run(days=None if args.all else args.days, providers=args.providers or PROVIDERS,
                                   budget_seconds=args.budget_seconds, budget_bytes=args.budget_bytes)
            print(json.dumps(result, separators=(',', ':')), flush=True)
            if result['errors']: raise SystemExit(2)
    except SyncError as error:
        attesting = args.sync_action == 'attest-installation'
        if state is not None and args.sync_action not in ('status', 'attest-installation'): state.set('last_error', error.code)
        print(json.dumps({'state': 'stopped', 'error': error.code, 'retryable': error.retryable}), file=sys.stderr)
        raise SystemExit((75 if error.retryable or error.code == 'collector_busy' else 1) if attesting else 2) from None
    except KeyboardInterrupt:
        print(json.dumps({'state': 'stopped', 'reason': 'interrupted'}), file=sys.stderr)
        raise SystemExit(130) from None
    except (OSError, ValueError, KeyError, sqlite3.Error):
        print(json.dumps({'state': 'stopped', 'error': 'sync_failed'}), file=sys.stderr)
        raise SystemExit(1 if args.sync_action == 'attest-installation' else 2) from None
    finally:
        if state is not None: state.close()


def attest_installation(state):
    """Server-attested installation identity; never reads or uploads transcripts.

    The embedding host enforces a seven-second wall-clock limit. Each network
    operation uses two seconds, including the existing bounded refresh replay.
    """
    import uuid
    from csl_sync_writer import Writer
    settings = state.get('settings', {})
    if not settings.get('enabled'):
        raise SyncError('sign_in_required')
    device = state.get('device_id')
    try:
        if not isinstance(device, str) or str(uuid.UUID(device)) != device:
            raise ValueError
    except (ValueError, AttributeError):
        raise SyncError('writer_device_required') from None
    http = HTTP(timeout=2, attempts=1)
    tokens = Tokens(state.root, settings, http=http, allow_inflight_replay=False)
    account = Writer(state, Registry(settings['endpoint'], tokens, http=http)).attest(device)
    if not isinstance(account, str) or re.fullmatch(r'[0-9a-f]{24}', account) is None:
        raise SyncError('writer_device_required')
    return {'account_id': account, 'device_id': device}


MAX_PULL = 2 * 1024 ** 3
SESSION_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}')


def output_directory(path):
    """Use an existing directory as it is; only a directory we create gets mode 0700."""
    path = Path(path).expanduser().absolute()
    if reparse(path):
        raise SyncError('unsafe_output_directory')
    if WINDOWS:
        from csl_sync_state import private_directory
        return private_directory(path)
    if not path.exists():
        path.mkdir(parents=True, mode=0o700)
    info = path.stat()
    if info.st_uid != os.getuid() or not stat.S_ISDIR(info.st_mode):
        raise SyncError('unsafe_output_directory')
    return path


def pull(registry, session_id, out_dir):
    """Fetch one package the signed-in account owns into a private file.

    The registry derives the owner from this device's own token, so another
    account's session id is not_found. Nothing is written for a refused request.
    """
    if SESSION_ID.fullmatch(session_id) is None:
        raise SyncError('invalid_session_id')
    directory = output_directory(out_dir)
    filename = session_id + '.zip'
    if WINDOWS and (filename != filename.lower() or os.path.isreserved(filename)):
        # '%' separates opaque names from the lowercase canonical registry IDs.
        filename = 'session%' + hashlib.sha256(session_id.encode('ascii')).hexdigest() + '.zip'
    target = directory / filename
    # The same session can be resumed again (or retried after a failed import): replace
    # a previous pull of it atomically, but never follow a symlink or replace a non-file.
    if reparse(target) or (target.exists() and not target.is_file()):
        raise SyncError('unsafe_output_file')
    fd, temporary = tempfile.mkstemp(prefix='.pull-', suffix='.part', dir=directory)
    try:
        protect_fd(fd)
        if WINDOWS:
            protect_path(Path(temporary))
        digest = hashlib.sha256()
        class Sink:
            def write(self, chunk):
                digest.update(chunk)
                os.write(fd, chunk)
        size = registry.download('/sessions/' + urllib.parse.quote(session_id, safe='') + '/package', Sink(), MAX_PULL)
        os.fsync(fd)
        os.close(fd); fd = None
        with open(temporary, 'rb') as check:
            if check.read(4) != b'PK\x03\x04':
                raise SyncError('invalid_server_response')
        replace(temporary, target); temporary = None
    finally:
        if fd is not None: os.close(fd)
        if temporary is not None: os.unlink(temporary)
    return {'session_id': session_id, 'path': str(target), 'bytes': size, 'sha256': digest.hexdigest()}


def positive(value):
    value = int(value)
    if not 1 <= value <= 86400: raise argparse.ArgumentTypeError('Choose a value from 1 to 86400')
    return value


def grace(value):
    value = int(value)
    if not 0 <= value <= 30: raise argparse.ArgumentTypeError('Choose 0..30 seconds, only when the issuer guarantees successor replay')
    return value


def byte_budget(value):
    value = int(value)
    if not 1 <= value <= 1024**4:
        raise argparse.ArgumentTypeError('Choose 1 to 1099511627776 bytes')
    return value


def port(value):
    value = int(value)
    if not 0 <= value <= 65535: raise argparse.ArgumentTypeError('Choose a port from 0 to 65535')
    return value


def add_commands(subparsers, csl):
    parser = subparsers.add_parser('sync', help='Explicit opt-in raw session sync to a configured registry; no automatic service.')
    base = os.environ.get('LOCALAPPDATA') if WINDOWS else os.environ.get('XDG_STATE_HOME')
    default = (Path(base) if base else Path.home() / '.local/state') / 'csl' / 'sync'
    parser.add_argument('--home', dest='sync_home', type=Path, default=default, help='Private state and credentials directory.')
    actions = parser.add_subparsers(dest='sync_action', required=True)
    signin = actions.add_parser('sign-in', help=WARNING, description=WARNING)
    signin.add_argument('--endpoint', required=True, help='Registry base URL, including its API prefix; no default.')
    signin.add_argument('--discovery-url', required=True, help='OAuth server metadata URL; no default.')
    signin.add_argument('--client-id', required=True, help='Registered public PKCE client; no client secret.')
    signin.add_argument('--scope', default='session_sync offline_access')
    signin.add_argument('--audience')
    signin.add_argument('--refresh-replay-grace-seconds', type=grace, default=None,
                        help='Limit issuer-advertised successor replay; 0 opts out. If omitted, use discovery (otherwise 0). Set a nonzero value only for a documented issuer policy.')
    signin.add_argument('--label', default='CSL device')
    signin.add_argument('--kind', choices=['local','environment'], default='local')
    signin.add_argument('--environment-name')
    signin.add_argument('--sign-in-timeout', type=int, default=180,
                        help='Seconds to wait for the browser login before giving up (default 180).')
    signin.add_argument('--callback-port', type=port, default=0, help='Loopback port; 0 chooses a free port. Useful with an explicit SSH forward.')
    signin.add_argument('--manual-browser', action='store_true', help='Write the authorization URL to a private state file, never logs; open it in your own system browser.')
    status = actions.add_parser('status', help='Short local collector status; no network calls or credentials.')
    status.add_argument('--follow', action='store_true', help='Read-only progress view; Ctrl-C closes this view, not the collector.')
    status.add_argument('--interval', type=positive, default=2, help='Seconds between local progress snapshots.')
    status.add_argument('--json', action='store_true', help='Versioned machine-readable status.')
    status.add_argument('--verbose', action='store_true', help='Include the per-session table.')
    config = actions.add_parser('configure', help='Choose local upload selection; never deletes remote sessions. Stop watch first.')
    config.add_argument('--selection', choices=['new', 'last', 'all', 'folders'])
    config.add_argument('--limit', type=positive)
    config.add_argument('--folder', action='append')
    actions.add_parser('register-device', help='Register this signed-in installation without uploading sessions.')
    attest = actions.add_parser('attest-installation', help='Read server-attested account/device identity; may refresh this installation sign-in. No transcript upload.')
    attest.add_argument('--json', required=True, action='store_true', help='Only account_id and device_id on success; no stdout on failure.')
    release = actions.add_parser('release-writer', help='Flush and release a positively ended managed native session. Unmanaged sessions must Fork.')
    release.add_argument('provider', choices=['codex', 'claude'])
    release.add_argument('native_id')
    release.add_argument('--registry-session-id', required=True)
    release.add_argument('--expected-snapshot-id', required=True)
    release.add_argument('--expected-generation', required=True, type=int)
    release.add_argument('--expected-device-id', required=True)
    pull_parser = actions.add_parser('pull', help='Download one of this signed-in account\'s sessions as a package ZIP for csl import. Uses this installation\'s own sign-in; no token in arguments or output.')
    pull_parser.add_argument('session_id')
    pull_parser.add_argument('--out-dir', type=Path, help='Private output directory (default: <sync home>/pulled); created with mode 0700.')
    actions.add_parser('sign-out', help='Disable this local installation and erase stored tokens; stop watch first. Does not revoke remotely.')
    for action in ('include','exclude'):
        selector = actions.add_parser(action, help='Local selection only; never deletes remote content.')
        selector.add_argument('provider', choices=PROVIDERS); selector.add_argument('native_id')
    for action in ('run','watch'):
        runner = actions.add_parser(action, help='One pass.' if action == 'run' else 'Continuous foreground sync; no daemon or timer installed.')
        runner.add_argument('--budget-seconds', type=positive, default=30, help='Cooperative pass deadline; an in-flight request may finish.')
        runner.add_argument('--budget-bytes', type=byte_budget, default=256*1024*1024, help='Maximum transcript I/O and upload bytes per pass (includes verification reads).')
        runner.add_argument('--days', type=positive, default=30)
        runner.add_argument('--all', action='store_true', help='Include older sessions too, except explicit exclusions.')
        runner.add_argument('--provider', dest='providers', action='append', choices=PROVIDERS)
        if action == 'watch': runner.add_argument('--interval', type=positive, default=30)
    parser.set_defaults(func=lambda args: command(args, csl))


def main(argv=None):
    parser = argparse.ArgumentParser(prog='csl')
    add_commands(parser.add_subparsers(required=True), None)
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == '__main__':
    main()

"""Explicit opt-in sync CLI. No startup side effects, daemon or provider parsing."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import tempfile
import time
import urllib.parse

from csl_sync_auth import Tokens, discover
from csl_sync_engine import Collector, PROVIDERS, Providers
from csl_sync_fs import WINDOWS, protect_fd, reparse, replace
from csl_sync_http import Registry, checked_url
from csl_sync_state import State, SyncError, exclusive, now, private_json
from csl_sync_watch import pass_lock

WARNING = ('Raw sync uploads the complete transcript bytes and supported companion files, '
           'including any secrets they contain. It does not scrub or encrypt them for the registry. '
           'Only enable it for a registry and account you trust. Nothing syncs until you sign in and run sync.')


def command(args, csl):
    state = None
    try:
        state = State(args.sync_home)
        action = args.sync_action
        if action == 'status':
            settings = state.get('settings', {})
            result = state.status()
            result['authentication'] = Tokens(state.root, settings).status()
            result['enabled'] = bool(settings.get('enabled'))
            print(json.dumps(result, separators=(',', ':')))
            return
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
            if action in ('include', 'exclude'):
                state.select(args.provider, args.native_id, action == 'include')
                print(json.dumps({'provider': args.provider, 'included': action == 'include', 'remote_deleted': False}))
                return
            if action == 'sign-in':
                print(WARNING, file=sys.stderr)
                settings = {'endpoint': checked_url(args.endpoint), 'client_id': args.client_id,
                            'discovery_url': checked_url(args.discovery_url), 'scope': args.scope,
                            'audience': args.audience, 'refresh_replay_grace_seconds': args.refresh_replay_grace_seconds,
                            'label': args.label, 'kind': args.kind, 'environment': args.environment_name,
                            'enabled': False}
                state.set('settings', settings)
                settings.update(discover(settings['discovery_url']))
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
            providers = Providers(state.root)
            collector = Collector(state, registry, providers)
            result = collector.run(days=None if args.all else args.days, providers=args.providers or PROVIDERS)
            print(json.dumps(result, separators=(',', ':')), flush=True)
            if result['pending']: raise SystemExit(2)
    except SyncError as error:
        if state is not None: state.set('last_error', error.code)
        print(json.dumps({'state': 'stopped', 'error': error.code, 'retryable': error.retryable}), file=sys.stderr)
        raise SystemExit(2) from None
    except KeyboardInterrupt:
        print(json.dumps({'state': 'stopped', 'reason': 'interrupted'}), file=sys.stderr)
        raise SystemExit(130) from None
    except (OSError, ValueError, KeyError, sqlite3.Error):
        print(json.dumps({'state': 'stopped', 'error': 'sync_failed'}), file=sys.stderr)
        raise SystemExit(2) from None
    finally:
        if state is not None: state.close()


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
    signin.add_argument('--refresh-replay-grace-seconds', type=grace, default=0,
                        help='Default 0: never replay an uncertain rotating refresh. Opt in only to a documented issuer grace policy.')
    signin.add_argument('--label', default='CSL device')
    signin.add_argument('--kind', choices=['local','environment'], default='local')
    signin.add_argument('--environment-name')
    signin.add_argument('--sign-in-timeout', type=int, default=180,
                        help='Seconds to wait for the browser login before giving up (default 180).')
    signin.add_argument('--callback-port', type=port, default=0, help='Loopback port; 0 chooses a free port. Useful with an explicit SSH forward.')
    signin.add_argument('--manual-browser', action='store_true', help='Write the authorization URL to a private state file, never logs; open it in your own system browser.')
    actions.add_parser('status', help='Local cached IDs, byte counts, pending states and last error; never tokens or transcript text.')
    actions.add_parser('register-device', help='Register this signed-in installation without uploading sessions.')
    pull_parser = actions.add_parser('pull', help='Download one of this signed-in account\'s sessions as a package ZIP for csl import. Uses this installation\'s own sign-in; no token in arguments or output.')
    pull_parser.add_argument('session_id')
    pull_parser.add_argument('--out-dir', type=Path, help='Private output directory (default: <sync home>/pulled); created with mode 0700.')
    actions.add_parser('sign-out', help='Disable this local installation and erase stored tokens; stop watch first. Does not revoke remotely.')
    for action in ('include','exclude'):
        selector = actions.add_parser(action, help='Local selection only; never deletes remote content.')
        selector.add_argument('provider', choices=PROVIDERS); selector.add_argument('native_id')
    for action in ('run','watch'):
        runner = actions.add_parser(action, help='One pass.' if action == 'run' else 'Continuous foreground sync; no daemon or timer installed.')
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

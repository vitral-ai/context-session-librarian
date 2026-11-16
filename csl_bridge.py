"""Optional experimental CLI for automatic coordination of existing Codex agents."""
import argparse
import fcntl
import os
from pathlib import Path
import signal
import sqlite3
import sys
import uuid

from csl_bridge_store import BridgeError, MAX_WIRE, Store, decode, encode, identifier, name
from csl_bridge_transport import local_call, ssh_argv


def wait_seconds(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError('wait --timeout must be an integer from 0 to 20 seconds') from None
    if not 0 <= number <= 20:
        raise argparse.ArgumentTypeError('wait --timeout must be 0..20 seconds; repeat wait with the returned cursor for longer waits')
    return number


def reviewed_text(path, scrub=False):
    from csl_secrets import SecretReview
    try:
        if str(path) == '-':
            raw = sys.stdin.buffer.read(MAX_WIRE + 1)
        else:
            with Path(path).open('rb') as stream:
                raw = stream.read(MAX_WIRE + 1)
        if len(raw) > 48000:
            raise BridgeError('Bridge text exceeds size limit')
        text = raw.decode('utf-8')
    except (OSError, UnicodeError):
        raise BridgeError('Cannot read selected UTF-8 text') from None
    review = SecretReview(argparse.Namespace(allow_secrets=False, scrub_secrets=scrub))
    review.scan({'text': text}, 'bridge-selected-text')
    if review.findings:
        if not scrub:
            raise BridgeError('Possible secrets detected; redact selected text or use --scrub-secrets for this message')
        review.decisions = {secret: 'scrub' for secret in review.findings}
        text = review.cleaned({'text': text})['text']
    return text


def configure(args):
    if args.action == 'init':
        identity = name(args.identity)
        path = args.runtime_socket.expanduser().absolute() if args.runtime_socket else None
        store = Store(args.home, create=True)
    else:
        store = Store(args.home)
    try:
        if args.action in ('grant', 'grant-revoke', 'grant-list', 'grant-show'):
            from csl_bridge_mailbox import grant, inspect_grants, MailRejected
            try:
                if args.action in ('grant-list', 'grant-show'):
                    return inspect_grants(store, getattr(args, 'reference', None), getattr(args, 'agent', None),
                                          getattr(args, 'after', ''), getattr(args, 'limit', 20))
                return grant(store, args.reference, getattr(args, 'peer', None), getattr(args, 'source', None),
                             getattr(args, 'target', None), getattr(args, 'permission', None), getattr(args, 'scope', None),
                             getattr(args, 'expires_at', None), revoke=args.action == 'grant-revoke', replace=getattr(args, 'replace', False))
            except MailRejected as exc:
                return {'state': 'rejected', 'reason': exc.reason}
        # Configuration is offline only. Running connector owns its immutable snapshot.
        lock = os.fdopen(os.open(store.home / 'connector.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600), 'a')
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.close()
            raise BridgeError('Stop connector before changing explicit configuration') from None
        with lock, store.transaction() as db:
            if args.action == 'init':
                old = db.execute("SELECT value FROM config WHERE key='identity'").fetchone()
                if old and decode(old['value']) != identity:
                    raise BridgeError('Bridge identity already initialized')
                if old:
                    raise BridgeError('Bridge already initialized; preserve its configuration')
                db.executemany('INSERT INTO config VALUES(?,?)',
                               [('identity', encode(identity)), ('runtime_socket', encode(str(path) if path else None))])
            elif args.action == 'agent':
                agent, thread = name(args.agent), identifier(args.thread)
                db.execute('INSERT INTO agents VALUES(?,?)', (agent, thread))
            elif args.action == 'peer':
                peer = name(args.peer)
                if peer == store.setting('identity'):
                    raise BridgeError('Peer must have a distinct bridge identity')
                agents = [name(a) for a in args.allow_agent]
                if not set(agents) <= set(store.agents()):
                    raise BridgeError('Allow only explicitly registered local agents')
                if args.ssh_config and not args.ssh_alias:
                    raise BridgeError('Explicit SSH config requires SSH transport')
                if args.receive_only:
                    config = {'transport': 'receive-only'}
                elif args.operator_helper:
                    helper = args.operator_helper.expanduser().absolute()
                    if not helper.is_file() or not os.access(helper, os.X_OK):
                        raise BridgeError('Select an existing executable local operator helper')
                    config = {'transport': 'operator', 'helper': str(helper)}
                elif args.lab_home:
                    config = {'transport': 'lab', 'home': str(args.lab_home.expanduser().absolute())}
                else:
                    ssh_argv(args.ssh_alias, args.ssh_config)
                    config = {'transport': 'ssh', 'alias': args.ssh_alias}
                    if args.ssh_config:
                        config['ssh_config'] = str(args.ssh_config.expanduser().absolute())
                config.update(allow_agents=agents, allow_ops=sorted(set(args.allow_op)))
                config['allow_sources'] = sorted(set(name(value) for value in args.allow_source))
                if args.send_session:
                    config['send_session'] = True
                db.execute('INSERT INTO peers VALUES(?,?)', (peer, encode(config)))
            elif args.action == 'peer-session':
                peer = name(args.peer)
                row = db.execute('SELECT config FROM peers WHERE name=?', (peer,)).fetchone()
                if row is None:
                    raise BridgeError('Configure this peer before selecting session headers')
                config = decode(row['config'])
                if args.enable:
                    config['send_session'] = True
                else:
                    config.pop('send_session', None)
                db.execute('UPDATE peers SET config=? WHERE name=?', (encode(config), peer))
            elif args.action == 'peer-sources':
                peer = name(args.peer)
                row = db.execute('SELECT config FROM peers WHERE name=?', (peer,)).fetchone()
                if row is None:
                    raise BridgeError('Configure this peer before admitting its source agents')
                config = decode(row['config'])
                config['allow_sources'] = sorted(set(name(value) for value in args.allow_source))
                db.execute('UPDATE peers SET config=? WHERE name=?', (encode(config), peer))
        return {'state': 'configured', 'action': args.action, 'inference': False}
    finally:
        store.close()


def uncertain_diagnostic(request, peer):
    print('Outcome uncertain; not cancelled. request_id=' + request['id'] +
          '. Query receipt --peer ' + peer + ' --operation-id ' + request['id'] +
          ' before retrying; keep this ID. Do not create a replacement task.', file=sys.stderr)


def run(args):
    request = None
    try:
        if args.action == 'notify':
            from csl_bridge_notify import main as notify_main
            values = ['--home', str(args.home), '--agent', args.agent, '--codex', str(args.codex),
                      '--runtime-socket', str(args.runtime_socket)]
            if args.wake_codex:
                values.append('--wake-codex')
            raise SystemExit(notify_main(values))
        if args.action == 'hook':
            from csl_bridge_hooks import main as hook_main
            values = ['--home', str(args.home), '--agent', args.agent, '--event', args.event, '--limit', str(args.limit)]
            if args.include_body:
                values.append('--include-body')
            if args.session_binding:
                values += ['--session-binding', args.session_binding]
            raise SystemExit(hook_main(values))
        if args.action in ('init', 'agent', 'peer', 'peer-session', 'peer-sources', 'grant', 'grant-revoke', 'grant-list', 'grant-show'):
            result = configure(args)
        elif args.action in ('send', 'receive', 'ack', 'reply', 'inbox-status'):
            request = {'local_op': 'mail_' + args.action.replace('-', '_'), 'agent': args.agent}
            if args.action in ('send', 'reply'):
                request.update(id=args.message_id or str(uuid.uuid4()), body=reviewed_text(args.text_file, args.scrub_secrets),
                               grant_ref=args.grant_ref, expires_at=args.expires_at)
                if args.action == 'send':
                    request.update(peer=args.peer, target=args.target, kind=args.kind)
                else:
                    request['in_reply_to'] = args.in_reply_to
            elif args.action == 'ack':
                request['message_id'] = args.message_id
            elif args.action == 'receive':
                request.update(after=args.after, limit=args.limit, outbox=args.outbox)
            else:
                request.update(after=args.after, limit=args.limit)
            if args.action == 'inbox-status' and (not 0 <= args.after <= 2**63-1 or not 1 <= args.limit <= 100):
                result = {'state': 'rejected', 'reason': 'invalid_mail_cursor_or_limit'}
            else:
                result = local_call(args.home, request)
            if args.action == 'inbox-status' and result.get('state') == 'rejected' and result.get('reason') == 'invalid_mail_fields':
                result = {'state': 'rejected', 'reason': 'connector_does_not_accept_inbox_status',
                          'observed_reason': 'invalid_mail_fields'}
        elif args.action == 'run':
            from csl_bridge_engine import Connector
            connector = Connector(args.home)
            previous = {}
            def stopping(*_):
                connector.stop.set()
            for sig in (signal.SIGINT, signal.SIGTERM):
                previous[sig] = signal.signal(sig, stopping)
            try:
                connector.start()
                print(encode({'state': 'running', 'foreground': True, 'runtime': 'tool-client' if connector.client_only else 'existing-only',
                              'notice': 'Existing compatible runtime only. Previously authorized pending followups may resume.'}), flush=True)
                while not connector.stop.wait(.5):
                    pass
            finally:
                connector.close()
                for sig, old in previous.items():
                    signal.signal(sig, old)
            return
        elif args.action == 'mcp-config':
            from csl_bridge_mcp import configuration
            print(configuration(args.home, args.agent, args.approve_tool, grant_admin=args.grant_admin))
            return
        elif args.action == 'agents' and not args.peer:
            result = local_call(args.home, {'local_op': 'agents'})
        elif args.action == 'wait':
            result = local_call(args.home, {'local_op': 'wait', 'agent': args.agent,
                                          'after': args.after, 'timeout': args.timeout})
        elif args.action == 'progress':
            result = local_call(args.home, {'local_op': 'progress', 'source': args.source,
                'task_id': args.task_id, 'id': args.request_id or str(uuid.uuid4()),
                'text': reviewed_text(args.text_file, args.scrub_secrets)})
        else:
            request = {'version': 1, 'op': args.action, 'id': args.request_id or str(uuid.uuid4())}
            if args.action in ('followup', 'send_message'):
                request.update(source=args.source, target=args.target,
                               text=reviewed_text(args.text_file, args.scrub_secrets))
            elif args.action in ('status', 'interrupt'):
                request['target'] = args.target
                if args.action == 'interrupt':
                    request['turn_id'] = args.turn_id
            elif args.action == 'receipt':
                request['request_id'] = args.operation_id
            result = local_call(args.home, {'local_op': 'call', 'peer': args.peer, 'request': request})
        print(encode(result))
        if request and args.action in ('followup', 'send_message', 'interrupt') and result.get('state') == 'uncertain':
            uncertain_diagnostic(request, args.peer)
        if args.action in ('send', 'receive', 'ack', 'reply', 'inbox-status', 'grant', 'grant-revoke', 'grant-list', 'grant-show'):
            if result.get('state') in ('rejected', 'undeliverable', 'expired'):
                raise SystemExit(1)
            if args.action in ('send', 'ack', 'reply') and result.get('delivery', {}).get('state') in ('rejected', 'undeliverable', 'expired'):
                raise SystemExit(1)
    except BridgeError as exc:
        print('Bridge: ' + str(exc), file=sys.stderr)
        if request and args.action in ('followup', 'send_message', 'interrupt') and 'uncertain' in str(exc).lower():
            uncertain_diagnostic(request, args.peer)
        raise SystemExit(1)
    except (OSError, ValueError, sqlite3.Error):
        # Do not expose DB/OS/remote errors that may contain text, account or paths.
        print('Bridge operation failed. Check explicit configuration and receipt/state; do not blindly resend uncertain work.', file=sys.stderr)
        raise SystemExit(1)


def add_commands(subparsers, csl):
    parser = subparsers.add_parser('bridge', help='Experimental automatic agent coordination (optional, existing runtime only).',
        description='Coordinate existing Codex agents through a foreground session connector. '
                    'Followup activates idle; messages/events enter active work automatically. '
                    'No native attach to standalone TUI, no cloning, no automatic approvals. '
                    'Requires endpoint/agent/peer setup. Experimental csl-pair adds LAN/private-relay pairing. See macos/README.md for validation limits.')
    parser.add_argument('--home', type=Path, required=True, help='Explicit private bridge home, separate from Codex histories/auth.')
    sub = parser.add_subparsers(dest='action', required=True)
    init = sub.add_parser('init', help='Initialize explicit private state; does not start anything.')
    init.add_argument('--id', dest='identity', required=True)
    hosting = init.add_mutually_exclusive_group(required=True)
    hosting.add_argument('--runtime-socket', type=Path, help='Existing app-server Unix WebSocket socket; never a new runtime.')
    hosting.add_argument('--client-only', action='store_true', help='Existing caller receives events through wait tool results; no runtime attach, activation or async injection into the caller.')
    agent = sub.add_parser('agent', help='Register an existing conversation as an agent, offline.')
    agent.add_argument('agent')
    agent.add_argument('--thread', required=True)
    peer = sub.add_parser('peer', help='Configure a peer and its explicit incoming permissions, offline.')
    peer.add_argument('peer')
    transport = peer.add_mutually_exclusive_group(required=True)
    transport.add_argument('--ssh-alias', help='Trusted SSH config alias; fixed receiver, no arbitrary SSH options.')
    transport.add_argument('--lab-home', type=Path, help='Synthetic same-UID local transport; NOT SSH authentication.')
    transport.add_argument('--receive-only', action='store_true', help='Peer initiates requests/watch; no reverse transport or network connection is attempted.')
    transport.add_argument('--operator-helper', type=Path, help='Trusted local executable with fixed operator SSH endpoint; JSON stdin only, no credentials in bridge.')
    peer.add_argument('--ssh-config', type=Path, help='Explicit private trusted SSH config file, separate from personal SSH setup.')
    peer.add_argument('--allow-agent', action='append', required=True)
    peer.add_argument('--allow-source', action='append', default=[],
                      help='Admit this remote registered agent name as a sender from this peer (followup, send_message, mail); '
                           'no implicit sender admission.')
    peer.add_argument('--allow-op', action='append', required=True,
                      choices=['followup', 'send_message', 'status', 'interrupt', 'receipt', 'watch', 'agents', 'mail'])
    peer.add_argument('--send-session', action='store_true',
                      help='Include the sender\'s registered conversation UUID in followups, messages and events to this peer. '
                           'Enable only for peers that accept the optional session field.')
    peer_session = sub.add_parser('peer-session', help='Enable or disable the optional sender session field for an existing peer, offline.')
    peer_session.add_argument('peer')
    toggle = peer_session.add_mutually_exclusive_group(required=True)
    toggle.add_argument('--enable', action='store_true', help='Send the registered sender conversation UUID to this peer.')
    toggle.add_argument('--disable', action='store_true', help='Stop sending it (default for every peer).')
    peer_sources = sub.add_parser('peer-sources', help='Replace the admitted remote source agents of an existing peer, offline.')
    peer_sources.add_argument('peer')
    peer_sources.add_argument('--allow-source', action='append', default=[],
                              help='Admit this remote registered agent name; repeat for several, omit all to admit none.')
    grant = sub.add_parser('grant', help='Owner-only local mailbox authority registry; never activates or executes work.')
    grant.add_argument('reference')
    grant.add_argument('--peer', required=True)
    grant.add_argument('--from', dest='source', required=True)
    grant.add_argument('--to', dest='target', required=True)
    grant.add_argument('--permission', action='append', choices=['communicate', 'activate', 'work'], required=True)
    grant.add_argument('--scope', required=True)
    grant.add_argument('--expires-at', help='Optional owner-chosen timezone-aware ISO8601 grant expiry.')
    grant.add_argument('--replace', action='store_true', help='Explicitly replace an existing reference, including revoked grants; changes authority of earlier messages that reference it.')
    revoke = sub.add_parser('grant-revoke', help='Owner-only local revocation; stored messages remain intact.')
    revoke.add_argument('reference')
    grant_list = sub.add_parser('grant-list', help='Inspect local owner grants, including expiry, revocation and descriptive scope.')
    grant_list.add_argument('--agent', help='Optional recipient endpoint filter.')
    grant_list.add_argument('--after', default='')
    grant_list.add_argument('--limit', type=int, default=20)
    grant_show = sub.add_parser('grant-show', help='Inspect one locally configured owner grant.')
    grant_show.add_argument('reference')
    grant_show.add_argument('--agent')
    status = sub.add_parser('inbox-status', help='Read counts and bounded pending headers only; no bodies, scopes, acknowledgement or wake.')
    status.add_argument('--agent', required=True)
    status.add_argument('--after', type=int, default=0)
    status.add_argument('--limit', type=int, default=20)
    hook = sub.add_parser('hook', help='Claude turn-boundary hook; metadata by default, never acknowledgement, model wake or work authority.')
    hook.add_argument('--agent', required=True)
    hook.add_argument('--event', required=True, choices=['SessionStart', 'UserPromptSubmit', 'PostToolUse', 'Stop'])
    hook.add_argument('--limit', type=int, default=10)
    hook.add_argument('--session-binding', help='Explicit owner-supplied UUID binding override for isolated tests; never inferred from message contents.')
    hook.add_argument('--include-body', action='store_true', help='Explicit local owner opt-in to surface bounded untrusted message bodies at this turn boundary.')
    notify = sub.add_parser('notify', help='Owner-run Codex native queue notification; opt-in and local activate grant required, no work execution.')
    notify.add_argument('--agent', required=True)
    notify.add_argument('--codex', type=Path, required=True)
    notify.add_argument('--runtime-socket', type=Path, required=True)
    notify.add_argument('--wake-codex', action='store_true')
    for action in ('send', 'receive', 'ack', 'reply'):
        mail = sub.add_parser(action, help='Task-independent mailbox v1 '+action+'; never starts or steers a model.')
        mail.add_argument('--agent' if action in ('receive', 'ack') else '--from', dest='agent', required=True)
        if action in ('send', 'reply'):
            mail.add_argument('--message-id', help='Stable UUID; retry with identical endpoints/content.')
            mail.add_argument('--text-file', type=Path, required=True)
            mail.add_argument('--scrub-secrets', action='store_true')
            mail.add_argument('--grant-ref')
            mail.add_argument('--expires-at', help='Optional message expiry; distinct from grant expiry; never erases stored mail.')
        if action == 'send':
            mail.add_argument('--peer', required=True)
            mail.add_argument('--to', dest='target', required=True)
            mail.add_argument('--kind', choices=['message', 'request'], default='message')
        elif action == 'reply':
            mail.add_argument('--in-reply-to', required=True)
        elif action == 'ack':
            mail.add_argument('--message-id', required=True)
        else:
            mail.add_argument('--after', type=int, default=0)
            mail.add_argument('--limit', type=int, default=20)
            mail.add_argument('--outbox', action='store_true')
    sub.add_parser('run', help='Run connector in foreground for this session; Ctrl-C stops it, not Codex work.')
    mcp_config = sub.add_parser('mcp-config', help='Print a reviewable local MCP profile; never applies consent/configuration.')
    mcp_config.add_argument('--agent', required=True, help='Explicit registered local identity bound to the tools.')
    mcp_config.add_argument('--grant-admin', action='store_true', help='Explicit local owner capability for grant mutation scoped to this bound recipient; does not apply configuration or approve execution.')
    mcp_config.add_argument('--approve-tool', action='append', default=[], choices=['send_progress', 'send_message', 'followup_task', 'send', 'ack', 'reply'],
        help='Explicit initial local consent for this message tool only; other side-effecting tools remain unavailable.')
    agents = sub.add_parser('agents', help='List registered local agents, or the agents a peer permits you to inspect.')
    agents.add_argument('--peer')
    agents.add_argument('--request-id')
    for action in ('followup', 'send_message', 'status', 'interrupt', 'receipt'):
        command = sub.add_parser(action, help={'followup': 'Delegate/continue in the SAME agent; starts idle, steers active.',
            'send_message': 'Deliver while running; hold idle without generation.', 'status': 'Observe remote agent state.',
            'interrupt': 'Request interruption of an exact turn; await confirmation event.',
            'receipt': 'Reconcile an uncertain operation; never blindly resend.'}[action])
        command.add_argument('--peer', required=True)
        command.add_argument('--request-id', help='Stable UUID for this operation; same ID/content returns its receipt.')
        if action == 'receipt':
            command.add_argument('--operation-id', required=True)
        else:
            command.add_argument('--to', dest='target', required=True)
        if action in ('followup', 'send_message'):
            command.add_argument('--from', dest='source', required=True)
            command.add_argument('--text-file', type=Path, required=True, help='Selected UTF-8 text, or - for stdin. No secrets in argv.')
            command.add_argument('--scrub-secrets', action='store_true', help='Partial scan/scrub for this text only; no secret-free guarantee.')
        if action == 'interrupt':
            command.add_argument('--turn-id', required=True)
    progress = sub.add_parser('progress', help='Publish progress for an active delegated task; delivered automatically to its coordinator.')
    progress.add_argument('--from', dest='source', required=True)
    progress.add_argument('--task-id', required=True)
    progress.add_argument('--request-id')
    progress.add_argument('--text-file', type=Path, required=True)
    progress.add_argument('--scrub-secrets', action='store_true')
    wait = sub.add_parser('wait', help='Wait for correlated advances/final without inference or aggressive polling.')
    wait.add_argument('--agent', required=True)
    wait.add_argument('--after', type=int, default=0)
    wait.add_argument('--timeout', type=wait_seconds, default=20, help='Seconds, 0..20; timeout is not cancellation. Repeat with returned cursor for longer waits.')
    parser.set_defaults(func=run)

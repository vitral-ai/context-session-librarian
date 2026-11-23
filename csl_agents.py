"""Registry CLI and hub delivery inside the existing bridge connector process."""
import argparse
import json
import logging
import os
from pathlib import Path
import ssl
import threading
import time
import urllib.error
import urllib.request
import uuid

from csl_bridge_store import BridgeError, decode, encode, identifier, message_header, name


def frame_message(source, task, session, text):
    # Keep the same envelope on older installed bridge stores too.
    body = ''.join('> ' + line if line.lstrip().startswith('[Message ') else line
                   for line in text.splitlines(keepends=True))
    return message_header(source, task, session) + body


DEFAULT_CONFIG = Path.home() / '.config/csl/agents.json'


def config(path=DEFAULT_CONFIG):
    from csl_bridge_claude_notify import owned_json
    value = owned_json(Path(path).expanduser())
    if not value['url'].startswith('https://'):
        raise BridgeError('Hub requires HTTPS')
    return value


def call(settings, request):
    token_path = Path(settings['token_file']).expanduser()
    descriptor = os.open(token_path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor) as stream:
        info = os.fstat(stream.fileno())
        import stat
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise BridgeError('Machine token must be an owned private regular file')
        token = stream.read(257).strip()
    if len(token) != 64 or any(c not in '0123456789abcdef' for c in token):
        raise BridgeError('Invalid machine token file')
    context = ssl.create_default_context(cafile=str(Path(settings['cafile']).expanduser()))
    request = urllib.request.Request(settings['url'].rstrip('/') + '/v1', encode(request).encode(),
                headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, context=context, timeout=10) as response:
            result = decode(response.read(65537))
    except urllib.error.HTTPError as error:
        with error:
            result = decode(error.read(65537))
    except (OSError, urllib.error.URLError):
        raise BridgeError('Hub unavailable; envelope outcome may be unknown. Retry the SAME message ID or inspect its receipt.') from None
    if not result.get('ok'):
        raise BridgeError(result.get('error', 'Hub rejected request'))
    return result['data']


def bind(connector, record):
    """Owner-configured hub registration joins an existing native session only."""
    target, session = name(record['name']), identifier(record['session'])
    existing = connector.agents.get(target)
    if existing is not None and existing != session:
        raise BridgeError('Local endpoint differs from registered hub session')
    if existing is None:
        with connector.store.transaction() as db:
            db.execute('INSERT INTO agents VALUES(?,?)', (target, session))
        connector.locks[target] = threading.RLock()
        connector.waiters[target] = 0
        connector.agents[target] = session


def deliver(connector, packet):
    """Called only over owned local IPC by this machine's hub connector."""
    settings = config()
    source, target = packet['source'], packet['target']
    if target['machine'] != settings['machine']:
        raise BridgeError('Envelope belongs to another machine')
    identifier(packet['id']); identifier(source['session']); name(source['name'])
    bind(connector, target)
    peer = 'agent-hub'
    # Hub, not a remote agent, supplies both endpoint pins and the pair decision.
    connector.peers[peer] = {'transport': 'receive-only', 'allow_agents': list(connector.agents),
                            'allow_sources': sorted(set(connector.peers.get(peer, {}).get('allow_sources', [])) | {source['name']}), 'allow_ops': ['mail', 'followup'], 'send_session': True}
    if target['cli'] == 'claude':
        from csl_bridge_mailbox import now
        request = {'version': 1, 'op': 'mail', 'message_version': 1, 'action': 'send',
                   'id': packet['id'], 'source': source['name'], 'target': target['name'],
                   'envelope': {'kind': 'message', 'body': frame_message(source['name'], packet['id'], source['session'], packet['text']),
                                'in_reply_to': None, 'grant_ref': None, 'created_at': packet['created_at'], 'expires_at': None}}
        result = connector.mailbox.incoming(peer, request)
        if result.get('state') == 'persisted':
            from csl_bridge_claude_notify import notify
            notify(connector.store.home, target['name'], packet['id'])
            return {'state': 'delivered', 'id': packet['id'], 'session': target['session'],
                    'delivery': 'durable mailbox and native boundary header attempted; not a read receipt'}
        return result
    if target['cli'] != 'codex' or connector.client_only:
        raise BridgeError('No native Codex runtime on this delivery home')
    with connector.locks[target['name']]:
        old = connector.store.rows('SELECT state,result FROM operations WHERE id=?', (packet['id'],))
        if old and old[0]['state'] in ('delivered', 'uncertain', 'invoking'):
            return decode(old[0]['result']) if old[0]['result'] else {'state': old[0]['state']}
        state = connector.current(target['name'])
        if state['status'] != 'idle':
            return {'state': 'queued', 'reason': state['status']}
        request = {'version': 1, 'op': 'followup', 'id': packet['id'], 'source': source['name'],
                   'session': source['session'], 'target': target['name'], 'text': packet['text']}
        if old:
            return connector.invoke(peer, request)
        return connector.incoming(peer, request)


def loop(connector):
    from csl_bridge_transport import local_call
    settings = config()
    failures, last_log = 0, 0
    while not connector.stop.is_set():
        try:
            packets = call(settings, {'action': 'pull'})['envelopes']
            for packet in packets:
                try:
                    target = packet['target']
                    home = Path(settings['delivery_homes'][target['cli']]).expanduser()
                    if home == connector.store.home:
                        receipt = deliver(connector, packet)
                    else:
                        receipt = local_call(home, {'local_op': '_hub_delivery', 'packet': packet})
                    if receipt.get('state') == 'delivered':
                        call(settings, {'action': 'ack', 'id': packet['id'], 'receipt': receipt})
                except (BridgeError, OSError, ValueError, KeyError):
                    # One unavailable recipient never blocks other envelopes.
                    continue
            failures = 0
        except (BridgeError, OSError, ValueError):
            failures += 1
            if time.monotonic() - last_log > 60:
                logging.getLogger(__name__).warning('Agent hub unavailable; durable envelopes remain queued')
                last_log = time.monotonic()
        connector.stop.wait(min(30, 2 ** min(failures, 5)) if failures else .5)


def _execute(args):
    settings = config(args.hub_config)
    request = {'action': args.hub_action}
    if args.hub_action == 'register':
        request.update({key: getattr(args, key) for key in ('name', 'session', 'cli', 'machine')})
    elif args.hub_action in ('allow', 'deny'):
        request.update(source=args.source, target=args.target, both=args.both)
    elif args.hub_action in ('show', 'remove'):
        request['name'] = args.name
    elif args.hub_action in ('send',):
        from csl_bridge import reviewed_text
        request.update(source=args.source, target=args.to, id=args.id or str(uuid.uuid4()),
                       text=reviewed_text(args.text_file, False), in_reply_to=getattr(args, 'in_reply_to', None))
        # Print the ID before networking: unknown outcomes are retried with this ID.
        import sys
        print('message_id=' + request['id'], file=sys.stderr)
    elif args.hub_action == 'receipt':
        request['id'] = args.id
    print(encode(call(settings, request)))


def execute(args):
    try:
        _execute(args)
    except (BridgeError, OSError, KeyError) as error:
        raise SystemExit('CSL agents: ' + str(error)) from None


def add_commands(subparsers, _):
    parser = subparsers.add_parser('agents', help='Owner-approved cross-machine agent registry and directed approvals.')
    parser.add_argument('--hub-config', type=Path, default=DEFAULT_CONFIG)
    sub = parser.add_subparsers(dest='hub_action', required=True)
    register = sub.add_parser('register')
    for key in ('name', 'session', 'machine'):
        register.add_argument('--' + key, required=True)
    register.add_argument('--cli', choices=('codex', 'claude'), required=True)
    for action in ('show', 'remove'):
        sub.add_parser(action).add_argument('name')
    sub.add_parser('list'); sub.add_parser('permissions')
    for action in ('allow', 'deny'):
        pair = sub.add_parser(action)
        pair.add_argument('source'); pair.add_argument('target'); pair.add_argument('--both', action='store_true')
    for action in ('send', 'reply'):
        send = subparsers.add_parser(action, help='Send a durable envelope and wake its registered recipient.')
        send.add_argument('--hub-config', type=Path, default=DEFAULT_CONFIG)
        send.add_argument('--from', dest='source', required=True)
        send.add_argument('--to', required=True)
        send.add_argument('--text-file', type=Path, default=Path('-'))
        send.add_argument('--id')
        if action == 'reply':
            send.add_argument('--in-reply-to', required=True)
        send.set_defaults(func=execute, hub_action='send')
    receipt = sub.add_parser('receipt'); receipt.add_argument('id')
    parser.set_defaults(func=execute)


def serve_main():
    from csl_agent_hub import Hub, server
    parser = argparse.ArgumentParser()
    for key in ('database', 'machines', 'certificate', 'key'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--bind', default='127.0.0.1'); parser.add_argument('--port', type=int, default=443)
    parser.add_argument('--ssh-port', type=int)
    args = parser.parse_args()
    hub = Hub(args.database, json.loads(Path(args.machines).read_text()))
    server(hub, (args.bind, args.port), args.certificate, args.key, args.ssh_port).serve_forever()


if __name__ == '__main__':
    serve_main()

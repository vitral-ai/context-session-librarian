"""Optional operator adapter: pull return mail over outbound-only SSH.

No listener, daemon, new wire operation, or native runtime access. The normal
connector owns its outbox and validates receipts; this adapter only carries them.
All endpoint arguments are fixed by operator-installed helpers, never messages.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

from csl_bridge_store import BridgeError, MAX_WIRE, Store, decode, encode, identifier, private_directory, validate
from csl_bridge_transport import local_call


def fingerprint(request):
    return hashlib.sha256(encode(request).encode()).hexdigest()


def ssh_command(alias, control_path, remote_command):
    return ['ssh', '-T', '-oBatchMode=yes', '-oStrictHostKeyChecking=yes',
            '-oClearAllForwardings=yes', '-oConnectTimeout=8',
            '-oServerAliveInterval=5', '-oServerAliveCountMax=2',
            '-oControlMaster=auto', '-oControlPersist=60',
            '-oControlPath=' + control_path, '--', alias, remote_command]


def read_packet(stream):
    raw = stream.readline(MAX_WIRE * 2 + 1)
    if len(raw) > MAX_WIRE * 2 or not raw.endswith(b'\n'):
        raise BridgeError('Invalid SSH pull packet size')
    return json.loads(raw)


def save_receipts(store, peer, cache, receipts):
    if not isinstance(receipts, list) or len(receipts) > 1:
        raise BridgeError('Invalid pull receipts')
    cache = private_directory(cache, create=True)
    for entry in receipts:
        if not isinstance(entry, dict) or set(entry) != {'request', 'result'}:
            raise BridgeError('Invalid pull receipt')
        request, result = validate(entry['request']), entry['result']
        if request['op'] != 'mail' or request['action'] not in ('send', 'ack'):
            raise BridgeError('Only mailbox results may be cached')
        rows = store.rows('SELECT request FROM operations WHERE id=? AND peer=?',
                          (request['id'], 'mail-out:' + peer))
        if not rows or decode(rows[0]['request']) != request:
            raise BridgeError('Receipt does not match an existing outgoing operation')
        if not isinstance(result, dict) or result.get('state') not in (
                'persisted', 'acknowledged', 'rejected', 'undeliverable', 'expired'):
            raise BridgeError('Uncertain results must remain pending')
        data = encode({'request': request, 'result': result}).encode()
        fd, temporary = tempfile.mkstemp(dir=cache, prefix='.receipt-')
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, cache / (request['id'] + '.json'))
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


def pending_mail(store, peer):
    # One bounded frame alongside the ordinary result. Eligibility precedes LIMIT.
    rows = store.rows("SELECT request FROM operations WHERE peer=? AND state IN ('pending','uncertain') "
                      "ORDER BY rowid LIMIT 1", ('mail-out:' + peer,))
    return [validate(decode(row['request'])) for row in rows]


def cached_result(cache, request):
    validate(request)
    if request['op'] == 'watch':
        # This endpoint is only a return-mail adapter, not a reverse coordinator.
        time.sleep(5)
        return {'events': []}
    if request['op'] != 'mail':
        raise BridgeError('Return adapter supports mailbox operations only')
    key = request['message_id'] if request['action'] == 'receipt' else request['id']
    identifier(key)
    try:
        entry = decode((private_directory(cache) / (key + '.json')).read_bytes())
    except FileNotFoundError:
        return {'state': 'not_found'} if request['action'] == 'receipt' else {'state': 'uncertain'}
    original = entry['request']
    if request['action'] == 'receipt':
        matches = (original['source'], original['target']) == (request['source'], request['target'])
    else:
        matches = fingerprint(original) == fingerprint(request)
    if not matches:
        raise BridgeError('Cached receipt endpoint or content mismatch')
    return entry['result']


def server(home, peer, cache, packet):
    if not isinstance(packet, dict) or set(packet) != {'request', 'receipts'}:
        raise BridgeError('Invalid SSH pull packet')
    store = Store(home)
    try:
        if peer not in store.peers():
            raise BridgeError('Unknown fixed peer')
        save_receipts(store, peer, cache, packet['receipts'])
        request = packet['request']
        result = None if request is None else local_call(home, {
            'local_op': '_incoming', 'trusted_peer': peer, 'request': validate(request)})
        return {'result': result, 'mail': pending_mail(store, peer)}
    finally:
        store.close()


def client(home, peer, request, exchange):
    result = exchange({'request': validate(request), 'receipts': []})
    if not isinstance(result, dict) or set(result) != {'result', 'mail'}:
        raise BridgeError('Invalid SSH pull response')
    if not isinstance(result['mail'], list) or len(result['mail']) > 1:
        raise BridgeError('Invalid pulled mailbox batch')
    receipts = []
    for outgoing in result['mail']:
        validate(outgoing)
        if outgoing['op'] != 'mail' or outgoing['action'] not in ('send', 'ack'):
            raise BridgeError('Invalid pulled mailbox operation')
        try:
            receipt = local_call(home, {'local_op': '_incoming', 'trusted_peer': peer, 'request': outgoing}, timeout=2)
        except (BridgeError, OSError):
            # Return mail remains durably queued remotely for the next pull.
            continue
        if receipt.get('state') != 'uncertain':
            receipts.append({'request': outgoing, 'result': receipt})
    if receipts:
        # If interrupted before this acknowledgement, ordinary mail dedup handles
        # the next pull. Never acknowledge before durable recipient persistence.
        try:
            exchange({'request': None, 'receipts': receipts})
        except (BridgeError, OSError, ValueError, subprocess.TimeoutExpired):
            # Losing the return-mail ACK cannot erase a known forward result.
            # The remote outbox stays pending and pulls the same durable ID.
            pass
    return result['result']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('client', 'server', 'cached'))
    parser.add_argument('--home')
    parser.add_argument('--peer')
    parser.add_argument('--cache')
    parser.add_argument('--ssh-alias')
    parser.add_argument('--control-path')
    parser.add_argument('--remote-command')
    args = parser.parse_args()
    try:
        if args.mode == 'server':
            output = server(args.home, args.peer, args.cache, read_packet(sys.stdin.buffer))
        elif args.mode == 'cached':
            output = {'ok': True, 'data': cached_result(args.cache, validate(decode(sys.stdin.buffer.readline(MAX_WIRE + 1))))}
        else:
            deadline = time.monotonic() + 22
            def exchange(packet):
                command = ssh_command(args.ssh_alias, args.control_path, args.remote_command)
                process = subprocess.run(command, input=(json.dumps(packet) + '\n').encode(),
                                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                         timeout=max(.01, deadline - time.monotonic()))
                if process.returncode or len(process.stdout) > MAX_WIRE * 2:
                    raise BridgeError('SSH pull unavailable; preserve IDs and reconcile')
                return json.loads(process.stdout)
            request = validate(decode(sys.stdin.buffer.readline(MAX_WIRE + 1)))
            output = {'ok': True, 'data': client(args.home, args.peer, request, exchange)}
        print(json.dumps(output, separators=(',', ':')))
    except (BridgeError, OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired):
        if args.mode == 'client':
            print(encode({'ok': True, 'data': {'state': 'uncertain',
                'reason': 'ssh_pull_unavailable',
                'recovery': 'Keep the operation ID; check its receipt after SSH reconnects'}}))
        else:
            print(encode({'ok': False, 'data': {'error': 'SSH pull failed; reconcile the same operation ID'}}))
        return 0 if args.mode == 'client' else 1
    return 0


if __name__ == '__main__':
    sys.exit(main())

"""Print bounded new bridge events/mail and retain a private per-recipient cursor.

One invocation exits after at most --limit items from each channel. Output is
flushed before advancing cursors: interrupted output may repeat, never disappear.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile

from csl_bridge_store import BridgeError, private_directory
from csl_bridge_transport import local_call


def updates(home, agent, cursor_file, output=sys.stdout, limit=100):
    import fcntl
    if not 1 <= limit <= 1000:
        raise BridgeError('Limit must be between 1 and 1000')
    path = Path(cursor_file).absolute()
    private_directory(path.parent, create=True)
    flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW
    fd = os.open(str(path) + '.lock', flags, 0o600)
    with os.fdopen(fd, 'r+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        identity = {'home': str(Path(home).resolve()), 'agent': agent}
        try:
            with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW)) as stream:
                state = json.load(stream)
        except FileNotFoundError:
            state = {**identity, 'events': 0, 'mail': 0}
        if any(state.get(k) != v for k, v in identity.items()):
            raise BridgeError('Cursor belongs to another home or agent')
        for key in ('events', 'mail'):
            if type(state.get(key)) is not int or state[key] < 0:
                raise BridgeError('Invalid cursor')
        for _ in range(limit):
            result = local_call(home, {'local_op': 'wait', 'agent': agent,
                                      'after': state['events'], 'timeout': 0})
            events = result.get('events', [])
            if not events:
                break
            for event in events:
                print(json.dumps({'channel': 'event', **event}), file=output)
                state['events'] = event['cursor']
        result = local_call(home, {'local_op': 'mail_receive', 'agent': agent,
                                  'after': state['mail'], 'limit': min(limit, 100), 'outbox': False})
        for message in result.get('messages', []):
            print(json.dumps({'channel': 'mail', **message}), file=output)
            state['mail'] = message['cursor']
        output.flush()
        temporary = None
        try:
            fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.cursor-')
            with os.fdopen(fd, 'w') as stream:
                json.dump(state, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)
        return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--home', required=True)
    parser.add_argument('--agent', required=True)
    parser.add_argument('--cursor-file', required=True)
    parser.add_argument('--limit', type=int, default=100)
    args = parser.parse_args()
    try:
        updates(args.home, args.agent, args.cursor_file, limit=args.limit)
    except (BridgeError, OSError, ValueError) as error:
        parser.exit(1, str(error) + '\n')


if __name__ == '__main__':
    main()

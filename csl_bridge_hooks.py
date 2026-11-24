"""Owner-installed boundary notifications. Never ACK, activate or execute mail."""
import argparse
import json
import sqlite3
import sys

from csl_bridge_mailbox import now, timestamp
from csl_bridge_store import BridgeError, Store, compact_message, decode, encode, identifier, name

EVENTS = ('SessionStart', 'UserPromptSubmit', 'PostToolUse', 'Stop')


def surface(home, agent, event, payload, include_body=False, limit=10, session_binding=None):
    """Return hook output; surfaced is an emission attempt, never proof of reading."""
    if event not in EVENTS or not isinstance(payload, dict):
        raise BridgeError('Unsupported hook event or input')
    session = payload.get('session_id')
    if not isinstance(session, str) or not 1 <= len(session) <= 200:
        raise BridgeError('Hook requires a bounded session_id')
    if payload.get('hook_event_name', event) != event:
        raise BridgeError('Hook event mismatch')
    if type(limit) is not int or not 1 <= limit <= 20:
        raise BridgeError('Hook limit must be 1..20')
    if event == 'Stop' and payload.get('stop_hook_active') is True:
        return ''
    store = Store(home)
    try:
        name(agent)
        if agent not in store.agents():
            raise BridgeError('Unknown hook endpoint')
        expected = session_binding if session_binding is not None else store.agents()[agent]
        identifier(expected)
        if session != expected:
            raise BridgeError('Hook session does not match the owner-configured endpoint binding')
        with store.transaction() as db:
            db.execute('CREATE TABLE IF NOT EXISTS mail_surfaced '
                       '(agent TEXT, session TEXT, id TEXT, surfaced_at TEXT, '
                       'PRIMARY KEY(agent,session,id))')
            rows = db.execute('SELECT m.* FROM mail_messages m WHERE direction=\'in\' '
                              'AND target=? AND acknowledged_at IS NULL AND NOT EXISTS '
                              '(SELECT 1 FROM mail_surfaced s WHERE s.agent=? AND s.session=? AND s.id=m.id) '
                              'ORDER BY seq', (agent, agent, session))
            headers = []
            for row in rows:
                envelope = decode(row['envelope'])
                if envelope['expires_at'] and timestamp(envelope['expires_at']) <= timestamp(now()):
                    continue
                item = {'id': row['id'], 'from': {'peer': row['peer'], 'agent': row['source']},
                        'kind': envelope['kind']}
                item['preview'] = compact_message(row['source'], row['id'], envelope['body'])
                headers.append(item)
                if len(headers) > limit:
                    break
            if not headers:
                return ''
            more_available = len(headers) > limit
            if more_available:
                headers.pop()
            def render():
                text = '\n\n'.join(item['preview'] for item in headers)
                if more_available:
                    text += '\nMore CSL messages waiting: csl receive.'
                if event == 'PostToolUse':
                    return encode({'hookSpecificOutput': {'hookEventName': event, 'additionalContext': text}})
                if event == 'Stop':
                    return encode({'decision': 'block', 'reason': text})
                return text
            output = render()
            while len(output.encode('utf-8')) > 48000:
                if len(headers) > 1:
                    headers.pop()
                    more_available = True
                else:
                    raise BridgeError('Notification exceeds byte limit')
                output = render()
            db.executemany('INSERT INTO mail_surfaced VALUES(?,?,?,?)',
                           [(agent, session, item['id'], now()) for item in headers])
            return output
    finally:
        store.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--home', required=True)
    parser.add_argument('--agent', required=True)
    parser.add_argument('--event', choices=EVENTS, required=True)
    parser.add_argument('--include-body', action='store_true',
                        help='Compatibility option; compact body previews are now always included')
    parser.add_argument('--limit', type=int, default=10)
    parser.add_argument('--session-binding', help='Explicit owner-only harness-session binding override for this profile')
    args = parser.parse_args(argv)
    try:
        raw = sys.stdin.buffer.read(65537)
        if len(raw) > 65536:
            raise BridgeError('Hook input exceeds limit')
        payload = json.loads(raw)
        output = surface(args.home, args.agent, args.event, payload, args.include_body, args.limit, args.session_binding)
        if output:
            print(output)
        return 0
    except (BridgeError, ValueError, UnicodeError, OSError, sqlite3.Error, RecursionError):
        # Do not leak stdin, transcript paths, bodies or authentication into logs.
        print('CSL boundary notification unavailable; inspect inbox explicitly.', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())

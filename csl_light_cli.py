"""Small user CLI; inbox lists messages, never background watch/receipt traffic."""
import argparse, json, os, sqlite3, sys
from pathlib import Path

def inbox(home, agent=None, after=None):
    home = Path(home)
    config = json.loads((home / 'config.json').read_text())
    agent = agent or config['default_agent']
    sql = "SELECT rowid,id,payload,state,error,created FROM rpc WHERE direction='in'\n AND coalesce(json_extract(payload,'$.request.target'),json_extract(payload,'$.event.target'))=?\n AND (json_extract(payload,'$.type')='event' OR json_extract(payload,'$.request.op') IN ('followup','send_message')\n OR (json_extract(payload,'$.request.op')='mail' AND json_extract(payload,'$.request.action')='send'))"
    args = [agent]
    if after is not None:
        sql += ' AND rowid>?'
        args.append(after)
    sql += ' ORDER BY rowid DESC LIMIT 100'
    db = sqlite3.connect('file:' + str(home / 'light.sqlite') + '?mode=ro', uri=True)
    try:
        rows = db.execute(sql, args).fetchall()
    finally:
        db.close()
    messages = []
    for cursor, i, payload, state, error, created in reversed(rows):
        frame = json.loads(payload)
        r = frame['request'] if 'request' in frame else {**frame['event'],'op':frame['event']['kind'],'text':frame['event']['text']}
        messages.append({'cursor': cursor, 'id': i, 'from': r['source'], 'session': frame.get('source_session'), 'to': r['target'], 'op': r['op'], 'body': r.get('text') or r.get('envelope', {}).get('body'), 'state': state, 'error': error, 'received_at': created})
    return {'agent': agent, 'cursor': max([m['cursor'] for m in messages], default=after or 0), 'messages': messages}

def cached_request(home, target, source, identifier, text, operation):
    home = Path(home)
    config = json.loads((home / 'config.json').read_text())
    source = source or config['default_agent']
    db = sqlite3.connect('file:' + str(home / 'light.sqlite') + '?mode=ro', uri=True)
    try:
        route = db.execute('SELECT agent FROM routes WHERE agent=? OR thread=?', (target, target)).fetchall()
        if len(route) != 1:
            raise ValueError('Unknown or ambiguous target')
        rows = db.execute("SELECT peer,payload,state,result FROM rpc WHERE direction='out' AND id=?", (identifier,)).fetchall()
    finally:
        db.close()
    if not rows:
        return None
    if len(rows) != 1:
        raise ValueError('Message id is not unique')
    peer, payload, state, result = rows[0]
    frame = json.loads(payload)
    r = frame['request']
    body = r.get('text') if operation == 'followup' else r.get('envelope', {}).get('body')
    if r.get('op') != ('mail' if operation == 'send' else operation) or r.get('source') != source or r.get('target') != route[0][0] or (body != text):
        raise ValueError('Message id collision: sender, target or content differs')
    return {'peer': peer, 'frame': frame, 'state': state, 'result': json.loads(result) if result else None}

def main(argv=None):
    values = sys.argv[1:] if argv is None else list(argv)
    head = argparse.ArgumentParser(add_help=False)
    head.add_argument('--home', type=Path, default=Path.home() / '.local/share/csl-light')
    head.add_argument('command', nargs='?')
    known, _ = head.parse_known_args(values)
    if known.command in ('send', 'followup') and any((x == '--id' or x.startswith('--id=') for x in values)):
        replay = argparse.ArgumentParser()
        replay.add_argument('--home', type=Path, default=known.home)
        replay.add_argument('command')
        replay.add_argument('target')
        replay.add_argument('file', type=Path)
        replay.add_argument('--from', dest='source')
        replay.add_argument('--id', required=True)
        args = replay.parse_args(values)
        cached = cached_request(args.home, args.target, args.source, args.id, args.file.read_text(), args.command)
        if cached:
            if cached['state'] == 'done':
                result = cached['result']
            else:
                sys.path.insert(0, str(Path(__file__).resolve().parent))
                from csl_light import local
                result = local(args.home, {'action': 'exchange', 'peer': cached['peer'], 'origin': cached['frame']['origin'], 'request': cached['frame']['request']})
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
            return
    if known.command != 'inbox':
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from csl_light import main as light_main
        return light_main(values)
    parser = argparse.ArgumentParser()
    parser.add_argument('--home', type=Path, default=known.home)
    parser.add_argument('command', choices=['inbox'])
    parser.add_argument('--agent')
    parser.add_argument('--after', type=int)
    args = parser.parse_args(values)
    print(json.dumps(inbox(args.home, args.agent, args.after), ensure_ascii=False, sort_keys=True))
if __name__ == '__main__':
    main()


def add_commands(subparsers, _context=None):
    parser = subparsers.add_parser('light', help='Persistent pool messaging over one reconnecting SSH stream.')
    parser.add_argument('--home', type=Path, default=Path.home() / '.local/share/csl-light')
    parser.add_argument('action', choices=['init', 'enroll', 'run', 'attach', 'exchange', 'send', 'followup', 'inbox'])
    parser.add_argument('light_args', nargs=argparse.REMAINDER)
    parser.set_defaults(func=lambda args: main(['--home', str(args.home), args.action, *args.light_args]))

"""Import explicitly selected, settled Hub history using the NATS owner API."""

import argparse
import asyncio
from contextlib import closing
from datetime import datetime
import json
from pathlib import Path
import os
import re
import sqlite3
import ssl
import stat
import sys
from urllib.parse import urlsplit
from uuid import UUID


AGENT = re.compile(r'[a-z0-9][a-z0-9-]{1,62}')
SETTLED = {'delivered', 'refused', 'dropped'}


def pair(value):
    names = value.split(',')
    if len(names) != 2 or names[0] == names[1] or not all(AGENT.fullmatch(name) for name in names):
        raise argparse.ArgumentTypeError('Use two distinct agent names separated by a comma')
    return tuple(sorted(names))


def load_history(database, pairs):
    rows = []
    with closing(sqlite3.connect(database.resolve(strict=True).as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        for a, b in dict.fromkeys(pairs):
            machines = {}
            for name in (a, b):
                agent = db.execute('SELECT record FROM agents WHERE name=?', (name,)).fetchone()
                if agent is None:
                    raise ValueError(f'Unregistered Hub agent: {name}')
                machines[name] = json.loads(agent['record'])['machine']
            for row in reversed(db.execute(
                'SELECT h.*,e.packet FROM history h LEFT JOIN envelopes e ON e.id=h.id '
                'WHERE pair_a=? AND pair_b=? ORDER BY sequence DESC LIMIT 200', (a, b),
            ).fetchall()):
                mid, sender, target = row['id'], row['source'], row['target']
                if row['state'] not in SETTLED:
                    raise ValueError(f'Hub envelope {mid} is {row["state"]}; settle it before import')
                if (set((sender, target)) != set((a, b)) or
                        (row['source_machine'], row['target_machine']) != (machines[sender], machines[target])):
                    raise ValueError(f'Hub envelope {mid} does not match the current machines')
                reply_to = json.loads(row['packet']).get('in_reply_to') if row['packet'] else None
                created = datetime.fromisoformat(row['time'])
                if (str(UUID(mid)) != mid or (reply_to is not None and str(UUID(reply_to)) != reply_to) or
                        created.utcoffset() is None or len(row['body'].encode('utf-8')) > 48 * 1024):
                    raise ValueError(f'Hub envelope {mid} is outside NATS history limits')
                rows.append({
                    'id': mid, 'from': sender, 'to': target, 'from_machine': machines[sender],
                    'to_machine': machines[target], 'created_at': created.isoformat(), 'state': row['state'],
                    'body': row['body'], 'reply_to': reply_to,
                })
    return rows


async def apply_history(args, rows):
    import nats

    root = f'csl.{args.tenant}'
    connection = await nats.connect(
        servers=[args.server], user_credentials=str(args.owner_creds),
        inbox_prefix=f'_INBOX.{root}.owner',
        tls=ssl.create_default_context(cafile=args.ca),
        allow_reconnect=False, connect_timeout=4,
    )
    counts = {'imported': 0, 'existing': 0}

    async def submit(action, row):
        request = {'action': action, 'history': row}
        reply = await connection.request(f'{root}.ctl.admin.owner', json.dumps(request).encode(), timeout=12)
        result = json.loads(reply.data)
        if result.get('id') != row['id']:
            raise ValueError(f'Owner {action} refused envelope {row["id"]}')
        return result

    try:
        payload = reserve = 0
        available = None
        for row in rows:
            plan = await submit('plan-hub-history', row)
            used, upper, free = (plan.get(key) for key in ('payload_bytes', 'reserve_bytes', 'available_bytes'))
            if not all(type(value) is int and value >= 0 for value in (used, upper, free)) or upper < used:
                raise ValueError('Owner returned an invalid archive capacity plan')
            payload += used
            reserve += upper
            available = free if available is None else min(available, free)
        print(json.dumps({'rows': len(rows), 'payload_bytes': payload, 'reserve_bytes': reserve,
                          'available_bytes': available, 'applied': False}), file=sys.stderr, flush=True)
        if available is not None and reserve > available:
            raise ValueError('Archive batch exceeds available capacity; no rows were written')
        for row in rows:
            result = await submit('import-hub-history', row)
            if result.get('state') not in counts:
                raise ValueError(f'Owner import refused envelope {row["id"]}; rerun is safe after correction')
            counts[result['state']] += 1
    finally:
        await connection.close()
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--hub-db', type=Path)
    source.add_argument('--read-selected', action='store_true', help='Read a private selected-row stream from stdin')
    parser.add_argument('--pair', action='append', type=pair, metavar='A,B')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--apply', action='store_true')
    mode.add_argument('--write-selected', action='store_true', help='Write the selected rows to stdout, including bodies')
    parser.add_argument('--server')
    parser.add_argument('--tenant')
    parser.add_argument('--ca', type=Path)
    parser.add_argument('--owner-creds', type=Path)
    args = parser.parse_args()
    try:
        if args.hub_db is not None and not args.pair:
            parser.error('--hub-db requires at least one --pair')
        if args.read_selected and (not args.apply or args.pair):
            parser.error('--read-selected requires --apply without --pair')
        if args.write_selected:
            for row in load_history(args.hub_db, args.pair):
                print(json.dumps(row))
            return
        rows = (load_history(args.hub_db, args.pair) if args.hub_db else
                [json.loads(line) for line in sys.stdin])
        if not args.apply:
            print(json.dumps({'pairs': len(set(args.pair)), 'rows': len(rows), 'applied': False}))
            return
        endpoint = urlsplit(args.server or '')
        if (endpoint.scheme not in {'tls', 'wss'} or not endpoint.hostname or endpoint.username is not None
                or endpoint.query or endpoint.fragment or ',' in args.server
                or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', args.tenant or '')
                or args.owner_creds is None):
            parser.error('--apply requires an explicit TLS writer, tenant and owner credentials')
        args.owner_creds = args.owner_creds.expanduser()
        info = args.owner_creds.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            parser.error('Owner credential must be an owned private file')
        print(json.dumps(asyncio.run(apply_history(args, rows))))
    except (ValueError, OSError, ImportError, sqlite3.Error) as error:
        parser.exit(1, f'{error}\n')


if __name__ == '__main__':
    main()

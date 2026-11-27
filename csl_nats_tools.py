"""One-shot operations on the authenticated NATS control API."""
import base64
import binascii
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import re
from tempfile import TemporaryFile
import uuid

from csl_bridge_store import BridgeError, contains_findings, encode, identifier, message_selector
from csl_nats import Client, agent_name, config, run_cli

CHUNK_BYTES = 256 * 1024
MAX_FILE_BYTES = 25 * 1024 * 1024


def file_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value):
        raise BridgeError('File ID must be a lowercase SHA-256 digest')
    return value


def file_name(value):
    if (not isinstance(value, str) or not value or len(value.encode('utf-8')) > 255
            or value in ('.', '..') or any(char in value for char in '/\\\0\r\n')):
        raise BridgeError('Invalid file name')
    return value


def attachment(value):
    if (not isinstance(value, dict) or set(value) != {'id', 'name', 'size'}
            or type(value['size']) is not int or not 0 <= value['size'] <= MAX_FILE_BYTES):
        raise BridgeError('Invalid NATS attachment metadata')
    file_id(value['id'])
    file_name(value['name'])
    return value


def attachments(values):
    if not isinstance(values, list):
        raise BridgeError('Invalid NATS attachment list')
    for value in values:
        attachment(value)


async def put(client, stream, name, recipients):
    allowed = sorted({agent_name(recipient, registration=True) for recipient in recipients})
    name = file_name(name)
    digest = hashlib.sha256()
    size = 0
    # Use a private snapshot so the bytes sent still match the digest if the input changes.
    with TemporaryFile() as snapshot:
        while chunk := stream.read(CHUNK_BYTES):
            size += len(chunk)
            if size > MAX_FILE_BYTES:
                raise BridgeError('File exceeds the 25 MiB limit')
            digest.update(chunk)
            snapshot.write(chunk)
        snapshot.seek(0)
        info = {'id': digest.hexdigest(), 'name': name, 'size': size}
        offset = 0
        while True:
            chunk = snapshot.read(CHUNK_BYTES)
            result = await client.request('ctl.put', {
                **info, 'allowed': allowed, 'offset': offset, 'data': base64.b64encode(chunk).decode('ascii'),
            })
            verify_file_receipt(result, info)
            next_offset = result['next_offset']
            if next_offset is None:
                # The server may already have a complete object with this exact digest and ACL.
                return info
            if type(next_offset) is not int or not offset + len(chunk) <= next_offset < size:
                raise BridgeError('Invalid NATS upload offset')
            offset = snapshot.seek(next_offset)


def verify_file_receipt(result, info):
    if type(result.get('size')) is not int or any(result.get(key) != value for key, value in info.items()):
        raise BridgeError('NATS file metadata changed')


@contextmanager
def download_target(directory, directory_fd, filename):
    owned = directory_fd is None
    parent = os.open(directory, os.O_RDONLY | os.O_DIRECTORY) if owned else directory_fd
    temporary = '.csl-download-' + uuid.uuid4().hex
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=parent)
        try:
            with os.fdopen(fd, 'wb') as stream:
                yield stream
            os.link(temporary, filename, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)
        finally:
            os.unlink(temporary, dir_fd=parent)
    finally:
        if owned:
            os.close(parent)


async def fetch(client, value, out=None, *, directory=Path('.'), directory_fd=None):
    identifier = file_id(value)
    first = await client.request('ctl.fetch', {'id': identifier, 'offset': 0})
    info = attachment({'id': identifier, 'name': first.get('name'), 'size': first.get('size')})
    size = info['size']
    dest = out if out is not None else directory / info['name']
    digest = hashlib.sha256()
    offset, page = 0, first
    # Publish the fully verified file without replacing an existing path.
    with download_target(dest.parent, directory_fd, dest.name) as temporary:
        while True:
            verify_file_receipt(page, info)
            try:
                chunk = base64.b64decode(page.get('data', ''), validate=True)
            except (binascii.Error, TypeError, ValueError):
                raise BridgeError('Invalid NATS download encoding') from None
            if len(chunk) != min(CHUNK_BYTES, size - offset):
                raise BridgeError('Invalid NATS download chunk size')
            digest.update(chunk)
            temporary.write(chunk)
            offset += len(chunk)
            next_offset = page['next_offset']
            if offset == size and next_offset is None:
                break
            if type(next_offset) is not int or next_offset != offset or offset >= size:
                raise BridgeError('Invalid NATS download offset')
            page = await client.request('ctl.fetch', {'id': identifier, 'offset': offset})
        if digest.hexdigest() != identifier:
            raise BridgeError('Downloaded file does not match its SHA-256 ID')
    return {**info, 'path': str(dest)}


def duration_seconds(value):
    match = re.fullmatch(r'([1-9][0-9]{0,8})([hm])', value or '')
    if not match:
        raise BridgeError('Use a positive duration such as 2h or 30m')
    count, unit = match.groups()
    return int(count) * (3600 if unit == 'h' else 60)


def rfc3339(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})', value):
        raise BridgeError('Expected an RFC3339 timestamp')
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        raise BridgeError('Expected an RFC3339 timestamp') from None


def since_timestamp(value):
    if not value.endswith(('h', 'm')):
        rfc3339(value)
        return value
    try:
        return (datetime.now(timezone.utc) - timedelta(seconds=duration_seconds(value))).isoformat()
    except OverflowError:
        raise BridgeError('History duration is out of range') from None


async def history(client, *, partner=None, message_id=None, last=None, since=None):
    if bool(partner) == bool(message_id):
        raise BridgeError('History requires exactly one of --with or --id')
    if last is not None and (type(last) is not int or not 1 <= last <= 10000):
        raise BridgeError('Invalid history limit')
    request = {'with': agent_name(partner, registration=True)} if partner else {'id': message_selector(message_id)}
    if since is not None:
        if not partner:
            raise BridgeError('--since requires --with')
        request['since'] = since_timestamp(since)
    if last is not None:
        request['last'] = last
    page = await client.request('ctl.history', request)
    if message_id:
        return page
    messages = page['messages']
    retention = page['retention']
    remaining = (last if last is not None else min(5, retention)) - len(messages)
    seen = set()
    while remaining > 0 and page.get('next_before'):
        before = identifier(page['next_before'])
        if not page['messages'] or before != page['messages'][0]['id'] or before in seen:
            raise BridgeError('Invalid NATS history cursor')
        seen.add(before)
        page = await client.request('ctl.history', {**request, 'last': remaining, 'before': before})
        messages = page['messages'] + messages
        remaining -= len(page['messages'])
    return {'messages': messages, 'retention': retention}


def latest_lines(rows):
    lines = []
    for row in rows:
        partner = agent_name(row['with'])
        source = agent_name(row['from'])
        at = rfc3339(row['at'])
        short_id = identifier(row['id'])[:8]
        preview = ''.join(char if char.isprintable() else ' ' for char in row['preview'][:120])
        lines.append((at, f'{partner}  {row["at"]}  {source}  {preview}  {short_id}'))
    return [line for _, line in sorted(lines, key=lambda item: item[0], reverse=True)]


def watch_request(args):
    if args.list or args.cancel:
        if args.after is not None or args.note is not None:
            raise BridgeError('--after and --note are only valid with --agent')
        return {'action': 'list'} if args.list else {'action': 'cancel', 'id': identifier(args.cancel)}
    if not args.after or not args.note or not args.note.strip():
        raise BridgeError('--agent requires --after and --note')
    if contains_findings(args.note):
        raise BridgeError('Possible secrets detected; redact the note before creating a watch')
    return {'action': 'create', 'agent': agent_name(args.agent, registration=True),
            'after_seconds': duration_seconds(args.after), 'note': args.note}


def call(path, source, operation, *, validate_settings=None):
    settings = config(path)
    agent = agent_name(source or os.environ.get('CSL_AGENT'), registration=True)
    if validate_settings is not None:
        validate_settings(settings)

    async def run():
        client = await Client.connect_active(settings, agent)
        try:
            return await operation(client)
        finally:
            await client.connection.close()

    try:
        return run_cli(run())
    except (OSError, ValueError, KeyError, BridgeError):
        raise
    except Exception as error:
        raise BridgeError(f'NATS operation unconfirmed ({type(error).__name__})') from None


def command(args):
    watch = watch_request(args) if args.hub_action == 'watch' else None

    async def operation(client):
        if args.hub_action == 'history':
            return await history(client, partner=args.partner, message_id=args.message_id, last=args.last,
                                 since=args.since)
        if args.hub_action in ('allow', 'deny'):
            return await client.request('ctl.pairs', {'action': args.hub_action,
                                        'a': agent_name(args.pair_a, registration=True),
                                        'b': agent_name(args.pair_b, registration=True)})
        if args.hub_action == 'pairs':
            result = await client.request('ctl.pairs', {'action': 'list', 'pending': args.pending})
            return {'pending': result['pending']} if args.pending else result
        if watch is not None:
            return await client.request('ctl.watch', watch)
        if args.hub_action == 'put':
            with args.path.open('rb') as stream:
                return await put(client, stream, args.path.name, args.to)
        if args.hub_action == 'fetch':
            return await fetch(client, args.id, args.out)
        request = {}
        if getattr(args, 'partner', None):
            request['with'] = agent_name(args.partner, registration=True)
        if getattr(args, 'task', None):
            request['task'] = args.task
        return await client.request('ctl.' + args.hub_action, request)

    if not args.nats_config:
        raise BridgeError('No NATS enrollment for this agent; identify it with --from or pass --nats-config PATH')
    from csl_agents import infer_agent

    source = args.source or os.environ.get('CSL_AGENT')
    if not source:
        source = infer_agent(config(args.nats_config)['agents'])
    if not source:
        raise BridgeError('Set CSL_AGENT or use --from to identify your agent')
    result = call(args.nats_config, source, operation)
    if args.hub_action == 'latest':
        for line in latest_lines(result):
            print(line)
    else:
        print(encode(result))


def add_options(parser):
    parser.add_argument('--nats-config', type=Path)
    parser.add_argument('--from', dest='source')

"""One-shot operations on the authenticated NATS control API."""
import base64
import binascii
import hashlib
import os
from pathlib import Path
import re
from tempfile import NamedTemporaryFile, TemporaryFile

from csl_bridge_store import BridgeError, encode, identifier, message_selector
from csl_nats import Client, agent_name, config, run_cli
from csl_version import build_version

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


async def fetch(client, value, out=None, *, directory=Path('.')):
    identifier = file_id(value)
    first = await client.request('ctl.fetch', {'id': identifier, 'offset': 0})
    info = attachment({'id': identifier, 'name': first.get('name'), 'size': first.get('size')})
    size = info['size']
    dest = out if out is not None else directory / info['name']
    digest = hashlib.sha256()
    offset, page = 0, first
    # Publish the fully verified file without replacing an existing path.
    with NamedTemporaryFile(dir=dest.parent) as temporary:
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
        temporary.flush()
        os.link(temporary.name, dest)
    return {**info, 'path': str(dest)}


async def history(client, *, partner=None, message_id=None, last=None):
    if bool(partner) == bool(message_id):
        raise BridgeError('History requires exactly one of --with or --id')
    if last is not None and (type(last) is not int or not 1 <= last <= 10000):
        raise BridgeError('Invalid history limit')
    request = {'with': agent_name(partner, registration=True)} if partner else {'id': message_selector(message_id)}
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


def call(path, source, operation):
    settings = config(path)
    agent = agent_name(source or os.environ.get('CSL_AGENT'), registration=True)

    async def run():
        client = await Client.connect(settings, agent)
        try:
            await client.request('ctl.hello', {'client_version': build_version()})
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
    async def operation(client):
        if args.hub_action == 'history':
            return await history(client, partner=args.partner, message_id=args.message_id, last=args.last)
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
        raise BridgeError('Use --nats-config PATH or set CSL_NATS_CONFIG')
    print(encode(call(args.nats_config, args.source, operation)))


def add_options(parser):
    parser.add_argument('--nats-config', type=Path, default=os.environ.get('CSL_NATS_CONFIG'))
    parser.add_argument('--from', dest='source')

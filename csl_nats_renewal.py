"""Renew an agent's private NATS credentials in place."""
import base64
import binascii
from datetime import datetime
import fcntl
import json
import os
import re
import stat
from tempfile import mkstemp
import time

from csl_bridge_store import BridgeError
from csl_version import build_version

RENEW_AHEAD = 4 * 60 * 60
JWT_START = '-----BEGIN NATS USER JWT-----'
SEED_START = '-----BEGIN USER NKEY SEED-----'


def claims(token):
    try:
        if not isinstance(token, str) or not re.fullmatch(r'[\w-]+\.[\w-]+\.[\w-]+', token, re.ASCII):
            raise ValueError
        payload = token.split('.')[1]
        result = json.loads(base64.urlsafe_b64decode(payload + '=' * (-len(payload) % 4)))
        if not isinstance(result, dict) or type(result.get('exp')) is not int:
            raise ValueError
        return result
    except (ValueError, binascii.Error):
        raise BridgeError('Invalid NATS credential claims') from None


def current_claims(path):
    from csl_nats import private_file

    try:
        with private_file(path).open(encoding='ascii') as stream:
            lines = stream.read(16385).splitlines()
        return claims(lines[lines.index(JWT_START) + 1])
    except (IndexError, ValueError):
        raise BridgeError('Invalid NATS credentials file') from None


def account(claim):
    return claim.get('nats', {}).get('issuer_account') or claim['iss']


def credential_text(result, previous, agent):
    import nkeys

    jwt, seed = result.get('jwt'), result.get('seed')
    claim = claims(jwt)
    try:
        expiry = datetime.fromisoformat(result['expires_at']).timestamp()
        if (claim['name'] != agent or previous['name'] != agent or account(claim) != account(previous)
                or claim['exp'] != expiry or claim['exp'] <= previous['exp']
                or not isinstance(seed, str) or not re.fullmatch(r'SU[A-Z2-7]{56}', seed)):
            raise ValueError
        key = nkeys.from_seed(seed.encode('ascii'))
        try:
            if key.public_key.decode('ascii') != claim['sub']:
                raise ValueError
        finally:
            key.wipe()
    except Exception:
        # Parsing failures must not echo the returned credentials.
        raise BridgeError('NATS renewal returned credentials for a different identity or expiry') from None
    return f'{JWT_START}\n{jwt}\n------END NATS USER JWT------\n\n{SEED_START}\n{seed}\n------END USER NKEY SEED------\n'


async def renew_if_due(client, path):
    previous = current_claims(path)
    if previous['exp'] > time.time() + RENEW_AHEAD:
        return False
    lock = os.open(path.with_suffix(path.suffix + '.renew.lock'), os.O_CREAT | os.O_NOFOLLOW | os.O_RDWR, 0o600)
    try:
        info = os.fstat(lock)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise BridgeError('Use a private NATS renewal lock')
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        previous = current_claims(path)
        if previous['exp'] > time.time() + RENEW_AHEAD:
            return False
        result = await client.request('ctl.renew', {'client_version': build_version()})
        text = credential_text(result, previous, client.agent)
        fd, replacement = mkstemp(prefix='.csl-renew-', dir=path.parent)
        try:
            with os.fdopen(fd, 'w', encoding='ascii') as temporary:
                temporary.write(text)
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(replacement, path)
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if os.path.exists(replacement):
                os.unlink(replacement)
    finally:
        os.close(lock)
    return True

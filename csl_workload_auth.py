"""Internal person-approved workload device flow and durable token journal.

No CLI or ambient credential discovery. Callers supply the reviewed origin and
one replica's private state child, and expose approval only to its operator.
"""
import base64
from contextlib import contextmanager
import hashlib
import math
import os
from pathlib import Path
import re
import stat
import time
import urllib.parse
from uuid import UUID

from csl_canonical import strict_json
from csl_sync_state import SyncError, exclusive, private_directory, private_json
from csl_workload_http import Origin, WorkloadHTTP
from csl_workload_identity import AUDIENCE, CLIENT_ID, SCOPES, WorkloadKey, b64, canonical, metadata, unb64

DEVICE_GRANT = 'urn:ietf:params:oauth:grant-type:device_code'
MANAGED_AUDIENCE = 'vitral-collector-assignment-enroll'
REPLAY_SECONDS = 30
TERMINAL = {'invalid_grant', 'session_revoked', 'access_denied'}
JOURNAL_FIELDS = {'version', 'binding', 'thumbprint', 'phase', 'boot', 'deadline', 'device_code', 'nonce',
    'interval', 'next_poll', 'account_id', 'device_id', 'access_token', 'refresh_token', 'expires_boot',
    'expires_wall', 'request_started', 'replay_used', 'fields', 'managed_assignment'}


def managed_binding(value):
    if not isinstance(value, dict) or set(value) != {'environment_id', 'generation'}:
        raise ValueError('Invalid managed assignment')
    environment_id, generation = value['environment_id'], value['generation']
    if (not isinstance(environment_id, str) or str(UUID(environment_id)) != environment_id
            or type(generation) is not int or not 0 < generation < 2**64):
        raise ValueError('Invalid managed assignment')
    return value


def boot_clock():
    if not hasattr(time, 'CLOCK_BOOTTIME'):
        raise SyncError('workload_clock_unavailable')
    return time.clock_gettime(time.CLOCK_BOOTTIME)


def boot_identity():
    try:
        value = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        if not re.fullmatch(r'[a-f0-9-]{36}', value): raise ValueError()
        return value
    except (OSError, ValueError):
        raise SyncError('workload_clock_unavailable') from None


def number(value, maximum):
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= maximum:
        raise SyncError('workload_auth_response_refused')
    return value


def token_text(value):
    if (not isinstance(value, str) or not 1 <= len(value) <= 16384
            or any(not 33 <= ord(c) <= 126 for c in value)):
        raise SyncError('workload_auth_response_refused')
    return value


def jwt_payload(token):
    # Consistency only: the authenticated issuer's HTTPS response supplies the
    # token. The registry remains the signature/current-authority verifier.
    try:
        parts = token.split('.')
        if len(parts) != 3 or any(not re.fullmatch(r'[A-Za-z0-9_-]+', part) for part in parts):
            raise ValueError()
        header, claims = [strict_json(base64.urlsafe_b64decode(part+'='*(-len(part) % 4)))
                          for part in parts[:2]]
        if not isinstance(header, dict) or header.get('alg') != 'RS256' or not isinstance(claims, dict):
            raise ValueError()
        return claims
    except (ValueError, UnicodeError, TypeError, RecursionError):
        pass
    raise SyncError('workload_auth_response_refused')


class WorkloadTokens:
    def __init__(self, root, origin, location, *, http=None, key=None,
                 clock=boot_clock, wall_clock=time.time, boot=boot_identity, stopped=lambda: False):
        self.origin = Origin(origin)
        self.location, digest = metadata(location)
        self.digest = bytes.fromhex(digest)
        self.http = http or WorkloadHTTP(origin, attempts=1)
        if not isinstance(self.http, WorkloadHTTP) or self.http.origin.value != origin:
            raise SyncError('workload_origin_refused')
        root = Path(root).absolute()
        if root.resolve() != root:
            raise SyncError('workload_auth_storage_refused')
        if root.exists():
            info = root.stat()
            if info.st_uid != os.getuid() or not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700:
                raise SyncError('workload_auth_storage_refused')
        self.root = private_directory(root)
        self.path = self.root/'workload-auth.json'
        self.key = key or WorkloadKey(self.root/'identity')
        self.clock, self.wall_clock, self.boot, self.stopped = clock, wall_clock, boot, stopped
        self.binding = {'origin': origin, 'client_id': CLIENT_ID, 'metadata_sha256': digest}

    def _now(self):
        value = self.clock()
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise SyncError('workload_clock_unavailable')
        return value

    def _check(self, deadline=None):
        if self.stopped(): raise SyncError('workload_stopped')
        if deadline is not None and self._now() >= deadline:
            raise SyncError('workload_auth_deadline')

    def _read(self):
        try:
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            return None
        except OSError:
            raise SyncError('workload_auth_storage_refused') from None
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size > 65536):
                raise SyncError('workload_auth_storage_refused')
            raw = stream.read(65537)
        try:
            value = strict_json(raw)
            if (not isinstance(value, dict) or not set(value) <= JOURNAL_FIELDS or len(raw) > 65536 or value.get('version') != 1
                    or value.get('binding') != self.binding or value.get('phase') not in {
                        'authorization_inflight', 'pending', 'device_inflight', 'ready', 'refresh_inflight', 'revoked', 'expired'}):
                raise ValueError()
            unb64(value.get('thumbprint'), 32)
            if 'managed_assignment' in value:
                managed_binding(value['managed_assignment'])
            phase = value['phase']
            if phase not in ('revoked', 'expired') and (not isinstance(value.get('boot'), str) or not value['boot']):
                raise ValueError()
            if phase in ('pending', 'device_inflight'):
                unb64(value.get('device_code'), 32); unb64(value.get('nonce'), 32)
                for field in ('deadline', 'interval', 'next_poll'): number(value.get(field), 10**12)
            if phase in ('ready', 'refresh_inflight'):
                token_text(value.get('access_token')); unb64(value.get('refresh_token'), 32)
                if (not isinstance(value.get('account_id'), str) or not re.fullmatch(r'[a-f0-9]{24}', value['account_id'])
                        or not isinstance(value.get('device_id'), str) or not re.fullmatch(r'ses_[A-Za-z0-9_-]{1,124}', value['device_id'])):
                    raise ValueError()
                for field in ('expires_boot', 'expires_wall'): number(value.get(field), 10**12)
            if phase == 'refresh_inflight':
                number(value.get('request_started'), 10**12)
                if type(value.get('replay_used')) is not bool: raise ValueError()
                fields = value.get('fields')
                if (not isinstance(fields, dict) or set(fields) != {'client_id', 'grant_type', 'refresh_token', 'host_key_proof'}
                        or fields['client_id'] != CLIENT_ID or fields['grant_type'] != 'refresh_token'
                        or fields['refresh_token'] != value['refresh_token']): raise ValueError()
                unb64(fields['host_key_proof'], 64)
            return value
        except (ValueError, UnicodeError, RecursionError, SyncError):
            pass
        raise SyncError('workload_auth_journal_refused')

    def _write(self, value):
        failed = False
        try:
            private_json(self.path, value)
        except (OSError, ValueError, TypeError):
            failed = True
        if failed: raise SyncError('workload_auth_storage_unavailable')

    @contextmanager
    def _lock(self):
        info = self.root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise SyncError('workload_auth_storage_refused')
        with exclusive(self.root, 'workload-auth.lock') as fd:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_mode & 0o077 or info.st_nlink != 1):
                raise SyncError('workload_auth_storage_refused')
            yield

    def _public(self, value=None):
        public, thumb = self.key.public() if value else self.key.ensure()
        if value and b64(thumb) != value.get('thumbprint'):
            raise SyncError('workload_key_binding_changed')
        return public, thumb

    def _form(self, path, fields, deadline=None):
        self._check(deadline)
        return self.http.request(self.origin.issuer+path, 'POST',
            data=urllib.parse.urlencode(fields).encode(),
            headers={'Content-Type': 'application/x-www-form-urlencoded'}, retry=False)

    def status(self):
        with self._lock():
            value = self._read()
            return {'state': value['phase'] if value else 'signed_out',
                    'account_id': value.get('account_id') if value else None,
                    'device_id': value.get('device_id') if value else None}

    def managed_identity(self):
        with self._lock():
            value = self._read()
            if value is not None and 'managed_assignment' not in value:
                raise SyncError('workload_identity_binding_changed')
            _, thumb = self._public(value)
            return {'workload_jkt': b64(thumb), 'state': value['phase'] if value else 'signed_out'}

    def enroll_managed(self, assertion):
        with self._lock():
            current = self._read()
            _, thumb = self._public(current)
            try:
                if len(token_text(assertion)) > 8192:
                    raise ValueError()
                claims = jwt_payload(assertion)
                assignment = managed_binding({key: claims[key] for key in ('environment_id', 'generation')})
                account = claims['sub']
                if (claims['iss'] != 'vitral-pool-controller' or claims['aud'] != MANAGED_AUDIENCE
                        or claims['owner_issuer'] != self.origin.issuer or claims['workload_jkt'] != b64(thumb)
                        or not re.fullmatch(r'[a-f0-9]{24}', account)):
                    raise ValueError()
            except (KeyError, ValueError, TypeError, SyncError):
                raise SyncError('workload_assignment_refused') from None
            if current:
                if current.get('managed_assignment') != assignment or current.get('account_id') != account:
                    raise SyncError('workload_identity_binding_changed')
                if current['phase'] in ('ready', 'refresh_inflight'):
                    return
                raise SyncError('credential_revoked')
            public, _ = self._public(current)
            nonce = hashlib.sha256(b'vitral-managed-collector-v1\0' + assertion.encode('ascii')).digest()
            proof = self.key.proof(self.origin.issuer, assertion, nonce, self.digest, expected_thumbprint=thumb)
            response = self._form('/managed_workload_token', {
                'assignment_assertion': assertion, 'workload': canonical(self.location).decode(),
                'host_public_key': canonical(public).decode(), 'host_key_proof': proof,
            })
            self._accept(response, {'version': 1, 'binding': self.binding, 'thumbprint': b64(thumb),
                                    'managed_assignment': assignment, 'account_id': account})

    def begin(self, *, timeout=600):
        timeout = number(timeout, 600)
        with self._lock():
            if self._read() is not None: raise SyncError('workload_auth_already_started')
            started, boot = self._now(), self.boot()
            deadline = started+timeout
            self._check(deadline)
            self.origin.discovery(self.http.request(self.origin.issuer+'/.well-known/oauth-authorization-server'))
            _, thumb = self._public()
            value = {'version': 1, 'binding': self.binding, 'thumbprint': b64(thumb),
                     'phase': 'authorization_inflight', 'boot': boot, 'deadline': deadline}
            self._check(deadline)
            self._write(value)
            response = self._form('/device_authorization', {'client_id': CLIENT_ID, 'scope': SCOPES,
                'workload': canonical(self.location).decode(), 'key_thumbprint': b64(thumb)}, deadline)
            if not isinstance(response, dict): raise SyncError('workload_auth_response_refused')
            echoed, digest = metadata(response.get('workload_metadata'))
            if echoed != self.location or digest != response.get('workload_metadata_sha256'):
                raise SyncError('workload_metadata_binding_changed')
            unb64(response.get('device_code'), 32); unb64(response.get('exchange_nonce'), 32)
            code, uri = response.get('user_code'), response.get('verification_uri')
            if (not isinstance(code, str) or not re.fullmatch(r'[23456789ABCDEFGHJKLMNPQRSTUVWXYZ]{4}-[23456789ABCDEFGHJKLMNPQRSTUVWXYZ]{4}', code)
                    or uri != self.origin.issuer+'/device'):
                raise SyncError('workload_auth_response_refused')
            expires = number(response.get('expires_in'), 600)
            interval = number(response.get('interval'), 60)
            # Anchor to request start, never extend a code's life by response latency.
            value.update(phase='pending', device_code=response['device_code'], nonce=response['exchange_nonce'],
                deadline=min(deadline, started+expires), interval=interval, next_poll=self._now()+interval)
            self._public(value); self._check(value['deadline']); self._write(value)
            return {'user_code': code, 'verification_uri': uri, 'expires_in': expires, 'interval': interval}

    def _accept(self, response, value, deadline=None):
        if not isinstance(response, dict) or str(response.get('token_type', '')).lower() != 'bearer':
            raise SyncError('workload_auth_response_refused')
        access = token_text(response.get('access_token'))
        refresh = response.get('refresh_token'); unb64(refresh, 32)
        lifetime = number(response.get('expires_in'), 3600)
        account, device = response.get('account'), response.get('device_id')
        if (not isinstance(account, dict) or not isinstance(account.get('id'), str)
                or not re.fullmatch(r'[a-f0-9]{24}', account['id']) or not isinstance(device, str)
                or not re.fullmatch(r'ses_[A-Za-z0-9_-]{1,124}', device)
                or response.get('session_id') != device or response.get('scope') not in (SCOPES, 'offline_access session_sync')):
            raise SyncError('workload_auth_response_refused')
        if value.get('account_id', account['id']) != account['id'] or value.get('device_id', device) != device:
            raise SyncError('workload_identity_binding_changed')
        claims = jwt_payload(access)
        if (claims.get('iss') != self.origin.issuer or claims.get('client_id') != CLIENT_ID
                or claims.get('sub') != account['id'] or claims.get('sid') != device or claims.get('device_id') != device
                or claims.get('aud') != [AUDIENCE] or claims.get('scope') not in (SCOPES, 'offline_access session_sync')
                or claims.get('cnf') != {'jkt': value['thumbprint']} or claims.get('token_use') != 'native_access'):
            raise SyncError('workload_identity_binding_changed')
        wall = self.wall_clock()
        expires_at = number(claims.get('exp'), 10**12)
        if expires_at <= wall or expires_at > wall+lifetime+30:
            raise SyncError('workload_auth_response_refused')
        expires_boot = self._now()+min(lifetime, expires_at-wall)
        write_deadline = min(deadline, expires_boot) if deadline is not None else expires_boot
        self._public(value); self._check(write_deadline)
        if expires_at <= self.wall_clock(): raise SyncError('workload_auth_deadline')
        result = {'version': 1, 'binding': self.binding, 'thumbprint': value['thumbprint'], 'phase': 'ready',
            'account_id': account['id'], 'device_id': device, 'access_token': access, 'refresh_token': refresh,
            'boot': self.boot(), 'expires_boot': expires_boot, 'expires_wall': expires_at}
        if 'managed_assignment' in value:
            result['managed_assignment'] = value['managed_assignment']
        self._write(result)
        self._check(write_deadline)
        return access

    def _terminal(self, value, phase='revoked'):
        result = {key: value[key] for key in ('version', 'binding', 'thumbprint', 'account_id', 'device_id',
                                             'managed_assignment') if key in value}
        result['phase'] = phase
        self._write(result)

    def poll(self):
        with self._lock():
            value = self._read()
            if not value or value['phase'] != 'pending': raise SyncError('workload_enrollment_outcome_unknown')
            if value['boot'] != self.boot() or self._now() >= value['deadline']:
                self._terminal(value, 'expired')
                raise SyncError('workload_approval_expired')
            if self._now() < value['next_poll']: return {'state': 'pending'}
            public, thumb = self._public(value)
            proof = self.key.proof(self.origin.issuer, value['device_code'], unb64(value['nonce'], 32),
                                   self.digest, expected_thumbprint=thumb)
            value['phase'] = 'device_inflight'
            self._check(value['deadline']); self._write(value)
            try:
                response = self._form('/token', {'client_id': CLIENT_ID, 'grant_type': DEVICE_GRANT,
                    'device_code': value['device_code'], 'host_public_key': canonical(public).decode(),
                    'host_key_proof': proof}, value['deadline'])
            except SyncError as error:
                if error.code in ('authorization_pending', 'slow_down'):
                    if error.code == 'slow_down': value['interval'] = min(600, value['interval']+5)
                    value.update(phase='pending', next_poll=self._now()+value['interval'])
                    self._write(value)
                    return {'state': 'pending'}
                if error.code in TERMINAL: self._terminal(value)
                if error.code == 'expired_token':
                    self._terminal(value, 'expired')
                    raise SyncError('workload_approval_expired') from None
                raise SyncError('workload_enrollment_refused' if error.code in TERMINAL else 'workload_enrollment_outcome_unknown') from None
            self._check(value['deadline'])
            self._accept(response, value, value['deadline'])
            return {'state': 'ready'}

    def access_token(self, force_refresh=False):
        with self._lock():
            value = self._read()
            if not value or value['phase'] == 'revoked': raise SyncError('credential_revoked')
            if value['phase'] not in ('ready', 'refresh_inflight'): raise SyncError('workload_enrollment_outcome_unknown')
            self._public(value); self._check()
            if value['phase'] == 'ready':
                if (not force_refresh and value['boot'] == self.boot()
                        and value['expires_boot'] > self._now()+30 and value['expires_wall'] > self.wall_clock()+30):
                    return value['access_token']
                start = self._now()
                try:
                    challenge = self._form('/host_refresh_challenge', {'client_id': CLIENT_ID, 'refresh_token': value['refresh_token']})
                except SyncError as error:
                    if error.code in TERMINAL:
                        self._terminal(value)
                        raise SyncError('credential_revoked') from None
                    raise SyncError('workload_refresh_challenge_unavailable') from None
                if not isinstance(challenge, dict): raise SyncError('workload_auth_response_refused')
                nonce = unb64(challenge.get('refresh_nonce'), 32)
                deadline = start+number(challenge.get('expires_in'), 60)
                proof = self.key.proof(self.origin.issuer, value['refresh_token'], nonce, self.digest,
                                       expected_thumbprint=unb64(value['thumbprint'], 32), refresh=True)
                self._check(deadline)
                fields = {'client_id': CLIENT_ID, 'grant_type': 'refresh_token',
                          'refresh_token': value['refresh_token'], 'host_key_proof': proof}
                value.update(phase='refresh_inflight', request_started=self._now(), boot=self.boot(),
                             replay_used=False, fields=fields)
            else:
                elapsed = self._now()-value['request_started']
                if value['boot'] != self.boot() or not 0 <= elapsed < REPLAY_SECONDS or value['replay_used']:
                    raise SyncError('refresh_outcome_unknown')
                value['replay_used'] = True
                fields = value['fields']
                deadline = value['request_started']+REPLAY_SECONDS
            self._write(value)
            try:
                response = self._form('/token', fields, deadline)
                self._check()
                return self._accept(response, value)
            except SyncError as error:
                if error.code in TERMINAL:
                    self._terminal(value)
                    raise SyncError('credential_revoked') from None
                raise SyncError('refresh_outcome_unknown') from None

    def revoke_locally(self):
        with self._lock():
            value = self._read()
            if value: self._terminal(value)

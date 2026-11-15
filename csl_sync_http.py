"""Bounded registry/OAuth HTTP without redirecting credentials or logging bodies."""
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import ipaddress
import uuid
import http.client

from csl_sync_state import SyncError
from csl_version import __version__ as VERSION

MAX_RESPONSE = 16 * 1024 * 1024
ERROR_CODES = {'unauthenticated', 'session_revoked', 'forbidden', 'not_found', 'invalid_input',
               'prefix_mismatch', 'revision_conflict', 'idempotency_conflict', 'limit_exceeded',
               'tombstoned', 'package_unavailable', 'unavailable', 'quota_exceeded', 'invalid_grant'}


def checked_url(value):
    parts = urllib.parse.urlsplit(value)
    try:
        loopback = parts.hostname == 'localhost' or ipaddress.ip_address(parts.hostname).is_loopback
    except (ValueError, TypeError):
        loopback = False
    if (not isinstance(value, str) or any(ord(c) <= 32 for c in value) or
            (parts.scheme != 'https' and not (parts.scheme == 'http' and loopback)) or not parts.hostname or parts.username or parts.password or parts.query or parts.fragment):
        raise SyncError('unsafe_endpoint_url')
    return value.rstrip('/')


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class HTTP:
    def __init__(self, *, timeout=15, attempts=3, sleep=time.sleep):
        self.timeout, self.attempts, self.sleep = timeout, attempts, sleep
        self.opener = urllib.request.build_opener(NoRedirect())

    def request(self, url, method='GET', *, data=None, headers=None, retry=False):
        for attempt in range(self.attempts if retry else 1):
            try:
                request = urllib.request.Request(url, data=data, method=method, headers=headers or {})
                with self.opener.open(request, timeout=self.timeout) as response:
                    raw = response.read(MAX_RESPONSE + 1)
                    if len(raw) > MAX_RESPONSE:
                        raise SyncError('response_limit_exceeded')
                    if not raw:
                        return {}
                    try:
                        result = json.loads(raw)
                    except (ValueError, UnicodeError):
                        raise SyncError('invalid_server_response') from None
                    if not isinstance(result, dict):
                        raise SyncError('invalid_server_response')
                    return result
            except urllib.error.HTTPError as error:
                try:
                    value = json.loads(error.read(256 * 1024))
                    detail = value.get('error', {})
                    code = detail.get('code') if isinstance(detail, dict) else detail
                    delay = detail.get('retry_after_seconds') if isinstance(detail, dict) else None
                except (ValueError, UnicodeError, AttributeError):
                    code, delay = None, None
                finally:
                    error.close()
                if code not in ERROR_CODES:
                    code = 'http_' + str(error.code)
                transient = error.code in (408, 429, 500, 502, 503, 504)
                delay = min(max(float(delay), 0), 30) if type(delay) in (float, int) else None
                failure = SyncError(code, status=error.code, retryable=transient, retry_after=delay)
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException):
                failure = SyncError('registry_unavailable', retryable=True)
            if not retry or not failure.retryable or attempt + 1 >= self.attempts:
                raise failure from None
            self.sleep(failure.retry_after if failure.retry_after is not None else min(2 ** attempt, 8))


class Registry:
    def __init__(self, endpoint, tokens, *, http=None):
        self.endpoint = checked_url(endpoint)
        self.tokens, self.http = tokens, http or HTTP()

    def call(self, method, path, *, body=None, binary=None, headers=None, key=None):
        if not path.startswith('/'):
            raise SyncError('invalid_registry_path')
        supplied = dict(headers or {})
        supplied.update({'X-Request-ID': str(uuid.uuid4()), 'X-App-Version': VERSION})
        if key:
            supplied['Idempotency-Key'] = key
        if body is not None:
            binary = json.dumps(body, separators=(',', ':'), ensure_ascii=False).encode()
            supplied['Content-Type'] = 'application/json'
        elif binary is not None:
            supplied['Content-Type'] = 'application/octet-stream'
        for auth_attempt in range(2):
            supplied['Authorization'] = 'Bearer ' + self.tokens.access_token()
            try:
                return self.http.request(self.endpoint + path, method, data=binary, headers=supplied,
                                         retry=(method in ('GET', 'PUT', 'DELETE') or bool(key)))
            except SyncError as error:
                if error.code == 'session_revoked':
                    self.tokens.revoke_locally()
                    raise
                if error.status != 401 or auth_attempt:
                    raise
                self.tokens.access_token(force_refresh=True)

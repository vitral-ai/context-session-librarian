"""Public-client browser PKCE and crash-aware rotating refresh, standard library only."""
import base64
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import secrets
import time
import urllib.parse
import webbrowser

from csl_sync_http import HTTP, checked_url
from csl_sync_state import SyncError, exclusive, private_json, read_private_json


def token_response(value):
    if value.get('token_type', '').lower() != 'bearer':
        raise SyncError('invalid_token_response')
    for key in ('access_token', 'refresh_token'):
        token = value.get(key)
        if not isinstance(token, str) or not token or len(token) > 65536 or any(not 33 <= ord(c) <= 126 for c in token):
            raise SyncError('invalid_token_response')
    expires = value.get('expires_in')
    if type(expires) not in (int, float) or not 0 < expires <= 366 * 86400:
        raise SyncError('invalid_token_response')
    return {'access_token': value['access_token'], 'refresh_token': value['refresh_token'],
            'expires_at': time.time() + expires, 'refresh_state': 'ready',
            'session_id': value.get('session_id')}


class Tokens:
    def __init__(self, root, config, *, http=None, clock=time.time):
        self.root, self.config = root, config
        self.path = root / 'credentials.json'
        self.http, self.clock = http or HTTP(), clock

    def status(self):
        value = read_private_json(self.path)
        return {'signed_in': bool(value and value.get('access_token') and value.get('refresh_state') != 'revoked'),
                'state': value.get('refresh_state', 'signed_out') if value else 'signed_out',
                'expires_at': value.get('expires_at') if value else None}

    def _save(self, value):
        private_json(self.path, value, durable=True)

    def revoke_locally(self):
        with exclusive(self.root, 'oauth.lock'):
            self._save({'refresh_state': 'revoked'})

    def _exchange(self, fields):
        return self.http.request(checked_url(self.config['token_endpoint']), 'POST',
                                 data=urllib.parse.urlencode(fields).encode(),
                                 headers={'Content-Type': 'application/x-www-form-urlencoded'}, retry=False)

    def access_token(self, force_refresh=False):
        with exclusive(self.root, 'oauth.lock'):
            value = read_private_json(self.path)
            if not value or value.get('refresh_state') == 'revoked':
                raise SyncError('sign_in_required')
            if value.get('refresh_state') == 'ready' and not force_refresh and value.get('expires_at', 0) > self.clock() + 30:
                return value['access_token']
            if not value.get('refresh_token'):
                raise SyncError('sign_in_required')
            grace = self.config.get('refresh_replay_grace_seconds', 0)
            if value.get('refresh_state') == 'inflight':
                elapsed = self.clock() - value.get('refresh_started_at', 0)
                if not grace or not 0 <= elapsed < grace or value.get('replay_attempted'):
                    raise SyncError('refresh_outcome_unknown')
                value['replay_attempted'] = True
            else:
                value.update(refresh_state='inflight', refresh_started_at=self.clock(), replay_attempted=False)
            self._save(value)  # Durable BEFORE consuming a rotating credential.
            for attempt in range(2):
                try:
                    response = self._exchange({'grant_type': 'refresh_token', 'refresh_token': value['refresh_token'],
                                               'client_id': self.config['client_id']})
                    result = token_response(response)
                    self._save(result)
                    return result['access_token']
                except SyncError as error:
                    if error.code in ('invalid_grant', 'session_revoked'):
                        self._save({'refresh_state': 'revoked'})
                        raise SyncError('credential_revoked') from None
                    elapsed = self.clock() - value['refresh_started_at']
                    if attempt == 0 and grace and 0 <= elapsed < grace and not value['replay_attempted']:
                        # Only opt in when the issuer documents successor replay, never generic retry.
                        value['replay_attempted'] = True
                        self._save(value)
                        continue
                    raise SyncError('refresh_outcome_unknown') from None

    def sign_in(self, *, browser=webbrowser.open, timeout=180, device_id=None, device_label=None, callback_port=0):
        verifier = secrets.token_urlsafe(48)
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
        state = secrets.token_urlsafe(32)
        result = {}

        class Callback(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass  # Query strings contain one-time credentials.

            def do_GET(self):
                query = urllib.parse.urlsplit(self.path)
                params = urllib.parse.parse_qs(query.query)
                valid = query.path == '/callback' and hmac.compare_digest(params.get('state', [''])[0], state)
                if valid:
                    result.update(code=params.get('code', [None])[0], error=bool(params.get('error')))
                self.send_response(200 if valid else 400)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.send_header('Cache-Control', 'no-store')
                self.end_headers()
                self.wfile.write(b'You may close this window.' if valid else b'Invalid callback.')

        with exclusive(self.root, 'oauth.lock'):
            try:
                callback_server = HTTPServer(('127.0.0.1', callback_port), Callback)
            except OSError:
                raise SyncError('loopback_callback_unavailable') from None
            with callback_server as server:
                server.timeout = .2
                redirect = 'http://127.0.0.1:' + str(server.server_port) + '/callback'
                params = {'response_type': 'code', 'client_id': self.config['client_id'], 'redirect_uri': redirect,
                          'scope': self.config.get('scope', 'session_sync offline_access'), 'state': state,
                          'code_challenge': challenge, 'code_challenge_method': 'S256'}
                if device_label:
                    params['device_label'] = device_label
                if self.config.get('audience'):
                    params['audience'] = self.config['audience']
                if not browser(checked_url(self.config['authorization_endpoint']) + '?' + urllib.parse.urlencode(params)):
                    raise SyncError('browser_unavailable')
                deadline = time.monotonic() + timeout
                while not result and time.monotonic() < deadline:
                    server.handle_request()
                if not result:
                    raise SyncError('sign_in_timeout')
                if result.get('error') or not result.get('code'):
                    raise SyncError('sign_in_denied')
                fields = {'grant_type': 'authorization_code', 'code': result['code'], 'redirect_uri': redirect,
                          'client_id': self.config['client_id'], 'code_verifier': verifier}
                if device_id:
                    fields['installation_id'] = device_id
                if device_label:
                    fields['device_label'] = device_label
                # An uncertain code exchange is not repeated; start a fresh browser flow.
                value = token_response(self._exchange(fields))
                self._save(value)
                return self.status()


def discover(url, *, http=None):
    value = (http or HTTP()).request(checked_url(url))
    if 'S256' not in value.get('code_challenge_methods_supported', []):
        raise SyncError('pkce_s256_unavailable')
    if 'none' not in value.get('token_endpoint_auth_methods_supported', ['none']):
        raise SyncError('public_client_unavailable')
    try:
        result = {'authorization_endpoint': checked_url(value['authorization_endpoint']),
                  'token_endpoint': checked_url(value['token_endpoint'])}
        if 'refresh_replay_grace_seconds' in value:
            grace = value['refresh_replay_grace_seconds']
            if type(grace) is not int or not 0 <= grace <= 30:
                raise SyncError('invalid_discovery')
            result['refresh_replay_grace_seconds'] = grace
        return result
    except (KeyError, TypeError):
        raise SyncError('invalid_discovery') from None

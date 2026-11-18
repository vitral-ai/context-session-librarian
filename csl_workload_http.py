"""Opt-in exact-origin workload HTTPS. Desktop HTTP behavior is unchanged."""
import ipaddress
import re
import ssl
import urllib.parse
import urllib.request

from csl_sync_http import ERROR_CODES, HTTP, NoRedirect
from csl_sync_state import SyncError


class Origin:
    def __init__(self, value):
        self.value, self.authority = self._parse(value, origin=True)
        self.issuer = self.value + '/native-auth/v1'
        self.registry = self.value + '/session-registry/v1'

    @staticmethod
    def _parse(value, *, origin=False):
        if (not isinstance(value, str) or len(value) > 8192 or not value.isascii()
                or any(ord(c) <= 32 or ord(c) == 127 for c in value) or '\\' in value):
            raise SyncError('workload_origin_refused')
        try:
            parsed = urllib.parse.urlsplit(value)
            host, port = parsed.hostname, parsed.port
            if (parsed.scheme != 'https' or not host or parsed.username is not None
                    or parsed.password is not None or parsed.fragment or host != host.lower()
                    or port is not None and not 1 <= port <= 65535):
                raise ValueError()
            try:
                address = ipaddress.ip_address(host)
                host = '[' + address.compressed + ']' if address.version == 6 else str(address)
            except ValueError:
                if len(host) > 253 or any(not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', label)
                                          for label in host.split('.')):
                    raise ValueError()
            authority = host + (':' + str(port) if port is not None else '')
            if parsed.netloc != authority or not value.startswith('https://' + authority):
                raise ValueError()
            if origin and (parsed.path or parsed.query or value != 'https://' + authority):
                raise ValueError()
            return value, (host, port or 443)
        except ValueError:
            raise SyncError('workload_origin_refused') from None

    def endpoint(self, value):
        value, authority = self._parse(value)
        if authority != self.authority:
            raise SyncError('workload_origin_refused')
        path = urllib.parse.urlsplit(value).path
        decoded = urllib.parse.unquote(path)
        if (not any(decoded.startswith(prefix + '/') for prefix in ('/native-auth/v1', '/session-registry/v1'))
                or any(part in ('.', '..', '') for part in decoded.split('/')[1:])
                or '%' in decoded or '\\' in decoded or any(ord(c) <= 32 or ord(c) == 127 for c in decoded)):
            raise SyncError('workload_endpoint_refused')
        return value

    def discovery(self, value):
        if not isinstance(value, dict) or value.get('issuer') != self.issuer:
            raise SyncError('workload_discovery_refused')
        expected = {'authorization_endpoint': self.issuer + '/authorize',
            'token_endpoint': self.issuer + '/token', 'revocation_endpoint': self.issuer + '/revoke',
            'jwks_uri': self.issuer + '/jwks', 'device_authorization_endpoint': self.issuer + '/device_authorization',
            'host_refresh_challenge_endpoint': self.issuer + '/host_refresh_challenge',
            'session_registry_endpoint': self.registry}
        for name, endpoint in expected.items():
            if name in value and value[name] != endpoint:
                raise SyncError('workload_discovery_refused')
        for name, required in {
                'grant_types_supported': {'urn:ietf:params:oauth:grant-type:device_code', 'refresh_token'},
                'scopes_supported': {'session_sync', 'offline_access'},
                'token_endpoint_auth_methods_supported': {'none'}}.items():
            choices = value.get(name)
            if not isinstance(choices, list) or any(not isinstance(v, str) for v in choices) or not required <= set(choices):
                raise SyncError('workload_discovery_refused')
        if any(value.get(name) != expected[name] for name in ('token_endpoint', 'jwks_uri', 'revocation_endpoint')):
            raise SyncError('workload_discovery_refused')
        return {'issuer': self.issuer, **expected}


class WorkloadHTTP(HTTP):
    error_codes = ERROR_CODES | {'authorization_pending', 'slow_down', 'expired_token',
        'access_denied', 'temporarily_unavailable', 'invalid_request', 'invalid_client', 'unauthorized_client'}

    def __init__(self, origin, *, ca_file=None, **options):
        self.origin = Origin(origin)
        super().__init__(**options)
        try:
            context = ssl.create_default_context(cafile=ca_file)
        except (OSError, ValueError):
            raise SyncError('workload_tls_unavailable') from None
        # No ambient proxy, credential store, redirect or hostname-validation override.
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),
            NoRedirect(), urllib.request.HTTPSHandler(context=context))

    def request(self, url, method='GET', *, data=None, headers=None, retry=False):
        if method not in ('GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'):
            raise SyncError('workload_method_refused')
        if (headers is not None and (not isinstance(headers, dict) or any(
                not isinstance(name, str) or not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]{1,128}", name)
                or name.lower() in {'host', 'proxy-authorization', 'proxy-connection'}
                or not isinstance(value, str) or not value.isascii() or len(value) > 8192
                or any(ord(c) < 32 or ord(c) == 127 for c in name+value)
                for name, value in headers.items()))):
            raise SyncError('workload_headers_refused')
        return super().request(self.origin.endpoint(url), method, data=data, headers=headers, retry=retry)

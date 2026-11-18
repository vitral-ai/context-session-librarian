"""Local-only probe against an explicit backend checkout and its real ASGI router.

Requires that checkout's test dependencies (FastAPI/httpx/etc.). Does not run the
backend lifespan, contact Mongo, enroll a device or obtain a token. This is not
a TLS/ingress or deployed-backend qualification.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
import urllib.parse


def check(backend):
    sys.path.insert(0, str(backend/'fastapi-backend'))
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from routers import native_auth
    from native_identity import config, device, host_proof
    from native_identity.runtime import set_runtime
    from csl_sync_state import SyncError
    from csl_workload_auth import WorkloadTokens
    from csl_workload_http import Origin, WorkloadHTTP
    from csl_workload_identity import CLIENT_ID, SCOPES, canonical, metadata, public_key

    origin = 'https://identity.example.test'
    location = dict(label='synthetic collector', provider='kubernetes', cluster='kind',
                    namespace='test', workload='fixture', replica='fixture-1')
    class Key:
        def ensure(self): return public_key(bytes(range(32)))

    app = FastAPI()
    app.include_router(native_auth.router)
    # Discovery checks signing readiness, not a signature. No private key/store
    # or application runtime is needed; every route and metadata field is real.
    set_runtime(SimpleNamespace(signing_keys=SimpleNamespace(get=lambda: object())))
    try:
        with patch.dict(os.environ, {'NATIVE_AUTH_ISSUER': origin+'/native-auth/v1'}), \
                TestClient(app, base_url=origin, follow_redirects=False) as server:
            old = server.get('/native-auth/v1/.well-known/openid-configuration')
            assert old.status_code == 404
            response = server.get('/native-auth/v1/.well-known/oauth-authorization-server')
            assert response.status_code == 200
            checked = Origin(origin).discovery(response.json())
            assert checked['token_endpoint'] == origin+'/native-auth/v1/token'
            assert checked['device_authorization_endpoint'] == origin+'/native-auth/v1/device_authorization'
            assert checked['host_refresh_challenge_endpoint'] == origin+'/native-auth/v1/host_refresh_challenge'

            class Transport(WorkloadHTTP):
                def __init__(self): self.calls = []
                def request(self, url, method='GET', *, data=None, headers=None, retry=False):
                    path = urllib.parse.urlsplit(Origin(origin).endpoint(url)).path
                    self.calls.append((method, path))
                    assert retry is False
                    if method == 'GET':
                        reply = server.get(path)
                        if reply.status_code != 200: raise SyncError('interop_discovery_failed')
                        return reply.json()
                    assert (method, path) == ('POST', '/native-auth/v1/device_authorization')
                    form = dict(urllib.parse.parse_qsl(data.decode()))
                    assert set(form) == {'client_id', 'scope', 'workload', 'key_thumbprint'}
                    assert form['client_id'] == config.WORKLOAD_CLIENT == CLIENT_ID
                    assert set(form['scope'].split()) == set(config.WORKLOAD_SCOPES) == set(SCOPES.split())
                    parsed = device.WorkloadMetadata.model_validate(device.json_object(form['workload'])).model_dump()
                    assert parsed == location
                    assert device.canonical_metadata(parsed) == canonical(location)
                    assert metadata(parsed)[1] == metadata(location)[1]
                    assert host_proof.decode_base64url(form['key_thumbprint'], 32) == Key().ensure()[1]
                    # Deliberately no POST to ASGI: no device grant or authority effect.
                    raise SyncError('interop_stopped_before_authorization')

            transport = Transport()
            transport.origin = Origin(origin)
            with tempfile.TemporaryDirectory(prefix='workload-discovery-interop-') as root:
                client = WorkloadTokens(Path(root)/'auth', origin, location, http=transport, key=Key())
                try: client.begin()
                except SyncError as error:
                    assert error.code == 'interop_stopped_before_authorization', error.code
                else: raise AssertionError('probe must stop before authorization')
                assert transport.calls == [('GET', '/native-auth/v1/.well-known/oauth-authorization-server'),
                                           ('POST', '/native-auth/v1/device_authorization')]
    finally:
        set_runtime(None)
    print(json.dumps({'discovery': 'pass', 'authorization_fields': 'pass',
                      'old_route_status': old.status_code, 'authorization_sent': False}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend', type=Path, required=True)
    check(parser.parse_args().backend.resolve())

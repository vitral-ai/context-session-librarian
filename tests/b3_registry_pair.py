"""Opt-in synthetic collector/real registry pairing; never uses personal tokens.

Run: python3 tests/b3_registry_pair.py --fixture /private/localhost-fixture.json
Fixture: endpoint, identity_origin, account_id, source/destination objects each
with device_id/access_token. The backend owner provisions both collector
principals in one disposable account. Only loopback HTTP endpoints are allowed.
"""
import argparse
from contextlib import contextmanager, redirect_stdout
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import urllib.parse
import uuid
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from csl_sync_engine import Collector, Providers
from csl_sync_http import Registry
from csl_sync_state import State, SyncError
from csl_sync_writer import Writer, native_lock
from csl_resume import import_same_id
import csl_claude
import csl_sync_providers as providers


def loopback(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost', '::1'):
        raise ValueError('fixture endpoints must use loopback HTTP')
    return url.rstrip('/')


class Tokens:
    def __init__(self, value): self.value = value
    def access_token(self, force_refresh=False): return self.value
    def revoke_locally(self): raise AssertionError('fixture token rejected')


@contextmanager
def environment(root):
    with patch.dict(os.environ, CLAUDE_CONFIG_DIR=str(root/'claude'), CODEX_HOME=str(root/'codex')):
        yield


def run(fixture):
    endpoint, identity = loopback(fixture['endpoint']), loopback(fixture['identity_origin'])
    identifier = str(uuid.uuid4())
    with tempfile.TemporaryDirectory(prefix='csl-b3-pair-') as temporary:
        root = Path(temporary)
        states = []
        try:
            for side in ('source', 'destination'):
                directory = root/side
                directory.mkdir()
                state = State(directory/'profile')
                states.append(state)
                state.set('device_id', fixture[side]['device_id'])
                state.set('settings', dict(endpoint=endpoint, kind='local',
                    token_endpoint=identity+'/native-auth/v1/token'))
                api = Registry(endpoint, Tokens(fixture[side]['access_token']))
                with environment(directory):
                    Collector(state, api, Providers()).register()
                    assert Writer(state, api).attest(fixture[side]['device_id']) == fixture['account_id']
            source, destination = states
            source_api = Registry(endpoint, Tokens(fixture['source']['access_token']))
            captured = {}
            original = source_api.call
            def capture(method, path, **kwargs):
                result = original(method, path, **kwargs)
                if method == 'PUT' and path == '/sessions/claude/'+identifier:
                    captured.update(result)
                return result
            source_api.call = capture
            source_root = root/'source'
            with environment(source_root):
                path = csl_claude.target_path(identifier, source_root)
                path.parent.mkdir(parents=True, exist_ok=True)
                def record(text):
                    return json.dumps(dict(type='user', uuid=str(uuid.uuid4()), sessionId=identifier,
                        timestamp=datetime.now(timezone.utc).isoformat(), cwd=str(source_root),
                        message=dict(role='user', content=text)))+'\n'
                path.write_text(record('Synthetic B3 source'))
                collector = Collector(source, source_api, Providers())
                collector.sync_session('claude', identifier, providers._metadata('claude', path, providers.provider_home('claude'), 120))
                registry_id = captured['id']
                snapshot = source.session('claude', identifier)['snapshot_id']
                row = source_api.call('GET', '/sessions/'+registry_id)
                assert row['sync']['complete'] is True and row['package']['available'] is True
                # This is a synthetic stopped source. No managed process is
                # claimed; the real keeper-receipt witness remains separate.
                with native_lock('claude', identifier):
                    Writer(source, source_api).release('claude', identifier, registry_id=registry_id,
                        snapshot=snapshot, generation=0, expected_device=fixture['source']['device_id'])
                package = root/'owner-package.zip'
                with package.open('xb') as output:
                    source_api.download('/sessions/'+registry_id+'/package', output, 16*1024*1024)
            destination_root = root/'destination'
            args = SimpleNamespace(bundle=package, sync_home=destination.root, cwd=destination_root,
                open=False, provider=None, list=False, name=None, target=None, leaf=None,
                repair_interrupted=False, history_policy='active', registry_session_id=registry_id,
                expected_snapshot_id=snapshot, expected_generation=0, request_id=str(uuid.uuid4()),
                expected_device_id=fixture['destination']['device_id'])
            with environment(destination_root), patch('csl_sync_auth.Tokens', return_value=Tokens(fixture['destination']['access_token'])), redirect_stdout(io.StringIO()):
                assert import_same_id(args, None) == [identifier]
                path = csl_claude.target_path(identifier, destination_root)
                with path.open('a') as output: output.write(record('Synthetic B3 destination append'))
                destination_api = Registry(endpoint, Tokens(fixture['destination']['access_token']))
                Collector(destination, destination_api, Providers()).sync_session('claude', identifier,
                    providers._metadata('claude', path, providers.provider_home('claude'), 120))
                row = destination_api.call('GET', '/sessions/'+registry_id)
                assert row['native_id'] == identifier and row['writer']['generation'] == 1
                assert row['writer']['device_id'] == fixture['destination']['device_id']
                assert row['sync']['complete'] is True and row['snapshot_id'] != snapshot
            # Exercise the server fence, bypassing the local fence deliberately.
            with environment(source_root):
                try:
                    source_api.call('POST', '/sessions/'+registry_id+'/writer/release',
                        body=dict(generation=0, expected_snapshot_id=snapshot))
                except SyncError as error:
                    assert error.code in ('writer_fenced', 'writer_snapshot_changed'), error.code
                else: raise AssertionError('stale source release accepted')
            return dict(result='PASS', native_id=identifier, registry_id=registry_id,
                generation=1, same_registry_session=True, managed_process_witness='NOT RUN')
        finally:
            for state in states: state.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(json.loads(args.fixture.read_text()))))

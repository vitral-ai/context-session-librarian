"""The public probe uses existing server identity without collector mutation."""
import argparse
from contextlib import redirect_stdout, redirect_stderr
import io
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from csl_sync import command
from csl_sync_state import State, SyncError, private_json


class AttestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name) / 'profile'
        state = State(self.home)
        self.device = state.get('device_id')
        state.set('settings', dict(enabled=True, kind='local', endpoint='https://registry.example/v1',
                  token_endpoint='https://identity.example/native-auth/v1/token'))
        state.set('registered', True)
        state.set('last_error', 'previous_diagnostic')
        state.close()
        private_json(self.home / 'credentials.json', dict(access_token='SECRET', refresh_state='ready', expires_at=time.time()+600))
        self.args = argparse.Namespace(sync_home=self.home, sync_action='attest-installation', json=True)

    def invoke(self, response=None, error=None):
        out, err = io.StringIO(), io.StringIO()
        with patch('csl_sync_http.HTTP.request', return_value=response, side_effect=error) as request, redirect_stdout(out), redirect_stderr(err):
            try:
                command(self.args, None)
                code = 0
            except SystemExit as stopped:
                code = stopped.code
        self.assertNotIn('SECRET', out.getvalue()+err.getvalue())
        state = State(self.home)
        self.assertEqual(state.get('last_error'), 'previous_diagnostic')
        self.assertEqual(state.status()['sessions'], [])
        state.close()
        return code, out.getvalue(), err.getvalue(), request

    def test_success_is_closed_server_attested_identity(self):
        code, out, _, request = self.invoke(dict(kind='collector', account_id='a'*24, device_id=self.device, extra='ignored'))
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), dict(account_id='a'*24, device_id=self.device))
        self.assertLess(len(out.encode()), 4096)
        self.assertEqual(request.call_count, 1)
        self.assertEqual(request.call_args.args[:2], ('https://identity.example/session-registry/authorize', 'POST'))
        self.assertEqual(request.call_args.kwargs['data'], b'{"action":"write"}')
        self.assertFalse(request.call_args.kwargs['retry'])

    def test_unverified_identity_never_outputs_success(self):
        for changes in (dict(account_id='A'*24), dict(account_id='token body'), dict(device_id='other'), dict(kind='workload')):
            with self.subTest(changes=changes):
                response = dict(kind='collector', account_id='a'*24, device_id=self.device)
                response.update(changes)
                code, out, _, _ = self.invoke(response)
                self.assertEqual((code, out), (1, ''))

    def test_transient_and_permanent_exit_contract(self):
        for retryable, expected in ((True, 75), (False, 1)):
            code, out, _, _ = self.invoke(error=SyncError('registry_unavailable', retryable=retryable))
            self.assertEqual((code, out), (expected, ''))

    def test_uncertain_refresh_is_not_replayed(self):
        private_json(self.home / 'credentials.json', dict(access_token='SECRET', refresh_token='SECRET', refresh_state='inflight', refresh_started_at=time.time()))
        code, out, err, request = self.invoke()
        self.assertEqual((code, out), (1, ''))
        self.assertIn('refresh_outcome_unknown', err)
        request.assert_not_called()

    def test_probe_does_not_take_collector_lock(self):
        from csl_sync_state import exclusive
        with exclusive(self.home):
            code, _, _, _ = self.invoke(dict(kind='collector', account_id='a'*24, device_id=self.device))
        self.assertEqual(code, 0)

    def test_real_parser_and_missing_profile(self):
        from csl_sync import add_commands
        root = argparse.ArgumentParser()
        add_commands(root.add_subparsers(dest='command'), None)
        args = root.parse_args(['sync', '--home', str(self.home / 'absent'), 'attest-installation', '--json'])
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as stopped:
            command(args, None)
        self.assertEqual(stopped.exception.code, 1)
        self.assertEqual(out.getvalue(), '')
        self.assertFalse((self.home / 'absent').exists())

    def test_fresh_refresh_uses_existing_auth_and_private_result(self):
        state = State(self.home)
        settings = state.get('settings')
        settings['client_id'] = 'collector'
        state.set('settings', settings)
        state.close()
        private_json(self.home / 'credentials.json', dict(access_token='SECRET', refresh_token='SECRET', refresh_state='ready', expires_at=0))
        responses = [dict(access_token='NEW_SECRET', refresh_token='NEW_SECRET', token_type='Bearer', expires_in=3600),
                     dict(kind='collector', account_id='a'*24, device_id=self.device)]
        out = io.StringIO()
        with patch('csl_sync_http.HTTP.request', side_effect=responses) as request, redirect_stdout(out):
            command(self.args, None)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(json.loads(out.getvalue()), dict(account_id='a'*24, device_id=self.device))
        self.assertNotIn('SECRET', out.getvalue())


if __name__ == '__main__':
    unittest.main()

"""Private credential renewal through the authenticated control client."""
import base64
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from csl_bridge_store import BridgeError
from csl_nats_renewal import JWT_START, SEED_START, current_claims, renew_if_due

try:
    import nkeys
except ImportError:
    nkeys = None


def token(**payload):
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip('=')
    return f'e30.{encoded}.c2ln'


class RenewalTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.path = Path(scratch.name) / 'worker.creds'
        self.now = 1_800_000_000
        self.old_token = token(exp=self.now + 60, name='worker', iss='signer', nats={'issuer_account': 'account'})
        self.original = f'{JWT_START}\n{self.old_token}\n'
        self.path.write_text(self.original)
        self.path.chmod(0o600)
        clock = patch('csl_nats_renewal.time.time', return_value=self.now)
        clock.start()
        self.addCleanup(clock.stop)
        self.client = SimpleNamespace(agent='worker', request=AsyncMock())

    def replacement(self, *, name='worker'):
        seed = nkeys.encode_seed(b'\0' * 32, nkeys.PREFIX_BYTE_USER)
        key = nkeys.from_seed(seed)
        try:
            expiry = self.now + 86400
            jwt = token(exp=expiry, name=name, iss='new-signer', sub=key.public_key.decode(),
                        nats={'issuer_account': 'account'})
        finally:
            key.wipe()
        return {'jwt': jwt, 'seed': seed.decode(), 'expires_at': datetime.fromtimestamp(expiry, timezone.utc).isoformat()}

    async def test_healthy_credentials_do_not_contact_control(self):
        self.path.write_text(f'{JWT_START}\n{token(exp=self.now + 86400)}\n')
        self.assertFalse(await renew_if_due(self.client, self.path))
        self.client.request.assert_not_awaited()

    @unittest.skipIf(nkeys is None, 'nkeys is not installed')
    async def test_renewal_replaces_private_file_and_skips_second_call(self):
        old_inode = self.path.stat().st_ino
        self.client.request.return_value = self.replacement()
        self.assertTrue(await renew_if_due(self.client, self.path))
        self.assertNotEqual(self.path.stat().st_ino, old_inode)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertIn(SEED_START, self.path.read_text())
        self.assertEqual(current_claims(self.path)['exp'], self.now + 86400)
        self.assertFalse(await renew_if_due(self.client, self.path))
        self.assertEqual(self.client.request.await_count, 1)
        self.assertEqual(self.client.request.await_args.args[0], 'ctl.renew')

    @unittest.skipIf(nkeys is None, 'nkeys is not installed')
    async def test_mismatched_agent_keeps_old_file(self):
        self.client.request.return_value = self.replacement(name='other')
        with self.assertRaisesRegex(BridgeError, 'different identity or expiry'):
            await renew_if_due(self.client, self.path)
        self.assertEqual(self.path.read_text(), self.original)

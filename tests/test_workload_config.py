"""Explicit provider/version and volume boundaries without any native process."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from csl_sync_state import SyncError
from csl_workload_config import Configuration, PREFIX, PROVIDER_PROFILES


class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='workload-config-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for name in ('transcripts', 'state'): (self.root/name).mkdir()
        self.env = {PREFIX+'ORIGIN': 'https://identity.example.test', PREFIX+'PROVIDER': 'codex',
            PREFIX+'PROFILE': PROVIDER_PROFILES['codex'], PREFIX+'TRANSCRIPT_ROOT': str(self.root/'transcripts'),
            PREFIX+'STATE': str(self.root/'state'), PREFIX+'REDACTION': 'required',
            'VITRAL_LOCATION_LABEL': 'synthetic workload', 'VITRAL_LOCATION_PROVIDER': 'kubernetes',
            'VITRAL_LOCATION_CLUSTER': 'kind', 'VITRAL_LOCATION_NAMESPACE': 'test',
            'VITRAL_LOCATION_WORKLOAD': 'fixture', 'VITRAL_LOCATION_REPLICA': 'replica-1'}

    def test_exact_profiles_never_execute_or_read_native_homes(self):
        class Inputs(dict):
            def get(self, name, default=None):
                if not name.startswith(('VITRAL_COLLECTOR_', 'VITRAL_LOCATION_')):
                    raise AssertionError('unapproved input')
                return super().get(name, default)
        for provider, profile in PROVIDER_PROFILES.items():
            env = Inputs({**self.env, PREFIX+'PROVIDER': provider, PREFIX+'PROFILE': profile,
                          'CODEX_HOME': 'canary', 'CLAUDE_CONFIG_DIR': 'canary', 'GOOGLE_API_KEY': 'canary'})
            with self.subTest(provider=provider), patch('subprocess.run', side_effect=AssertionError('no native process')):
                config = Configuration.from_environment(env)
                self.assertEqual(config.profile, profile)
                self.assertNotIn('canary', repr(config))

    def test_missing_foreign_profiles_and_redaction_opt_out_refuse(self):
        for field, value in (('PROFILE', None), ('PROFILE', 'auto'), ('PROFILE', PROVIDER_PROFILES['claude']),
                              ('PROVIDER', 'antigravity'), ('PROVIDER', 'unknown'), ('CLIENT_ID', 'another-client'),
                              ('REDACTION', None), ('REDACTION', 'false'), ('FLUSH_SECONDS', '21'),
                              ('FLUSH_SECONDS', '0'), ('SPOOL_LIMIT_BYTES', '536870913')):
            with self.subTest(field=field, value=value), self.assertRaises(SyncError):
                Configuration.from_environment({**self.env, PREFIX+field: value})

    def test_origin_metadata_and_overlapping_or_symlinked_mounts_refuse(self):
        nested = self.root/'transcripts'/'state'; nested.mkdir()
        link = self.root/'alias'; link.symlink_to(self.root/'transcripts', target_is_directory=True)
        for field, value in ((PREFIX+'ORIGIN', 'https://elsewhere.example.test/path'),
                              (PREFIX+'STATE', str(self.root/'transcripts')), (PREFIX+'STATE', str(nested)),
                              (PREFIX+'TRANSCRIPT_ROOT', str(link)), (PREFIX+'STATE', '~/state'),
                              ('VITRAL_LOCATION_REPLICA', '')):
            with self.subTest(field=field), self.assertRaises(SyncError):
                Configuration.from_environment({**self.env, field: value})

    def test_mount_modes_are_preserved_and_public_projected_ca_is_explicit(self):
        state = self.root/'state'; state.chmod(0o770)
        ca = self.root/'public-ca.crt'; ca.write_text('synthetic fixture, not loaded by pure config')
        link = self.root/'ca.crt'; link.symlink_to(ca)
        config = Configuration.from_environment({**self.env, PREFIX+'TRUST_CA': str(link)})
        self.assertEqual(config.ca_file, str(link))
        self.assertEqual(state.stat().st_mode & 0o777, 0o770)
        self.assertEqual(list(state.iterdir()), [])


if __name__ == '__main__': unittest.main()

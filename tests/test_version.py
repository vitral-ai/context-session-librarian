import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

import csl_version


class ReleaseVersionTests(unittest.TestCase):
    @patch.object(Path, "exists", return_value=True)
    @patch.object(subprocess, "check_output", return_value="a" * 40 + "\n")
    def test_checkout_uses_head(self, _git, _exists):
        self.assertEqual(csl_version.build_version(), "0.5.0+" + "a" * 40)

    @patch.object(Path, "exists", return_value=False)
    @patch.object(csl_version, "_ARCHIVE_COMMIT", "b" * 40)
    def test_archive_uses_embedded_commit(self, _exists):
        self.assertEqual(csl_version.build_version(), "0.5.0+" + "b" * 40)

    @patch.object(Path, "exists", return_value=True)
    @patch.object(subprocess, "check_output", side_effect=FileNotFoundError)
    @patch.object(csl_version, "_ARCHIVE_COMMIT", "$Format:%H$")
    def test_source_without_build_identity_keeps_release_version(self, _git, _exists):
        self.assertEqual(csl_version.build_version(), "0.5.0")

import subprocess
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import csl_version


class ReleaseVersionTests(unittest.TestCase):
    @patch.object(Path, "exists", return_value=True)
    @patch.object(subprocess, "check_output", return_value="a" * 40 + "\n")
    def test_checkout_uses_head(self, _git, _exists):
        self.assertEqual(csl_version.build_version(), "0.5.1+" + "a" * 40)

    @patch.object(Path, "exists", return_value=False)
    @patch.object(csl_version, "_ARCHIVE_COMMIT", "b" * 40)
    def test_archive_uses_embedded_commit(self, _exists):
        self.assertEqual(csl_version.build_version(), "0.5.1+" + "b" * 40)

    @patch.object(Path, "exists", return_value=True)
    @patch.object(subprocess, "check_output", side_effect=FileNotFoundError)
    @patch.object(csl_version, "_ARCHIVE_COMMIT", "$Format:%H$")
    def test_source_without_build_identity_keeps_release_version(self, _git, _exists):
        self.assertEqual(csl_version.build_version(), "0.5.1")

    def test_same_second_tag_upgrade_invalidates_previous_release_bytecode(self):
        # Published v0.5.0 csl_version.py is 841 bytes. Timestamp-based pycs
        # compare whole seconds and length, so preserve both during this upgrade.
        old = b'__version__ = "0.5.0"\n'
        old += b'#' + b' ' * (841 - len(old) - 2) + b'\n'
        current = Path(csl_version.__file__).read_bytes()
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "csl_version.py"
            target.write_bytes(old)
            os.utime(target, (1700000000, 1700000000))
            command = [sys.executable, "-I", "-c",
                       "import sys; sys.path.insert(0, sys.argv[1]); "
                       "import csl_version; print(csl_version.__version__)", temporary]
            self.assertEqual(subprocess.check_output(command, text=True).strip(), "0.5.0")
            self.assertTrue(list((Path(temporary) / "__pycache__").glob("*.pyc")))
            (Path(temporary) / "VERSION").write_text("0.5.1\n")
            target.write_bytes(current)
            os.utime(target, (1700000000, 1700000000))
            self.assertEqual(subprocess.check_output(command, text=True).strip(), "0.5.1")

    def test_identical_module_size_and_mtime_reads_updated_version_data(self):
        current = Path(csl_version.__file__).read_bytes()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target, version = root / "csl_version.py", root / "VERSION"
            target.write_bytes(current)
            version.write_text("0.5.0\n")
            for path in (target, version):
                os.utime(path, (1700000000, 1700000000))
            command = [sys.executable, "-I", "-c",
                       "import sys; sys.path.insert(0, sys.argv[1]); "
                       "import csl_version; print(csl_version.__version__)", temporary]
            self.assertEqual(subprocess.check_output(command, text=True).strip(), "0.5.0")
            cache = next((root / "__pycache__").glob("*.pyc"))
            cached = cache.read_bytes()
            before = [(p.stat().st_size, p.stat().st_mtime_ns) for p in (target, version)]
            target.write_bytes(current)
            version.write_text("0.5.1\n")
            for path in (target, version):
                os.utime(path, (1700000000, 1700000000))
            self.assertEqual(before, [(p.stat().st_size, p.stat().st_mtime_ns)
                                      for p in (target, version)])
            self.assertEqual(subprocess.check_output(command, text=True).strip(), "0.5.1")
            self.assertEqual(cache.read_bytes(), cached)

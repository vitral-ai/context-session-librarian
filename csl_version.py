"""Release identity for CSL 0.5.2, independent of bundle and canonical formats."""

import re
import subprocess
from pathlib import Path

__version__ = Path(__file__).with_name("VERSION").read_text(encoding="ascii").strip()
_ARCHIVE_COMMIT = "$Format:%H$"


def build_version() -> str:
    """Identify a Git checkout or source archive by its build commit."""
    commit = _ARCHIVE_COMMIT
    root = Path(__file__).resolve().parent
    if (root / ".git").exists():
        try:
            commit = subprocess.check_output(
                ["git", "-C", str(root), "rev-parse", "--verify", "HEAD"],
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=1,
            ).strip()
        except (OSError, subprocess.SubprocessError):
            pass
    if re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit):
        return f"{__version__}+{commit}"
    return __version__

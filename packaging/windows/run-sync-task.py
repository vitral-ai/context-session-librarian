"""Run an enrolled Windows collector with the profiles chosen at registration."""
import base64
import json
import os
from pathlib import Path
import sys
import contextlib


if __name__ == '__main__':
    _, state, encoded_profiles = sys.argv
    profiles = json.loads(base64.b64decode(encoded_profiles, validate=True))
    if (not isinstance(profiles, dict)
            or set(profiles) != {'CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'CSL_ANTIGRAVITY_HOME'}
            or not all(isinstance(path, str) and Path(path).is_absolute() for path in profiles.values())):
        raise SystemExit('Invalid scheduled collector profiles')
    os.environ.update(profiles)

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from csl_sync import main

    # pythonw has no console streams. Progress/errors remain in collector state
    # (csl sync status); avoid an unbounded second log or a console dependency.
    with open(os.devnull, 'w', encoding='utf-8') as sink:
        with contextlib.redirect_stdout(sys.stdout if sys.stdout is not None else sink), \
                contextlib.redirect_stderr(sys.stderr if sys.stderr is not None else sink):
            main(['sync', '--home', state, 'watch'])

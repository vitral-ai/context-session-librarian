"""Run an enrolled Windows collector with the profiles chosen at registration."""
import base64
import json
import os
from pathlib import Path
import sys


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

    main(['sync', '--home', state, 'watch'])

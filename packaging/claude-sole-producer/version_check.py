"""Build-time check: pinned version from a fresh private home; final home stays empty."""
import os
from pathlib import Path
import subprocess
import tempfile

with tempfile.TemporaryDirectory(prefix='claude-version-') as home:
    config = Path(home)/'.claude'
    config.mkdir(mode=0o700)
    env = {'PATH': '/usr/local/bin:/usr/bin:/bin', 'HOME': home, 'CLAUDE_CONFIG_DIR': str(config),
           'DISABLE_AUTOUPDATER': '1', 'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC': '1'}
    version = subprocess.check_output(['/opt/vitral/claude/bin/claude', '--version'],
                                      env=env, text=True, timeout=20).strip()
    assert version == '2.1.308 (Claude Code)', version
assert os.listdir('/agent-private') == ['.claude']
assert os.listdir('/agent-private/.claude') == ['projects']
assert not os.listdir('/agent-private/.claude/projects')
print(version)

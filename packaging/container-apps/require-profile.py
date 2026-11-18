"""Require a reviewed workload profile before executing the collector."""
import json
import os
from pathlib import Path
import sys


COLLECTOR = '/usr/local/bin/csl-workload-collector'
PROFILES = Path(__file__).with_name('profiles.json')


def main():
    profiles = json.loads(PROFILES.read_bytes())
    expected = profiles.get(os.environ.get('VITRAL_COLLECTOR_PROVIDER'))
    if expected is None or os.environ.get('VITRAL_COLLECTOR_PROFILE') != expected:
        raise SystemExit('VITRAL_COLLECTOR_PROFILE must match the selected transcript provider')
    os.execv(COLLECTOR, [COLLECTOR, *sys.argv[1:]])


if __name__ == '__main__':
    main()

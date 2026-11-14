#!/usr/bin/env python3
"""Frozen v0.3.0 operation whitelist and negative receiver response control.

Source: csl_bridge_store.validate and csl_bridge_transport.receiver_main at
53e4ccfa312615f2bca54ea2bf5599d1ec908873. This isolated negative-path fixture
does not emulate successful native operations, authentication or transport.
It deliberately does not import the current bridge validator.
"""
import json
import sys

LEGACY_FIELDS = {
    'followup': {'source', 'target', 'text'},
    'send_message': {'source', 'target', 'text'},
    'status': {'target'}, 'interrupt': {'target', 'turn_id'},
    'receipt': {'request_id'}, 'watch': {'after'}, 'agents': set(),
}
request = json.loads(sys.stdin.buffer.readline(65537))
op = request.get('op')
assert op not in LEGACY_FIELDS or set(request) != LEGACY_FIELDS[op] | {'version', 'op', 'id'}, 'This fixture only represents the legacy rejection path'
print(json.dumps({'ok': False, 'data': {'error': 'Bridge receiver rejected request'}}))
raise SystemExit(1)

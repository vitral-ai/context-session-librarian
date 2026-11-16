# Experimental pairing integration validation

This records the integration of the macOS/LAN/private-relay candidate into the
current CSL line. It is not a release record or a universal compatibility claim.

## Source and scope

- Candidate: `73425f0d705e07d521789f0d556defddbbc28a89`.
- Main incorporated: `ae2e7a210db33291985ef63706ef98aadf4a172e`, including the
  sender/session framing changes from PR #29.
- Existing mailbox, sync and provider work is retained. Pairing configures only
  coordination operations, not mailbox routing or transcript sync.
- New pairs admit the source agent named in their authenticated pairing exchange.
  Existing profiles without source admission do not receive an automatic grant.
  Session UUID transmission remains an explicit compatible-peer opt-in.
- Generated MCP profiles preserve mailbox queries and explicit owner grant-admin
  selection. Delegation remains opt-in; interruption is not auto-approved.

## Reproducible automated checks

On 2026-11-16, Linux with Python 3.14.8:

```sh
python3 scripts/run_isolated_tests.py --output /path/to/new-results
```

Result: **721 tests discovered, 683 passed, 38 skipped, no failures or errors**.
Fresh HOME, Codex, Claude and XDG directories; no inherited provider auth and no
paid model calls. This is base mode, not the optional full native/media suite.
Read `scripts/README.md` before selecting full mode.

Relevant regressions cover actual loopback TLS/IPC and nested relay TLS,
single-use/expired invitations, wrong pins, peer scope, duplicate receipts,
uncertain delivery, exact-ID retry, independent lease renewal, fragmented TLS,
MCP profiles, app lifecycle, packaging and scoped macOS power assertions.
`tests/test_bridge_session_header.py` and the mailbox/sync suites also pass.
These tests use synthetic/fabricated native runtimes where indicated in source;
they do not establish real-model behavior.

The integration initially failed because old pairing profiles lacked the newly
required `allow_sources`, retry fixtures lacked source admission, and app profile
expectations omitted current mailbox queries. These were corrected without
relaxing the admission rules. The LAN regression additionally rejects an
uninvited sender before a native turn can start.

## Earlier real-device observations and remaining gates

Before integration, the candidate was built and locally ad-hoc signed on macOS
arm64 with bundled Python 3.12.16. An existing Linux session and an existing Mac
session coordinated through the private relay while on different local networks.
Operator-controlled relay interruption, reconnection, duplicate delivery and an
explicit same-ID recovery were exercised; neither account nor session was replaced.
Those operator observations concern the predecessor source, not this merged tree.

The integrated SHA still needs a fresh native Mac build and real-device check.
There is no notarized/signed public download, public relay enrollment service,
Windows validation, independent human GUI pilot or guarantee on networks that
block the chosen relay port. Availability assertions cannot override lid closure
or explicit OS sleep. Loopback TLS tests do not demonstrate DoS resistance.

No deployment, installed connector migration, account change or new release is
performed by this PR. Keep the operational predecessor available until the
integrated candidate passes its native checks.

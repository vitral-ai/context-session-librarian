# Private Codex execution publisher — task and impact

Historical initial implementation record below. The separate ordinary source
correction, external Claude/Fable review credit and current findings/testing
are recorded in [EXECUTION_PUBLISHER_CORRECTION.md](EXECUTION_PUBLISHER_CORRECTION.md).
The earlier interrupted Codex review remains incomplete, not a clearance.

Baseline: actual merged origin/main `67ebb21954d2176220e85876b19a0bdff2c1d4de`.
Branch: `feat/execution-codex-publisher`. Sole author: Codex W4 worker.
Commission: PLAN Lane D; frozen import candidate3 SHA
`738040c315c58bb67e657e883fbeb5faac6adb0ade4670b83cd81bd11a0fba4d`
and its source amendments; next-slice `w4-after-703-next-implementation-20261119.md`.

Problem: execution reservation stores a complete mapping but no durable member
publication or result. Ordinary import allocates fresh UUIDs and cannot recover
a killed process. A receipt must never adopt a foreign same-hash inode.

Scope: private Linux Codex raw singleton/uncompressed complete history_base
publisher with one keeper engine, deterministic rewrite, root-last links,
durable typed native_unverified receipt/provenance and cold SIGKILL recovery.
Strict private atomic rename metadata avoids transient hard-linked records.
Original checker is mandatory and unchanged across all effects. Lookup is
read-only; new lookup authority cannot renew publication. Claude publication
refuses before effects, with existing reservation/lookup preserved.

Impact: new unwired private module, optional history rewrite checker and focused
synthetic tests/documentation. Ordinary import/receive/backup remain unchanged.
No CLI, native Access/launcher constructor, network, index, Start, child sync,
activation, credential copy or A7/A10 claim. Native W2 authority mapping and
post-Start update provenance remain separate owners' work.

Validation: isolated owned homes, actual subprocess SIGKILL and cold readback,
race/substitution/fault/expiry cases, affected history/transfer/backup/FD9
regressions and full standard-library unittest suite. Independent source review
through principal precedes any commit/push/publication.

## Journal

- 2026-11-19: fetched main; created new isolated worktree from67ebb21. Read
  AGENTS, README, provider matrix, contribution and execution/transport/history/
  transfer contracts. Checked prior author roots clean and preserved. No native
  resources, histories or credentials used. Implementation begun locally.
- Implemented one actual private member engine for singleton/full Codex graph,
  strict v2 expected-state atomic metadata advancement, typed durable receipt and
  keeper retirement. Old strict metadata reader remains unchanged. Added optional
  checked history rewrite without changing ordinary absent-check/map behavior.
- Source self-checks found two important error boundaries: integer/bool receipt
  equality now compares canonical typed bytes, and post-open authority loss closes
  its newly owned descriptor while retaining uncertain filesystem custody.
  Syscall-error paths repeat original checks and preserve known denial priority.
- Python3.14.8 and retained Go1.27.4: affected149 cases,148PASS/one expected
  offline-nativeSKIP, zero failures,17.060s. Includes17 real graph SIGKILL cuts
  with separate cold-process readback, additional first-metadata pre-rename kill,
  real Go/Python receipt round-trip and parent-death-before-link reaping.
- Full standard-library discover first ran1185 with15 errors/one failure/48skips:
  the long isolated TMPDIR caused AF_UNIX path-too-long in unrelated bridge/light
  fixtures. Retained `.local/execution-codex-publisher/full.log`; no bridge/light
  source or assertions changed. Repeated with short owned isolated subprocess
  HOME/TMPDIR/CODEX_HOME/CLAUDE_CONFIG_DIR:1185 cases,1137PASS/48SKIP, zero
  failures/errors,148.122s. Required Go process fixtures enabled. Corrected log:
  `.local/execution-codex-publisher/full-short-isolation.log`.
- Existing optional native/live/media/packaging checks remain their recorded
  skips, not native/context/A7/A10 passes. No paid model, login, remote resource,
  personal history or copied credentials. Every deliberate child was reaped;
  owned synthetic short-isolation trees were removed after the suite.
- Source/tests frozen locally for principal's independent review; no commit,
  push or PR. Byte manifest and review patch live under ignored
  `.local/execution-codex-publisher/`; any subsequent correction needs a new
  manifest and appropriate tests before publication.

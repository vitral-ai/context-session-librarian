# Ordinary publisher source correction R3

Commissioned by root after the complete appended62-line external Claude report,
commissioned by Fable (fable-next), round2 verdict CHANGES REQUIRED. Credit that
external work in any future owner-authorized PR. This is author implementation
and local testing only, not a review or clearance. The interrupted Codex review
remains stopped/incomplete; no review retry/rephrasing/replacement is undertaken.

New worktree `csl-execution-codex-publisher-correction-r3`, branch
`feat/execution-codex-publisher-correction-r3`; actual fetched merged baseline
`3632bf4cc300aa7ea663c75e9f468357f8745016`. Copied only the verified16-file R2
source/docs/test snapshot `97c3c4593a70e6c16364f9132c83c205f5e770835c4de12ef51091b5b1d9f35c`;
those paths have no conflicting newer-main changes. Both previous freezes,
including original258156b2 and R2 manifests/patches/logs, remain unchanged.

Scope before edits: explicit private0700 scratch components under umask002/027;
recover only durably acknowledged owned partial planning/rewrite work under the
same original guard/operation lock; strict per-entry inode custody/reset and
bounded unknown Pending; post-receipt owned scratch cleanup with durable crash
progress/receipt preservation and no foreign deletion. Also bound read-only
pre-receipt progress reconciliation if inexpensive. No CLI/index/Start/runtime/
native constructor, activation, child sync, publication, commit/push/PR or A7/A10.

Validation: synthetic isolated roots and occurrence-selectable actual SIGKILL,
affected/full standard-library suites with retained Go1.27.4 REQUIRED. Capture
raw actual Go FD9 output and exact source/tool/host provenance; this workstation
is not a guest. Prepare a hashed source-only fixture bundle and exact command
for root's separately arranged guest executor, with no login/auth/history files.

## Work journal

- Read full external report and new task. Established actual merged baseline,
  verified source-only copy/no overlap and recorded scope before implementation.
- New internal metadata schema3 makes `planning` an explicit pre-map intent.
  Before publication, complete mapping is durably reserved as before. Older
  unpublished publisher shapes are refused, while legacy v1 reservation stays
  supported. A durably owned scratch root now has a capped512-entry inode ledger:
  path/kind/identity and allocating/owned/deleting/deleted phases. Every file or
  directory inode is acknowledged before writing bytes. Explicit component mkdir
  mode0700 and file0600 replace umask-dependent recursive mkdirs; no foreign chmod
  repair or relaxation of root/metadata/link checks is used.
- Owned partial planning/rewrite can reset only checked ledger contents under
  the original guard and lifetime lock. Preflight rejects extra, replaced,
  symlinked, linked or unacknowledged objects. Deletion is write-ahead and
  idempotent after SIGKILL; parent and target inode bindings repeat after waits.
  Unknown created-before-ack entries remain bounded Pending. Fixed scratch names
  never become authority by basename/hash.
- After durable receipt and keeper retirement, planning/rewrite files and their
  roots are removed using that same checked ledger, including durable retiring/
  retired states. Kill/fsync/permission/authority failures preserve the receipt
  and retain exact cleanup custody. Unrelated/replaced contents survive. No broad
  erasure or post-Start ownership claim follows from this scoped scratch cleanup.
- Read-only reconciliation also permits bounded validated pre-receipt forward
  progress, with immutable request/target/first mapping once established. It
  performs no writes or deadline renewal; unchanged corruption still fails.
- Updated earlier author regressions whose former conservative expectations
  required owned scratch to stay permanently Pending/retained. Those are precisely
  the behaviors rejected by external round2. New tests require recovery and
  absence of raw scratch copies after success; unknown ownership remains refused.
- Python3.14.8 / retained Go1.27.4 affected168 cases:167PASS, one expected existing
  offline-nativeSKIP, zero failures/errors,70.171s. Nine new R3 cases include
  umask002/027, partial data-write SIGKILL occurrences during planning/rewrite,
  six post-receipt cleanup cuts, reset interruption, unknown/unacknowledged data,
  foreign extra/replacement including an admission race, and pre-receipt read race.

## Exact actual FD9 evidence and guest handoff

The source-only runner is
`tests/fixtures/execution_fd9/run_publisher_evidence.py`. It builds/runs the actual
retained Go1.27.4 parent, captures raw stdout/stderr, validates the actual receipt
and parent-death reaping, and writes source/tool/kernel/hostname provenance.
Compiler/child environment keys are closed and recorded, with isolated synthetic
homes; no credential/auth/history file is read or copied. `host_role` is explicitly
supplied by the executor. Author runs here are **workstation**, never guest.

Exact command for root's separately arranged existing guest, from the extracted
source bundle (substitute its existing Go path and a new owned0700 output path):

```sh
python3 -I -B tests/fixtures/execution_fd9/run_publisher_evidence.py --go /absolute/existing/go1.27.4/bin/go --out /owned/private/fd9-r3-evidence --host-role guest
```

The guest must return provenance.json plus all raw build/scenario stdout/stderr;
role text alone is not evidence of a guest. This source task performs no guest
login/provisioning or native Access integration. Root owns that executor and
qualifies its actual host/source receipts separately. No A7/native recall claim.

## Final author test and source receipts

- Affected168 cases:167PASS/one existing offline-nativeSKIP, zero failures/errors,
  70.171s. Full standard-library discovery on actual merged3632bf4 base:1228
  cases,1180PASS/48SKIP, zero failures/errors,201.353s. Python3.14.8 and retained
  Go1.27.4, with `CSL_REQUIRE_EXECUTION_PROCESS_TESTS=1`; Go fixture did run.
  Full subprocess homes/temp/provider directories were short, owned, isolated
  and removed after the run. No native/live/media optional skip becomes PASS.
- Direct actual Go FD9 raw evidence is under ignored
  `.local/execution-codex-publisher-r3/fd9-workstation-final/`: build and both
  scenario stdout/stderr plus provenance.json. Successful actual output is
  imported/native_unverified; actual parent-death output is
  `{effect:false,reaped:true,signal:9}`. The host role is workstation, not guest.
  Tools, executable hashes, kernel/hostname, closed environment-key lists and all
  source hashes are recorded. No login/auth/history data or ambient credential
  environment is sent to the fixture child.
- Source-only bundle `csl-publisher-r3-fd9-source.tar.gz`:312403 bytes,
  SHA-256 `d43091ab089eab3c4afb0afbbd2cd78567573c326a5807c820701fc51093f46f`;
  75 Python/Go/synthetic-fixture sources plus BUNDLE.json, no `.git`, runtime,
  credentials or histories. Code-tree SHA-256
  `58dc89fc5e5735c9bc2f9f0d51da69c33d202e60e09cc7223c528f07ab17b4d2`.
  Extracted that exact archive into another owned synthetic source tree and ran
  the exact runner successfully; all75 runtime source hashes matched BUNDLE.json.
  That portability check is also workstation evidence, never guest evidence.
  Raw output/provenance is in `fd9-bundle-workstation/` alongside direct evidence.
- Root received the exact bundle/command for its separately arranged existing
  guest executor. Guest execution is NOT_RUN by this author and is not certified
  by a role argument alone. Root must bind the returned raw host/source/tool
  evidence to its actual guest control receipt. No provisioning/login performed.
- Both original12-file258156b2 and R2 sixteen-file97c3c459 snapshots and all their
  artifact SHA receipts were rechecked unchanged. All R3 source/tests stay local,
  uncommitted/unpublished. Final manifest, full and R2 correction patches and raw
  log/provenance hashes are under `.local/execution-codex-publisher-r3/`.
  No additional independent review or Codex review continuation occurred. External
  CHANGES REQUIRED remains the recorded verdict until root's external process.

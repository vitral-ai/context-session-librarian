# Unwired Local Composition Prototype

Separate from published PR64. This local-only module and private-file tests add
no launcher/configuration hook, network call, actual worker run, image inventory,
driver CLI or acceptance PASS. It is not ready for publication or activation.
It was prepared locally on the source cut reviewed in PR64, now coordinator-
merged. Publication must still be independently commissioned from a current
merged base, never stacked or inferred from these prototype fixtures.

## Evidence State, Not Delivery Authority

`Evidence` owns a private `observation` child under an explicitly supplied state
root without chmod of that volume root. A closed run context binds a fresh run
UUID, collector scope hash, approved account/device binding hash, source pin and
selected session UUID. Its JSON and lock are owned, mode0600, regular and single
link; symlinks/special/shared files refuse. The lock uses nonblocking/no-follow
descriptor validation before flock. Atomic/fsynced private JSON bounds storage;
the record is a compact summary, not an unbounded list or body archive.

The future parent must durably admit each fresh worker UUID before its requests.
The future actual Request hook must persist a body-only intent before dispatch,
then the existing validated AND persisted ACK hook may advance the range. The
prototype accepts those prevalidated facts; it does NOT independently turn a
caller-supplied digest into an authoritative registry receipt. Future integration
must route only the exact existing checked facts from PR64/Collector/ProofRegistry.

Coverage starts at zero. A later worker can continue an ACKed prefix only when
its existing verified prefix hash equals the earlier observed-and-ACKed prefix.
The new cumulative digest must come from existing exact ACK checks, not a digest
over an unobserved earlier range. A known-public-canary prefix length survives
workers; arbitrary payload bytes never do. Flags are sticky across the run.
View sequence coverage can survive a worker; a new generation starts at one.
A new file revision requires a new paired view generation and zero content
coverage. Snapshot updates invalidate final evidence and require new observation.

Unfinished pre-request intent across worker death remains permanently uncertain
for this run. A later worker cannot clear it based on a remote length/hash or
new approval. Failed operations poison evidence, not upload authority. No request
replay/reupload, source re-read, grant repair, ACK mutation, expiry or deadline
renewal exists here. Normal product retries may continue unchanged but this
prototype refuses qualification after ambiguity. All budgets are evidence caps,
not transport/credential lifetimes.

## Final Reader Contract

Only a final worker's freshly observed snapshot acknowledgement plus the existing
authoritative final proof and EOF/deadline acceptance may call `finalized`.
That source precondition is NOT established by this pure prototype. It returns
a unique evidence seal to bind the eventual private expected-worker result.

`consume` reads only the expected UUID-named receipt beneath this state root's
private runtime/receipts directory. Before opening it, canonical path/parent
checks reject unrelated or symlinked paths. The caller supplies the independently
expected current run, scope/binding/source/session, final worker and seal. The
receipt must have the same sequence/scope/run/seal, successful existing worker
status, zero pending, one synced source and one manifest. Full current coverage
and final IDs are rechecked. Only fixed per-surface booleans/canonical matched
IDs emerge. This is canary evidence, never producer-stop/close or overall A8
authority. A detected canary is not PASS. Missing evidence yields none.

## Required Next Integration

- Bind the parent run/worker nonce and source pin without adopting an old run.
- Carry private context across actual workers, with durable write failures and
  pre-dispatch crashes leaving no qualified worker seal.
- Feed only actual serialized Request bodies and existing checked/persisted ACK
  facts; preserve origin/auth/request/retry/deadline/EOF semantics byte-for-byte.
- Finalize only after real proof/EOF/deadline checks; retain/consume the private
  expected-worker result before existing cleanup removes it. Do not import or
  copy host credentials, journals or approvals into any workload.
- Independently review crash, stale seal, record rollback, concurrent run,
  generation replacement, spool/capacity and lost-ACK cases in the actual worker.
- Rebase is not the integration method: reapply as an unstacked contribution
  after dependencies merge, then normal G2/CI and separately commissioned runs.

No real observer worker/collector execution or artifacts include this prototype.
The earlier single-worker source cut and its explicit limits remain unchanged.

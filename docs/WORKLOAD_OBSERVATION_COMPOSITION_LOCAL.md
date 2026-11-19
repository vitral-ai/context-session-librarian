# Private Asynchronous Observation Source

This local source corrects the blocking optional-I/O finding against375ac0c9.
That cut's independent verdict was CHANGES REQUIRED, not source acceptance.
No qualified image receipt contains this correction. Default CLI/watch/chart
behavior is unchanged; there is no new product CLI, network service, consent
authority, deadline, request/replay/reupload or automatic enrollment.

## Private Driver Contract

`AcceptanceDriver` requires independently reviewed `source` (runtime-content
fingerprint), existing runner `scope`, expected approved `account_id`, `device_id`
and canonical `session`. These facts MUST NOT come from an observer receipt.
The existing explicit observation-session configuration must agree. The driver
creates each run and worker seal independently in parent memory.

```python
driver = AcceptanceDriver(source=reviewed_runtime_fingerprint, scope=scope(config),
    account_id=expected_account, device_id=expected_device, session=expected_session)
result = Runner(config, observation_driver=driver).run(literal_producer_argv)
evidence = driver.result
```

This is source API documentation, not authorization for an actual invocation.
Optional `driver.prepare(config, scope(config))` starts preparation asynchronously;
`driver.poll()` is nonblocking. Production never waits for preparation. Cold or
lagging preparation can leave the whole run unqualified, without delaying the
producer or changing its product result. Preparation callers own `close()` if
they never enter Runner. A driver is single-use. Custom observation-enabled
pass factories must forward the private composition keyword to PassProcess.

`driver.result` is a copy of either `None` (UNQUALIFIED, not clean) or exactly
three surfaces: `snapshot`, `content`, `view`. Each contains only a fixed
`canary_present` boolean and canonical `matched_identity` UUID. A true flag is
detection, not success; a non-null result is NOT A8 PASS or producer-stop proof.
Runner stdout/public result remains unchanged. StoredDriver/Evidence are helper
internals, not alternate acceptance-reader APIs.

## Actual Independence

Optional fingerprint/auth/journal work, including helper exec startup, runs
outside the parent's stop/control thread. One private, parent-death-bound helper
process performs ALL optional disk I/O. Parent/worker IPC uses inherited AF_UNIX
SEQPACKET descriptors and bounded MSG_DONTWAIT sends/reads, never filesystem RPC,
blocking select, sleep, helper wait or synchronous fsync. Only descriptor numbers
enter argv; no body, account, key, bearer or observation value enters argv/env.
The helper has no listener/socket path and can never start an approval/refresh.

The helper validates the existing private auth record with the existing atomic
strict reader, without acquiring a product auth lock. It never opens the sync
database or acquires product sync locks. Normal worker admission/HTTP/auth/ACK
operations remain the original ones. Helper shutdown kills first and reaps in
an independent daemon; the product path never joins it.

Expected delivery plans are separate product proof facts, not observer claims.
After the EXISTING validated/persisted final proof and source EOF, the actual
product worker copies the already-verified in-memory plan into its private
receipt. No extra state read/request or proof acceptance occurs. The parent
retains this closed fact from a prior successful/reaped live worker. The final
product fact, helper plan and matched IDs must all equal that retained plan.
No expected nonce is adopted from either receipt. Changed-final-plan and cold
final-only qualification remain deliberately unsupported.

## Durable Readiness, Never Waiting

At begin-call, the helper prepares an intent from the existing canonical plan
and expected body digest. This is NOT Request observation or product ACK.
The intent starts `observed=false`. Only AFTER its fsync can the helper send
readiness bound to the exact worker/seal/call/URL/body digest.

At merged64's actual immutable `Request.data` point, the real worker checks that
readiness nonblockingly and verifies the exact bytes. If readiness is absent,
late, mismatched, oversized or backpressured, evidence fails immediately and the
ORIGINAL request proceeds. No delay is inserted to make observation qualify.
Only a matched actual Request event may mark that intent observed. Only the
existing checked AND persisted content/view ACK callbacks may advance coverage.
Prepared intent alone cannot become acknowledged or final evidence.

Content starts at zero and resumes only an earlier observed-and-ACKed prefix;
cross-chunk/worker matching retains only a prefix length of the fixed public
canary. Actual bytes match the prepared digest before any qualifying ACK. New
revisions require paired generations. Lost/interrupted/unknown coverage, retries,
unobserved earlier content, missing surfaces and storage failure stay unqualified.

After actual proof AND EOF, the worker enqueues finalization and IMMEDIATELY
continues to its normal product receipt. A pending ticket is NOT evidence. The
helper checks the SAME original BOOTTIME deadline before/after sealing. The
parent's nonblocking reader accepts only an already-completed current seal and
the original successful product status/plan/deadline/producer quiescence. It
consumes before receipt cleanup. A missing/late seal yields None permanently;
there is no later promotion, previous seal reuse or budget extension.

Every run/worker clears the old result before I/O or launch. The parent retains
an exact predecessor record hash; old worker/run/receipt/journal states cannot
silently become current. Same-UID current-nonce mutation/whole-state forgery is
outside the existing trusted boundary: this is NOT cryptographic tamper resistance.

## Bounds, Tests And Remaining Gaps

The optional helper is one process per parent run. IPC caps are96KiB per packet,
48KiB per body and8192 events; ordinary larger product bodies are NOT refused or
split/reuploaded, only their evidence is unavailable. Store caps remain1024
attempts, 128MiB cumulative bodies and126 pre-existing directory entries. Private
0700 children/0600 files do not change mount-root permissions. No arbitrary
payload bytes are retained in the durable summary. This is logical I/O/lock
independence, not isolation from whole-filesystem failure, kernel scheduling,
shared cgroup OOM or shared-device congestion; real resource acceptance is open.

Owned synthetic TLS/real-worker tests include40-second helper stalls at startup,
retention, pre-dispatch intent and final sealing. They require timely producer
stop, unchanged completed product delivery and honest None under the original
budget. Positive fixtures independently arrange helper/request/receipt readiness;
that fixture padding is NOT product behavior or a latency/availability claim.
Interruption, lost proof, unsafe storage, stale seal/predecessor, helper-plan
mismatch, unsupported size and same-UID limitation controls remain separate.

REAL NATIVE BINDING GAP: the current61 empty transcript root plus new one-shot
Codex invocation does not supply an independently approved session UUID before
observation. This driver still REQUIRES that preselected UUID. No supported fresh
native-binding mechanism is implemented here; it must not guess/adopt a session
from an observer receipt, another run, edited native rows or home scanning.
Thus this source does NOT close fresh-native packaged A8. It adds no model turn,
native probe, credential copy, identity reset or actual image/Pod/cloud operation.

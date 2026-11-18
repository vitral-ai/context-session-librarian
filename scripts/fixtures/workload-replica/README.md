# Replica-local enrollment fixture

This is test source, not a product CLI, packaged image entry point, listener,
consent server or real-account enrollment workflow. It reuses the merged
`WorkloadTokens`, `WorkloadKey`, packaging configuration and exact-origin
deadline transport. It never approves a sign-in. Only an explicitly configured
HTTPS loopback/`localhost`/`.test` origin with an explicit public fixture CA is
accepted. DEV, staging, PROD and arbitrary real identity origins refuse.

## Invocation and boundary

Mount this exact reviewed script read-only into the collector replica, separate
from its image; it is intentionally absent from the runtime inventory and build
context. The replica receives the existing `VITRAL_COLLECTOR_*` and
`VITRAL_LOCATION_*` packaging inputs, including the exact profile, one origin,
public CA and existing transcript/private-state mount paths. No bearer, approval
code, refresh token, private key or harness credential is an input argument or
environment variable.

The authorized fixture operator runs it through a **private exec pipe**, not
the pod's logging command and not a TTY. For example, the existing local
fixture's exec transport invokes this argv inside the collector:

```text
/usr/local/bin/python3 -E -s -S /fixture/enroll.py \
  --runtime /opt/vitral/collector --consent-fd 1 --budget-seconds 90
```

FD1 here is the dedicated exec response pipe owned by the fixture controller,
not shared container logs. A regular file, socket, input-only FD or TTY refuses.
The controller must keep it access-controlled and not mirror its records to
shared logs. The writer is nonblocking/bounded; peer loss or backpressure fails
without reissuing enrollment. The overall budget is 1-120 seconds on BOOTTIME;
the same immutable deadline is enforced after TLS and at HTTPS write/acceptance
boundaries by the existing transport. TERM/INT aborts, with no state cleanup or
new deadline. The controller also bounds/reaps its exec process.

Cancellation is sticky: it never starts another pipe wait for a failure notice.
A failed rendezvous is not retried. Other failures make at most one immediate
nonblocking reporting attempt within the original cap; a full pipe or elapsed
cap can therefore mean no final record. Both consent/result writes, callbacks
and exit0 acceptance recheck the original deadline after their work. Bytes
already copied to a pipe cannot be undone: a late ready record without bounded
exit0 is incomplete, not success. An already approved/uncertain private journal
is retained unchanged; a pipe failure never deletes or reissues credentials.

The script creates only collector-owned `0700` STATE/identity and STATE/auth
children, with existing private key/token journal modes. It does not chmod the
mount root, read transcripts, import host state, or touch other replicas. The
key is generated and used inside this replica's volume. The collector runner
later consumes that same volume directly; no journal/key/token is exported or
copied to the controller, another replica, or a harness.

This is one-shot: any existing auth journal, including ready/pending/uncertain,
refuses a new `begin`. It never clears, repairs, rotates, re-enrolls or retries a
lost authority result. A missing public rendezvous after a possibly sent request
is uncertainty, not permission to reset the volume. Ordinary reuse/refresh of an
already approved binding belongs to the existing collector, not this adapter.

## Public rendezvous and existing W7 consent fixture

At most one consent record is sent before polling. Exact keys:

```json
{"v":1,"kind":"consent","user_code":"ABCD-EFGH","verification_uri":"https://identity.fixture.test/native-auth/v1/device","expires_in":90,"interval":5,"metadata_sha256":"<64 hex>","provider":"codex","profile":"codex-0.169.0-text-v1"}
```

No device_code, nonce, proof, JWK/private key, token, account email or raw server
response crosses this channel. Unknown consent fields refuse. The only final
record is `{"v":1,"kind":"result","state":"ready"}` or a fixed failure
state (`pending`, `denied`, `expired`, `existing_state`); stderr stays empty.
Deadline/transport/cancel uncertainty remains pending, never denial or expiry.
An absent final record/failed exec is also incomplete, never success.

Env7's existing synthetic W7 consent fixture at backend source
`2cc9b21fe09199817a8025f6a468d69e02d4748a` provides the real counterpart seam:
`scripts/fixtures/workload-joined/test_joined.py` queues the successful
device-authorization response server-side, then uses its existing
`approve(identity, owner, grant)` and `due(identity, grant)` helpers.
Replace only the fixture's host-local enrollment call with the private replica
exec above. The controller matches the public user_code, verification URI,
metadata digest and profile to its expected replica and the server-side queue,
then uses that existing synthetic consent helper. The controller does not
forward a grant/device_code over the rendezvous or copy approval state into the
replica. The replica's native W7 HTTPS client receives and persists its own
device_code directly in its private volume as usual. No auto-approval against a
real identity is implemented or authorized.
Wait for the ready result and exec exit0 before normal fixture collection.

Env4 supplies the pod-reachable single origin and CA/SAN, exact reviewed image
manifest reference, independently mounted per-replica state and separate
producer-stop/EOF/fresh-content-and-view-ACK composition. The adapter does not
invent an external stop witness, make bare watch complete, change Helm/ingress,
or qualify real-harness/kind/ACA/A8. Fixture script and runtime/image are
separate source/artifact pins; do not silently substitute the source-mode
fixture's exact CSL pin or copy its host-created credentials into a pod.

## Evidence

Stdlib tests cover real subprocess/TLS/OpenSSL Ed25519 proof verification,
public-pipe schema/canaries, private volume modes and consumption, separate
replica keys, denial, deadline/TERM, failed rendezvous and lost-ACK/restart.
The local TLS peer **models** W7 and uses synthetic unsigned token claims; it
does not qualify real W7 consent, Mongo authority, registry, browser or a pod.
Env7's actual service fixture and env4's exact image/kind composition must return
separate exact-pin evidence. No live account, provider call, credential copying,
image publication, cloud operation or guest daemon/privilege workaround occurs.

For source tests, `VITRAL_FIXTURE_CSL_RUNTIME=/clean/exact/csl-checkout` selects
the separate runtime for the real enrollment/consumer subprocess cases. The
adapter itself stays at this contribution's pin. Default tests use the current
source runtime. Current main includes #50 shared history-helper changes since
image pin e00fd7ee, so do not infer full-tree/image identity from the default
source test. Exact e00fd7ee process compatibility is tested separately; it is
still not an execution of the OCI image or the actual W7 service fixture.

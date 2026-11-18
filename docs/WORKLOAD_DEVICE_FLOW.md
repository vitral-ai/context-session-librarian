# Workload Device And Refresh Journal

Internal source-only client following the Collector Anywhere Phase 1 freeze.
There is no command, image entry point, automatic login, registry upload or
activation. Desktop PKCE, raw sync and their defaults are unchanged.

`WorkloadTokens` uses the merged exact-origin transport and Ed25519 proof helper.
Its modeled counterpart is vitralai PR #672 at
`16d61f9a5a2261594ee2f6d9921516352646b9b4`: device authorization and token handlers
in `fastapi-backend/native_identity/device.py` and
`fastapi-backend/routers/native_auth.py`. That is a source contract pin, not a
deployed or merged counterpart release. Server workload identity, registry
namespace enforcement, same-origin ingress, UI and image/helper qualification
remain independent dependencies. This client cannot qualify them or A8.

## One Replica, One Private State

Pass a dedicated collector-created private child beneath the mounted volume,
not `/collector-state` itself. The owned auth child must be 0700; existing
permissive directories refuse without chmod. The default key lives in that
child's own `identity` directory. No harness home, authentication store or
transcript discovery is performed. The volume root is never modified.

The 0600 journal is atomically replaced and fsynced, including its directory.
Readers validate a nonblocking/no-follow opened descriptor: owned, regular,
single link and at most 64 KiB. Malformed or foreign configuration refuses.
An exclusive private lock serializes all journal and credential effects. The
journal contains credentials intentionally: it belongs only to this approved
replica. Never copy it into another replica, reports, logs or packages.

The immutable local binding includes the fixed public client, canonical HTTPS
origin and canonical workload metadata digest. A server-supplied account and
device are bound on the first successful exchange; refresh cannot change them.
The key thumbprint must still match before effects, cached-token use and token
acceptance. A changed replica/location/key refuses rather than enrolling anew.
Replacement replicas require fresh person approval and separate private state.
There is no automatic state reset, key repair, reapproval or credential fallback.

## Device Approval

`begin(timeout=600)` checks fixed discovery, initializes only its own key and
persists `authorization_inflight` BEFORE the authorization POST. It requests
only the frozen six-field workload metadata, fixed client and registry scopes.
The response must echo identical metadata/digest, canonical device code/nonce,
bounded interval/expiry, and the exact issuer `/device` verification URL.
Only the human-facing code, verification URI and timing are returned to the
caller. The later runner must expose those through the approved access-controlled
operator interaction, never shared logs or an automatically opened browser.

`poll()` performs at most one token request, only when due. Pending and slowdown
responses preserve the approval and advance the polling interval. Every request
carries the existing W7 JWK/proof fields; no owner/account can be selected by
metadata. A completed token response binds the approving account/device.
Lost authorization or redemption responses remain durably uncertain; device
redemption has no replay grace and is never repeated automatically. A known
denial is terminal. Approval expiry is recorded as `expired`, not revocation.

Linux CLOCK_BOOTTIME supplies suspend-inclusive timing. Its boot identity is
persisted; pending approvals do not resume across a reboot. Bounds are checked
again after helper work and responses, immediately before effects/publication.
No sleep is performed inside `poll`; the future supervisor owns scheduling.

## Token Contract And Refresh

The authenticated issuer's HTTPS response supplies the credentials and account.
The client checks response scope, session/device equality, and JWT claim
consistency: RS256 envelope, issuer, fixed client, only the registry audience,
scope, subject, session/device, key thumbprint, native-access use and expiry.
This is NOT independent JWT signature verification or authorization. The
registry must verify signature and current native authority on every operation.
No self-minted token, unverified discovery destination or opaque auth fallback
is accepted in place of the issuer exchange. The tests' token signatures are
synthetic placeholders, not a claim of real server admission.

Cached tokens need both boot-clock and wall-clock validity, with a 30-second
margin. Suspend cannot extend their life. A reboot requires a fresh proof-based
refresh instead of reusing an old boot-clock deadline. Helper/response latency
never extends expiry; acceptance checks again after helper work and journal write.

Refresh obtains the existing `/host_refresh_challenge` nonce, signs the frozen
workload proof and durably persists `refresh_inflight` plus the EXACT request
BEFORE consuming the rotating credential. Generic HTTP retries are disabled.
On a lost response, cancellation or successor-persistence failure, the journal
remains inflight. A reopened caller may replay the exact request ONCE, within
30 seconds on the same boot, after durably marking that replay as used. It
never obtains a new nonce/proof for an ambiguous exchange. An expired, already
spent or cross-boot replay stays `refresh_outcome_unknown`. There is no silent
re-enrollment or rollback to the old ready state.

Proven `invalid_grant`, `session_revoked` or `access_denied` at challenge or
exchange clears credentials into a terminal `revoked` marker. Local sign-out
does the same without claiming remote revocation. Denials do not become expiry
and expiry is not evidence that the authority revoked a session. Remote account
erasure remains enforced by the existing issuer/registry, not this local cache.

Only the workload HTTP subclass recognizes device OAuth pending/slowdown error
codes. Desktop HTTP error selection remains unchanged. Exceptions expose fixed
codes, never bodies, credential values or helper output. Status exposes phase
and non-secret account/device IDs only.

## Evidence And Limits

Synthetic tests exercise proof arguments, approvals, delays, metadata/key/account/
device/audience mismatch, pending/slowdown, loss/restart and exact replay, disk
failure, cancellation, concurrent initialization, suspend/reboot, revocation,
private modes and special/hard-linked journal refusal. An owned loopback TLS
fixture exercises the real transport and OpenSSL helper, including OAuth error
parsing. It models the frozen server, not a real W7/Mongo approval or registry.

No real credential, Google login, provider invocation, native harness, live
device approval/refresh, UI, image, cluster, cloud or spend is used. Actual
server/client integration and helper/image pinning remain to qualify. There is
no process wrapper, live watching or final-exit delivery claim: the subsequent
runner still needs a 20-second suspend-inclusive ACK-based final drain and
truthful pending/incomplete outcomes.

The separate [runner source](WORKLOAD_RUNNER.md) now exercises that lifecycle
with modeled local fixtures; image/external-producer and A8 gates remain.

# Local A8 Body Observation Preparation

Default OFF. This source-local contribution is not a commissioned collector run,
published image, real registry/identity test, A8 PASS, G4/GO or rollout approval.
It builds on the verified public env7 prototype bundle SHA256
`a2d4da4a7b39b28047d53c4f658b8d317585916a339fa2b4b1b049f0f99b12f0`.
That pure prototype was not itself a worker hook or live receipt.

## Selection And Boundary

The explicit source configuration option `VITRAL_COLLECTOR_OBSERVE_SESSION`
accepts one canonical UUID native session in the already selected provider and
approved replica. Absence leaves the old configuration and results unchanged.
It is not a grant: normal account/device/owner binding must pass before the
worker constructs an observer. The runner's existing private configuration file
transmits the choice to each real PassProcess worker. No origin/session/secret
is added to worker argv or its minimal environment. The instrumentation option
does not change account scope, identity, redaction, transport or ACK authority.

The hook sees the exact immutable `bytes` object on each constructed urllib
Request, immediately before the existing opener. Workload origin/header checks
still precede it. Only selected snapshot PUT, content PUT and view PUT bodies
are scanned. Device writes, identity endpoints, authentication, GETs and headers
are not observed. There is no extra request, proxy, TLS override, body log,
credential lookup, snapshot replay, changed retry or renewed deadline. Existing
idempotency digests bind JSON plans without a second serialization for auditing.
Snapshot bytes include metadata and the unavailable-package descriptor.

Content coverage advances only after the existing uploader validates revision,
length and cumulative digest and persists its ACK checkpoint. View coverage
advances after the existing successful page response and persisted generation/
sequence. A transport success alone is not matched evidence. Only after fresh
ProofRegistry file inventory/local journal checks and its exact final snapshot,
view generation/last sequence and authoritative sync.complete response pass are
the canonical IDs eligible for a receipt. The worker's existing post-ACK source
EOF and deadline checks still gate its result; observer data never supplies them.

## Closed Private Result

An enabled worker's existing private result may contain `observation: null`, or
exactly snapshot/content/view entries, each with `canary_present` boolean and
canonical `matched_identity`. Snapshot, revision and generation IDs come from
the validated plan and authoritative final proof, not arbitrary server text.
Malformed evidence or a failed worker becomes null at the receipt boundary.
Runner's public stdout/result does not include observation, body, routing,
offset, response, auth or failure detail. No extra durable ACK store is added.
Existing private worker-result lifetime/removal remains unchanged; an external
driver consuming that boundary and independently checking its run selection is
separate reviewed integration work. No public receipt CLI/server is introduced.

The fixed public synthetic canary is a literal pattern detector, not a general
secret detector or a replacement for workload redaction. All three surfaces
must be observed and matched before any non-null result, even when booleans are
false. The external acceptance driver must still select the expected identities
and require complete evidence; absence/null can never become a clean claim.

## Conservative Scope And Bounds

One session, one file revision, within ONE worker pass, from content byte zero
and view sequence one. Each body is at most 8 MiB; at most 1024 attempts and
128 MiB cumulative observed bytes. Cross-chunk content matching retains only a
prefix of the fixed public canary plus a hash state, never arbitrary body bytes.
Plans, routing, ranges and hash state stay private. Unsupported routes/shapes,
changed plans/metadata, oversized bodies, exhausted bounds, missing callbacks,
incomplete/unmatched ACKs, failed/retried attempts, lost responses or final proof
loss poison evidence without changing the network call or product result.

No observation authority crosses workers or restarts. Reused remote content,
nonzero resume ranges and already-delivered or empty views have missing surface
coverage and remain UNQUALIFIED. In particular a final worker after a live pass
will often have no content/view writes and cannot qualify those prior uploads.
The current source intentionally does NOT solve durable multi-pass observation,
lost-response reconciliation or earlier-range coverage. It must not borrow prior
ACK digests or replay uploads to manufacture evidence. A later reviewed evidence
composition would be needed for that broader acceptance lane.

Generic image source inventory includes the new module. Existing pinned #60/#61
artifacts do not contain it; no receipt or pin is silently substituted. Future
image builds, overlays, native harness turns, consent, registry/UI joining and
execution remain separately commissioned. Validation here is in-process source
units with synthetic bodies and mocked transport/process creation, not actual
worker/collector processes, enrollment, provider calls or containers.

# Private Multi-Pass Observation Source

This source extends merged64's immutable serialized Request-body hook through
the actual parent/pass-worker/private-result lifecycle. It is default OFF and
has no CLI, environment activation, listener, credential import or extra network
operation. Source tests use owned synthetic TLS/process fixtures, not an actual
account, W7 consent, model, collector deployment, OCI build or A8 run. Existing
qualified image receipts do not contain this code.

## Driver Contract

Acceptance owners may import `AcceptanceDriver` from
`csl_workload_observation_driver`. Its explicit constructor requires independently
reviewed `source` (runtime content fingerprint), `scope` (existing runner scope),
`account_id`, `device_id` and selected canonical `session`. These expectations
must not come from the receipt being checked. `runtime_fingerprint()` hashes the
explicit module/data inventory; an assembled runtime verifies its existing
source manifest first. A Git commit ID is a separate provenance fact, not this
content fingerprint.

Source-only integration example (not authorization to run an actual collector):

```python
driver = AcceptanceDriver(source=reviewed_fingerprint, scope=scope(config),
    account_id=expected_account, device_id=expected_device, session=expected_session)
result = Runner(config, observation_driver=driver).run(literal_producer_argv)
evidence = driver.result
```

The existing explicit observation session configuration must agree. The parent
checks the existing private workload status without begin/poll/refresh, generates
a fresh run UUID and independently assigns each actual worker UUID and seal.
No receipt chooses the reader's expected nonce. The parent retains these facts
in memory, including the original worker deadline and an unchanged one-file plan
read after a completed live pass from the existing product ACK journal (read-only,
zero SQLite busy timeout, bounded query). A cold final-only pass or plan changed
during the final pass is conservatively unqualified. No replay/reupload is added
to make it qualify. Assignment uses a bounded4096-byte nonblocking private pipe;
only its FD number enters argv. No evidence-store fsync or SQLite read is added
to the parent's final-drain budget. New worker admission/intent/seal writes stay
inside the existing killable worker. The parent also retains the exact predecessor
record hash; an older durable record cannot silently replace that anchor.

Only the actual successful final worker, after the existing producer-quiescence
and completion/deadline checks, reaches the private reader. Consumption happens
once, before the existing receipt cleanup. The reader requires exact current
run/worker/seal/scope/context, retained plan and original result status. Its result
is a copied map of exactly `snapshot`, `content`, `view`, each containing only
`canary_present` (boolean) and `matched_identity` (canonical snapshot/revision/
generation UUID). There is no body, route, offset, header, credential or failure
detail in this result or in Runner's unchanged public output.

`None` means unqualified, not clean. A non-null result is detection evidence, NOT
an A8 PASS. A true canary flag is a detection, not success. External acceptance
must separately require its expected surfaces/identities, real producer stop,
EOF, fresh content AND view proof, timing and every other original acceptance
condition. No caller should expose `Evidence` internals as a substitute reader.

## Durable Coverage

`STATE/observation` is private0700 without changing the mount root. Records/locks
are owned0600 regular single-link no-follow files. Each bounded compact record
contains hashes, counters, IDs, flags and at most a public canary-prefix length,
never arbitrary payload bytes. Creation refuses beyond 126 existing entries;
old runs are not automatically deleted or adopted. One run permits at most1024
attempts, 8MiB per body and 128MiB cumulative bodies.

The exact actual Request bytes are scanned and a durable intent is persisted
before dispatch. Failure to store intent never blocks product delivery; it
invalidates evidence. Only existing checked AND persisted content/view ACK hooks
advance coverage. Content must start at zero; resumed ranges require equality
with the previous observed-and-ACKed prefix hash. Remote reconciliation cannot
adopt an unobserved prefix. Cross-chunk/worker matching retains only a prefix of
the fixed public canary. New revisions require paired generations and zero
coverage. Detection flags remain sticky. Unsupported routes/size, retries, lost
ACKs, interrupted intents and missing live-worker evidence stay unqualified.

Final sealing runs inside the killable worker only AFTER existing authoritative
final proof AND source-manifest EOF checks, with the SAME BOOTTIME deadline
checked before/after storage. Seals are not product ACKs. Any later storage error,
worker loss, cancellation, deadline or mismatch returns no qualifying result.
Every run entry/worker assignment clears the parent's old result before I/O.
An old worker/run seal cannot be adopted after journal/receipt rollback.

## Trust And Limits

This is freshness binding within the existing trusted same-user boundary, NOT
cryptographic tamper resistance. Another process with that UID can read current
nonces, modify product/evidence state or forge a current receipt; same-worker
whole-state rollback or in-process mutation is not detectably prevented. Tests
explicitly demonstrate this limit as well as rejected prior-worker rollback.
No stronger claim is made from file modes or Git cleanliness.

The supported composition is one selected session/file with an unchanged final
plan and completed live-worker observations within one parent run. Restart does
not adopt prior-run coverage. Empty/missing views remain unqualified. Observing
bytes passed to urllib does not prove a failed request arrived intact. Real
three-provider acceptance, packaged driver launch, actual image/container/Pod
composition, account/device approval and A8 remain separate commissions.

# Workload Identity Primitives

Source-only building blocks for the frozen Collector Anywhere Phase 1 contract.
They do not register a client, enroll a device, approve a person, issue/refresh
tokens, publish transcripts, launch a process or activate an image. The desktop
PKCE collector remains unchanged. There is no CLI entry point in this PR.

## Exact-Origin HTTPS

`csl_workload_http.Origin` accepts one canonical HTTPS origin without userinfo,
path, query or fragment. Every `WorkloadHTTP` request, regardless of method,
must match its scheme, host and effective port before the opener is called.
Only `/native-auth/v1/` and `/session-registry/v1/` namespaces are usable; path
traversal, malformed aliases and `Host`/proxy-header overrides refuse. HTTPS
default port 443 and an explicitly spelled 443 denote the same port.

The HTTP wrapper reuses the existing bounded response/error/retry implementation.
Redirects are never followed, ambient proxies are disabled, and TLS certificate
and hostname verification stay enabled. An optional public CA file supports the
owned local fixture. Discovery checks exact issuer and fixed endpoint locations,
device/refresh grants, registry scopes and public-client support. Existing W7
discovery need not advertise the device or refresh-challenge endpoint: their
locations are fixed under the checked issuer. Off-origin discovery or an
endpoint override refuses rather than delegating machine traffic. The normal
human approval browser flow is separate; this module never opens that browser.

This establishes a client routing boundary, not a kernel/network egress policy,
an end-to-end authorization proof or a qualified global shutdown deadline. It
does not change registry ingress, DNS, TLS issuance or any deployed route.

## Workload Metadata And Proof

`csl_workload_identity` enforces the closed six-string metadata schema (`label`,
`provider`, `cluster`, `namespace`, `workload`, `replica`). Identifiers are 1-128
ASCII letters/digits or `._-:/`; provider is lowercase. Label is trimmed,
printable UTF-8 up to 128 bytes. Sorted compact UTF-8 JSON is hashed with SHA-256.
No owner, account, credential or permissions can be supplied in metadata.
Known CSL readable-text secret patterns also refuse in any metadata field,
before future enrollment serialization. This fail-closed check is partial,
not a claim to detect arbitrary encoded or unknown secrets.

Proof bytes reuse W7 LP-v1 domains and field order: issuer, the fixed public
client `vitral-workload-collector`, SHA-256 of the credential, nonce, JWK
thumbprint and canonical metadata digest. Each field has a big-endian 32-bit
length prefix; device and refresh domains are distinct. The existing
`vitral-host-key-proof-v1` / `vitral-host-refresh-proof-v1` domains are intentional:
the client ID and metadata digest separate workload approval from host approval.
Canonical unpadded base64url and fixed-size public key/nonce/digest are enforced.
The registry audience is `vitral-session-registry`; requested scopes are
`session_sync offline_access`. Actual server/device/account binding and audience
checks belong to the subsequent enrollment/token state machine, not metadata.

## Optional Crypto Helper

The core imports only the standard library. Ed25519 is performed by the optional
OpenSSL executable, never a custom crypto implementation. This PR qualifies
OpenSSL 3.5.5 on Linux with synthetic fixtures; other versions/platforms remain
unqualified. A future workload image must contain its reviewed/pinned helper.
There is no runtime download or installation. Missing/failed helpers refuse.

`WorkloadKey` owns a dedicated private directory and its generated Ed25519 key.
The caller must hold the existing exclusive collector lock. An existing root
must already be owned 0700; unrelated/shared directories are not chmodded. The
key is owned 0600, regular and no-follow. Initialization is exclusive and fsynced;
restart reuses the same key. A malformed, permissive, symlinked or mismatched key
refuses; there is no automatic rotation, replacement or import from harness
credential stores. Each replica needs independent state and person approval.
Key admission opens nonblocking, then checks the opened descriptor for regular
type, one link, owner, private mode and bounded size. FIFO/special files cannot
hold the collector lock waiting for a writer. A hard-linked key refuses in both
replica roots, without unlinking, rotating or modifying either link.

`VITRAL_COLLECTOR_STATE` is the per-replica volume root, not the key directory.
The runner must create a private child such as `/collector-state/identity` and
leave the kubelet/fsGroup-managed mount root untouched. Tests qualify an owned
0770 parent with a 0700 key child and 0600 key. The runner must also pass
`VITRAL_COLLECTOR_TRUST_CA`, when present, to `WorkloadHTTP(ca_file=...)`; hostname
validation remains enabled. This is a source integration contract, not evidence
of a deployed image or in-pod run.

OpenSSL gets a minimal environment, no inherited cloud/harness credentials or
config overrides, no password prompt, captured output and suppressed stderr.
Secret bytes never enter argv. Proof input contains a credential digest, not the
credential. Temporary proof payloads are private and removed on failure. Helper
timeouts are scrubbed without retaining the subprocess exception. The private
key is deliberately persisted locally; this is not a claim that process memory
is secret-free or that a malicious same-user process cannot read owned state.

## Qualification And Remaining Work

`tests/test_workload_identity.py` uses the public RFC 8032 test seed shared by the
W7 vectors. Independent payload/signature expectations were constructed with
cryptography 45.0.3; tests run with site packages disabled and use OpenSSL instead.
The original public `tests/fixtures/native-workload-key-proof-v1.json` bytes are
now included and independently verified as SHA-256
`552c7d6957173b9ef219da3a7f2459b6400d663d012cdd6ac21994691539bb77`.
Tests compare canonical metadata/JWK, exact device/refresh LP payload bytes and
actual OpenSSL signatures against the original fixture. Its public RFC seed is
test data, never a live credential or workload identity.

`tests/test_workload_http.py` uses an owned loopback HTTPS server and public test
CA. It proves GET/PUT path/header/body preservation, wrong-CA/hostname refusal,
off-origin refusal before any opener call, no redirect follow and strict
discovery. No live identity API, registry, Google account or harness is contacted.

Subsequent work supplies person-approved device polling, echoed-metadata checks,
immutable account/device binding, refresh rotation/uncertainty journal, revocation
handling and the opt-in workload runner. This PR makes no 20-second final-flush,
five-second visibility, workload-image or live account qualification claim.

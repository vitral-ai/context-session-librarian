# Optional Codex sole-producer image

Source-only, Codex 0.169.0 first. This is NOT the default collector/watch chart,
a published artifact, login, model turn, deployment or A8 result. The generic
collector and all of its product modules stay unchanged. Env4 owns Job wiring
and actual execution, env7 owns acceptance, and root commissions live actions.
Claude/Gemini require separate artifact, normal-login and native qualifications.

## Immutable inputs and offline assembly

`inputs.json` records every public URL, compressed size, SHA-256, exact archive
member inventory and selected output. Root downloads ONLY those eight public
inputs into a fresh directory outside any checkout/home/STATE. No native binary
was downloaded or executed on the author guest. Sources:

- [Codex release](https://github.com/openai/codex/releases/tag/rust-v0.169.0),
  tag resolving to `d32063f96c2dff1dc96af839b3952abb6acfeda4`.
- Its [release archive construction](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/.github/workflows/rust-release.yml)
  and [package layout](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/scripts/codex_package/layout.py).
  Keep the actual executor, code-mode host and bwrap, not a lone main executable.
- The [pinned ripgrep input](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/scripts/codex_package/rg)
  and [its release inventory](https://github.com/BurntSushi/ripgrep/blob/e89fff89ac9af12e8d4ce9d5fd07beb408ca730f/.github/workflows/release.yml#L214).
- Preserve upstream Codex LICENSE/NOTICE, vendored bubblewrap LICENSE and
  ripgrep COPYING/LICENSE-MIT/UNLICENSE under `/opt/vitral/codex/licenses`.
  Corresponding sources remain pinned above; no redistribution-compliance or
  support claim for optional voice/app-server features is made here.

The pinned bubblewrap `LICENSE` Git entry is a symlink containing `COPYING`.
Its raw URL returns those seven link bytes, even though GitHub's Contents API
may dereference it. Download the regular `COPYING` path at the SAME commit;
the expected 25383-byte license SHA remains unchanged. No fallback/repin.

Before sending the named public-input context to BuildKit:

```sh
python3 -B -I packaging/codex-sole-producer/install.py --check-only "$PUBLIC_INPUTS"
```

Fail fast on missing/extra/nonregular/symlink/hardlinked files, wrong size or
digest. The installer repeats these descriptor-first checks before any output
or extraction. No download or shell execution exists in the installer. Gzip
expansion is capped at 512 MiB per archive, total member sizes at 768 MiB,
metadata reads at 64 KiB and inventory at 32 members. Reject unlisted/duplicate/
missing members, traversal, links, devices, FIFOs, sparse/PAX records and non-
amd64 ELF payloads. Manually copy only closed selected files; no `extractall`.
Assembly uses a private staging tree and does not replace an existing output.
After the full closed inventory is validated, copy each regular payload by
its parsed offset/size from the bounded decompressed spool, in <=64 KiB chunks
with range/EOF checks. Payload buffering never uses the capped metadata reader.
Root's actual target-Python 3.12 manual assembly passed before this separation;
host Python 3.14's `ExFileObject` readinto requested 128 KiB of ordinary payload
through that reader. This separation retains the 64 KiB metadata cap and
oversized-PAX refusal; it does not claim blanket Python 3.14 runtime/image/native
qualification.
License text is verified and retained verbatim; unused reviewed rg docs are
validated but not installed. `native-manifest.json` records assembled file hashes.

The base is the already inspected root-local #60 artifact, NOT a pullable
registry repository. Convert its verified archive into an owned OCI layout
with the existing operator tools, retaining all blob digests. Select the actual
amd64 manifest, not a mutable tag or image config ID. BuildKit's documented
[named OCI context](https://docs.docker.com/reference/cli/docker/buildx/build/#use-an-oci-layout-directory-as-build-context)
supports this without a registry push:

```sh
python3 -B -I packaging/codex-sole-producer/build.py \
  "$BASE_LAYOUT" "$PUBLIC_INPUTS" "$EVIDENCE/codex-sole-producer.tar"
```

Run from the exact clean reviewed full source SHA, with owner-private EVIDENCE
outside the checkout. The wrapper verifies the actual pinned manifest/config
JSON by descriptor-first bounded reads, platform and public inputs before any
Docker call. BuildKit verifies the selected descriptor's referenced layers. It
refuses existing archive/metadata outputs, records the full source revision and
uses the fixed manifest URI, never a caller-supplied parent pin. Paths must be
canonical absolute ASCII letters/digits/slash/dot/underscore/hyphen. Do NOT
bypass this wrapper, build without both named contexts or replace
`reviewed_collector` with an arbitrary image/path/tag. The index receipt is
`sha256:be53797b9bbc0d5e807fe88e0ba606bdba2fb50eccfa71a834d5232af61eec80`;
the selected manifest above is the reviewed Linux/amd64 image. No ARG accepts a
moving parent. The recipe's default context is deny-by-default with recursive
descendant exclusions and explicit source files only. Existing image CI runs
13 additional real scratch COPY admission cases for this filter; it does NOT
build this unpublished-parent/native overlay. Author guest has no Docker daemon.

Root actual build/inspection must record source SHA, input lock, parent/output
manifest/config/index identities, archive hash, platform, native manifest and
`codex --version`. Compare base runtime-manifest, launcher, helper and check
script bytes unchanged against #60. Compare prepare.py/launcher bytes to this
source. Verify licenses, ELF inventory/modes and empty image-owned UID10001
native home. Version checks run network-off; helper presence/version alone is
not actual sandbox, sign-in, model, native-format or Kubernetes qualification.
No runtime installers, sandbox-disabling flags, capabilities or privilege
workarounds are introduced. Preserve e00 and #60 receipts separately.

## Public Job interface for env4

Use a separate opt-in one-container Job, not the sidecar chart's writer wrapper:

- Command: `["/usr/local/bin/csl-codex-prepare"]`.
- Args: `["--prepare-seconds", "900", "--", <literal Codex exec arguments>]`.
  Preparation budget is 1..1800 seconds, immutable CLOCK_BOOTTIME. At least one
  native argument is required; max 128 arguments / 64 KiB UTF-8. No shell,
  interpolation, command selection, retry or automatic resume is performed.
- Fixed `HOME=/agent-private`, `CODEX_HOME=/agent-private/.codex` are image-owned
  0700 directories on a bounded writable ephemeral layer, NOT mounted login
  homes. Never copy credentials from a host or another container/replica.
- ONE dedicated transcript volume mounted RW at
  `/agent-private/.codex/sessions` and RO at `/transcripts`. The gate checks
  matching device/inode plus opposite read-only mount flags, and requires an
  EMPTY source at preparation and immediately before the actual native exec.
- Separate private W7 STATE mounted at `/collector-state`; preserve its root
  mode and independent per-replica person-approved identity. Same existing
  `VITRAL_COLLECTOR_*` / `VITRAL_LOCATION_*` ABI, exact codex/profile, configured
  HTTPS origin and CA. Do not forward harness credentials in env/argv/logs.
- Run UID/GID10001, non-root/no-new-privileges/cap-drop, private PID namespace,
  no host/shared PID, socket, service account, RBAC, listener or SSH. Explicit
  CPU/memory/PID/ephemeral-storage/volume bounds, restartPolicy Never and
  backoffLimit 0. The native private writable layer is an explicit difference
  from the generic read-only smoke, not a weakened default chart.
- Supply an ordinary reviewed public Git project as the native working directory
  (and literal `--cd` argument when needed), not the collector code directory or
  a host home. Do not fake Git metadata or inject trust/sandbox bypass flags.
  This overlay is not a general development-tools image or a Git/tool readiness
  qualification; normal missing prerequisites remain visible failures.

Gate validation inspects directory/mount metadata and emptiness only, never
native auth/config contents. Same-UID co-location is a shared trust domain in
BOTH directions: separate volumes do not prevent a malicious native process or
collector from reading the other's files. No kernel credential isolation is
claimed; reviewed collector readers still use only /transcripts
and the real pass worker has its own minimal env/HOME. Do not mount or export
the enclosing native home as collector input or evidence.
Collector HTTP still enforces its exact configured origin. Native sign-in/model
traffic has separate provider destinations; this profile does not claim an
entire co-located Pod can use a one-hostname network policy. Env4 records those
distinct egress requirements without weakening the collector client.

## Preparation and actual completion

PID1 blocks USR1/TERM/INT before validation; it waits silently. Through the
existing approved PRIVATE exec TTY, the operator performs the native CLI's
normal independent sign-in inside that container and the existing W7 replica
flow. Never use the Pod's primary stdout/logs for sign-in. No automation bypass,
native credential parser or fixture autoapproval is added. Before the one start
signal, inspect PID1's fixed signal-mask readiness through that same private
operator route: `/proc/1/status` SigBlk must include 0x4202 (USR1/TERM/INT).
This is readiness only, never authority or completion proof. Do not blindly
retry a missing handoff. No real sign-in/model/Pod action is commissioned by
this source procedure.

One operator USR1 starts the declared scenario. Pending TERM/INT wins over USR1;
repeated USR1 cannot renew the deadline or launch another command. Revalidate
fresh input and deadline at handoff. PID1 calls the UNCHANGED Runner.main in the
same interpreter, with cancellation installed until Runner installs its own
handlers: there is no interpreter-exec TERM gap or base runtime patch.

The existing guardian owns a fixed exec-only native shim in the same process
group. That shim rechecks the SAME original deadline, layout and empty source,
then `execve`s `/opt/vitral/codex/bin/codex exec` with unchanged native argument
bytes. It never creates a transcript, spawns another writer, returns success
instead of exec, or uses an old source. It refuses PID1 invocation. Thus the
actual Codex replaces that owned PID; no inert/dummy producer supplies proof.
Prep refusal/timeout is silent exit 2, TERM 143, INT 130; no completion report.
After handoff the existing Runner alone emits its existing sanitized result
and exit semantics. Preparation budget constrains native launch, not the model
run's duration; the commissioned Job supplies that separate bound.
Delivery completion is not model success: acceptance also requires the actual
native version, writer exit status and every independently expected marker.

Observe normal native complete records, the actual guardian's durable stop,
then complete EOF and fresh distinct content/view ACKs for that exact capture
within Runner's original at-most-20s BOOTTIME drain. Reserve >=30s plus actual
writer-stop/flush/overhead. Unknown writer/partial EOF/outage/ambiguous or stale
ACK is pending; never renew drain, relaunch argv or reset STATE to compensate.
Use the previously reviewed public marker inventory and synthetic canary only
after a real turn is commissioned. Existing bare-watch runs remain strictly
pending. Source tests, synthetic process fixtures, native/container/kind/cloud
and A8 acceptance are separate receipts, not interchangeable passes.

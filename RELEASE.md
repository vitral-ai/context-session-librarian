# Release operation and published state

## Current provider scope

The current provider set is Codex, Claude Code and Antigravity CLI. Removed
providers are rejected, including in existing v3 packages; see [CHANGELOG](CHANGELOG.md).
The version records below refer to their original published tags, which are unchanged.

## Version records

### v0.5.9 — managed Codex source isolation release candidate

Managed Codex collection isolates known-identity format refusals so supported
recordings upload alongside retained below-floor sources. Preserve the parser
floor, profile and recipe. Header provenance and duplicate native-ID checks stay
mandatory across admitted and refused sources. Refused sources remain pending;
a partial pass cannot certify a complete final flush.
Publish after independent release review and exact-head hosted/native checks.
Preserve collector profiles and authentication. For a retained tagged checkout,
run `git fetch --tags` and `git switch --detach v0.5.9` after publication, then
re-run `./install.sh` with the original prefix (`./install.sh PREFIX` for a custom
prefix). With that prefix's `bin` first on `PATH`, verify `csl --version` reports
0.5.9 and `csl-resume-action --help` succeeds. README has the current install steps.

### v0.5.8 — original-device in-place Resume release candidate

A fully synced keeper-managed recording at legacy generation 0 can acquire its
first writer and resume in place on its original attested device. Require exact
synced bytes and a positive PID/start-bound end receipt. Unmanaged recordings
return `resume_stop_proof_required`; other devices still require handoff.
Journal the request before fencing and acquiring generation 1 under the native
lock. Exact retries recover interrupted publication. After a lost launch response
and a newer snapshot, only an existing exact run journal plus the same active
writer and host readback-only live proof can recover; readback never launches.
Successful run returns the actual writer generation. The advanced-snapshot path
requires a host supporting `readback_only:true`; older hosts refuse safely.
Publish after independent release review and exact-head hosted/native checks.
Preserve collector profiles and authentication. For a retained tagged checkout,
run `git fetch --tags` and `git switch --detach v0.5.8` after publication, then
re-run `./install.sh` with the original prefix (`./install.sh PREFIX` for a custom
prefix). With that prefix's `bin` first on `PATH`, verify `csl --version` reports
0.5.8 and `csl-resume-action --help` succeeds. README has the current install steps.

### v0.5.7 — typed managed Resume adapter release candidate

Install `csl-resume-action --json-stdin` for bounded owner-local check, release,
import and run. Browser payloads select no paths or commands. The Linux managed
host must compare the exact canonical target, writer incarnation and collector
binding under the native lock before launch or replay (compatible host #903).
Existing Fork stays separate. Require a private personal collector binding and
owner-provisioned target; stock workload-only installations refuse. Local source
release still requires the exact positive managed process end receipt.
Publish after exact-head hosted/native checks and independent review. Preserve
profiles and authentication. Paired source/local keeper admission is verified;
DEV and real provider product-flow verification remain separate.

### v0.5.6 — installation attestation release candidate

Add `csl sync --home PROFILE attest-installation --json` for the owner-local
Fork destination check. Success exposes only the server-validated account and
device. Uncertain prior refreshes refuse under the OAuth lock; fresh refresh
uses the profile's existing policy. No transcript upload or writer mutation.
Publish after independent review and exact-head hosted/native validation.
Preserve existing profiles and authentication. Verify the immutable published
commit-addressed archive before handing its pin to the guided installer owner.

### v0.5.5 — Windows CLI startup release candidate

Load optional curses only for the interactive cockpit, falling back to plain
output when unavailable. Official CLI sync and import work with guided Windows
Python. Resolve npm provider shims through Node without shell expansion for
provider open and native-picker indexing. Windows CI imports all root CLI entry
points and checks startup without
curses. Publish after independent review, exact-head hosted/native validation,
and a Windows witness covering pull, fresh-ID import, provider open and Unicode
paths. Keep existing collector profiles and authentication in place.

### v0.5.4 — Windows collector reliability release candidate

Choose one Python executable deterministically and use its matching pythonw.exe
for the independent, console-free Scheduled Task. Preserve registered provider
homes. Apply legacy-ID exclusions before history analysis. Collect verified
attached-thread lineage without offering unsupported native transfer; retain
per-session error isolation so healthy sessions upload beside malformed history.
Publish only after exact-head hosted/native checks, independent release review
and a synthetic Windows task witness on vitral-win.

### v0.5.3 — managed Codex compatibility release candidate

Keep the `codex-0.169.0-text-v1` profile and recipe identity while accepting stable
Codex versions at least 0.169.0 through the existing closed schema validator.
Unknown formats remain refused and short status explains the refusal. This does
not broaden the supported content schema or change Claude's version qualification.
Publish after independent review, exact-head hosted and native checks; verify the
published commit-addressed archive and a fresh installation before handing off pins.

### v0.5.2 — same-ID Resume release candidate

Select reviewed main including #139–#142: runtime version data, discovery-bound
refresh replay policy, interrupted-view finalization coverage and explicit registry
single-writer Resume. Default import remains Fork. Source release requires an exact
managed positive end receipt; a fresh destination retains the native ID and appends
to the same registry session after installation attestation and acquisition.
Direct --open and native history-base Resume remain refused. No universal native
continuation claim is made. Existing ambiguous refreshes still need fresh sign-in.

Publish only after independent release review, exact-head hosted checks and isolated
native validation. Record the final immutable archive URL, bytes and SHA-256 in the
GitHub Release and verify a fresh tagged installation. Existing authentication and
selection state remain in place; installing the source starts no service.

### v0.5.1 — collector UX release candidate

This patch selects reviewed main including #134: first-run upload choices,
short/verbose/JSON status, progress with background-service guidance, and explicit
sign-in recovery including headless callback settings. Existing authentication,
selections and uploaded sessions remain intact. It also includes reviewed native
validation and private FD9 fixture corrections (#135–#137).

Publish only after independent release review, exact-head hosted checks and
isolated native validation pass. Record the final commit, immutable source archive
URL, byte count and SHA-256 in the GitHub Release, then verify a fresh tagged
installation. Source distribution still requires Python 3.11+ and starts no service.
No Mac missing-session fix or new native continuation claim is included.

### v0.5.0 — published collector release

Published from fa6f8c7232ec6f7970d72a9cbdcd2d56ec52b8cf with collector #132.
The later #134 UX is included in v0.5.1, not this fixed v0.5.0 source.
The source version is 0.5.0. Publish an annotated tag only after exact-head hosted
checks and independent review pass. Record the final source SHA, immutable archive
URL, byte count and SHA-256 in the GitHub Release for the guided installer.
Distribution remains a source checkout/archive with Python 3.11+; no wheel,
package registry, bundled interpreter or automatic updater is introduced.
Branch-tracking development checkouts update with `git pull --ff-only`. Tagged
installs have a detached HEAD: update with `git fetch --tags` followed by
`git switch --detach v0.5.0` after publication. After either update, re-run
`./install.sh` with the same original prefix (`./install.sh PREFIX` for a custom
prefix) to install newly added launchers. Verify `csl --version` reports 0.5.0
with that prefix's `bin` first on `PATH`. See README for current release updates.
Keep local edits separately; do
not reset or discard them. Pinned guided installers need their owner to update
the archive pin. Downloading or installing
starts no service and preserves local authentication/selection state.

This release includes bounded collector passes, upload selection and short/JSON
status, session error isolation, approved agent communications and reviewed
Connect/workload changes. Raw sync remains experimental and uploads secrets
unchanged. Native loader evidence is version-specific; no universal macOS,
Windows or live continuation claim follows from source publication.

The release owner records exact hosted results and an isolated source install/demo
in the published notes. A laptop live-upload witness is separate: it needs that
installation's own valid DEV sign-in. Do not retry an ambiguous rotating refresh
or transfer credentials to manufacture this witness.


### v0.4.2

This release removes Gemini CLI support. Current native provider profiles are
Codex, Claude Code and Antigravity CLI. Commands selecting the removed provider
fail explicitly; existing v3 packages naming it are rejected before installation
or native launch. No automatic conversion or replacement is performed. Historical
tags and Release notes retain their original contents.

Run the full isolated regression suite, available offline native gates, isolated
installation and synthetic demo at the selected SHA. Publication is established
by the annotated tag and [v0.4.2 Release](https://github.com/vitral-ai/context-session-librarian/releases/tag/v0.4.2).
Mailbox and raw sync remain experimental; raw sync uploads secrets unchanged.
Updating the backup producer starts a fresh capture; finish pending runs with
their original version and keep historical recovery keys.

### v0.4.1

Patch release for raw-sync snapshot identity: metadata-only changes now receive
distinct identities, and canonical JSON requests support stable retries after
lost responses or process restart. Provider coverage and communication transports
are unchanged. The experimental status and privacy limits of v0.4.0 still apply:
raw sync uploads selected bytes, including secrets, unchanged.

Validate the fixed candidate with the full isolated suite and available offline
native gates, isolated installation and synthetic demo. Publication is established
by the annotated tag and [v0.4.1 Release](https://github.com/vitral-ai/context-session-librarian/releases/tag/v0.4.1),
not by this preparation record. No historical tag or Release is changed.
Updating the backup producer version starts a fresh capture; finish pending runs
with their original producer and retain historical recovery keys.

### v0.4.0

This version packages task-independent agent messaging, opt-in raw session sync,
multi-file native provider reads/packages and bounded Codex history-base graphs.
Mailbox and sync remain experimental. Mail persists independently from tasks;
ACK, recipient persistence and reply are separate facts. Optional hooks and
native notifications require explicit owner setup; installation grants no work
authority and does not start a service.

Sync has no default registry and requires explicit authentication/configuration.
It uploads selected raw transcripts and supported companion files **including
their secrets, unchanged**. It does not scrub them or apply backup encryption.
Incremental reuse and crash-contained foreground watch passes do not constitute
a durability SLA or model-continuity guarantee.

Local provider reads and raw v3 packages were added in this release. See the
[tagged provider matrix](https://github.com/vitral-ai/context-session-librarian/blob/v0.4.0/docs/PROVIDER_MATRIX.md)
for its historical provider set and limits.
Antigravity has bounded CLI 1.5.9 reconstruction and text-recall evidence, not
universal compatibility. Codex 0.169.0 bounded history-base import preserves
references, but cold-home `thread/read` needs explicit activation to reconstruct
the native projection cache. Import never activates a model to hide that step.
CSL 0.3.0 cannot import v3 packages.

Backup remains experimental/manual with its existing key and age requirements.
The new producer version starts a fresh capture; finish pending runs/recipient
plans with their original version and retain older checkpoints' recovery keys.
Historical tags, notes and checkpoints are not rewritten.

Validate this release's fixed candidate SHA with the full isolated suite and
available offline native gates, isolated prefix installation/help/capabilities
and the offline synthetic demo. The GitHub Release records final commit, counts,
skips, CLI versions and clean installation from the published annotated tag.
Preparation or a merged PR alone is not proof of publication. See
[v0.4.0](https://github.com/vitral-ai/context-session-librarian/releases/tag/v0.4.0),
[sync privacy](docs/SYNC.md), [provider limits](docs/SYNC_PROVIDERS.md) and
[communication](docs/AGENT_COMMUNICATION.md).

### v0.3.0

This version adds experimental live coordination of existing Codex conversations,
an optional bound MCP adapter and explicit client-only origins. The bridge does
not create a model, native runtime, SSH service or account. Clients and peers keep
their own permissions. Client-only callers receive events in tool results and do
not gain automatic idle wakeup.

Bounded real Linux–Mac LAN and separately authenticated Linux-container exercises
covered delegation, progress/final responses, interruption and continued context.
These checks are distinct from synthetic runtime tests and do not establish
Internet, Windows or live Claude–Codex compatibility. Native Mac app source and
packaging tools are included, without a validated binary or clean-install claim.
See [bridge compatibility and setup](docs/BRIDGE.md) and [app status](macos/README.md).

Conversion/import now reports recognized content, known omissions and structural
validation separately from native acceptance or live model recall. The native
import comparison explains which use cases require a conversation copy rather
than configuration migration.

Backup remains experimental, manual and Linux/Codex only, with the same external
age v1.3.2 and recovery-key requirements. Updating the producing CSL version starts
a fresh backup capture. Finish pending runs and recipient-change plans using their
original version before upgrading; older checkpoints retain their original keys.
Historical backup data, tags and Release notes are not rewritten.

The release owner must verify this candidate's exact SHA, checks, isolated install
and publication. This preparation record alone is not proof of availability;
the [v0.3.0 Release](https://github.com/vitral-ai/context-session-librarian/releases/tag/v0.3.0)
and its fixed tag establish the published version.

### v0.2.2 — release candidate

This patch improves finding and inspecting conversations, sharing guidance and
local secret review without changing provider coverage, native formats, IDs,
configuration/storage paths or the `csl` command.

- Installation prints the exact installed launcher's version and explains a
  different `csl` on PATH without running that other launcher.
- Team connection/status shows local-session and inbox next steps as appropriate
  to write/read permissions; it does not transfer anything automatically.
- `why` includes labeled inherited evidence in a selected session by default;
  global searches retain duplicate controls and explain relevant filters.
- `show` reads full source history with `--tail` and `--reverse`, role labels and
  message-count limits. Timeline/cut numbering retains its previous behavior.
- Interactive `scrub-all` covers all detected values in the current export/share
  review, confirming replacement of earlier keep decisions. Originals remain
  unchanged; partial scanning is not a secret-free guarantee.
- Native cuts are created exclusively with `0600` permissions under a permissive
  umask; existing files, symlinks and concurrent destinations are not overwritten.

Backup remains experimental, manual and Linux/Codex only; it requires externally
installed age v1.3.2 and operational/external recovery recipients. Positive
system-keyring/headless operation is unvalidated. Scheduling, automatic retention
and historical re-encryption are not included.

The producing CSL version is part of the backup fingerprint. Updating to 0.2.2
starts a fresh capture instead of reusing objects from another producer version.
Older checkpoints retain their original fingerprints and recovery-key needs.
Finish pending backup runs and recipient-change plans using their producing
version before updating. This patch does not re-encrypt historical backups.

Validation must refer to this candidate's own fixed SHA, including native/media/
migration opt-ins and isolated installation/demo. Earlier candidate results remain
attached to their own commits. Publication is established by the
[v0.2.2 Release](https://github.com/vitral-ai/context-session-librarian/releases/tag/v0.2.2)
and its exact tag, not by this preparation record. Historical v0.2.1 and earlier
tags and notes remain unchanged.

### v0.2.1 — release candidate

This patch release adopts the visible name **Context Session Librarian (CSL)**
and simplifies the README around installation, finding conversations and transfers.
The `csl` command, `csl.vitral.ai`, provider coverage, formats, IDs and storage paths
remain compatible. The canonical repository is now
[`vitral-ai/context-session-librarian`](https://github.com/vitral-ai/context-session-librarian).
It adds no new backup or transfer capabilities.

Backup remains experimental, manual and Linux/Codex only, with externally
installed age v1.3.2 and operational/external recovery recipients. Positive
system-keyring/headless operation is unvalidated; scheduling, automatic retention
and historical re-encryption are not included. Published v0.1.x/v0.2.0 tags and
notes remain unchanged.

The producing CSL version is part of the backup fingerprint. Updating to 0.2.1
therefore starts a fresh capture instead of reusing objects from a different
producer version. Old checkpoints retain their original fingerprints and key
requirements; finish pending runs or rotation plans with their producing version
before updating. This patch does not re-encrypt old backups.

Validation belongs to this candidate's own fixed SHA: full media/native/migration
opt-in suite, isolated-prefix install/version/help and offline synthetic demo.
Actual results and the published commit belong to the corresponding GitHub
Release; no earlier result is attributed to this candidate. Preparation alone
does not establish publication.

### v0.2.0 — authorized release candidate

Luis authorized publication on2026-11-10. The owner selects and publishes the exact
reviewed commit after checks; this record is preparation until the
[v0.2.0 Release](https://github.com/vitral-ai/codex-session-librarian/releases/tag/v0.2.0)
exists. It is a normal version release with an explicitly experimental backup
feature. Existing v0.1.0/v0.1.1 tags and notes must not be changed.

Scope: existing library/transfer commands plus backup F0–F3: manual Linux/Codex
native/portable encrypted checkpoints, bounded containers, logical-head
incrementals, partial coverage, durable resume, fencing and future recipient
rotation. External pinned age1.3.2 is optional for core CSL and required for backup;
CSL does not install it. Manual stdin/memory/FD external-key recovery has synthetic
and bounded backend evidence. A system-keyring reference fails closed if unavailable;
positive keyring/headless operation is not validated. F4 timers, F5 retention/GC,
historical re-encryption, native-sanitized and new human pilots are not included.

Release validation must name its own fixed SHA: full media/native/migration opt-in
suite, isolated-prefix installation/version/capabilities/help and offline synthetic
demo. Earlier phase results remain attached to their own commits. The release
owner records final SHA, CI, publication date and clean tag-install result in the
GitHub Release; local preparation alone is not proof that a tag is published.
No private evidence directories, credentials, keys or real histories are assets.

### Earlier published versions

[v0.1.1](https://github.com/vitral-ai/codex-session-librarian/releases/tag/v0.1.1)
includes the historical-tool display, secret-review and recall corrections from
main. Exact publication SHA, date and CI are recorded in that GitHub Release.
The tag is official only once the Release exists; a prepared version string is
not publication. Runtime validation: 268 tests with native/media/migration opt-ins.

[v0.1.0](https://github.com/vitral-ai/codex-session-librarian/releases/tag/v0.1.0)
was published November 5, 2026 from
`edd05bb20eb145dea6178d548f1363b9516102af`, with its 253-test runtime record.
Its tag and notes remain unchanged; it does not include the later display/recall
corrections. Transfer to Vitral preserved its repository and Release identity.

The PR #9 historical 260-test result belongs to that earlier code, not all
subsequent main commits. Display, structural fidelity and model continuation
remain distinct checks. The current human pilot stays pinned to v0.1.0 unless
its version is explicitly changed; never mix participant versions silently.

## Procedure for a future authorized release

1. Obtain a scoped release decision. Select and record the exact upstream SHA;
   never substitute a moving main. Review license, version, changelog and scope.
2. Verify regressions, isolated installation and synthetic demo at that SHA.
   Do not count skipped tests as passes or attribute a prior run to another commit.
3. Verify the proposed tag does not already exist. Publish only the selected SHA
   and reviewed notes; never overwrite or retag an existing version. No private
   histories or test credentials may be release assets.
4. Install from the published tag in a fresh isolated environment. Check version,
   resolved SHA and demo. Align README, web and user documentation with reality.

Use the operator's own authorized GitHub identity.
Repository access does not itself authorize a new version, a transfer or scope
expansion. Package-registry publication requires its own plan and verification.

## Native evidence gate

Ordinary CI covers Python 3.11/3.13 and optional media in process mode; it does not
by itself establish native CLI loading or migration E2E. The manual isolated native
workflow pins Codex 0.169.0 and Claude Code 2.1.308, enables native/migration opt-ins
and requires bubblewrap for media. Use synthetic temporary homes, no personal
credentials and no model inference. Record actual results, not workflow existence.
Paid continuations are a separate explicitly authorized stage.

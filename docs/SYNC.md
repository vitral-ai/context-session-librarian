# Optional raw session sync — experimental

`csl sync` publishes selected local sessions to a registry implementing the sync
protocol. It is opt-in, separate from ZIP export, encrypted backup and agent
messaging. It reads Codex, Claude Code and Antigravity CLI through
local provider adapters. These commands are included in CSL 0.4.0; earlier
v0.3.0 does not include `csl sync`.

**Raw sync uploads the complete transcript bytes and supported companion files,
including any secrets they contain.** It does not scrub them or apply backup
encryption. Metadata can include the working directory and repository identity.
Choose a registry/account you trust and review local include/exclude choices.
The registry authenticates ownership; client-supplied metadata is not an owner grant.
Byte preservation does not prove that every harness/model can resume every opaque
state. Native package/import compatibility remains the provider's responsibility.

## Enable explicitly

No service, timer or daemon is installed. Installing CSL, using its other commands
or running `sync status` does not sign in or upload sessions. The tool has no
registry, identity URL or client ID default. An operator must register a public
PKCE client permitting `http://127.0.0.1:<port>/callback` and the required scope
and registry audience.

```sh
csl sync sign-in \
  --endpoint https://registry.example.test/session-registry/v1 \
  --discovery-url https://identity.example.test/.well-known/oauth-authorization-server \
  --client-id YOUR_REGISTERED_PUBLIC_CLIENT
csl sync register-device
csl sync status
csl sync run
```

The system browser performs Authorization Code + PKCE S256; no password or client
secret is accepted by CSL. Sign-in saves credentials but does not start syncing.
HTTPS is required except for explicit localhost/loopback development endpoints.
HTTP redirects are not followed by authenticated registry/token requests.

State defaults to `$XDG_STATE_HOME/csl/sync` or `~/.local/state/csl/sync`.
Use `csl sync --home /your/private/state …` consistently for another installation.
The directory is private and credential files are atomically written with mode
0600. Tokens never appear in command output, arguments or operational logs.
The implementation uses private files, not a required system-keyring dependency.

Refresh is serialized and marks a rotating token in-flight before sending it.
After an ambiguous response or crash, an old generation is not blindly retried.
An issuer explicitly guaranteeing replay of the already-issued successor can be
advertised in discovery as `refresh_replay_grace_seconds` (integer 0–30), or
configured at sign-in with `--refresh-replay-grace-seconds 30` (maximum 30; default
0 when discovery omits it). The issuer's advertised policy is saved at sign-in.
That recovery is bounded to one replay within the recorded window. Otherwise
`refresh_outcome_unknown` requires a new browser sign-in. `invalid_grant` or
revocation clears stored credentials and stops collection.
An explicit replay limit takes the smaller of the flag and advertised policy;
`--refresh-replay-grace-seconds 0` always opts out, including in sign-in recovery.

## Budgeted collector passes

`csl sync run` and `csl sync watch` accept `--budget-seconds` (default 30)
and `--budget-bytes` (default 268435456). Discovery reads filenames and stat
metadata, without opening or freezing transcripts. Pending sessions precede
already-synced history; smaller files lead, with newer activity breaking ties,
across providers. The first-run selection remains `new` unless configured otherwise.
Recorded transcript metadata is still verified before applying selection policy.

A pass prepares one transcript at a time using streamed reads and disk-backed
view/tool indexes. The byte budget counts cumulative reads, verification and
upload payloads, so preparing a file can cost several times its size. A normal
budget yield reports `deferred` / `pass_budget`, exits successfully, retains
acknowledged upload offsets and resumes on a later pass. Once smaller pending
work is clear, one previously deferred session gets a dedicated pass with a
fresh, larger time/byte allowance. Repeated deferrals double that allowance
until a finite preparation fits; newly arriving small work still goes first
and does not reset the earned allowance. Transcript memory limits remain the
same. Incomplete freezing and indexing are discarded rather than published.
The configured budget is the normal-pass allowance, not a hard ceiling on
these automatic dedicated retries. JSONL records above 8 MiB
are rejected per session (`record_size_limit`); a bad session does not abort
healthy peers. These limits do not modify the original transcripts.

Legacy Codex filenames need a metadata identity check before duplicate-safe
uploads. That check stops at `session_meta` and caches the verified ID by path,
size, timestamps and file identity. It has its own per-file 16 MiB read allowance
and initial one-second cooperative deadline, separate from a small session's
upload budget. Long preambles save a record position for the next pass; their
time allowance doubles up to 30 seconds. A changed file invalidates the cache.
Unresolved identities defer upload rather than allowing a possible duplicate.

The time limit is cooperative between file operations and HTTP requests; an
in-flight request retains its existing network timeout. Directory discovery
memory scales with file count, while transcript processing uses bounded records,
chunks and SQLite pages rather than loading a whole conversation into memory.

## Pull a session onto this machine

### Explicit same-ID Resume (experimental)

Default imports remain independent Forks. `import --resume-same-id` opts into a
registry writer handoff for one raw v3 Codex or Claude session. It requires a
registered local collector installation; workloads cannot claim a writer.
The first managed producer supports opted-in Linux Connect launches with an
explicit native ID known before spawning. Older hosts without `keeper-receipt`,
implicit selection, and in-provider session switching cannot release a writer.
Use Fork for those sessions. Native history-base bundles are currently refused.

On the source, stop the managed provider normally, then run:

```sh
csl sync --home PROFILE release-writer codex NATIVE_ID \
  --registry-session-id REGISTRY_ID --expected-snapshot-id SNAPSHOT \
  --expected-generation GENERATION --expected-device-id SOURCE_DEVICE
```

This checks the authenticated host's positive ended-process receipt, flushes
the transcript, and verifies the exact complete snapshot before releasing.
If the flush changes the snapshot, inspect the completed session and repeat
with its new snapshot ID. A missing managed receipt refuses with
`writer_unsupported_source`; it does not stop unrelated provider processes.

Pull the released package using the destination's own collector sign-in, then:

```sh
csl import PACKAGE --cwd PROJECT --resume-same-id --sync-home PROFILE \
  --registry-session-id REGISTRY_ID --expected-snapshot-id SNAPSHOT \
  --expected-generation GENERATION --request-id REQUEST_UUID \
  --expected-device-id DESTINATION_DEVICE
```

The destination verifies its server-attested installation, the owner-scoped
released snapshot, package checksums, and native ID. An existing local ID also
needs byte-identical files and its exact positive managed stop receipt. Files
are published without replacement, retaining their original ID and bytes.
Use the same request UUID and arguments after an ambiguous acquire or partial
publication. It never silently forks, rolls back by deleting files, or opens
the provider directly. Resume afterward through the managed Vitral Connect
route; `--open` is refused until that route is integrated.

The collector sends the acquired writer generation on snapshot, raw, and view
writes. Under the same resolved provider home, CSL and the managed launcher
share `.csl-writers/<sha256(UTF-8 native ID)>.lock`. A durable sibling JSON fence
contains `schema_version:1`, `provider`, `native_session_id`, and `state`.
Missing permits legacy starts; `active` permits managed starts;
`pending`, `publishing`, `releasing`, `released`, unreadable, or malformed fences
refuse starts. CSL also pins the local profile, device, and registry in `binding`.
Only the owning profile can upload; ambiguous handoffs remain fenced. No token
or transcript text is stored in these fences. Hosts must hold the same lock
while checking the fence, invalidating an older end receipt, reserving the ID,
and recording the new process incarnation.

`csl sync --home PROFILE attest-installation --json` returns only `account_id`
(lowercase 24 hex) and `device_id` (canonical lowercase UUID), after validation by
the trusted identity endpoint. It requires a registered local collector, may
refresh its own sign-in, and refuses an unresolved prior refresh. It does not
read transcripts, acquire the collector pass lock or change collector diagnostics.
Failures produce no stdout: exit 75 means transient network/issuer or auth-lock
contention; exit 1 means unverified identity or required sign-in. Network calls
use two-second timeouts without generic retries; callers must enforce a seven-second
total deadline (including DNS and any issuer-authorized refresh replay), and cancel
the process on channel closure. Successful JSON is under 4 KiB and contains no token.
The guided invocation is `PYTHON -E SOURCE/csl sync --home PROFILE attest-installation --json`.

`csl sync pull SESSION_ID` downloads one session package the signed-in account
owns, using this installation's own sign-in. The registry derives the owner from
that token, so another account's session id is refused (`not_found`) and nothing
is written. No token appears in arguments, output or shell history.

```sh
csl sync pull SESSION_ID --out-dir ~/.csl-resume
csl import ~/.csl-resume/SESSION_ID.zip --cwd ~/work/project --open
```

The file is written with mode 0600. A directory that does not exist is created
with mode 0700; an existing directory keeps its mode. Pulling the same session again replaces
its earlier file atomically (a symlink or directory at that name is refused), so a
re-resume or a retry after a failed import works. It is refused if the body is
not a ZIP or exceeds 2 GiB, and runs
while `csl sync watch` is running (it does not take the collector lock).
`csl import ... --open` then starts the matching native CLI (`claude --resume` or
`codex resume`) on the imported copy. Imports allocate fresh session identities.

Prerequisite on a container or headless host: sign in and register the device
first (`csl sync sign-in --manual-browser --callback-port PORT ...`, with that
loopback port forwarded to a browser you control, then `csl sync register-device`).
A device-code sign-in is not available yet.

## Upload choice and installer contract

Before enabling the platform service, configure the local selection once. In a
terminal, `csl sync --home STATE configure` opens a four-choice prompt, defaulting
to **New only**. A first sign-in on a fresh profile offers the same choice;
non-interactive first sign-in defaults to `new`. Existing profiles keep their
policy. End-of-input cancels without changing the selection. Installers and
scripts use one explicit command:

```sh
csl sync --home STATE configure --selection new
csl sync --home STATE configure --selection last --limit 30
csl sync --home STATE configure --selection all
csl sync --home STATE configure --selection folders --folder ~/work --folder ~/other
```

Choose one command. `new` selects sessions **created after configuration**, not
old sessions edited recently. This is the default on a new sign-in. `last` takes
one snapshot of the N most recently active sessions across providers, then follows
new sessions. `all` follows everything; `folders` follows sessions whose recorded
working directory is inside a selected folder. Unknown creation times require
explicit inclusion for `new`. Local `include`/`exclude` overrides the policy;
changing policy never removes remote content. Existing installations retain their
legacy age/tracked-session policy until explicitly configured.

Pairing is the same on Windows, macOS and Linux: `sync --home STATE sign-in`
with the explicit deployment endpoint, discovery URL and public client ID shown
above, then `sync --home STATE register-device`. Browser PKCE is required; this
collector does not offer a device-code login. Installers own platform service
setup and run `csl sync --home STATE watch --interval 30`. The `--home` argument
always precedes the subcommand. Sign-in and configuration do not start uploads.

`csl sync --home STATE status` prints a short summary; add `--verbose` for the
cached session table. `--json` returns `schema_version: 1`, `paired`, `enabled`,
`collector: {running, busy}`, `policy: {selection, configured_at, limit, folders}`
(`last` also has `initial_sessions`), and `totals: {selected, synced, pending,
excluded, synced_bytes, planned_bytes}`. Existing `authentication`, `device_id`,
`last_error`, `last_pass_at` and `sessions` fields remain. `paired` means local
sign-in plus successful device registration, not a fresh remote health check.
Totals cover cached sessions and the latest completed selection scan; exclusions
include explicit opt-outs not yet scanned. Status does no network or transcript
reads. When a collector holds its installation lock, the status view says you
can close it; closing a foreground **watch** still stops that collector.

For a read-only progress view, run `csl sync --home STATE status --follow`
(optionally `--interval 2`). Ctrl+C closes only this view; the separate collector
service keeps running. `--json --follow` emits one JSON object per line for each
refresh. Progress comes from cached checkpoints, not a network health check.
If no separate collector is running, this view does not start uploads.

When authentication needs a fresh browser sign-in, status includes an additive
`recovery: {reason, command, argv, instructions}` field. `argv` is the normal sign-in command
for this profile's public endpoint/client settings; it never includes a token.
Sign-in saves the public manual-browser/callback-port options for recovery.
Older profiles without those options include explicit headless forwarding
instructions; choose the port that is actually forwarded. Missing deployment
settings yield null command/argv. Short status prints “Sign in again”
with that command. `refresh_outcome_unknown` is not retried or cleared by status.
A profile requiring recovery reports `paired: false` until sign-in succeeds.

`collector.watch_pid` is optional: it identifies the foreground watch supervisor,
not its pass worker or a platform service by name. It is emitted only with a busy
installation lock and a matching live OS process creation identity. Linux uses
boot ID plus process start ticks; Windows uses process creation time. Unsupported
or inaccessible process identity (currently including macOS) omits the field.
Installers must compare it to their actual service PID, not infer service ownership
from `running`. Status is a point-in-time observation, not a process lease.

Explicit native titles are preserved. Otherwise session titles use the first real
user text, skipping known native bootstrap, instruction and tool-result records,
with the native ID as the fallback. Raw transcripts are never changed.

## Scope and status

```sh
csl sync run --days 30
csl sync run --all
csl sync run --provider claude
csl sync include claude SESSION_ID
csl sync exclude claude SESSION_ID
csl sync status
```

By default sessions updated in the last 30 days enter the collection; previously
tracked sessions continue to be followed. Explicit include overrides age, exclude
stops future uploads and does **not** delete remote content. This command group
does not perform remote deletion or automatic tombstone restoration. An explicit
exclusion also survives a fresh sign-in. Fresh authorization resets old transfer
checkpoints so another account/registry cannot inherit stale view cursors.

Status is local cached state: device/session IDs, byte counts, last successful
sync time, pending/error state and a sanitized error code. It is not a live server
availability probe. `source_changed`/`source_incomplete` means no mixed or partial
snapshot was announced; try again after a complete source record exists.
Limit, prefix/revision conflict, tombstone, authentication and service errors are
visible rather than silently dropping files. A prefix mismatch starts a new raw
revision on the next pass. Native source files are never modified.

A malformed Codex/Claude session is reported by native ID and skipped for that
pass while healthy sessions continue uploading. Duplicate native IDs skip every
copy with `ambiguous_native_id`; no winner is chosen. Errors stay pending and
are retried on later passes (or can be explicitly excluded). Sources and existing
remote data remain unchanged. A provider discovery failure still reports a
provider-level error.

For active JSONL files, only complete newline-terminated records are captured.
Rewritten JSON must be a complete valid provider snapshot. Selected raw files are
temporarily copied into private snapshot files and hash-checked before upload;
this costs disk space roughly equal to the selected session's raw set. Interrupted
copies remain private and are cleaned at the next exclusively locked pass.

Chunks are at most 8 MiB, with prefix/content hashes and stable idempotency keys.
Chunk identities include mode, range and hashes. Metadata snapshots separately
bind the entire input, including activity state and other metadata: a metadata-only
change receives a new snapshot ID without forcing a new raw-file revision.
Registry file inventories are queried before resuming. Rewritten files receive
new revisions; changed view prefixes receive new generations. View records retain
provider ordering and raw offsets; the provider's opaque page position is saved
only after upload acknowledgment. SQLite databases and their committed WAL prefixes
use replace-mode revisions. Every DB/WAL input to a view must match the captured
inventory, including when a WAL appears, disappears or is rewritten. Changing
sources remain pending for another pass rather than publishing a mixed snapshot.
Reading a complete file/request is not a model
recall test, and this collector does not activate models.

## Foreground operation

```sh
csl sync watch --interval 30
```

Stop with Ctrl-C. Temporary registry outages use bounded request retries and
foreground backoff. Revoked or unresolved credentials stop the process instead
of looping on an old refresh token. Only one collector/sign-in operation can own
a state directory at a time; another returns `collector_busy`. Different state
directories are different installations, not a way to share one device journal.

### Incremental collection and verification

For Codex and Claude JSONL histories, the collector keeps a private derived cache
under the selected sync state directory. Unchanged files are checked by inventory,
device/inode, size, nanosecond modification time and change time. They do not
repeat transcript parsing, view rendering or per-session HTTP calls. This reuses
the previous acknowledged snapshot; it **does not advance its completeness time**.
Antigravity retains the full provider verification path.

The metadata hint is not an integrity proof. Every five minutes, or when the
clock goes backwards, cached complete prefixes are fully hashed and incomplete
suffixes checked for newly completed records. Changed/replaced/truncated files
are captured and verified before publication. Ordinary same-size/mtime rewrites
are detected by change time; a change concealing all file metadata is bounded
by the periodic full check. Source and view hashes still bind the exact complete
prefix. Changed append prefixes are fully verified; no opaque or serialized SHA
state is trusted to guess their integrity.

Appending to a JSONL renders only new native records, retaining historical tool
correlation and acknowledged view positions. A changed historical prefix rebuilds
the derived view and triggers its normal generation/reset protocol. Sources that
continue appending can publish a frozen complete prefix; later bytes are collected
on the next pass, and incomplete final records are never sent.

Codex history-base graphs include their ancestor files in the snapshot and its
view binding. Candidate paths in active/archive storage, including compressed
siblings, invalidate the cache when added, removed or changed. Periodic checks
hash every graph input, not only the leaf or inherited byte prefix. A changed
graph is resolved and rendered again through the bounded native history adapter;
missing dependencies or missing view anchors remain errors. Single-file JSONL
appends keep their incremental rendering path.

`watch --interval 60` also checks lightweight source-change hints once a second
and wakes early for changed/new histories. Its regular deadline includes pass
duration rather than adding another full sleep afterward. Hints cause normal
verification, never a completeness claim by themselves. Cold initialization,
large rewrites, sustained source changes or a slow registry can still exceed an
interval; the interval is not a universal network-latency guarantee.

Every pass runs in a fresh child interpreter. The foreground supervisor keeps
the installation lock and only reads small cache hints while waiting. A native
crash or leak in JSON/provider parsing therefore ends one worker rather than
the watcher, and the worker's memory is returned on exit. A failed pass resumes
from durable acknowledgements in a new process, with exponential backoff from
1 to 60 seconds. A signal produces a structured diagnostic such as
`{"state":"pass_crashed","exit_code":139,"signal":"SIGSEGV",...}`;
ordinary failures include the exit code and safe error code. Native stderr is
not echoed as transcript data. Authentication requiring human action stops the
watcher instead of repeatedly consuming an old refresh token.

Ctrl-C or SIGTERM shuts down an active worker before releasing the lock. No
SQLite connection is used across interpreter processes. This isolates native
failures; it does not establish their root cause or prevent the OS from killing
the supervisor, and it cannot guarantee recovery from every host failure.

The cache contains transcript-derived text, tool names and metadata and may
therefore contain secrets. Its directory/files use private permissions and it is
not an encrypted backup. Temporary raw copies are removed after preparation;
derived view/cache files remain local. Stop watch before deleting `provider-cache`
to rebuild it; this leaves native histories, credentials and remote sessions intact.

To disable locally, first stop the foreground collector or your user-managed
service, then run `csl sync sign-out`. This erases local tokens and disables the
installation; it does not revoke the server session or delete remote transcripts.

### Optional user-managed service

CSL never installs or enables these. Sign in manually first and select an
appropriate restart policy. Keep the checkout and private state path stable.

For systemd, create your own `~/.config/systemd/user/csl-sync.service`:

```ini
[Unit]
Description=Optional CSL session collector
[Service]
Type=simple
ExecStart=/absolute/path/to/csl sync --home /absolute/private/state watch --interval 30
Restart=on-failure
RestartSec=30
KillMode=control-group
[Install]
WantedBy=default.target
```

Enable only if desired using `systemctl --user daemon-reload` and
`systemctl --user enable --now csl-sync`; stop/disable it before signing out.
Authentication failures still require human sign-in; do not configure an aggressive
restart loop to manufacture consent.

On macOS, a user-managed LaunchAgent can restart an unsuccessful supervisor
(`KeepAlive.SuccessfulExit=false` is launchd's counterpart to systemd's
`Restart=on-failure`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>local.csl.sync</string>
  <key>ProgramArguments</key><array>
    <string>/absolute/path/to/csl</string><string>sync</string>
    <string>--home</string><string>/absolute/private/state</string>
    <string>watch</string><string>--interval</string><string>60</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
  <key>ThrottleInterval</key><integer>30</integer>
</dict></plist>
```

Load/unload it with your normal `launchctl bootstrap gui/$(id -u) …` / `bootout`
workflow. Sign in manually first. No unit/plist is generated, installed or enabled
by CSL; these service-manager arrangements require their own platform validation.

### Windows Scheduled Task

Run the native collector with Python 3.13 or later and a stable checkout.
From the checkout, use `python -m csl_sync sync` in place of `csl sync` for
the same sign-in, registration, status, and watch commands. The default state is
`%LOCALAPPDATA%\csl\sync` and its ACL is restricted to the current user,
SYSTEM, and local administrators. Native profiles are read from
`%USERPROFILE%\.claude`, `%USERPROFILE%\.codex`, and
`%USERPROFILE%\.gemini\antigravity-cli`. The existing profile overrides apply.
Windows file notifications wake watch early. Periodic verified scans also run.

After signing in, register the task from PowerShell:

```powershell
.\packaging\windows\register-sync-task.ps1
Start-ScheduledTask -TaskName 'CSL Session Collector'
```

The task runs as the current user at logon. Pass `-State` if you signed in
with a custom `--home`. If PATH contains several Python installations, the
first `python.exe` wins; use `-Python C:\path\to\python.exe` to choose explicitly.
The matching `pythonw.exe` must exist beside it: the task runs without a console
and continues after registration terminals close. Progress and errors are read
with `csl sync --home STATE status --verbose`, not a task console.
The task uses the profile paths from the PowerShell
session that registers it. To update an existing task, stop it with
`Stop-ScheduledTask`, run the registration script with `-Replace` and the
same task name and options, then start the task again. A sync pass already
in progress can still hold the collector lock after the task stops. Recheck
the named task with `Get-ScheduledTask` after startup to confirm it stays
`Running`. If it stops during startup, let the old pass finish, retry
`Start-ScheduledTask`, and recheck the task. For a persistent failure, inspect
the task's last result and run `python -m csl_sync sync status` with the same
state directory to check `last_error`.
For a headless host, an administrator can register
with `-AtStartup`. Task Scheduler then runs the collector under the same
user without storing that user's password. The S4U startup task cannot use
Windows-authenticated network shares or encrypted files. Start and stop it
with the normal Scheduled Task controls.

### Remote environments without a desktop browser

Use your own browser with an explicitly forwarded loopback callback. For example,
forward local port 8765 to the remote collector's 127.0.0.1:8765, then sign in on
the remote host with `--callback-port 8765 --manual-browser`. The authorization URL
is placed in `authorization.json` in its private sync state directory, never in
logs, and removed when sign-in finishes. Open it in your local system browser.
The identity URLs must also be reachable from that browser; a localhost development
identity endpoint may need a separate forward. CSL does not configure SSH, expose
the callback publicly or install credentials on another machine. This remote
arrangement requires its own end-to-end validation; local fake-issuer tests do
not establish it.

## Validation boundaries

The repository tests use synthetic providers and in-test HTTP peers; no real
accounts or model calls. Actual registry/identity deployments and remote-environment
acceptance need their own checks. Provider-native fidelity has separate, bounded
evidence: Codex supports bounded raw
history-base graphs while missing or unsupported dependencies are rejected. See
[provider coverage](PROVIDER_MATRIX.md) and [Antigravity evidence](ANTIGRAVITY.md).
Raw sync success does not prove model recall. Public v0.3.0 cannot import native
package v3; use a client containing the v3 importer. Core uses only Python's standard
library. Windows is not claimed; existing CSL is Linux-oriented, and the lock
and optional service examples assume Unix.

The desktop collector archives attached Codex history using the same verified
lineage boundaries, ordinals and checksums as other paginated sessions. Native
transfer of attached relationships remains unavailable. A failed session is
reported separately while healthy sessions continue; a nonzero pass exit does
not mean all uploads failed. Exclusions are checked before history preparation,
after a bounded identity lookup when an older filename contains only a hint.

## Owner-local typed Resume adapter

`csl-resume-action --json-stdin` accepts one JSON object of at most 4096 bytes.
Install with `install.sh`; guided callers may invoke the absolute source entrypoint
with their verified Python. It is currently limited to Linux managed Connect
launches with a separately provisioned personal collector, not a workload token.
It resolves `$VITRAL_TUNNEL_HOME` (default `~/.vitral-tunnel`), verifies private
`api.HOST_ID` against `expected_host_id`, and reads private `collector_setup.json`
with absolute `home`, `runtime`, and `source`. Source must be this installed
checkout. Host is the enrolled home's `vitral-host`, or the fixed signed Connect
installation `/opt/vitral-connect/bin/vitral-host`. Missing binding refuses.

The closed schema has `schema_version:1`, `action` (`check`, `release`, `import`,
`run`), lowercase UUID `request_id`, `provider` (`codex`, `claude`), lowercase UUID
`native_id`, owner-derived `registry_session_id` (`ss_` plus 64 lowercase hex),
lowercase UUID `expected_snapshot_id`, integer `expected_generation` (0 through
2^53-1), `expected_host_id` (`host-` plus 16 lowercase hex), and lowercase UUID
`expected_device_id`. Only destination check may omit the device to discover it.
Check requires `intent:source|destination|in_place`; other actions forbid intent.
Import, run and destination/in-place checks require `target_id`; source check
and release forbid it.
Destination/in-place checks use an owner-provisioned target, never an arbitrary
browser path. Target IDs contain 1–128 ASCII letters, digits, underscores or hyphens.

Targets use the host's private `managed-resume-targets/<target_id>.json` schema:
`schema_version:1`, `provider`, absolute `cwd`, `provider_home`, `executable`.
Provider home must match the collector's resolved local home. The host further
qualifies the native executable. No target is selected or created automatically.
Check does installation attestation, owner/session/snapshot validation and local
source stop or destination writer-state checks; full package/byte suitability is
validated by import before acquisition. Destination check accepts the exact
active/legacy source generation before release; import still requires released
state. Source release rechecks the managed end
receipt under the native lock, flushes, and requires the exact complete snapshot.
Import reuses the #142 raw single-writer handoff and atomic no-replace publication.
For a stopped keeper-managed recording still at legacy generation 0, in-place
check requires the original attested device, a positive PID/start-bound end
receipt, and local fully synced bytes matching the exact complete snapshot.
Run journals the request, fences launches and uploads, and acquires generation 1
under the native lock before publishing its active writer. Interrupted acquisition
reuses the exact request; check never changes writer or fence state. Missing or
insufficient stop proof returns `resume_stop_proof_required`; unmanaged recordings
cannot resume in place yet. A different device must use the explicit handoff.
Run releases the adapter lock before invoking `keeper-managed-resume --json-stdin`;
the host reacquires it and compares `expected_target` (the exact five target fields),
`expected_writer` (`registry_session_id`, `account_id`, `device_id`, `generation`)
and `expected_binding` (`profile`, `device_id`, `endpoint`) against its resolved
target and active fence before journal, spawn or replay. Active fences carry the
writer incarnation; the adapter upgrades an older fence only from the verified
active local record under lock. Missing or changed admission fields refuse.
The host also requires exact live process proof for replay. If a run response
is lost and the active collector advances the snapshot, only an already-existing
exact durable run journal can authorize `readback_only:true` host admission.
The same target, active writer generation and collector binding must still match.
Readback cannot reserve or launch a keeper; missing or ended launch proof refuses.
First acquisition and all other actions still require the original snapshot.

Success is one schema-v1 JSON object repeating request/action/provider,
`native_session_id`, `registry_session_id`, `host_id`, attested `account_id` and
`device_id`, plus `target_id` when selected. State is `eligible`, `released`,
`imported` (with generation), or `launched` (with actual generation, keeper ID and process proof).
Mutating retries preserve their full request and local target binding. Failure
has no stdout, a bounded diagnostic code on stderr, exit 75 for retryable failure
or busy state and exit 1 otherwise. No credentials are returned. An unmanaged
source refuses `writer_unsupported_source`; offer Fork instead. No provider is
started by check/release/import, and default Fork import remains unchanged.

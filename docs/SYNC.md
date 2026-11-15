# Optional raw session sync — development

`csl sync` publishes selected local sessions to a registry implementing the sync
protocol. It is opt-in, separate from ZIP export, encrypted backup and agent
messaging. It reads Codex, Claude Code, Gemini CLI and Antigravity CLI through
local provider adapters. These commands are not in the published v0.3.0 tag;
use a development checkout that includes `csl sync`.

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
configured at sign-in with `--refresh-replay-grace-seconds 30` (maximum 30; default
0). That recovery is bounded to one replay within the recorded window. Otherwise
`refresh_outcome_unknown` requires a new browser sign-in. `invalid_grant` or
revocation clears stored credentials and stops collection.

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

For active JSONL files, only complete newline-terminated records are captured.
Rewritten JSON must be a complete valid provider snapshot. Selected raw files are
temporarily copied into private snapshot files and hash-checked before upload;
this costs disk space roughly equal to the selected session's raw set. Interrupted
copies remain private and are cleaned at the next exclusively locked pass.

Chunks are at most 8 MiB, with prefix/content hashes and stable idempotency keys.
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
Gemini and Antigravity retain the full provider verification path.

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
evidence: Gemini native loading remains unverified; Codex sessions depending on
an external history base cannot produce a self-contained package. See
[provider coverage](PROVIDER_MATRIX.md) and [Antigravity evidence](ANTIGRAVITY.md).
Raw sync success does not prove model recall. Public v0.3.0 cannot import native
package v3; use a client containing the v3 importer. Core uses only Python's standard
library. Windows is not claimed; existing CSL is Linux-oriented, and the lock
and optional service examples assume Unix.

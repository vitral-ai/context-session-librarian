# Local provider interface for session collectors — v1

Module: `csl_sync_providers.py`. Standard library only. No network, login, model
calls or transcript logging. This interface is provider-local and independent of
Vitral. The initial providers are `codex`, `claude`, `gemini`.

```python
list_sessions(provider, *, home=None, active_seconds=300) -> list[dict]
raw_files(provider, native_id, *, home=None) -> list[dict]
view_records(provider, native_id, *, position=None, limit=500,
             text_limit=16000, home=None) -> dict
package_descriptor(provider, native_id, *, home=None) -> dict
```

`home` explicitly overrides that provider's root, including in tests. Defaults
use CODEX_HOME, CLAUDE_CONFIG_DIR and the Gemini user directory respectively.
For Gemini, `GEMINI_CLI_HOME` is the harness's **parent home**; its storage is
`.gemini` below it (or `.cache/.gemini` in its macOS seatbelt mode).
`CSL_GEMINI_HOME` or the API's `home` overrides the storage directory directly.
Errors are `ProviderError` with a stable `code`; exception messages never include
transcript contents. An uninstalled CLI does not prevent reading known formats.

## Session metadata

Every row has `provider`, `native_id`, `title`, `cwd`, `repository` (null or
`{"remote_url": ..., "branch": ...}`), `created_at`, `updated_at`, `model`,
`record_count`, `byte_count`, `parent` (null or `{provider, native_id}`), `origin`
(`native`, `imported`, `converted`), and `state` (`active`, `idle`, `archived`).
Unknown values are null, not invented. Repository reads are local Git operations;
embedded HTTP credentials/query parameters are not returned. Titles are bounded.
`byte_count` is the sum of complete raw lengths, `record_count` is the number of
complete native primary records (JSONL lines; Gemini message objects).

`state=active` means a source was recently written, not that a process/model was
observed running. `updated_at` uses the last recorded timestamp where available.
Claude discovery lists main sessions, not each subagent as a separate session.

## Raw file enumeration

Each descriptor has:

```json
{
  "file_key": "main",
  "path": "/absolute/local/file",
  "relative_path": "session.jsonl",
  "mode": "append",
  "complete_length": 123,
  "size": 145,
  "sha256": "digest-of-the-first-complete_length-bytes",
  "role": "transcript"
}
```

`mode` is `append` or `rewrite`. Append is an incremental strategy, not a guarantee
that a user/harness never truncates or replaces a file. The collector MUST verify
prefix hashes and start a new raw revision on mismatch. Hashes describe precisely
the complete prefix, never the incomplete suffix. Recheck prefix/hash while
reading before upload; a descriptor is not an atomic snapshot lease.

For JSONL, only complete newline-terminated records are in `complete_length`;
invalid complete records raise an error. For a rewritten JSON document, the
whole stable parseable document is the complete unit; an in-progress invalid
document reports an incomplete snapshot, never a fabricated partial JSON file.

Claude companions (subagent JSONLs and persisted tool-result files) are separate
descriptors rooted under its session directory, with `role` `subagent` or
`tool_result`; subagent metadata is `context`. No credentials, project files, arbitrary referenced paths or
symlinks are followed. File keys are `main`, or `f-` plus SHA-256 of the
normalized relative path. The relative paths, not local absolute paths, are used
to reconstruct a native package. Gemini legacy JSON is rewritten; newer native JSONL journals are append-oriented
and may include patches/rewinds. The descriptor reports the actual format.

## Incremental view

Result:

```json
{
  "records": [],
  "position": {"version": 1, "provider": "claude", "native_id": "...",
               "next_seq": 1, "prefix_sha256": "..."},
  "reset_required": false,
  "reason": null,
  "complete": true
}
```

The position is opaque: persist it unchanged after uploading the returned page.
`source_file` in the result is `{file_key, length, sha256}` for the primary prefix
used to render that page. Bind it to the raw snapshot inventory; if they differ,
refresh the inventory before publishing a completeness claim. View rendering is
bounded to that prefix and rechecks its hash; `source_changing` asks for a retry.
`seq` starts at one, matching the wire contract. Continuation verifies the already-emitted **normalized view
prefix**, so rewriting a Gemini JSON document only to append messages does not
force a view reset. If prior view records change/disappear, return no records,
`reset_required=true`, `reason="view_prefix_changed"`; do not append contradictory
records under existing sequence numbers. The returned reset position starts at
sequence 1 with an empty prefix, and `complete=false`. Start a new view generation
and call again with that position (or no position), as the wire v1 contract requires.

Each record has `seq`, `time`, `role` (`user`, `assistant`, `system`, `tool`),
`kind` (`text`, `tool_call`, `tool_result`, `image`, `document`, `notice`), bounded
`text`, nullable `tool` and `call_id`, and `raw_offset`:

```json
{"file_key":"main","offset":123}
```

JSONL offsets identify the beginning of a native record. Gemini legacy JSON uses offset zero
for the complete document, with a separate optional `native_pointer` field
(`/messages/N/...`) and `block_index` on the view record. `truncated`
flags view-only truncation. Raw content is never truncated to fit the view.
The initial view renders the primary conversation; subagent transcripts remain
raw companions, not interleaved into a guessed chronology. Unsupported blocks
produce a notice/reference, not invented text or executed tools.

## Fidelity boundaries

Raw means unchanged source bytes plus required companion files. Portable means
the explicit account-independent transfer transformation; it cannot promise the
same opaque state as raw. Even a complete raw set does not supply authentication,
project files, model availability or prove model recall. Exact context fidelity
requires loader-level comparison and, where appropriate, an authorized model
continuation. Reports distinguish these levels.

## Opaque native package descriptor

`package_descriptor` returns `available`, `reason`, `descriptor_version: 1`,
`manifest` and `members`. When available, `manifest` is a template with
`format: codex-session-librarian`, `version: 3`, `mode: raw`, provider-set
`created_at` (last recorded source time, then creation time, then Unix epoch;
stable for unchanged sources), and one `sessions` entry:

```json
{"id":"NATIVE_ID","provider":"claude","title":"...","cwd":"...",
 "primary":"sessions/NATIVE_ID/main.jsonl",
 "files":[{"file_key":"main","file":"sessions/NATIVE_ID/main.jsonl",
           "relative_path":"session.jsonl","role":"transcript"}]}
```

`members` maps each `file_key` to its archive `member`; every file is under
`sessions/NATIVE_ID/`. The registry copies the complete bytes for the snapshot's
file revision, fills `bytes` and `sha256` in each manifest file entry from stored
state, and writes `manifest.json`. No provider parsing or rewriting is required.
The primary file uses `main.jsonl` or `main.json` according to its source format;
companion paths use `files/<normalized-relative-path>`.

The registry must refuse an incomplete snapshot or unavailable descriptor. Format
v3 import support is being implemented on this branch; existing public v0.3.0
only accepts v1/v2. Do not assume a v3 package is loadable by old CSL. Availability
will be true only for profiles the new importer implements; native Gemini loader
verification is separately reported, not implied by byte packaging.

`available=false, reason=no_conversation` rejects metadata-only histories. Codex
sessions referencing an external `history_base` are still collectible byte for
byte but return `external_history_base_not_packaged`: this increment cannot
claim a self-contained native package for those sessions. Raw bytes alone are
not a proof of same-context resume. The registry must keep that distinction.

For development checks (explicit roots avoid personal histories):

```bash
csl providers list --provider claude --home /isolated/claude
csl providers files --provider claude --session UUID --home /isolated/claude
csl providers view --provider claude --session UUID --home /isolated/claude
csl providers package --provider claude --session UUID --home /isolated/claude
csl sessions --provider gemini --json
csl export GEMINI_UUID --provider gemini --raw --to /private/session.csl.zip
csl import /private/session.csl.zip --list
csl import /private/session.csl.zip --cwd /local/project
```

Gemini must be explicitly selected for export; automatic source selection,
portable conversion and team sharing remain Codex/Claude only. Native imports
create a new session ID and rebind local paths/policy; the archive's raw bytes
stay unchanged. They do not execute historical tool calls.

## Antigravity CLI extension

`provider=antigravity` uses the same four functions. Storage defaults to
`~/.gemini/antigravity-cli`, overridden directly by `CSL_ANTIGRAVITY_HOME`/`home`.
Gemini CLI remains separate. See [Antigravity profile](ANTIGRAVITY.md).

Inventory: `main` -> `session.db` (`transcript`), optional `sqlite-wal` ->
`session/session.db-wal` (`context`); both `mode=rewrite`. WAL includes only the
complete committed prefix. Global indexes, authentication and settings are
excluded. SQL operates on private copies, never the origin.

View results additionally include `source_files: [{file_key,length,sha256},...]`
for all DB/WAL inputs. The collector must bind **each** to its raw snapshot;
`source_file` alone cannot express a WAL-only change. This is additive; existing
providers retain their prior shape. `raw_offset.offset=0` means the whole DB,
with `native_pointer=/steps/N` for logical provenance, not a physical byte offset.

V3's primary member is `sessions/ID/main.db`, `relative_path=session.db`; WAL
uses the normal `files/` member rule. Registry assembly remains opaque. Active,
external-parent and subtrajectory-dependent sessions have an unavailable
descriptor; raw collection remains available. Native import creates fresh IDs
and rebinds local policy. Loader reconstruction was verified only on CLI 1.5.9
with a localhost fixture, not a commercial model or real auth.

Wire/collector/panel must admit the distinct provider value `antigravity`; never
alias it to `gemini`. CLI export requires `--provider antigravity --raw`;
portable/team/cross-harness routes are unchanged.

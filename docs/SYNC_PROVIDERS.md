# Local provider interface for session collectors — v1

Module: `csl_sync_providers.py`. Standard library only. No network, login, model
calls or transcript logging. This interface is provider-local and independent of
Vitral. The initial providers are `codex`, `claude`, `gemini`.

```python
list_sessions(provider, *, home=None, active_seconds=300) -> list[dict]
raw_files(provider, native_id, *, home=None) -> list[dict]
view_records(provider, native_id, *, position=None, limit=500,
             text_limit=16000, home=None) -> dict
```

`home` explicitly overrides that provider's root, including in tests. Defaults
use CODEX_HOME, CLAUDE_CONFIG_DIR and the Gemini user directory respectively.
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
  "role": "primary"
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
`tool_result`. No credentials, project files, arbitrary referenced paths or
symlinks are followed. File keys are `main`, or `f-` plus SHA-256 of the
normalized relative path. The relative paths, not local absolute paths, are used
to reconstruct a native package. Gemini JSON is rewritten, not JSONL append.

## Incremental view

Result:

```json
{
  "records": [],
  "position": {"version": 1, "provider": "claude", "native_id": "...",
               "next_seq": 0, "prefix_sha256": "..."},
  "reset_required": false,
  "reason": null,
  "complete": true
}
```

The position is opaque: persist it unchanged after uploading the returned page.
`seq` starts at zero. Continuation verifies the already-emitted **normalized view
prefix**, so rewriting a Gemini JSON document only to append messages does not
force a view reset. If prior view records change/disappear, return no records,
`reset_required=true`, `reason="view_prefix_changed"`; do not append contradictory
records under existing sequence numbers. The registry/collector needs a new view
revision or an explicit replacement operation for this case. A 409-only append API
without revision/reset cannot faithfully handle all source rewrites.

Each record has `seq`, `time`, `role` (`user`, `assistant`, `system`, `tool`),
`kind` (`text`, `tool_call`, `tool_result`, `image`, `document`, `notice`), bounded
`text`, nullable `tool` and `call_id`, and `raw_offset`:

```json
{"file_key":"main","byte_offset":123,"json_pointer":null,"block_index":0}
```

JSONL offsets identify the beginning of a native record. Gemini uses offset zero
for the complete JSON document and `/messages/N/...` JSON pointers. `truncated`
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

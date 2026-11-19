# Codex And Claude Workload Text Profiles

Internal source-only providers for the existing Collector. No new CLI, device
flow, image, watch or final-drain behavior is activated. Existing desktop raw
providers and capabilities are unchanged. These profiles depend on the separate
W7 person-approved workload identity, server namespace/view-only enforcement and
the later one-origin runner. They do not qualify A8 or any live harness.

## Qualification Pins

`WorkloadTextProvider` requires explicitly selected JSONL transcript files,
never a harness home, registry, credential file or companion-file search:

- `codex-0.169.0-text-v1`: a first `session_meta` header declaring
  `cli_version: 0.169.0`, complete response-item text/call/result records, bounded
  auxiliary event metadata, model and title updates. A fresh paginated stream is
  accepted only without a base and with exact contiguous integer ordinals from
  zero, matching `csl_codex_history._node`'s no-base boundary. Multiple headers,
  gaps, boolean ordinals and external bases refuse; no ancestor is resolved.
  Before selection, both header and payload refuse attached threads, parent/fork
  links and CSL import/conversion markers. An explicit source must be `cli`,
  `vscode` or `exec`; structured subagent/internal/custom and unknown sources
  refuse in legacy and fresh paginated streams. An absent source is compatible
  with existing main-stream fixtures, not evidence of independently verified
  native provenance.
- `claude-2.1.308-text-v1`: main-stream user/assistant records declaring
  `version: 2.1.308`, stable session identity and an unambiguous linear UUID
  chain. Parent links may cross known auxiliary records; branches, duplicate
  main UUIDs, unresolved main parents, sidechains and compact-summary records refuse.

These are synthetic fixture profiles using CSL's existing provider renderer and
documented native versions, not new evidence of an actual executable or model
turn. Source references are the existing CSL provider/tests and
[provider matrix](PROVIDER_MATRIX.md). Claude is not inferred from Gemini or
Antigravity. Missing/different versions refuse. No native process is launched to
discover a version, and a transcript version field is not an identity authority.

## Selected Representation

Every completed native input record produces one canonical compact UTF-8 JSONL
record with `workload_profile_v1`. Only the declared readable text/tool fields,
necessary IDs, timestamp/model/title and safe event tags are selected. CWD,
repository, instructions, auxiliary private payloads and unneeded native
metadata are not copied. The source file itself is unchanged.

Known Codex reasoning records and Claude thinking/redacted-thinking blocks are
explicitly excluded from this view-only representation. Their contents,
signatures, ciphertext and per-field lengths/hashes are NOT uploaded or retained
in the sanitized stream. Private source cursors still hold the raw complete-file
length/hash for change detection, separate from delivery and never on the wire.
A fixed `workload_omission_v1` notice states that reasoning is
omitted. Codex native compaction state likewise becomes a fixed notice, never
an assertion that a resumable checkpoint survived. This is a qualified omission
policy, not a decoder/scan of opaque bytes or a raw fallback.

Images, documents, unknown content blocks/control records, opaque DB/WAL,
external Codex history and CSL-imported/converted/forked provenance refuse.
Auxiliary event tags are a closed subset. Timeline rollback/reset and Claude
`compact_boundary` controls are not qualified and refuse rather than leaving a
misleading old view active under a claimed successful new snapshot.
The profiles do not follow parent sessions, tool-result files or arbitrary
paths. They do not reconstruct hidden context, execute tools or promise native
resume. A literal readable tool-result reference remains readable text only;
referenced file bytes are not collected. Packages always report unavailable.

CSL's existing readable-text patterns run over selected text, views and
metadata before serialization or truncation. Structured credential fields are
removed; Codex JSON-encoded call arguments are decoded before that check, then
encoded again. Detection is partial: arbitrary unknown, encoded or split
secrets can be missed. Structural IDs which cannot survive safely refuse.

## Codex Tool Declaration Omission

The declared `codex-0.169.0-text-v1` subset additionally recognizes the native
`response_item` / `additional_tools` catalog. The primary schema is
[`ResponseItem::AdditionalTools` at Codex rust-v0.169.0,
`d32063f96c2dff1dc96af839b3952abb6acfeda4`](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/protocol/src/models.rs#L1097).
Its wire envelope is the pinned
[`RolloutItemWire::ResponseItem`](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/history/src/rollout_payload.rs#L41).

This extension accepts only `type`, `role`, `tools` and optional `id`. Role must
be system/developer/user/assistant; tools must be a JSON array of objects
(including an empty array), not encoded JSON or opaque bytes. Every native
descendant is structurally validated before omission. Known encrypted/binary
field names and media blocks (including native `input_audio`) refuse even beneath
credential keys. Native `image_url`/`audio_url` inline data URLs also refuse,
including wrapped `url` values and normalized scheme spelling; no decoding or
fetch occurs. These field names in readable JSON-schema declarations remain
valid, and ordinary HTTPS reference strings are omitted without dereferencing.
Schemas are not executed; this does not qualify every arbitrary JSON value
allowed by the native schema, or broaden tool-call argument parsing.

The entire catalog becomes the fixed notice "Native tool declarations omitted;
workload text is view-only." No declaration, identifier, role, description,
schema, argument, count or catalog hash is transmitted. Its notice offset is
in the sanitized stream. A catalog-only edit leaves projected bytes unchanged;
no raw-source coordinate becomes an ACK. Other previously unsupported response
types, fork/import/attached/history-base cases and opaque payloads still refuse.

The Codex policy fingerprint adds `tool_declarations_omission: 1`; Claude and the
provider/profile packaging ABI are unchanged. Unaffected Codex projected bytes
stay identical, and identical sanitized bytes/view may retain acknowledged
lineage across this policy change. This is a separately reviewed source
extension, not a substitution into the frozen `e00fd7ee` runtime/image. Operators
must select an explicitly reviewed corrected source/artifact pin and rerun the
offline qualification on the already-created real source. Synthetic tests and
an operator's record-type report do not qualify a new real upload or A8.

## Codex World And Usage Metadata

The Codex subset also recognizes exactly the native `world_state` and
`token_usage_record` envelopes from the same pinned 0.169.0 source:
[`WorldStateItem`](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/protocol/src/protocol.rs#L3643)
and [`TokenUsageRecord` / `TokenUsage`](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/protocol/src/protocol.rs#L2545).

World state requires only `full` (an actual boolean) and `state` (a JSON object).
Both full snapshots and patches become the same fixed notice: "Native world
state omitted; workload text is view-only." No state is replayed, merged or used
as authority. The existing bounded structural validation and reviewed opaque /
inline-media traversal apply before omission, including credential descendants.

Usage requires the five bounded native identity strings plus `usage`,
`turn_token_usage` and `thread_token_usage`. Thread/session IDs must match the
main header. Each counter object is closed to the pinned signed-i64 fields:
`input_tokens`, `cached_input_tokens`, `output_tokens`, `reasoning_output_tokens`,
`total_tokens`, and optional `cache_write_input_tokens` (the upstream serde
default). Booleans, floats, out-of-range values, missing/unknown fields and
structured/opaque counter payloads refuse. This validates representation, not
provider accounting; no sums, costs or nonnegative-use claim are inferred.
Every usage record becomes only "Native token usage omitted; workload text is
view-only." No native counter values/names, turn/response IDs, state contents or
body hashes are transmitted. Notice coordinates remain sanitized coordinates.

Codex policy adds `native_metadata_omission: 1`; the previous tool-declaration
policy, package refusal, provenance/history/argument checks and global redaction
are unchanged. Catalog-only / world-state-only / usage-only edits that leave
sanitized bytes and view identical retain their ACK lineage. Unknown record
types still refuse; this is not a general skip-metadata rule. The corrected
runtime is separately pinned and reviewed: do not modify an older checkout or
image to impersonate this source. Reuse the existing executor-selected real
transcript for a fresh full provider preflight after exact-head review and CI;
neither enrollment nor another model turn is required to qualify source parsing.

## State And Delivery

The shared `PreparedProvider` contains the formerly Gemini-only explicit-root,
private spool, complete-prefix check, view pagination and unavailable-package
boundary. Gemini's source-key mapping, redaction fingerprint, native reducer and
projected bytes stay unchanged. Its append/patch/replacement regressions remain.
Codex/Claude policy fingerprints additionally bind the selected profile.

Raw source depth/value limits are checked before selecting fields, including
unselected auxiliary data. Decoded Codex argument trees are additionally charged
to that same capture-wide value budget across every call, at their nested depth,
before credential replacement. The original encoded strings remain charged;
there is no per-call reset or credit for removed credential subtrees. Every
decoded key/value, even under an escaped or nested credential key, must have
valid UTF-8 text and finite numbers. The shared redactor also validates its whole
input before replacing any subtree. Record/source,
serialized output/view and total spool limits retain the merged bounds; view
construction is incremental rather than building an unbounded rendered list.
Unknown formats fail before session upload; no source path/Git discovery occurs.

Private native source cursors are distinct from ACKed sanitized offsets/hashes
and view cursors. Each view offset lands on its sanitized record, not a native
file offset. Existing durable pre-write plans, revision/view pairing and
idempotent upload are reused. An identical projected result, including a changed
excluded reasoning payload, retains its revision. Appended title metadata keeps
the raw revision and unchanged view; historical selected-byte changes replace
the revision and view, even for unchanged rendered text. Lost responses and
journal reopen resume the same plan.

As before, one collector owns an exclusively locked store. Runtime integration
must use private state children beneath the per-replica volume, not chmod its
mount root. A partial tail is outside the current projected prefix; a pass
synced that prefix, not the producer's final exit. The future runner must still
implement the frozen 20-second suspend-inclusive ACK-based final drain and
report pending/incomplete on partial EOF, outage, deadline or ambiguity.

## Evidence Limits

`tests/test_workload_text.py` uses owned temporary synthetic transcripts and the
existing loopback registry contract peer. It checks canaries across metadata,
spool, view and wire, explicit reasoning omissions, native source preservation,
UTF-8/incomplete tails, branch/history/media/version refusal, bounded discarded
data, policy changes, byte offsets and actual uploader lost ACK/restart.
All merged Gemini cases also run after the plumbing extraction.

This does not exercise a live Codex/Claude login, provider call, native loader,
W7 approval/token refresh, production registry, UI, kind/ACA, image or process
exit. Healthy visibility and final-delivery targets remain unqualified.

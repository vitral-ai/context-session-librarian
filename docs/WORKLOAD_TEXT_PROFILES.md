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

## State And Delivery

The shared `PreparedProvider` contains the formerly Gemini-only explicit-root,
private spool, complete-prefix check, view pagination and unavailable-package
boundary. Gemini's source-key mapping, redaction fingerprint, native reducer and
projected bytes stay unchanged. Its append/patch/replacement regressions remain.
Codex/Claude policy fingerprints additionally bind the selected profile.

Raw source depth/value limits are checked before selecting fields, including
unselected auxiliary data. Non-finite numeric values refuse. Record/source,
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

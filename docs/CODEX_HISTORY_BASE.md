# Referenced Codex histories

Some Codex forks and reverts store their earlier history in other rollout files.
The leaf JSONL alone does not contain that context. CSL 0.4.0's experimental raw v3
profile includes those files and preserves the native reference on import.
Published v0.3.0 does not read v3 packages.

## Use and boundaries

```bash
csl providers files --provider codex --session UUID --home /isolated/codex
csl providers package --provider codex --session UUID --home /isolated/codex
csl export UUID --provider codex --raw --to /private/session.csl.zip
csl import /private/session.csl.zip --list
csl import /private/session.csl.zip --cwd /local/project
```

The provider and collector include all required ancestors, not project files,
credentials, tools, external attachments, or a complete working environment.
This profile targets **Codex 0.169.0 paginated JSONL**. Portable, converted and
batch exports of referenced sessions are outside this profile; portable/batch
export rejects rather than silently omitting a base. Title overrides, static
context limits and scrubbing during raw export are rejected: changing raw records
requires rebuilding byte coordinates, not deleting the reference. Review raw
exports before sharing. Imported historical instructions are context, not
authority to execute tools or resume work.

The existing single-file library/search/clone commands are not upgraded by this
provider change. Compressed **ancestors** are supported; discovery of a session
whose current primary rollout is compressed remains outside this profile.

## Reference semantics

Pinned official source: [`rust-v0.169.0`, commit
`d32063f96c2dff1dc96af839b3952abb6acfeda4`](https://github.com/openai/codex/tree/d32063f96c2dff1dc96af839b3952abb6acfeda4).
The installed native CLI used for this profile reports `codex-cli 0.169.0`.

- [`HistoryPosition`](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/protocol/src/protocol.rs)
  contains `thread_id`, `end_ordinal_exclusive`, `end_byte_offset`. There is no
  path or content hash in the reference. The ID denotes the **physical rollout**,
  which can differ from the stable owner/thread ID after a revert.
- [`rollout_file_name.rs`](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/rollout/src/rollout_file_name.rs)
  specifies `rollout-TIMESTAMP-OWNER[_ROLLOUT].jsonl[.zst]`.
  [`list.rs`](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/rollout/src/list.rs)
  searches active and archived files by rollout ID. CSL rejects conflicting valid
  copies of an immutable rollout rather than selecting an arbitrary history.
- [`rollout_lineage.rs`](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/thread-store/src/local/rollout_lineage.rs)
  resolves chains. Offsets count **decoded JSONL bytes**, even for compressed
  files. The cutoff must end at a complete record; ordinals must agree with it.
  A child header starts at the inherited end ordinal; subsequent local records
  advance it. Ancestor headers do not become additional model instructions.
- [`model_context.rs`](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/rollout/src/model_context.rs)
  and the thread store select native context from these segments, including
  compaction/window rules. CSL preserves those records; it does not substitute
  a reconstructed conversation or claim all records fit a model window.
- [`compression.rs`](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/rollout/src/compression.rs)
  can replace plain archived files with compressed copies. Native
  [`delete_thread.rs`](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/thread-store/src/local/delete_thread.rs)
  protects externally referenced rollouts; manual deletion or concurrent changes
  can still make a snapshot unavailable. CSL rechecks required prefix hashes.

## Snapshot and independent import

The primary file retains key `main`, role `transcript`. Each ancestor uses a
stable `base-<rollout-UUID>` key and existing role `context`. A plain ancestor
contributes only its required byte prefix, in append mode. A compressed ancestor
contributes the original compressed file in rewrite mode: **it may also contain
later, non-inherited source records**. Export secret review inspects the entire
decoded compressed stream, including that tail. Review remains partial and does
not make raw sync a sanitized upload.

Compressed input needs Python's `compression.zstd` (3.14+) or an already installed
`zstd` executable. The core does not install a decoder or Python dependency.
Missing decoders, corrupt streams, missing bases, symlinks, cycles, ordinal/byte
disagreement, unknown reference shapes and conflicting copies fail explicitly.
Attached-thread histories are explicitly unverified and rejected in this profile.
The maximum is 32 ancestor links, 2 GiB per stored/decoded file and 4 GiB per
package. A referenced plain prefix can be bounded within a larger source file.

The v3 manifest maps every ancestor and primary file to its native basename,
owner, physical rollout ID and decoded boundary. Import validates the complete
graph, stages it, assigns new rollout IDs and rebinds owner identities. It writes
ancestors under `archived_sessions` and the new primary under `sessions`.
Source account/user discovery bindings are removed from installed metadata;
they remain untouched in the raw archive. Context-window records are retained.
`history_mode: paginated` and every `history_base` remain present. Offsets are
recomputed after identity/cwd changes; inherited ordinal cutoffs stay unchanged.
Opaque records and response/tool payloads are retained. No original is modified;
publication rolls back created files on failure. Repeated imports create disjoint
graphs. Historical external logical-parent metadata is provenance, not a live
link to an unavailable source context.

## Evidence and the native projection cache

Three claims remain distinct:

1. **Structural/raw:** fixtures test exact bounded source bytes, chains, reverts,
   compressed ancestors, offsets, inherited views, checksums, source fences,
   repeat imports, rollback and negative inputs.
2. **Native load:** the opt-in test in `tests/test_codex_history.py` uses Codex
   0.169.0 with disposable homes, no credentials and no `turn/start`. After a
   native bounded ephemeral fork reconstructs projection caches, `thread/read`
   shows both ancestor and child messages. The same sequence was checked on a
   local three-file real-history snapshot: 5,352 visible items were identical
   before/after import; no transcript text is shipped as a fixture or report.
3. **Live model recall:** not tested for this profile. Native UI reconstruction
   is not a universal same-context/model-continuation guarantee.

**Fresh-home caveat:** `thread/read` alone returned zero turns for raw paginated
files. Codex 0.169.0 reads its SQLite projection without rebuilding it. A bounded
ephemeral `thread/fork` with a completed `lastTurnId` reconstructs required
projections through Codex's own parser, then `thread/read` returns the history.
This was an explicit verification step, not an action performed by import. CSL
does not silently activate threads, create forks or edit native SQLite databases
to hide this behavior. Immediate native UI display in a fresh home remains
unverified without that reconstruction step. Source/model-context bytes are
present; this is a separate native cache/discovery usability limitation.

The collector already transfers multiple opaque raw files; this change additionally
requires every Codex graph input to be bound in each view page. The registry's
existing v3 assembler preserves extra entry fields and all inventory members;
no new provider, role, endpoint or registry parser is required. Registry behavior
was checked against its contract and assembler source, not a new deployed
end-to-end registry test.

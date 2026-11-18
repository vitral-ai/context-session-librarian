# Sanitized Workload Provider Foundation

This is an internal, source-only integration seam, not an enabled collector
command or a deployable workload image. Existing desktop raw sync and the public
provider/command matrix are unchanged. Workload device authorization, registry
namespaces, continuous collection, final flush and the job wrapper are separate
changes. Do not route workload transcripts through the existing raw CLI.

## Qualified Input

`csl_gemini_workload.WorkloadGeminiProvider` reads explicitly selected transcript
files beneath explicit, non-symlink roots. It never discovers a harness home,
project registry, credential file, SQLite database or WAL. Its only qualified
format is the text-only main conversation JSONL written by Gemini CLI 0.68.0,
pinned at `5a5c7180ff3f0a02511f54b54db7f228d4982a30`:

- [Recording types](https://github.com/google-gemini/gemini-cli/blob/5a5c7180ff3f0a02511f54b54db7f228d4982a30/packages/core/src/services/chatRecordingTypes.ts)
- [Recording reader/writer](https://github.com/google-gemini/gemini-cli/blob/5a5c7180ff3f0a02511f54b54db7f228d4982a30/packages/core/src/services/chatRecordingService.ts)

Synthetic fixtures exercise metadata, text messages, tool arguments/results,
patches, duplicate IDs, ordering, inclusive rewinds and checkpoints. They do not
prove a real Gemini invocation or every possible transcript from that release.
Bare metadata requires `sessionId` and `projectHash`; its embedded messages merge
into the ordered map, preserving existing history and duplicate-ID positions.
Only `$set.messages` replaces the history. Other bare shapes refuse.
Unknown fields/control records, non-text parts, subagents, opaque attachments and
legacy JSON snapshots refuse rather than falling back to raw bytes. Antigravity
remains a distinct provider; Codex and Claude workload projections are not yet
implemented by this module. There is no native Gemini import/resume support.
In particular, structured `functionResponse` parts and non-text `resultDisplay`
objects are not qualified by this initial profile; their presence refuses the
session, not just the individual part. General Gemini tool-session coverage
requires a subsequent qualified extension; these tests establish the explicit
text-result subset only.

## Sanitized Coordinates

Projection v1 parses only complete newline-terminated records. A partial trailing
record, including incomplete UTF-8, remains local until complete. Invalid complete
records refuse. The existing `csl_secrets` readable-text patterns run by default
over text and metadata, along with credential-field removal and optional literal
redactions. Structural IDs containing a detected secret refuse. Known opaque
representations refuse. Pattern matching is partial: unknown secrets and encoded
or split secrets can be missed. This is not universal secret detection.

The projection is sorted, compact UTF-8 JSONL. Only sanitized bytes, sanitized
metadata and normalized views reach the existing uploader. `raw_files` is the
legacy interface name, but its path here points exclusively to the sanitized
private spool. Package descriptors always report `available: false` with
`workload_view_only`; no raw package, native resume or opaque fallback is offered.

The private `source-cursors` files record source inode/complete-prefix hash and
projection policy fingerprint separately from uploader state. They describe a
local capture, NOT delivery. The existing `State` journal alone records ACKed
sanitized lengths/hashes, snapshot identities, idempotency keys and view cursors.
All wire ranges/hashes and view `raw_offset` values refer to sanitized bytes.
Offsets identify the sanitized record which last defined/patched a message.

Appending compatible sanitized bytes retains the spool inode and raw revision.
An appended patch which changes historical rendered text retains that raw
revision but resets the view generation. Rewriting historical sanitized bytes,
including a policy change which changes that prefix, atomically replaces the
spool; the uploader starts a new raw revision and matching view generation.
Identical projected bytes under a new policy can resume the existing revision.
For this provider the uploader durably binds the planned view generation to its
file revision map before remote writes. A replacement revision resets the view
even if only metadata/unrendered fields changed or the view is empty. A lost ACK
and restart replay the same persisted plan; source-capture checkpoints cannot
authorize or advance it. Journals predating this binding derive the prior map
from their persisted file plan. Existing desktop providers do not opt into this
additional pairing rule.
Lost metadata/content/view responses use existing ACK validation and idempotent
retry, including after state reopen. No source cursor asserts delivery success.

## Bounds And Ownership

Defaults are 32 explicit files, 16 MiB per source and sanitized file, 1 MiB per
record, 20,000 physical records and view rows, 200,000 parsed values, depth 32,
and 32 MiB total sanitized spool. Source size is checked before payload reads;
record reads, output construction and view construction are bounded. Spool
capacity includes prior streams and refuses rather than silently deleting them.
Operators may supply smaller bounds. These are source safety bounds, not a
qualified packaging memory budget or throughput claim.

Only regular transcript files are opened through anchored no-follow directory
FDs. Paths must stay below the selected root. No referenced files are opened.
The complete source prefix is revalidated, allowing concurrent append but not a
rewrite. Original files are never modified. Private directories/files use
0700/0600. One collector holding the existing exclusive collector lock owns a
projection store; concurrent unlocked use is unsupported. Temporary and
checkpoint files never contain original transcript text. A private source hash
is local bookkeeping, never uploaded provenance.

`Collector` accepts a trusted provider object's explicit `supported_providers`
tuple; the ordinary provider adapter still uses the unchanged desktop defaults.
This seam is not authentication or server-side authorization. Production use
must first supply the separately reviewed workload identity/registry boundary,
one-origin HTTPS enforcement and independent per-replica private state.

## Evidence And Remaining Lifecycle Work

`tests/test_workload_gemini.py` uses synthetic transcripts and an owned loopback
HTTP registry contract fixture. It checks sanitization of metadata, upload,
spool and view; complete-record boundaries; bounded reads/storage; mutation and
append races; lost ACKs; restart; and raw/view generation changes. No harness,
provider request, Google login, real registry or personal transcript is used.

This PR does not implement a continuous watch, device proof or termination
handler. The frozen lifecycle contract for the subsequent runner is a maximum
20-second, suspend-inclusive final drain after producer quiescence, with at least
30 seconds packaging termination grace. Only ACKed final bytes AND view may be
called complete. Deadline, outage, partial EOF or ambiguity must remain pending
or incomplete. The 3-second record-to-ACK plus 2-second foreground visibility
targets remain unqualified until the full fixture and UI integration runs.

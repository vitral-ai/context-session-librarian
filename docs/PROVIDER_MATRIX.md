# CLI Coverage And Limits

Experimental mailbox commands, local grant inspection, own-endpoint
MCP, session-bound boundary hooks and separately opted Codex queue notifications
are included in CSL 0.4.0 and specified in [agent communication](AGENT_COMMUNICATION.md).
Physical cross-OS mailbox and WAN tests remain distinct from coordination tests.

CSL supports **native Codex -> Codex and Claude Code -> Claude Code handoffs**.
Explicit experimental conversion in both directions is integrated through
canonical v2 for text, references, completed tools, embedded images and PDFs. See
[conversion and continuity](CONVERSION.md) for the exact profile and commands.
"Provider" in CSL means the transcript/CLI adapter (`codex`, `claude`, or the
experimental local `antigravity` adapter), not
the model vendor, API endpoint, subscription or billing account.

This matrix describes commands in this checkout, not capabilities that exist only
in a separate portability experiment. `supported` is bounded by the limits below;
it does not mean every historical transcript or future CLI version is compatible.
`experimental` depends on evolving native formats. `not-integrated` is unavailable
in this public tool, even if a related laboratory experiment has passed.

## Command Matrix

The experimental `sync` command publishes through the local provider interface
for Codex, Claude Code and Antigravity CLI to a registry you configure.
Raw sync uploads transcripts including any secrets they contain. See [sync](SYNC.md)
and [Antigravity scope and limitations](ANTIGRAVITY.md).
Codex 0.169.0 external `history_base` chains are included in raw v3 snapshots and
single-session `export --raw`. Portable/batch export of referenced histories
rejects rather than omitting ancestors. Native `thread/read` requires rebuilt
projection caches in a fresh home; import does not activate threads or rebuild
those caches. See [Codex lineage profile](CODEX_HISTORY_BASE.md).
Antigravity CLI is a distinct experimental SQLite/protobuf provider. CLI 1.5.9
has localhost reconstruction evidence and a bounded authenticated owner recall
check; broader profiles remain unverified. See [scope and limitations](ANTIGRAVITY.md).

The table is mirrored by `csl capabilities` / `csl capabilities --json`.
Tests require every registered command to appear and this table to match the
registry. Update implementation, tests and this document in the same change.

| Command | Codex | Claude Code | Antigravity CLI |
| --- | --- | --- | --- |
| `annotate` | supported | not-integrated | not-integrated |
| `backup` | experimental | not-integrated | not-integrated |
| `bridge` | experimental | experimental | not-integrated |
| `capabilities` | supported | supported | not-integrated |
| `cockpit` | supported | not-integrated | not-integrated |
| `convert` | experimental | experimental | not-integrated |
| `cut` | supported | not-integrated | not-integrated |
| `doctor` | supported | supported | not-integrated |
| `export` | supported | supported | experimental |
| `find-work` | supported | not-integrated | not-integrated |
| `import` | supported | supported | experimental |
| `inbox` | supported | supported | not-integrated |
| `index` | supported | not-integrated | not-integrated |
| `light` | experimental | experimental | not-integrated |
| `migrate` | experimental | not-integrated | not-integrated |
| `native-clone` | experimental | not-integrated | not-integrated |
| `native-cut` | experimental | not-integrated | not-integrated |
| `native-sanitize` | experimental | not-integrated | not-integrated |
| `patches` | supported | not-integrated | not-integrated |
| `pinpoint` | supported | not-integrated | not-integrated |
| `providers` | experimental | experimental | experimental |
| `queue` | supported | not-integrated | not-integrated |
| `recall` | supported | not-integrated | not-integrated |
| `receive` | supported | supported | not-integrated |
| `recent` | supported | not-integrated | not-integrated |
| `related` | supported | not-integrated | not-integrated |
| `search` | supported | not-integrated | not-integrated |
| `sessions` | supported | supported | experimental |
| `share` | supported | supported | not-integrated |
| `show` | supported | not-integrated | not-integrated |
| `stats` | supported | not-integrated | not-integrated |
| `summarize` | supported | not-integrated | not-integrated |
| `sync` | experimental | experimental | experimental |
| `team` | supported | supported | not-integrated |
| `timeline` | supported | not-integrated | not-integrated |
| `verify-live` | experimental | experimental | not-integrated |
| `why` | supported | not-integrated | not-integrated |

`convert` is an experimental, fail-closed adapter rather than a universal format
translator. `doctor` reports structural/static/native evidence separately and
does not call a live model. Native import preflight runs by default; isolated
native replay is opt-in with `doctor --native`.

`migrate upload/download` loops over existing Codex share/receive operations for
the normal interactive resume list, with a small migration index. It excludes
archives/non-interactive sources and does not migrate project files or accounts.
`migrate cleanup` previews by default and archives unchanged originals only with
`--confirm-restored`; it never deletes files. See [migration](SHARING.md#computer-migration).

`bridge` coordinates existing Codex conversations through an explicitly configured
compatible runtime, transport and peer permissions. Linux–Mac LAN coordination has
bounded real-model evidence; Internet, Windows and live Claude coordination remain
unvalidated. The macOS app is source only, not a validated binary. See
[bridge compatibility and setup](BRIDGE.md).

**Development mailbox v1:** task-independent `bridge send`, `receive`, `ack` and
`reply` also support shell/MCP clients such as Claude Code, using their own
registered session endpoint. This capability persists mail without waking or
steering a model. Explicit receive and session-bound hooks are available;
bounded real CLI exchanges and Claude hook/MCP sessions have been exercised.
The separate owner-opted Codex notifier requires a local activate grant and an
existing owned runtime. Claude support does not imply arbitrary idle wake,
search or conversion guarantees. See the [communication specification](AGENT_COMMUNICATION.md)
for evidence boundaries. Earlier v0.3.0 predates this capability, included in 0.4.0.

`backup` is included in the
[v0.3.0 release](https://github.com/vitral-ai/context-session-librarian/releases/tag/v0.3.0)
as an **experimental manual Linux/Codex-only feature**, separate from migration
and team sharing. It provides encrypted native/portable conversation checkpoints,
explicit policies, logical-head incremental reuse, partial coverage, multiple
sources, durable resume and explicit future recipient rotation.

Backup requires externally installed age v1.3.2 and distinct operational/external
recovery public recipients. Keep the external recovery private identity outside
the protected computer; repository access alone cannot decrypt backups. Positive
system-keyring/headless operation remains unvalidated. Scheduling, automatic
retention/garbage collection, historical re-encryption and native-sanitized backup
profiles are not included. See [backup scope and custody](BACKUP.md).

Codex portable export/share and migration upload support explicit
`--repair-legacy-compaction` for the recognized orphaned final summary annotation
of old portable copies. Defaults remain strict; readable turns and source files
are preserved, missing encrypted context is labeled, and other damage is rejected.
See [legacy recovery](SHARING.md#legacy-compaction-recovery).

Codex-only `native-clone --visibility interactive|agent|archived` controls the
new clone's presentation. Default `interactive` uses source `cli`; `agent` uses
native source `exec` and stays outside the default resume picker; `archived`
stores the clone in `archived_sessions`. Agent continuations/restarts were
checked offline on Codex 0.166.0. This is not access control or a permanent hidden
flag, and is not integrated for Claude, import/receive, cut or sanitize.
See [commands and lifecycle limits](COMMANDS.md#native-clone).

`sessions --provider claude` scans main project transcripts and filters UUID/title
with `--query`; it is not full-history semantic search. Codex listing uses its
existing index, refreshed with `csl index`. Inbox and team commands manage a
shared exchange, not the CLI's own built-in picker. `summarize` prepares a local
context package; it does not buy model inference or generate a cloud summary.
Only explicit `verify-live --allow-paid` performs budgeted synthetic model calls;
see [live verification](LIVE_VERIFICATION.md).

## Commands For Each CLI

`share` and `export` auto-detect the source CLI. They search both histories for
UUIDs/prefixes and titles; an explicit JSONL path is detected from its contents.
One match is selected automatically. Matches in both CLIs stop with a request
for `--provider codex` or `--provider claude`, without exporting/uploading a package.
Multiple matches within one CLI require a more specific UUID or file path.
`--provider` remains an optional source filter, never a conversion request.
Basic discovery with `sessions` still selects one CLI (default: Codex):

```bash
csl sessions --provider codex
csl export CODEX_UUID --to ~/Downloads
csl share CODEX_UUID --title "Codex handoff"

csl sessions --provider claude --query "backend" --json
csl export CLAUDE_UUID --to ~/Downloads
csl share CLAUDE_UUID --title "Claude handoff"
# If the UUID/title also matches Codex:
csl share CLAUDE_UUID --provider claude --title "Claude handoff"
```

Both source adapters accept a UUID, unambiguous title or JSONL path (Codex title
lookup requires its index). Claude UUID lookup excludes nested subagent files.
Without selectors the existing indexed Codex picker remains available and labels
its entries as Codex; the chosen file path is unambiguous even if another CLI has
the same UUID. `--provider claude` without a selector still requires a UUID/path
(discover with `csl sessions --provider claude`). Mixed-provider batches remain
unsupported; export/share them separately.

The receiver auto-detects the native CLI from the validated package. An optional
`--provider` asserts the expected CLI; it does **not** request conversion:

```bash
csl import ~/Downloads/handoff.csl.zip --cwd ~/work/project --open
csl receive csl-xxxxxxxxxxxx --cwd ~/work/project --open
csl receive csl-xxxxxxxxxxxx --provider claude --cwd ~/work/project --new --open --model haiku
csl receive csl-xxxxxxxxxxxx --target codex --cwd ~/work/project --open
csl convert SOURCE_UUID --to claude --cwd ~/work/project
csl doctor SOURCE_UUID --native --json
```

Everything after `--open` is a literal CLI argument. CSL options must come first.
No shell evaluation or automatic translation of provider flags occurs:

| Purpose | Codex argument after `--open` | Claude Code argument after `--open` |
| --- | --- | --- |
| Model | `--model MODEL` | `--model MODEL` |
| Additional working directory | `--add-dir PATH` | `--add-dir PATH` |
| Approval/permission policy | `--ask-for-approval POLICY` | `--permission-mode MODE` |
| Explicit bypass | `--dangerously-bypass-approvals-and-sandbox` | `--dangerously-skip-permissions` |

These are different controls, not equivalent security guarantees. Neither bypass
is enabled by CSL. Flag validity and model availability belong to the installed
CLI. Forwarding a prompt or `--print` can explicitly start inference; plain
`--open` does not submit a prompt. Claude launches with process CWD set to
`--cwd`; Codex uses `-C`. Neither downloads the project repository.

## Transfer And Content Matrix

| Area | Codex | Claude Code |
| --- | --- | --- |
| Native destination | `$CODEX_HOME/sessions` | `$CLAUDE_CONFIG_DIR/projects` |
| Default home | `~/.codex` | `~/.claude` |
| Identity | Fresh session UUID | Fresh session and graph-node UUIDs |
| Historical text/tool results | Preserved; modern UI events projected when needed | Preserved; graph references rebound, tools not rerun |
| Portable cleanup | Encrypted reasoning/state removed with aligned checkpoint metadata | Thinking/redacted-thinking removed; graph node retained with an omission marker if empty |
| System prompt snapshots | Native Codex replay behavior; local settings not copied | Sender's `system-prompt` snapshots omitted on import, including raw import |
| Compaction | Structured checkpoints retained; inconsistent metadata rejected | Native records/summary retained; summary may start without older ancestors; graph checked |
| Parent history / branches | Missing fork ancestry warned, not invented | Branch graph retained; missing ordinary parents, conflicts and cycles rejected; no branch selector |
| External attachments / repository / credentials | Not collected | Not collected |
| Embedded content | Existing JSONL content preserved; no fetching | Existing message blocks preserved; no fetching |
| Raw mode | Ciphertext may fail under a different account/model | Signed thinking may fail under a different account/model |
| Cross-CLI conversion | Codex -> Claude: experimental text/tools/references/images/PDF | Claude -> Codex: experimental text/tools/references/images/PDF |

Conversion supports bounded embedded images/PDFs with an optional isolated
decoder; external assets, animated images, audio/video/SVG and unknown state
remain unsupported. Native same-CLI preservation remains unchanged.
Source policies are quarantined only
by the canonical conversion path, not retroactively removed from native Codex
history. Updated portable exports mark lost encrypted compaction state; exact
conversion then requires readable state, or an explicit labeled visible recovery.
Native probes do not perform paid inference or guarantee local prompt overhead fits.

Mixed-provider bundles are rejected; send each CLI's sessions separately. Codex
bundles remain format v1 for compatibility; Claude bundles/releases use v2 so
older CSL installations reject/skip them instead of importing as Codex. Both
sender and receiver must update CSL for Claude sharing. Repeat receives reuse the
latest local copy by provider; `--new` creates another independent copy.
Claude transcripts currently have a 64 MiB per-session adapter limit because
graph validation is in memory; Codex retains its 2 GiB per-session limit. ZIP
validation still applies the shared 4 GiB uncompressed total limit.

## Authentication And Backend Providers

CSL does not log into either service, copy credentials, select accounts, install
cloud resources or translate model names. Each CLI uses the recipient's own
authentication and configuration. Export/import without `--open` does not need
either CLI binary or a model login.

| Backend/account mode | What CSL does | Verification scope |
| --- | --- | --- |
| Codex ChatGPT login or API/custom endpoint | Transfers native Codex records, leaves local auth/config alone | No guarantee that a source model is available to the recipient |
| Claude subscription or Anthropic API | Transfers native Claude records, leaves local auth/config alone | Localhost loader tested; synthetic live suite additionally checked Anthropic API, not subscription routing |
| Claude Bedrock / Vertex / Foundry | Same native adapter; local CLI owns routing/auth | Backend-specific continuation not tested by CSL |
| Other CLIs | No adapter | Not supported |

Private GitHub access and checksums are not secret redaction. Conversation text
can contain secrets. No transcript, real ZIP, auth file or provider credential
belongs in this public repository.

## Verification And Maintenance

Codex import/receive attempts native picker metadata discovery after installing
files; migration defers this to one refresh per batch attempt, including partial
success. `index --native` refreshes previously installed histories explicitly.
This uses native scan-and-repair rather than editing the native DB, keeps source
classification/UUIDs unchanged, and performs no inference. Codex remains optional
for file-only import; a missing/failing binary leaves a retry warning. Claude
does not use this native index path. See [picker troubleshooting](SHARING.md#imported-but-missing-from-resume).

Native checks are opt-in, isolated and model-free. The Codex check reads loaded
turns through app-server; the Claude check captures a reconstructed message request
on localhost and stops before generation. They establish loader behavior, not
successful paid inference, universal context fit, or full recovery of hidden state.
This integration was checked with Codex 0.166.0 and Claude Code 2.1.308 on Linux.
Separately, 22/22 explicitly authorized synthetic live checks passed using Codex
ChatGPT login and Claude Anthropic API, including media and second-generation
roundtrips. This is bounded evidence, not every account/backend/version or
universal context fit. See [results and reproduction](LIVE_VERIFICATION.md).

```bash
python3 -m unittest discover -s tests -q
CSL_RUN_CODEX_NATIVE_TESTS=1 CSL_RUN_CLAUDE_NATIVE_TESTS=1 python3 -m unittest discover -s tests -q
csl capabilities --json
```

For any new command/provider route: update `csl_capabilities.py`, this matrix and
synthetic tests. Add native isolated evidence before claiming CLI compatibility;
add an explicitly authorized model continuation before claiming live recall.
Never test with private conversations in the public repository or pollute a
personal CLI history with synthetic sessions.

Native behavior references: [Claude session management](https://code.claude.com/docs/en/sessions)
and [Claude CLI flags](https://code.claude.com/docs/en/cli-reference). Local CLI
`--help` is authoritative for the installed version's argument set.


## Provider reads and native-file packages — experimental, included in 0.4.0

`csl providers list|files|view|package --provider codex|claude|antigravity` exposes the
local collector interface in [SYNC_PROVIDERS.md](SYNC_PROVIDERS.md). Claude
session listing includes the shared metadata fields; primary views include
text, correlated tool blocks and raw offsets. Subagents are preserved as raw
companions rather than merged into an invented chronology.

Native-file package v3 preserves raw byte prefixes and companions. New CSL can
inspect/import it; published v0.3.0 cannot. Claude raw export includes its sibling
session directory when present. Native loader acceptance is not established
merely by file installation. No network,
credentials, model calls, or Vitral dependency exist in the provider layer.

## Persistent pool messaging (development)

`csl light` is the default transport for newly configured pool bridge peers.
It persists messages on both endpoints, reconnects one SSH stream per environment,
and explicitly enables native Codex activation for enrolled recipients. Client-only
homes receive mail/results. Legacy SSH/operator transports remain deprecated
opt-in fallbacks. See [light runbook](LIGHT_RUNBOOK.md). This does not change
legacy mailbox-only activation/grant semantics or environment lifecycle.

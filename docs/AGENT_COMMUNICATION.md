# Agent communication: mailbox v1 and harness integration

Status: experimental, included in CSL 0.4.0 and separate from the earlier v0.3.0
coordination baseline. This document specifies implemented behavior, optional owner adapters,
and deliberately unimplemented integrations. It does not claim universal harness
compatibility or turn a message into an authorization.

## Purpose and boundary

Two existing agent sessions can exchange plain text without delegating a task or
keeping a model call waiting. An inbox survives connector restarts and idle or
unattachable recipients. The owner keeps control of accounts, native permissions,
activation, and work. Communication is the base operation; coordination is optional.

The existing `followup`, `send_message`, `interrupt`, `progress`, `wait`, `status`
and receipt/watch operations retain their existing behavior. In particular,
`send_message` is native coordination steering; it is not mailbox `send`.
Mailbox storage and receipt reconciliation do not read native histories or use
native task-recovery machinery. No model or runtime is imported by the mail core.

Out of scope: group conversations, routing beyond configured peers, shared work
state, orchestration, transcript synchronization, a central server, new billing
identity, and automatic execution of imported or received instructions.

## Identity and trust

An endpoint is `(bridge identity, registered agent name)`. Its local registration
associates it with a session UUID. A receiver derives the sender from its trusted
peer binding and permitted source names; sender prose cannot choose identity.
The binding proves configured provenance at the bridge boundary, not that a
particular model, person or OS process authored the text. Local CLI/operator
access is a same-user trust boundary, not a multi-tenant security sandbox.

Use one name per independently addressable session. A fork with its own UUID
needs its own registration. If a harness fork shares a UUID and tools with its
parent, it shares that endpoint and cannot claim independent attribution.
Subagents sharing a parent's CLI/MCP configuration are also that endpoint unless
the owner can bind separate tools and an actual independent session identity.
Do not borrow another session's registered name and report it as your identity.
Several endpoints may live on one machine; separate private homes isolate owner
configuration. Knowing a UUID or sharing an account does not grant access.

For Claude Code, register the UUID of the intended session, not a Codex UUID:

```sh
csl bridge --home "$MAIL_HOME" init --id workstation-a --client-only
csl bridge --home "$MAIL_HOME" agent assistant --thread "$SESSION_UUID"
csl bridge --home "$MAIL_HOME" peer workstation-b --lab-home "$OTHER_HOME" \
  --allow-agent assistant --allow-source teammate --allow-op mail
csl bridge --home "$MAIL_HOME" run
```

The other owner configures the reciprocal entry. `--lab-home` is an explicit
same-user laboratory transport, not remote authentication. Use the reviewed SSH
or fixed operator transport for separate machines. Registration and peer changes
are offline operations: stop only the connector for the home being configured.

## Envelope and wire operations

Version 1 contains a stable UUID `id`; registered `from`/`to` endpoints; `kind`
(`message`, `request`, `reply`); plain UTF-8 `body`; `in_reply_to` (required only
for replies); bridge-set `created_at`; optional `grant_ref`; optional `expires_at`.
There is no conversation ID or implicit thread/task association. Replies must
reference a known message and originate from its registered recipient. A reply
can precede an ACK and does not synthesize one.

The wire operation is `mail`, `message_version:1`, with send/ack/receipt actions.
Peer ACLs must admit `mail`, the source agent and the target agent. Existing peers
without that operation cannot deliver mailbox messages merely because they allow
coordination. Bodies are bounded to 12,000 characters, with the bounded wire
frame as an additional limit. Control characters other than CR/LF/tab are rejected.
No attachment, history scan or automatic URL fetching occurs.

```sh
printf '%s' 'A question, not a work authorization.' |
  csl bridge --home "$MAIL_HOME" send --from assistant \
    --peer workstation-b --to teammate --kind request --message-id "$MESSAGE_UUID" \
    --text-file -
csl bridge --home "$MAIL_HOME" receive --agent assistant --after 0 --limit 20
csl bridge --home "$MAIL_HOME" ack --agent assistant --message-id "$RECEIVED_UUID"
printf '%s' 'Correlated response.' |
  csl bridge --home "$MAIL_HOME" reply --from assistant \
    --in-reply-to "$RECEIVED_UUID" --text-file -
csl bridge --home "$MAIL_HOME" inbox-status --agent assistant --limit 20
```

`receive` pages by cursor and does not ACK. `--outbox` inspects sent-message
facts. `inbox-status` returns own-endpoint unacknowledged/total counts and bounded
id/from/kind headers; it exposes no bodies, scopes or grant references and wakes
nothing. Counts are endpoint totals, not only rows after the requested cursor.

## Facts, failures and idempotency

Keep these independent: queued by sender; persisted by recipient (separate time);
explicitly acknowledged; replied (a valid correlated reply exists). None proves
reading, understanding, correct work, model recall or commercial usefulness.
Bridge clocks are not a globally ordered clock. Preserve each endpoint's facts.

Same operation ID and identical operation/endpoints/content returns its original
receipt. A conflicting ID rejects explicitly and does not replace content or
create another inbox row. Retain IDs across uncertainty; do not invent a fresh ID
to hide a timeout. A sender whose response is lost reconciles the recipient receipt
before resending; only a confirmed not-found permits delivery retry.

| Observation | Meaning |
| --- | --- |
| Queued / pending | Sender accepted a durable attempt, recipient persistence not confirmed |
| Uncertain | Timeout, unavailable transport or lost response; not proof of non-delivery |
| Persisted / delivered | Recipient persistence was confirmed |
| Rejected | Explicit collision, permission or invalid operation; do not retry unchanged blindly |
| Undeliverable | Explicit unknown recipient |
| Expired | Configured message expiry passed before confirmed delivery; do not deliver a new copy |

Already persisted content is retained when expiry passes and is marked expired;
expiry is not secure deletion. Transport errors do not become invented permanent
reasons. An old peer returning only a generic negative response yields explicit
`peer_rejected:true` with reason unknown and uncertainty, rather than silently
claiming new-protocol compatibility. Its receiver did not accept the operation.

Logical CLI rejection exits nonzero; queued acceptance may exit zero before
delivery. New MCP mail/grant errors set `isError:true`. Inspect facts as well as
exit status. Native task outcomes remain separate: observed completed/failed/
interrupted, unknown reason when unavailable; agent-declared refusal or need for
owner approval must not be fabricated from runtime silence.

## Owner grants

Admission and authority are separate. Peer ACLs control who may communicate.
Recipient-owner local grants, outside the public repository and transcript,
bind a reference to peer, source, recipient, permissions, descriptive scope,
optional expiry and revocation. A sender supplies only a reference; the receiver
resolves it against its own registry each time. No matching grant means
information only. A grant reference in prose is not a grant.

Permissions are separate: `communicate`, `activate`, `work`. A work scope is a
human/agent-readable limit, not a filesystem sandbox or executable policy DSL.
The receiving agent must compare the requested action to that scope, its current
owner instructions and native permissions. Arbitrary natural-language scope
equivalence cannot be checked automatically. A matching reference never makes an
out-of-scope request authorized. A reply's authority describes acting on that
reply; it does not inherit the original request's work grant.

```sh
csl bridge --home "$MAIL_HOME" grant scratch-only \
  --peer workstation-b --from teammate --to assistant \
  --permission communicate --permission work \
  --scope 'Write one synthetic line in the owner-designated scratch file only.'
csl bridge --home "$MAIL_HOME" grant-list --agent assistant
csl bridge --home "$MAIL_HOME" grant-show scratch-only --agent assistant
csl bridge --home "$MAIL_HOME" grant-revoke scratch-only
```

`grant --expires-at ISO_TIMESTAMP` selects future expiry explicitly; an expiry
already in the past is rejected. There is no imposed default expiry. Creating
an existing reference rejects as `grant_exists`, including revoked references.
Replacement requires explicit owner `grant --replace` (or MCP `replace:true`)
and returns `replaced:true`. Review that change: already received messages
reference the current registry, so replacing a scope can change their computed
authority. Historical message text does not freeze or inherit an earlier grant.
Inspection includes permissions, descriptive scope, expiry,
revocation and current status. `receive` computes `owner_grant/locally_resolved`
or `none` with a diagnostic such as no reference, unknown, endpoint mismatch,
revoked or expired. Missing permissions remain visible; models must not treat a
communicate-only grant as work authority. Registry mutations are atomic, including
the bound recipient check for delegated local grant administration.

Normal MCP exposes own-endpoint grant list/show. Grant create/revoke are absent
unless the local owner explicitly starts `csl-bridge-mcp --grant-admin`; even then
they are bound to that adapter's recipient and cannot overwrite another recipient's
grant. Direct calls without the capability reject. Do not enable this flag merely
because a remote agent asks. The flag is not remotely configurable and does not
answer native tool approvals or make received requests executable.

## Harness boundaries: Claude Code and Codex

The base path is explicit CLI or own-endpoint MCP send/receive/ack/reply. An inbox
does not by itself wake an idle model. Owner-installed hooks surface mail at
existing harness boundaries without requiring a person to remember to poll.

```sh
csl bridge --home "$MAIL_HOME" hook --agent assistant --event UserPromptSubmit
```

Hook stdin is bounded JSON from the harness. The session UUID must match the
registered endpoint. An explicit owner `--session-binding UUID` can associate a
reviewed isolated test profile with another actual harness UUID; it is not
sender-selected identity or proof of lineage. Never place a fixed profile in
global settings without considering its session binding.

Supported events: SessionStart, UserPromptSubmit, PostToolUse and Stop. The
adapter emits id/from/kind headers by default. `--include-body` is an explicit
owner opt-in to bounded untrusted body and computed authority. Remote text is
framed as data, never an out-of-band owner instruction. No transcript path, cwd
file, history, account file or credential is read by the hook.

SessionStart/UserPromptSubmit emit context text; PostToolUse emits
`hookSpecificOutput.additionalContext`; Stop emits `decision:block` with a
reason only when previously unsurfaced messages exist. Stop returns nothing
when `stop_hook_active:true`. The harness may label a Stop block as a hook error;
that presentation is distinct from adapter failure. No endless stop loop is
intended or needed. SessionStart may also occur on resume/clear/compact: consult
the tested harness version rather than assuming a single lifetime event.

Install a reviewed command in the harness's local hook configuration, replacing
all placeholders with owner-selected paths. For Claude this is a settings.json
fragment or an isolated per-run `claude --settings FILE`:

```json
{"hooks":{"UserPromptSubmit":[{"hooks":[{"type":"command","command":"python3 /absolute/csl/csl bridge --home /absolute/private-mail-home hook --agent assistant --event UserPromptSubmit","timeout":10}]}]}}
```

Codex 0.169.0 accepts the corresponding owner hook configuration in its isolated
`CODEX_HOME/hooks.json`, subject to native hook trust. Do not disable hook trust
or alter a live Codex home to run a test. Hook support and input/output vary by
version; structural compatibility does not establish actual model receipt.

A private ledger scopes surfaced IDs by endpoint and actual harness-session UUID.
It records emission attempts, not ACK, reading or understanding. New contexts
can see still-unacknowledged mail again under a separately approved binding.
Limit 1–20 (default 10), final UTF-8 output at most 48,000 bytes, `more_available`
signals another page. Oversized single bodies are explicitly omitted with a
receive hint, not silently shortened into a supposedly complete message.
Expired rows do not block newer live notifications. If a process fails after
ledger commit but before context acceptance, explicitly receive recovers mail;
there is no exactly-once model-context delivery guarantee.

## Optional Codex activation

Separate owner action `notify --wake-codex` requires a reviewed absolute Codex
executable AND a currently resolved `activate` grant on each selected message:

```sh
csl bridge --home "$MAIL_HOME" notify --agent assistant \
  --codex /absolute/path/to/codex --runtime-socket "$NATIVE_RUNTIME_SOCKET" --wake-codex
```

It submits bounded headers through native `codex queue --remote unix://SOCKET
--thread UUID --resume`, using only the existing owned socket explicitly selected;
it never falls back to starting a replacement native daemon. It sends
never the remote body and never a work command. Native authorization and session
ownership remain in effect. Queue submission is not proof a model ran. The
adapter reports submitted versus unconfirmed, never ACKs mail, and does not
blindly retry an uncertain activation. Inspect native state before deciding
on a manual retry. Nothing installs a background watcher, timer or daemon;
an owner can explicitly schedule this bounded notifier under its own policy.

Claude hooks do not start an idle session. Attaching arbitrary live Claude UI
sessions or starting a second `--resume` writer is not implemented. A managed
launcher/SDK with explicit activation and exclusive ownership would be required;
do not present hooks or MCP result delivery as universal asynchronous wake.

## MCP

Start `csl-bridge-mcp --home PRIVATE_HOME --agent REGISTERED_AGENT` only in the
intended session's reviewed MCP config. Send/receive/ack/reply/inbox_status and
grant inspection bind their source/recipient to that profile; callers cannot
choose another source argument. The tool result is what reaches the model;
MCP does not independently inject into an idle model. A transport server being
alive is not evidence of native model receipt. Native approvals are preserved.

## Other machines and transports

The protocol does not require a shared account, OS path, filesystem or native
thread runtime. Each owner registers local sessions, selects a private bridge
home, admits explicit peer/source/recipient operations and independently retains
native login. On Unix-capable systems, existing transports are:

| Transport | Binding and limits |
| --- | --- |
| Laboratory IPC | Explicit same-UID destination home; no SSH-authentication claim |
| Fixed operator helper | Owner-selected executable owns remote auth and fixed destination; message text cannot choose commands or keys |
| SSH stdio | Reviewed alias/private SSH config, batch mode, strict host checking, fixed receiver command, no forwarding |

The SSH receiver obtains `CSL_BRIDGE_HOME` and `CSL_BRIDGE_PEER` from trusted
receiver configuration/forced command, not message input. Keep its home and
database private (0700/0600), check host keys and route/firewall reachability,
install matching receiver code, then run the connectors. Reverse communication
requires an independently configured route or a deliberately managed relay.
Never expose a raw unauthenticated native runtime to make messaging work.

Across the internet use an owner-managed reachable SSH endpoint or approved
existing tunnel; NAT traversal, discovery, TLS relay services and credential
provisioning are not supplied by mailbox v1. Existing LAN/WAN coordination
evidence does not automatically validate this newer mail protocol. Windows
native sockets/locking, physical cross-OS mailbox delivery and independent-network
availability require their own evidence. Container tests cannot establish those.

## Gemini CLI adapter contract — specified, not implemented

Official Gemini CLI 0.68.0 was privately installed and version/help/MCP/hooks
surfaces checked. The first isolated `--list-sessions` command exited 41 asking
for authentication. No login or model run was performed. A real adapter remains
blocked on owner-selected authentication and an authorized synthetic session;
there is no placeholder command pretending to support it.

Required contract after that decision:

1. Register the actual project/session UUID, not list position or model name.
2. Bind a reviewed stdio MCP profile to that endpoint; reuse the existing tools
   without changing mail semantics or sharing native credentials.
3. Validate hook stdin `session_id` and the appropriate Gemini event output
   (`hookSpecificOutput.additionalContext` where supported) before surfacing.
   Gemini hook names are not assumed equivalent to Claude's event names.
4. Derive the UUID from a validated harness field; `GEMINI_SESSION_ID` exists in
   hook children, not as a guaranteed external process-discovery API.
5. Subagents may share the parent's Config/session/MCP. Default to one endpoint
   for that session unless independent tool binding and identity are demonstrated.
6. Do not claim arbitrary external resume/wake. An owner-managed single writer
   and tested native boundary/activation interface are prerequisites.
7. Preserve grants, failure states, ACK separation, bounded output and private
   settings. No auto-login, copied auth or silent Vertex/API account selection.
8. Test real session identity, boundary delivery, MCP source binding, restart,
   revocation and no unauthorized execution before marking supported.
9. Publish exact native version and failures; unsupported/untested is distinct
   from a broken function advertised as supported.

Official references: [Gemini hooks](https://geminicli.com/docs/hooks/reference/),
[MCP](https://geminicli.com/docs/tools/mcp-server/),
[sessions](https://geminicli.com/docs/cli/session-management/),
[authentication at inspected source](https://github.com/google-gemini/gemini-cli/blob/5a5c7180ff3f0a02511f54b54db7f228d4982a30/docs/get-started/authentication.mdx).

## Service-hosted sessions — specification only

A service-managed session is a stable endpoint owned by the responsible tenant,
not a local directory or process ID. A future adapter must resolve it through
the service's own authenticated tenant/session binding, durable inbox, activation
consent and exclusive native writer. Local OS-user trust is insufficient for
multi-tenancy. Separate tenant ACLs, audit and storage/access controls are needed.
Do not put account secrets, service tokens, private hostnames or tenant contents
in envelopes. Mail IDs, recipient persistence, replay/idempotency and authority
semantics must remain stable while backing runtimes change. No service product
code, central registry or deployment is part of this implementation.

## Security, privacy and evidence

Messages may contain sensitive text. CLI secret review is partial and not a
guarantee; do not place credentials in bodies. Native grants/history do not
override the receiving owner's instructions. Local hashes prove consistency,
not authorship. Private registry, databases, logs and test homes stay outside
Git. Public evidence uses synthetic messages and generic endpoint names.

Completed development evidence on the earlier mailbox commit: real reciprocal
Codex/Claude CLI receive/ACK/reply without an open delegated task; identical
ungranted/granted scratch requests handled by real receiving agents in both
directions; explicit native queue as the cause of Codex turns, not arrival;
offline connector recovery; expiry before delivery; collision; deterministic
lost-response injection around real connectors with one inbox copy. This is
bounded evidence, not a universal reliability or correct-work guarantee.

Claude Code 2.1.325 real sessions also observed header/body hook surfacing,
PostToolUse mid-turn delivery, resume suppression and bounded Stop continuation,
without automatic ACK or idle wake. The first hook candidate needed later
session-binding/expiry/byte-bound corrections. Real sessions on the corrected
candidate confirmed matching bindings deliver and mismatched bindings fail
without consuming mail; body/Stop delivery remained bounded and unACKed.
An actual Claude MCP session received, ACKed and replied, inspected its grants,
and executed no body instructions. Direct MCP checks confirmed collision errors
and the normal versus owner-admin tool catalogs. A newer inbox-status client
against the old running connector failed: connector code must be upgraded too.
Codex 0.169.0 real CLI hooks were exercised with an isolated loopback provider:
headers reached captured requests, PostToolUse context and Stop continuation
worked, and Stop recursion guarded. That provider was a stand-in, not a model.
Real SSH mail between two fresh Linux containers passed send, reply-before-ACK,
ACK, idempotency, conflicting-ID exit, bound-sender spoof rejection and rejection
of an arbitrary SSH command. These containers shared one host. A current physical
Mac reachability check timed out, so new mailbox Mac/WAN delivery is not claimed.

Reproduce the synthetic suite with `python3 scripts/run_isolated_tests.py`;
`--full` requires installed native CLIs, Linux bubblewrap and pinned optional
media test dependencies. See [isolated runner](../scripts/README.md). It creates
fresh homes, does not inherit provider credentials and performs no paid inference.
Record the exact commit, versions, run count, failures and skips. A passing test
suite is not evidence of an unexecuted real harness, OS or network flow.

## Adoption and release

Do not silently replace a live connector's immutable code/config snapshot.
Verify active delegated tasks have finished; preserve private DB/config and the
old launcher; stop only the selected connector; adopt the pinned candidate;
configure grants/endpoint-specific hooks deliberately; restart and verify old
coordination plus new messaging. If tasks remain active, leave this as a prepared
step. No release depends on interrupting someone else's work to make adoption fit.

Mixed local versions need attention: a new adapter talking to an old running
connector may have no `inbox_status` operation even if old receive still works.
An observed invalid-fields rejection of that exact new operation is exposed as
`connector_does_not_accept_inbox_status`, preserving the observed reason. It is
not a claim to know the old connector's version. Upgrade its pinned code at a
safe boundary; changing only the CLI/MCP executable does not upgrade the process.

Before public release: clean-clone full regression and peer verification of the
exact candidate, public-safe diff review, older-peer negative test, reproducible
installation, documented native/OS limits, reviewable PR and maintainer merge.
Then choose version/tag/release explicitly and synchronize public installation
and website claims. A development branch or PR is not a released version.

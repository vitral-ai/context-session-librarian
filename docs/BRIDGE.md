# Experimental Agent Coordination

**Pool default (development):** use [light transport](LIGHT_RUNBOOK.md) for a
persistent connection and pushed replies. The per-request SSH setup below is the
explicit deprecated fallback; existing configurations are not silently migrated.
Lifecycle holds and keep-alive publishers stay separate.


The optional `csl bridge` commands coordinate
existing Codex agents. A followup starts an idle agent in its own conversation;
a message enters active work at a runtime boundary. Progress and completed turns
return automatically. `wait` blocks on events rather than asking a model to poll.
Interrupt targets an exact delegated turn; a later followup continues the same
conversation. A completed turn is not proof that its technical conclusion is valid.

This is an experimental integration using internal runtime interfaces. Do not
assume compatibility with every Codex version, interface or account combination.

| Check | Evidence and scope |
| --- | --- |
| Synthetic runtime and terminal UI | Codex 0.167.1, isolated loopback backends; no real model inference |
| Linux container transport | Restricted SSH, simulated interruption, recovery and latency controls |
| Existing-runtime control socket | Codex 0.167.2 direct WebSocket and official byte proxy, isolated no-inference check |
| Real Linux container coordination | Codex 0.167.1, separately authenticated worker; progress, clarification, final, interrupt and continuation; initial MCP approvals required local assistance |
| Real Linux–Mac LAN coordination | macOS 27.2 arm64, Codex app-server 0.169.0; actual existing conversations, messages, progress/final, interrupt and context-preserving followup |
| Internet, Windows, Claude live client | Not validated |
| Native macOS app bundle | Source prototype only; build, clean installation and GUI interaction not validated |

The real LAN exercise is bounded evidence, not a reliability or compatibility
guarantee. It used explicitly configured transport and MCP access. Synthetic
laboratory answers are controls, not evidence of real model behavior. The macOS
app is separate from the CLI connector used in that exercise.

## Initial Setup

For task-independent `send`/`receive` messaging available in a development checkout,
see [Mailbox v1](#task-independent-mailbox-v1--development) below. Those messages do
not start or steer native work. The coordination setup and commands in the rest of
this guide retain their existing behavior.

Use Python 3.11+ on a system with Unix domain sockets and `fcntl`. Keep an explicit,
private bridge home separate from Codex homes, histories and authentication. The
core installer does not start or configure a bridge, service, daemon or timer.

Each participating conversation must already be loaded in its owner's chosen
app-server, accessible through a private **WebSocket-over-Unix** endpoint. The
bridge checks `thread/loaded/list` and rejoins that runtime. It never creates a
thread, imports history, starts another runtime or adopts login credentials.
Keep the owner's local UI available for approvals; the bridge never answers them.

An app-server `--listen unix://PATH` endpoint was tested. Native **0.167.2** was
also tested in an isolated Linux lab with no model calls: its default existing
control endpoint is `$CODEX_HOME/app-server-control/app-server-control.sock`,
which can be a symlink to a shorter private path. That control endpoint accepts
WebSocket framing. Resolve the explicit endpoint before connecting, especially
on Darwin where the original rendezvous path may exceed the Unix path limit.

The official `app-server proxy --sock PATH` relays **raw bytes** through its
stdin/stdout; it does not translate JSON lines into WebSocket frames. The optional
Python `Runtime(path, proxy_executable=owner_selected_codex)` transport preserves
the HTTP upgrade and masked WebSocket frames over those pipes. A synthetic
0.167.2 check confirmed the same already-loaded thread through both transports;
closing the proxy left the server alive. This launches only a transport child,
never a native server or replacement agent. Plain JSON sent directly to proxy
stdin is an invalid test of this WebSocket endpoint. See the official
[0.167.2 proxy dispatch](https://github.com/openai/codex/blob/rust-v0.167.2/codex-rs/cli/src/main.rs)
and [byte-relay implementation](https://github.com/openai/codex/blob/rust-v0.167.2/codex-rs/stdio-to-uds/src/lib.rs).

These checks do not establish access to an arbitrary daemon, account or Mac.
First confirm the owner-selected socket and that the actual conversation is
already in `thread/loaded/list`; do not load a thread just to make this check pass.
A standalone TUI using `--no-daemon` is not
automatically attachable. Its owner would have to stop it at a safe boundary and
host the **same** conversation in a compatible runtime, then verify continuity.
Never start another server against a conversation that another process owns.

Choose names and actual existing thread UUIDs locally. For example, with
`ALICE_BRIDGE_HOME`, `ALICE_RUNTIME_SOCKET` and `COORDINATOR_THREAD_UUID` set to
your own explicit locations and selected conversation:

```sh
csl bridge --home "$ALICE_BRIDGE_HOME" init --id alice --runtime-socket "$ALICE_RUNTIME_SOCKET"
csl bridge --home "$ALICE_BRIDGE_HOME" agent coordinator --thread "$COORDINATOR_THREAD_UUID"
```

Bob independently initializes his own bridge as `bob` and registers `worker`
with his existing conversation. He keeps his own Codex tools, configuration,
permissions, account and consumption. Knowing a UUID, using the same login, or
being on the same LAN does not grant access to the other computer.

Configure each peer offline, explicitly naming which **local** agents and
operations that peer may access, and which of its **remote** agents may send
followups and messages. The following gives Bob access only to Alice's
coordinator, as his agent `worker`. Bob configures the reciprocal entry allowing
Alice's operations on his worker. `watch` carries progress/final events; it
cannot read native histories.

```sh
csl bridge --home "$ALICE_BRIDGE_HOME" peer bob --ssh-alias bob-bridge \
  --allow-agent coordinator --allow-source worker --allow-op followup --allow-op send_message \
  --allow-op status --allow-op interrupt --allow-op receipt --allow-op watch --allow-op agents
```

A followup or message whose `source` is not in that peer's `--allow-source`
list is rejected. To admit senders for a peer configured before this rule,
replace its list offline (omitting every `--allow-source` admits none):

```sh
csl bridge --home "$ALICE_BRIDGE_HOME" peer-sources bob --allow-source worker
```

The SSH adapter uses a fixed `csl-bridge-receiver` command and JSON on stdin.
Owners must separately install that optional executable on the trusted host's
PATH and provide preauthorized SSH access. Use a dedicated restricted/forced
command whose trusted environment supplies `CSL_BRIDGE_HOME` and
`CSL_BRIDGE_PEER`. The peer identity comes from this local configuration, not
from a sender field in the message. The forced command must accept only the fixed
receiver command, disallow shell/forwarding/PTY access, and use normal verified
host keys. No program here edits `authorized_keys`, SSH settings or permissions.

Do not use an unrestricted SSH key as a substitute for this boundary: a remote
user who can choose the receiver's environment or execute arbitrary commands
already has broader OS authority. Each direction's trusted setup belongs to
that machine's owner. Restricted SSH has been tested in isolated Linux
containers; a separate real Linux–Mac LAN exercise used an owner-authorized
operator transport as described below.

For a synthetic same-UID laboratory only, `peer --lab-home DIR` selects another
explicit bridge home instead of SSH. This does not authenticate a real person,
account or remote device.

An owner-authorized operator test can instead select `peer --operator-helper
/absolute/local/helper`. That trusted local executable receives only one JSON
request on stdin and returns the receiver's JSON response. It owns an explicitly
fixed remote endpoint, absolute interpreter/receiver paths, and the existing SSH
credential integration; model and peer messages cannot supply shell commands or
paths. This permits a Mac receiver to use its private Python without changing
login profiles or PATH. Operator SSH retains the owner's normal OS authority;
the selected bridge operations do not reduce that separate key's privileges.
The helper and its credential storage are local operator setup, not bundled
credentials or an automatically installed remote service.

### Message Header And Optional Sender Session

Text that a followup, message or progress/final event places in a running
native conversation starts with one header line, followed by the selected text:

```text
[Message from=<source agent>; session=<sender conversation UUID>; task=<task/request UUID>]
```

`from` is the sender's registered agent name. `session` is the conversation UUID
that the sender's bridge home registers for that agent; a caller cannot choose
it. `task` is the followup/message request ID, or the task ID of an event. When
the peer supplies no session, the header is `[Message from=<agent>; task=<UUID>]`;
none is inferred. The header carries no authorization notice; tool results such
as `wait` still return a `notice` field. The session is asserted by the peer's
bridge and authenticated only as far as the peer binding itself.

The header does not name the peer, so `from` and `session` identify a sender
only together with per-peer `allow_sources`: a receiver accepts a followup or
message only when its `source` is admitted for the authenticated peer that
delivered it, so no peer can claim another peer's agent name. Events are
accepted only from the peer a task was sent to, and only when their `source` is
the agent that task targeted. Do not treat a name and session as authorization
on a bridge whose peers share admitted source names.

Every line of the selected text that starts with `[Message ` (after optional
whitespace, at any line boundary) is prefixed with `> `, so text cannot imitate
a header. Request text may not contain a carriage return outside a CRLF pair.

Receivers accept requests and events with or without the optional `session`
field, but honour it only from peers they have enabled. Bridges up to 0.4.2
reject it, so senders include it only for peers that explicitly opt in. It is
off by default. Enable it when configuring a peer that runs a compatible
version, or later, offline, for an existing peer:

```sh
csl bridge --home "$ALICE_BRIDGE_HOME" peer bob --ssh-alias bob-bridge \
  --allow-agent coordinator --allow-op followup --send-session
csl bridge --home "$ALICE_BRIDGE_HOME" peer-session bob --enable   # or --disable
```

The setting is stored as `send_session: true` in that peer's configuration and
applies in both directions with that peer:

- Sending: followups and messages sent to that peer, and progress/final events
  it watches, carry the session. Published events always store the session
  locally; `watch` removes it at serve time while the setting is off. If a peer
  that still rejects the field stalls on an event, stop the connector, run
  `peer-session PEER --disable` and restart: pending events are then served
  without `session`.
- Receiving: a followup or message carrying `session` from a peer without the
  setting is rejected, and a `session` on its events is dropped before storage
  (rejecting an event would stall every later event from that peer).

Like other configuration, stop the connector first; it reads peers at start.
Enable it on both sides once both run a compatible version.

Start the connector explicitly in a terminal for the session on each side:

```sh
csl bridge --home "$ALICE_BRIDGE_HOME" run
```

It remains in the foreground. Ctrl-C stops the connector and its private IPC
endpoint, not the native agent's work. It installs no global listener or autostart.
Configuration changes require stopping this connector. If the selected runtime
or conversations are not already available, state remains disconnected; it
does not start a substitute. Local IPC is private to the OS user; programs with
that user's full privileges are inside the local trust boundary.

## Coordinator And Worker

For tools used **by a model**, prefer the optional stdio MCP adapter. Sandboxed
shell commands can be denied access to the connector's Unix socket even when
the file is inside the writable workspace. Do not broadly enable command
network access to work around that boundary. Terminal/operator CLI calls remain
available separately.

Configure the adapter explicitly for the existing local agent, after registering
that conversation and starting its connector. This initial setup grants only the
bridge's existing operations, not shell, filesystem, runtime or login APIs:

```toml
[mcp_servers.csl_bridge]
command = "/absolute/path/to/context-session-librarian/csl-bridge-mcp"
args = ["--home", "/absolute/private/bridge-home", "--agent", "coordinator"]
required = true
tool_timeout_sec = 25
```

Use the worker's own fixed agent name on its side. Reload the local MCP
configuration through the supported Codex interface before sending work. The
adapter never changes configuration itself or starts a substitute conversation.
Its tools are `followup_task`, `send_message`, `send_progress`, `wait_agent`,
`list_agents`, `agent_status`, `interrupt_agent`, and `operation_receipt`.
Source identity and home are fixed by the local configuration; model arguments
cannot replace them. Messages remain remote data subject to the receiver's
mandate. Selected text with possible secrets is rejected; there is no persistent
allow-secrets setting. Waits and other tool calls can run concurrently.

The native runtime can request local approval before a side-effecting MCP call,
including `send_progress`. A laboratory with no approval UI can therefore pause
even when the selected operation is permitted by bridge configuration. The
bridge never accepts that approval or treats a peer message as local consent.
The runtime owner must inspect and decide the native request locally. A
one-call approval does not establish unattended operation or persistent trust.

For an explicit initial local grant limited to communication tools, print a
profile for review without applying it:

```sh
csl bridge --home "$BOB_BRIDGE_HOME" mcp-config --agent worker --approve-tool send_progress
```

The output binds the existing local agent/home, exposes the selected message
tools plus read-only queries, keeps the server default at `writes`, and sets
only the selected tool's `approval_mode` to `approve`. No configuration is
written or reloaded. The owner must review and apply the profile locally;
this is an explicit initial grant to that bounded tool, not consent derived
from a peer message. `send_message` can be selected separately. Task activation
and interruption are excluded from this communication profile. Peer allowlists,
active-task binding, fixed source identity and secret checks still apply.

Native 0.167.1 with a synthetic backend verified that `auto` still prompted
for `send_progress`, while a per-tool `approve` allowed that tool and left
an unselected side-effecting tool requiring approval. This does not change
shell policies or require false read-only annotations. See the official
[per-tool MCP configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference).

### Current Caller Without A Runtime Endpoint

If the caller's existing UI exposes shell tools but no compatible runtime
endpoint, an explicit client can send work and return progress/finals through
its `wait` tool results. Register the caller's actual conversation UUID for
local provenance; it does not provide runtime access or authentication:

```sh
csl bridge --home "$ORIGIN_HOME" init --id origin --client-only
csl bridge --home "$ORIGIN_HOME" agent coordinator --thread "$ACTUAL_CALLER_THREAD_UUID"
csl bridge --home "$ORIGIN_HOME" peer worker-host --ssh-alias worker-bridge \
  --ssh-config "$PRIVATE_SSH_CONFIG" --allow-agent coordinator \
  --allow-op watch --allow-op status --allow-op agents --allow-op receipt
csl bridge --home "$ORIGIN_HOME" run
```

The SSH configuration must be an owned private regular file. The receiver can
configure this origin using `peer origin --receive-only` and its own explicit
permissions: the origin initiates requests and watches; no reverse SSH access
or origin listener is needed. The worker publishes progress/final normally.
The caller sends and waits using the same commands below, in its actual
conversation. It retains stable operation IDs, receipts and event cursors.

This client never attaches to a native conversation, launches a model, or creates
a replacement coordinator. Its status is `tool-client`; it rejects incoming
activation, steering and interruption of the caller. An explicit `wait` result
delivers events to the calling model. Asynchronous injection into a caller that
is busy elsewhere, or waking that caller when idle, is **not** provided by this
mode. It is a partial integration for testing a real current caller, not full
runtime coordination equivalence.

For the specific tool host that exposes `functions.exec`, `yield_control()` and
`notify()`, [current_caller_collector.js](../experiments/bridge/current_caller_collector.js)
is a reviewable bounded collector. The existing caller runs it itself, with its
own origin home and consumed cursor. It leaves the tool invocation alive,
waits without inference, and emits remote events as late tool outputs while
the caller can continue other work. It sends no assignments, changes no policy,
and never addresses a different caller. Stop the cell to cancel collection;
an outstanding CLI wait ends within its bounded timeout. Durable bridge events
can be replayed from the last consumed cursor.

A synthetic delayed notification was observed in the implementer's active
conversation during another tool operation, before any explicit cell wait.
The actual coordinator's collector integration remains pending review/test.
These helpers are supplied by that tool host, not a documented general Codex
app-server or MCP push-to-model API. The snippet does not establish delivery
while the caller is idle or after the tool invocation/session has ended.

Use selected UTF-8 text in a file or stdin (`--text-file -`). Bodies never enter
SSH command strings or Codex CLI arguments. No entire history, binary attachment,
repository or credential file is collected. References may be ordinary text;
they are not automatically fetched.

From Alice's existing coordinator conversation, a tool can call:

```sh
csl bridge --home "$ALICE_BRIDGE_HOME" followup --from coordinator --peer bob --to worker \
  --request-id "$TASK_UUID" --text-file selected-request.txt
csl bridge --home "$ALICE_BRIDGE_HOME" send_message --from coordinator --peer bob --to worker \
  --text-file selected-clarification.txt
csl bridge --home "$ALICE_BRIDGE_HOME" agents --peer bob
csl bridge --home "$ALICE_BRIDGE_HOME" status --peer bob --to worker
csl bridge --home "$ALICE_BRIDGE_HOME" wait --agent coordinator --after 0 --timeout 20
```

`TASK_UUID` is a fresh stable UUID chosen before sending. Followup returns a
receipt with request/thread/turn IDs. Reusing the same ID and content returns
the receipt; changing the content under that ID is rejected. A message to an
idle agent stays pending without starting generation, then enters automatically
when that conversation becomes active. This is not a manually consulted inbox.

While Bob's worker is running the delegated task, its local tool can send progress:

```sh
csl bridge --home "$BOB_BRIDGE_HOME" progress --from worker --task-id "$TASK_UUID" \
  --text-file selected-progress.txt
```

That event returns automatically to the originating coordinator. If it is
waiting, its wait returns; if it is actively working, the runtime receives the
event at a safe boundary. If idle/disconnected, the event is retained and delivered
on its next activation. No model is started just to announce every update.
Turn completion returns the final text/state automatically by the same route.
Advance the local `wait --after` cursor only after consuming the returned event;
retrying a wait with the old cursor rereads it without repeating work.
Each wait is limited to **0..20 seconds**; repeat with the returned cursor for
longer waits. A coordinator's `send_message` clarification enters the worker's
native turn through steering. It is not a progress/final event addressed to that
worker's `wait`; the worker should continue after the input reaches its runtime
boundary. Use `wait` for results of tasks you delegated. A message retained while
idle enters on activation; `followup` is the operation that activates an idle agent.

To interrupt, use the exact current delegated turn from the receipt/status:

```sh
csl bridge --home "$ALICE_BRIDGE_HOME" interrupt --peer bob --to worker --turn-id "$TURN_UUID"
```

`interrupt_requested` is an acknowledgement, not confirmation of cessation.
Wait for a final event with `status: interrupted`. A fresh followup later uses
the same registered conversation, context and tools.

## Recovery And Limits

Messages are remote agent data, never system/developer instructions or approval
decisions. The receiver's original mandate and local permissions still apply.
The peer has no API for shell commands, policy changes, accounts, arbitrary
filesystem reads, all conversations, or approvals. Permissions are per peer,
operation and registered local agent. Interrupt also requires a matching
delegated task/turn.

Bridge data uses private SQLite transactions, one connector per home and durable
IDs. Chronology uses local sequences, not clocks shared across machines. Result
events stay retained; there is no automatic deletion or history synchronization.
Transport loss does not cancel work. If a request becomes `uncertain`, query its
receipt rather than sending another task:

```sh
csl bridge --home "$ALICE_BRIDGE_HOME" receipt --peer bob --operation-id "$TASK_UUID"
```

On native reconnection the connector checks native client-message IDs and turn
history for evidence before updating uncertain state. Recovery inspects at most
200 recent turns in each explicitly registered conversation; missing evidence
remains uncertain. It never silently replays an invocation or guarantees
exactly-once execution across a crash. A receipt `not_found` is not authority to
create a replacement task while the prior outcome is unresolved.
New or settled bridge state does not scan native history. Recovery reads are
needed only for uncertain operations/event delivery or an unfinished delegated
task whose completion may have been missed. Completion events unrelated to a
delegated task are ignored before any fallback history lookup.

Event watch requests block up to 15 seconds and reconnect with bounded backoff;
tool waits block at most 20 seconds. They do not spend inference to poll. There
is no automatic chain of new assignments on receipt of a result. The owner
controls when to delegate again and remains responsible for scope and cost.

Selected outgoing text has partial secret detection. Findings stop the send;
`--scrub-secrets` applies to that one selected text only. It is not persistent
permission to retain secrets or a guarantee that all secrets are removed.
Automatic final text with detected findings is withheld while completion status
still returns. Review locally before sending selected evidence. Diagnostics do
not print bodies, credentials or account configuration; explicit read/wait
outputs contain the selected remote content and its provenance.

## Reproduce The Confined Laboratory

The normal synthetic tests do not launch Codex or SSH:

```sh
python3 -m unittest discover -s tests -p test_bridge.py -v
```

With Codex 0.167.1 installed, these explicit laboratory commands start only
temporary app-servers, private Unix sockets and loopback HTTP backends. They
use fabricated provider responses, isolated homes/environments and no model
credentials, personal sockets or authentication. Each process is stopped at
the end. Reports and synthetic traces are retained in the printed temp directory.

```sh
python3 experiments/bridge/runtime_gate.py
python3 experiments/bridge/connector_gate.py
```

The first checks join, concurrent input, steering, interruption, protocol UI
approval routing and reconnect. The second runs two native engines with the
production connector: delegation, clarification, progress waking wait,
automatic final delivery, idle-message semantics, interruption and recovery.
An additional Linux PTY gate exercises the actual terminal UI with the same
synthetic backend:

```sh
python3 experiments/bridge/tui_gate.py
```

Passing these gates does not complete acceptance with real Codex models or
establish Mac, SSH or cross-account compatibility. Restarting a connector may
resume previously authorized pending followups; it does not create new assignments.

## Task-Independent Mailbox V1 — Development

See the standalone [agent communication specification](AGENT_COMMUNICATION.md)
for the complete envelope, grants, session-bound hooks, optional native queue
activation, remote transports, failure semantics and verification limits.

This capability is included in CSL 0.4.0, not the earlier v0.3.0 tag. It adds `send`, `receive`, `ack`
and `reply` independently of delegated tasks. A message is persisted in a private
inbox even when its recipient is idle or a client-only harness. **No mailbox
operation starts, resumes, steers or interrupts a model.** The existing
`followup`/`send_message` coordination operations remain separate and unchanged.

### Register the actual sessions and admit their endpoints

Each session needs its own operator-associated agent name and actual session UUID.
For example, initialize two private client-only homes on their respective machines:

```sh
csl bridge --home "$ALICE_HOME" init --id alice-host --client-only
csl bridge --home "$ALICE_HOME" agent alice --thread "$ALICE_SESSION_UUID"
csl bridge --home "$BOB_HOME" init --id bob-host --client-only
csl bridge --home "$BOB_HOME" agent bob --thread "$BOB_SESSION_UUID"
```

An existing Codex runtime-backed home can also use the mailbox. Claude Code uses
its actual own session UUID, not another session's borrowed registration. Obtain
that ID through the session's supported interface; the bridge does not scrape
histories, copy authentication or create a session to discover it. Fork workers
sharing a Claude session ID are one endpoint, not independent identities.
Registration records an operator association; it is **not process attestation**.
Programs acting as the same OS user are inside the local trust boundary.

Using already-authorized SSH aliases and fixed receivers as described above,
configure reciprocal admission on each machine:

```sh
csl bridge --home "$ALICE_HOME" peer bob-host --ssh-alias bob-bridge \
  --allow-agent alice --allow-source bob --allow-op mail
csl bridge --home "$BOB_HOME" peer alice-host --ssh-alias alice-bridge \
  --allow-agent bob --allow-source alice --allow-op mail
```

`--allow-agent` selects the local inbox; `--allow-source` binds admitted remote
agent names to the authenticated peer. Neither is inferred from message prose.
Missing sender admission rejects mail. A peer granted only `mail` does not poll
the legacy coordination event stream. Replies and acknowledgements need an
outbound route as well: a receive-only peer cannot create that route automatically.
Transport and fixed receiver setup remain explicit; no network service is created.

Registering agents or changing peer admission requires a controlled connector
stop/restart to adopt that configuration; do not restart someone else's live
connector as an onboarding shortcut. Owner grant changes below are dynamic and
do not require a connector restart. Start each configured connector explicitly:

```sh
csl bridge --home "$ALICE_HOME" run
csl bridge --home "$BOB_HOME" run
```

Run each command in its own terminal on the appropriate host. The connectors
deliver queued mail in the background without model calls. No global service,
autostart or notification hook is installed.

### Send, receive, acknowledge and reply

Choose a message UUID and put selected text in a UTF-8 file. With Alice's home and
session configured, send information without any outstanding delegated task:

```sh
csl bridge --home "$ALICE_HOME" send --from alice --peer bob-host --to bob \
  --message-id "$MESSAGE_UUID" --kind request --text-file question.txt
csl bridge --home "$BOB_HOME" receive --agent bob
csl bridge --home "$BOB_HOME" reply --from bob --in-reply-to "$MESSAGE_UUID" \
  --message-id "$REPLY_UUID" --text-file answer.txt
csl bridge --home "$BOB_HOME" ack --agent bob --message-id "$MESSAGE_UUID"
csl bridge --home "$ALICE_HOME" receive --agent alice
csl bridge --home "$ALICE_HOME" receive --agent alice --outbox
```

`send` accepts `message` (default) or `request`. `reply` creates the third kind,
requires a locally known inbox message and derives its destination from that
message. Only its original registered recipient may reply or acknowledge it.
Replying before acknowledgement is valid and does not fabricate acknowledgement.
Receiving does not mark a message acknowledged. `--after CURSOR --limit N` pages
through inbox/outbox rows; pages are also byte-bounded and may contain fewer than
the requested limit. Save the returned cursor, not an assumed count.

The envelope has a version, UUID, registered `from`/`to` peer-and-agent endpoints,
kind, plain-text body, optional `in_reply_to` and `grant_ref`, and bridge-generated
timestamps. `created_at`/`queued_at` are fixed on first sender acceptance;
`persisted_at` is set separately by the recipient. Sender time is not evidence of
recipient delivery or a trustworthy remote clock. Optional timezone-aware
`--expires-at` bounds delivery: expired, not-yet-persisted mail is not newly sent
or accepted and has delivery state `expired`. An uncertain expired send is queried
for prior persistence without sending it again. Already-persisted evidence is
retained and marked expired. Message expiry is independent of grant expiry:
an expired grant does not prevent admitted information from being stored.
The usual partial secret scan applies; opaque
secrets and missed patterns remain the sender's responsibility.

The facts `queued`, `persisted`, `acknowledged` and `replied` are independent.
Acknowledgement means only an explicit call by that endpoint, not proof of reading,
understanding, agreement or successful work. `reply_ids` is bounded to 100 IDs;
`reply_count` reports the total valid correlated replies. No free-text marker is
needed to correlate a reply.

Stable IDs bind the operation, endpoints and content. Repeating identical input
returns its existing message/receipt and current facts, retaining first timestamps;
changing content, kind or endpoints under the same ID returns explicit
`state: rejected, reason: id_collision`, nonzero CLI exit and MCP `isError:true`.
A queued send returns
promptly without waiting for a remote network timeout. The outbox exposes later
delivery facts. A lost transport response is `uncertain`, not permanent failure.
Recovery first queries the recipient's receipt and resends that same immutable
operation only after confirmed absence (or a confirmed missing acknowledgement).
An unavailable receipt leaves uncertainty intact. Connector restart preserves
inboxes, outboxes, acknowledgement timestamps, IDs and queued recovery state.

`undeliverable` requires a confirmed permanent reason, such as a configured inbox
no longer existing. A generic negative older-peer response records the observed
fact `peer_rejected: true` and `peer_rejected_reason_unknown`, retaining delivery
state `uncertain`: that response cannot distinguish unsupported protocol, policy
rejection or an internal failure. Recovery queries a receipt rather than blindly
resending the body. Timeouts do not imply a peer rejection or permanent failure.

### Optional locally resolved owner authority

Inbox admission permits communication only. With no `--grant-ref`, received
authority is `none`, even if a matching grant happens to exist. Unknown, revoked,
expired or endpoint-mismatched references also yield `none` and a diagnostic
reason without exposing another grant's scope. Text is never permission.

The recipient's owner may configure a narrow grant through the local CLI:

```sh
csl bridge --home "$BOB_HOME" grant review-only --peer alice-host --from alice \
  --to bob --permission communicate --permission work --scope "Review the synthetic fixture"
csl bridge --home "$BOB_HOME" grant-revoke review-only
```

An optional grant `--expires-at` is chosen by the owner and must be in the future;
expiry is not mandatory. Existing references reject unless the owner explicitly
uses `grant --replace` or MCP `replace:true`, including re-creation after revoke.
Replacement is reported and affects locally recomputed authority for old mail.
Registry permissions `communicate`, `activate` and `work` are distinct. `receive`
recomputes them locally against the authenticated peer and registered endpoints,
so later revocation/expiry is reflected without erasing the message. Even a valid
`work` or `activate` grant causes **no execution or model activation** through mail
operations. A separate owner-opted `notify --wake-codex` can submit headers for
messages carrying a currently resolved activate grant; it never executes work.
The local registry, grant contents and operational policy remain in
the private bridge database. They are not transcript authority, public fixtures,
remote wire operations. CLI `grant-list`/`grant-show` inspect status and scope.
MCP own-endpoint inspection is available; create/revoke additionally require the
explicit owner `--grant-admin` startup capability and cannot mutate another
recipient's grant.

### MCP and harness boundaries

The bound stdio adapter exposes `send`, `receive`, `ack` and `reply`. `send` uses
`peer`, `target`, `body`, optional `id`, `kind`, `grant_ref` and `expires_at`;
`reply` uses `in_reply_to`, `body` and the optional send metadata. `ack` takes
`message_id`; `receive` takes optional `after`, `limit` and `outbox`. The source
agent is fixed by the adapter's reviewed local registration, never a model field.
The MCP `receive` tool is read-only. Other mailbox tools require the harness's
ordinary local consent; `mcp-config --approve-tool send --approve-tool ack
--approve-tool reply` can print a bounded profile for explicit review without
applying it. Owner grant administration is absent unless `--grant-admin` was
explicitly enabled in that local profile. It grants no native tool permissions.

The wire adds one operation, `op: mail`, with `message_version: 1` and actions
`send`, `ack`, `receipt`. Existing protocol version and coordination operations
retain their behavior. Peers running v0.3.0 reject the new operation rather than
treating it as progress or a task. The mailbox creates additional SQLite tables
without rewriting native histories or existing coordination rows. Old code does
not process the new queued mailbox operations; use the new connector for them.

| Harness | Mailbox-v1 behavior | Evidence and limits |
| --- | --- | --- |
| Codex | Durable mail; session-bound hooks; separate opted native queue notification | Real CLI mail exchange; real hook CLI with loopback provider; native queue evidence separately scoped |
| Claude Code | CLI/MCP mailbox; session-bound boundary hooks | Real CLI exchange and real Claude hook sessions; no arbitrary idle-session wake |

Earlier real Codex coordination and a Claude MCP caller reaching a Codex task are
separate observations. New reciprocal mailbox, scoped agent-action and fault
tests are described in the standalone specification; they do not establish
universal OS/runtime support. Native task outcomes (completed, failed, interrupted) remain
separate from messages; an unobserved cause remains unknown. An agent's prose
claim such as refusal or need for approval is not a runtime fact or owner grant.

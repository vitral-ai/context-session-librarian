# Agent hub: register, approve, send

The experimental hub is a single-owner TLS service. Each existing machine
connector polls it over outbound HTTPS; no inbound agent-machine port, watcher,
or extra agent-machine service is needed. SSH and TLS can share its port 443:
the hub proxies SSH identification streams to the relay's local sshd.

An owner provisions one random 256-bit machine token **on that machine** (0600).
Only its SHA256 is stored in the hub machines JSON, with `admin: true` for the
owner machine. Native login files, SSH private keys and Claude peer tokens stay
local. HTTPS validates the configured CA and hostname; a private self-signed
certificate must be pinned through a separately authenticated provisioning path.

Each machine has an owned `~/.config/csl/agents.json`:

```json
{
  "url": "https://hub.example.com",
  "cafile": "/private/config/hub-ca.pem",
  "token_file": "/private/config/machine.token",
  "machine": "laptop",
  "connector_home": "/private/bridge/codex",
  "delivery_homes": {"codex": "/private/bridge/codex", "claude": "/private/bridge/claude"}
}
```

`connector_home` selects ONE existing connector to poll per machine. Other local
bridge homes use their existing private IPC socket. Delivery validates the local
name/session binding; an existing different binding is never overwritten.

```sh
csl agents register --name client --session UUID --cli codex --machine threadripper
csl agents register --name coordinator --session UUID --cli claude --machine laptop
csl agents allow coordinator client --both
csl agents list
csl agents list --owner coordinator
csl agents status --owner coordinator   # current Hub (unfiltered status is the NATS command)
csl agents show client
csl agents permissions
printf '%s\n' 'Your message' | csl send --from coordinator --to client --text-file -
csl agents receipt MESSAGE_UUID
printf '%s\n' 'Your reply' | csl reply --from client --to coordinator --in-reply-to MESSAGE_UUID --text-file -
csl agents deny coordinator client --both
csl agents remove client
```

Registration is allowed for the authenticated machine's agents; only the owner
machine can approve pairs or manage other machines. Changing a binding removes
its approvals. The hub derives sender session and machine from its registry,
never message text. Agent names on one machine share that machine's trust domain.
Revoke a machine by removing its hash and restarting the hub; do not copy tokens.

`send` prints its ID before networking. `queued` means durable at the hub, not
native delivery. Check `receipt`; reuse the SAME ID and text after a lost HTTP
response. ID collisions refuse. Active Codex recipients receive native `turn/steer` input at their next
boundary; they need not finish the turn first. Unopened or unavailable recipients
stay queued until open, without blocking siblings. Delivered Codex receipts include the
actual Native turn ID. Interrupted/uncertain Native invocations are NOT replayed.
Replies are explicit `reply` messages, not implicit final-turn forwarding.

Claude delivery persists the envelope in its existing bridge mailbox, then sends
a compact preview through its authenticated local native peer socket. The session-bound
hook is a fallback: a successful native write and the hook share one emission record,
so the hook does not repeat it. A failed native write leaves the fallback eligible; idle wake is native-version-dependent. Its
receipt is durable mailbox delivery plus a wake attempt, not a read receipt.
A closed Claude session retains mail; once open, polling retries its wake.

Hub storage is SQLite; undelivered messages survive hub/connector restarts.
Approvals and pinned source/target bindings are checked again before delivery.
An already emitted envelope cannot be recalled by subsequent revocation.
Pair history retains the last 200 messages by default, including queued, delivered,
refused and dropped outcomes. Set `--history-retention N` (1..10000) on the existing
hub service to change it; restarting applies the smaller limit immediately.
History trimming does not delete pending envelopes or their idempotency ledger.
Keep the database private, back it up normally, and provision disk space.

Missed or lost a message? `csl agents history --with <agent>`.
Set `CSL_AGENT` to your registered name, or pass `--from NAME`:

```sh
export CSL_AGENT=client
csl agents history --with coordinator              # last 5, oldest to newest
csl agents history --with coordinator --last 20    # no more than retention
csl agents history --id MESSAGE_UUID_OR_PREFIX     # unique prefix, at least 8 characters
```

Results contain id, source, target, time, state and complete body in both directions.
Only an authenticated endpoint machine acting as a participant, or the owner
machine, can read a pair. Machines remain trust domains: this is not isolation
between agents that share one machine token. Rebinding a name to another machine
does not grant that machine access to the prior machine's history. The existing
`csl-bridge-mcp` exposes the same read-only `history` tool with `with`/`last` or `id`,
bound to its configured agent; no approval or runtime call is needed.

Native Claude and Codex input uses `CSL from <agent> (<id8>):` followed by at most
600 body characters. Truncated previews say ` [truncated, csl receive for full text]`;
hub history returns the full body. Transport identity and pair authorization still
come from the registry, never from text imitating a header. Local IPC emission is
not a proof of model consumption; a process crash between an IPC write and its
SQLite commit cannot provide distributed exactly-once delivery.
Messages are limited to 10,000 UTF-8 bytes; pull batches and HTTP requests are bounded.

Server (the only new permanent process):

```sh
python3 csl_agents.py --database /private/hub.sqlite --machines /private/machines.json \
  --certificate /private/tls.pem --key /private/tls.key --bind 0.0.0.0 --port 443 --ssh-port 22
```

Use an unprivileged service user with only `CAP_NET_BIND_SERVICE`, private state,
and restart-on-failure. Keep existing sshd local and expose only 443 in the NSG.
This replaces the cross-machine message route, not native runtime ownership or
CLI-specific authorization. Existing SSH routes may remain as an opt-in fallback.

## PR completion notices

A default-branch `workflow_run` handler calls the pinned reusable
`pr-completion-notice.yml` job after a reviewed PR workflow completes. The job
uses GitHub PR/run metadata and the overall success or failure. It builds the
publisher from a trusted checkout pinned to a full SHA. The notice identifies
the workflow, exact PR head and run attempt URL. A rerun has its own notice ID.

Use `codex/<agent>/<topic>` for agent branches. Use exact branch overrides in
`CSL_NATS_CI_ROUTES` for established branch names. The notifier skips an unmapped
branch, and the control service quietly ignores an unregistered recipient.

Store `CSL_NATS_CI_CREDS` only in the `csl-ci-notices` environment. Keep no
repository-level or organization-level copy. Allow only the single exact
default branch as a deployment branch, without tags or wildcard rules. The credential
can publish only to tenant CI subjects and has no subscriptions. The environment
also holds `CSL_NATS_CI_URL`, `CSL_NATS_CI_CA_PEM` and `CSL_NATS_CI_ROUTES`.
Configure the protected environment before setting credentials. The trusted job
uses a separate read-only GitHub token to check the current PR head once.
It skips a superseded head and keeps the exact run head if the lookup fails.

The NATS-only pin publishes through `nats-control/cmd/csl-ci-notice`. Publication
failure fails the notifier job. Repositories still on the older dual-delivery
pin can publish to NATS after a failed Hub request, but the obsolete request can
fail the notifier run until the caller is repinned.

Direct bridge tasks retain their full bodies because they have no hub-history fallback. Hub Codex messages use previews; Claude durable mail retains its legacy full-body fingerprint across upgrades. An unavailable native wake does not unset durable mailbox delivery; the hook remains eligible.

Hub native messages include a one-line `Reply via hub: csl reply FULL_ID --from YOUR_NAME --text ...` footer. Replace `...` with quoted reply text (or use `--text-file FILE`). Inline native final answers are not mailed automatically. `csl reply ID_OR_PREFIX --from YOUR_NAME --text "reply"` resolves the sender from retained incoming history. History `--id` and reply `--in-reply-to` also accept unique UUID prefixes of at least 8 characters; ambiguous prefixes ask for more characters. Explicit `--to` plus `--in-reply-to` works beyond history retention using the durable delivery ledger.

## Experimental NATS adapter

The NATS adapter supports text delivery to Codex and Claude. Install
`nats-py[nkeys]` in the Python environment used by CSL. Use a private JSON file
(mode 0600) with owner-issued credentials and an existing private bridge home:

```json
{
  "tenant": "vi",
  "servers": ["tls://broker.example.net:4222"],
  "cafile": "/private/config/broker-ca.pem",
  "connector_home": "/private/bridge/codex",
  "agents": {
    "alice": {
      "session": "22cb43cf-8044-4ac2-82e9-e0bec9fd7188",
      "credentials": "/private/config/alice.creds"
    }
  }
}
```

Set `CSL_NATS_CONFIG` to that file in the existing Codex connector's environment
to enable receiving. The local name and native thread must already be
registered. For a NATS-only name, use
`csl bridge --home HOME agent --nats NAME --thread UUID`.
Send with `csl send --nats-config PATH --from alice --to bob --text
"Hello"`. Add `--urgent --reason blocked` for an urgent message. If the send
outcome is unknown, retry with `--id` set to the printed `message_id`, the
printed `retry_pin` and `retry_from` options, and the same target, text and reply parent.
For a NATS reply, use the printed `retry_reply` arguments in place of the
original parent and target arguments.

To opt in an individual sender, add its name and absolute NATS config path
to `senders` in the owner-managed `~/.config/csl/sender-routes.json`. Set
`nats_recipients` to the names enrolled in that NATS tenant:

```json
{"senders":{"alice":"/private/config/vi.json"},"nats_recipients":["alice","bob"]}
```

Plain `csl send` uses the current Claude session, current Codex thread, or sole
local enrollment to identify the caller when `--from` and `CSL_AGENT` are unset.
An enrolled sender uses NATS for all recipients on NATS-only installations.
When a Hub is also configured, its default caller applies if no NATS session
matches, and the local recipient list selects the enrolled caller's transport.
`csl reply ID_OR_PREFIX` resolves the incoming parent and uses its original
transport when the parent is still retained there. Prefixes must match exactly
one message, starting with at least eight characters of its UUID. The CLI prints
the selected transport. A retry using `--id` requires the printed config pin
and `retry_from` option on either transport.
For a pinned NATS retry, use the printed full parent ID and `--to`.

Before a NATS send or reply, the CLI reports unread messages from the recipient
and lists their IDs on stderr. The send still proceeds if the unread lookup
is unavailable.

For Claude, add `"cli": "claude"`, `"mailbox_home": "/private/bridge/claude"`
and `"claude_config": "/private/claude"` to the agent entry. Register the
exact Claude session in the mailbox home. Run
`csl bridge --home "$CLAUDE_MAIL_HOME" hook --agent carol --event PostToolUse --nats-tenant vi`
at each tool boundary. Use the actual event name for `SessionStart`,
`UserPromptSubmit` and `Stop` hooks. The native wake carries a short alert.
The hook supplies the message text and records delivery after writing it.
An unsurfaced batch can retry its wake once after 30 seconds.

The local `nats_delivered` table in the connector's private `bridge.sqlite`
records the pinned session, turn, and acknowledgment state. A text digest
retains the original message IDs and order. If a Claude digest exceeds the
rendered hook limit, it continues at the next boundary without splitting a
member or acknowledging the digest before its last part is emitted.
During dual delivery, matching CI completion notices reuse the confirmed
native turn for the same message ID, recipient thread and body, including
notices delivered inside a digest.

### NATS history and files

These commands use the agent's local enrollment in
`~/.config/csl/sender-routes.json`. NATS agent tools use the current
Claude session, the current Codex thread, or the sole local enrollment.
Use `--from NAME` or set `CSL_AGENT` to select the caller explicitly.
Pass `--nats-config PATH` to select a specific NATS configuration, or
`--hub-config PATH` to read Hub history.

```sh
csl history --with coordinator --last 10 --from alice
csl history --id MESSAGE_ID --from alice
csl agents status
csl agents history --with coordinator --since 2h
csl agents latest
csl agents allow alice bob
csl agents deny alice bob
csl agents pairs --pending
csl agents watch --agent alice --after 30m --note "Send the build result"
csl agents watch --list
csl agents watch --cancel WATCH_UUID
csl agents versions --from alice
csl agents close --with coordinator --from alice
csl agents complete --with coordinator --from alice
csl put report.txt --to coordinator --from alice
csl fetch FILE_SHA256 --out received.txt --from coordinator
csl send --nats-config PATH --from alice --to coordinator --attach FILE_SHA256 --text "Report attached"
```

`put` returns the SHA-256 file ID. Repeat `--to` for each permitted reader and
`--attach` for each file in a message. `fetch` verifies the digest before
publishing the file and refuses to overwrite an existing path. Fleet status
requires the broker's orchestrator role.

NATS pair changes apply in both directions and require the owner or a
coordinator responsible for both endpoints. A send to an unapproved pair is
held pending approval. Watches are one-shot reminders for orchestrators.

Set `--nats-config PATH` on the existing `csl-bridge-mcp` invocation to enable
NATS history, status, versions, close, and complete for its bound agent.
To enable `put_file` and `fetch_file`, also set `--transfer-dir PATH` to a private,
owned staging directory. File tools accept names relative to that directory.

### Coordinator ownership on the current Hub

The owner machine can set or change a registered agent's coordinator with
`csl agents register --name AGENT --session UUID --cli codex --machine MACHINE --owner COORDINATOR`.
Register coordinators with themselves as owner. Services `ci-notices` and
`hub-notifier` have no owner. `agents show AGENT` includes ownership; show,
list and status accept `--owner COORDINATOR`. Ownership is routing metadata,
not a grant: updates preserve session bindings, queued packets and pair approvals.
An omitted owner preserves the existing value (the API accepts null to clear it).
Tell each new worker who its coordinator is through a channel the worker already trusts.

Refusals notify the sender and its coordinator once each, or only once when
both are the same agent. Service refusals notify only a known target's owner;
unknown CI-notice targets remain quiet drops. No unrelated coordinator receives
the refusal. Removing a coordinator clears its ownership references.

Deployment: back up the Hub database, deploy the module, then immediately backfill
owners using the existing exact session/CLI/machine values with register --owner.
Before backfill, unowned senders notify only themselves and unowned service
recipients produce no notice. For the current deployment, assign laptop-orchestrator
to cx-backend, cx-win, cx-client, cx-land, cx-verify and bridge-builder;
releases-orchestrator to cx-rel; comms-orchestrator to nats-control, nats-infra
and nats-clients. Set each of those three coordinators to own itself. Services
remain unowned and only the admin machine may register ci-notices. Confirm all
bindings, queued packet bytes and pair approvals are unchanged after backfill.

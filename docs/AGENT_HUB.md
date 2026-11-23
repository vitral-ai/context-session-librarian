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
response. ID collisions refuse. Busy or unopened Codex recipients stay queued
until idle/open, without blocking siblings. Delivered Codex receipts include the
actual Native turn ID. Interrupted/uncertain Native invocations are NOT replayed.
Replies are explicit `reply` messages, not implicit final-turn forwarding.

Claude delivery persists the envelope in its existing bridge mailbox, then sends
a header through its authenticated local native peer socket. The session-bound
hook exposes headers at boundaries; idle wake is native-version-dependent. Its
receipt is durable mailbox delivery plus a wake attempt, not a read receipt.
A closed Claude session retains mail; once open, polling retries its wake.

Hub storage is SQLite; undelivered messages survive hub/connector restarts.
Approvals and pinned source/target bindings are checked again before delivery.
An already emitted envelope cannot be recalled by subsequent revocation.
There is no history UI, tenancy system, or automatic retention in this initial
shape. Keep the database private, back it up normally, and provision disk space.
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

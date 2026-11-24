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
response. ID collisions refuse. Active Codex recipients receive native `turn/steer` input at their next
boundary; they need not finish the turn first. Unopened or unavailable recipients
stay queued until open, without blocking siblings. Delivered Codex receipts include the
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

## PR completion notices

A default-branch `workflow_run` handler calls the pinned reusable
`pr-completion-notice.yml` job after a reviewed CI workflow completes. It runs on
GitHub-hosted runners without executing PR code or consuming its artifacts. Each
completion posts once using GitHub's PR/run metadata and overall conclusion.
The notice identifies the workflow, PR head and run link, rather than claiming
that unrelated workflows have finished. Forks receive no notice secret.

Use `codex/<hub-agent-name>/<topic>` for agent branches. For existing branches,
the hub owner can add an exact `routes: {"branch": "agent"}` mapping to that
repository's notice credential. The destination must be registered and allowed
from `ci-notices`. Unknown branches refuse clearly; they never default to the
coordinator or another agent.

Each repository stores `CSL_HUB_NOTICE_TOKEN` ONLY in environment
`csl-ci-notices`, with custom deployment branches allowing only the default
branch (`main`). No repository-level copy, PR refs, tags or wildcard policies are
allowed. A candidate workflow naming this environment is rejected by GitHub, not
merely by an editable YAML condition. Configure this policy before storing the
secret, and verify it on readback. Each repository also has public variables
`CSL_HUB_URL` (HTTPS `/v1`) and `CSL_HUB_CA_PEM` (pinned certificate). Its token hash
is a hub machines entry with `scope: notices`, `repository: owner/repo`,
`source: ci-notices`, `source_machine: laptop` and optional `routes`. This token
can only post structured completion metadata for that repository: no arbitrary
text, mailbox reads, agent registration, approvals or delivery acknowledgments.
The hub constructs the text and the UUID from repository/run/attempt, so retries
cannot duplicate a completion. An intentional GitHub rerun gets a new notice.
A notice failure is visible in its job log but does not change the CI verdict.

To revoke a credential, remove its entry from the hub machines file, reload the
existing hub service, then `gh secret delete CSL_HUB_NOTICE_TOKEN --env csl-ci-notices -R owner/repo`.
No machine login token is used by GitHub Actions. There is no polling job or new
service. Pin reusable workflow references and its trusted client to full SHAs.

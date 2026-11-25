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
branch (`main`, or `development` for vitral_landing). No repository-level copy, PR refs, tags or wildcard policies are
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


During the NATS migration a protected notifier may also publish the same
completion ID and text. Only a verified current-head success or failure goes
to NATS. The HTTPS hub remains active and retains its existing behavior when
the head lookup fails. The NATS copy uses the repo's exact
`CSL_NATS_CI_ROUTES` branch overrides, then the normal `codex/<agent>/...`
route. Missing routes are skipped and the control service quietly drops an
unregistered recipient.

The NATS credential belongs in `csl-ci-notices` as `CSL_NATS_CI_CREDS`
under the same default-branch policy. It may only publish to the tenant's CI
subjects and has no subscriptions. The trusted notifier builds
`nats-control/cmd/csl-ci-notice` from the same reviewed checkout and provides
its path, public TLS URL and pinned CA as `CSL_NATS_CI_PUBLISHER`,
`CSL_NATS_CI_URL` and `CSL_NATS_CI_CA_PEM`. With the NATS credential absent,
the notifier keeps the original hub-only path.

To revoke a credential, remove its entry from the hub machines file, reload the
existing hub service, then `gh secret delete CSL_HUB_NOTICE_TOKEN --env csl-ci-notices -R owner/repo`.
No machine login token is used by GitHub Actions. There is no polling job or new
service. Pin reusable workflow references and its trusted client to full SHAs.

Completion notices read the current PR head once before posting. A different head skips the superseded notice; a failed lookup still posts the exact completed run head. The lookup uses GitHub read-only PR metadata access; notice credentials and hub delivery stay unchanged.

Direct bridge tasks retain their full bodies because they have no hub-history fallback. Hub Codex messages use previews; Claude durable mail retains its legacy full-body fingerprint across upgrades. An unavailable native wake does not unset durable mailbox delivery; the hook remains eligible.

Hub native messages include a one-line `Reply via hub: csl reply FULL_ID --from YOUR_NAME --text ...` footer. Replace `...` with quoted reply text (or use `--text-file FILE`). Inline native final answers are not mailed automatically. `csl reply ID_OR_PREFIX --from YOUR_NAME --text "reply"` resolves the sender from retained incoming history. History `--id` and reply `--in-reply-to` also accept unique UUID prefixes of at least 8 characters; ambiguous prefixes ask for more characters. Explicit `--to` plus `--in-reply-to` works beyond history retention using the durable delivery ledger.

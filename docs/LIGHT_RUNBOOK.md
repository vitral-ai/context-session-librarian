# Light pool transport

Development checkout feature; not included in the v0.4.2 release. Linux,
Python 3.11+, existing CSL bridge homes and registered Codex threads. Start each
new agent with its verbatim first brief before enrollment; the owner records
`{"agent":"worker","thread":"UUID"}` in a guest marker file afterwards.

## Setup once

Use an existing bridge home/connector from this checkout. Light attaches to its
existing native endpoint; it never creates a thread, runtime or login. Client-only
homes can exchange mail and read results; native followup activation needs Codex.

```sh
csl light init --node hub --router --bridge-home /path/host-bridge \
  --agent coordinator --service
```

Repeat `--bridge-home` and `--agent` for multiple local homes. `--service` installs
one persistent user unit, `csl-light.service`; alternatively run `csl light run`.

## Add a running environment

Use an existing authenticated SSH alias, with its host key already pinned:

```sh
csl light enroll sandbox8 --agent worker --ssh-alias owner-sandbox8 \
  --guest-bridge-home /path/guest-bridge \
  --first-brief-marker /path/first-brief-started.json
```

One command verifies the registered first-brief thread, creates a per-env key,
installs the guest broker/client and restricted public key, and adds one `links`
entry to `~/.local/share/csl-light/config.json`. Existing streams stay open;
brokers and connectors reload light additions, removals and exact rollbacks.
A connection has at most 90 seconds to produce its first application frame while
owner authentication runs; afterwards 20 seconds without a frame reconnects it. `--thread UUID` can additionally
assert the expected thread. Enrollment audits and exact prior config/peer rows
are kept under the light home. Guest source contains code only, never auth files.

For a CA-only pool, supply `--bootstrap-command '["owner-installer"]'` and
`--connect-command '["owner-stream","{attach}"]'` instead of a plain-key channel.
The installer hook reads JSON `{script,payload}` from stdin and returns the JSON
installation proof; the stream hook runs the fixed `{attach}` command. These
owner-controlled hooks obtain existing access only at installation/reconnection.
No controller call occurs per message. No CA/private credential is exported.
With an owner stream hook, the per-env key file is a local revoke gate; SSH uses
the hook's existing identity. `--no-services` supports separately supervised guests.

## Send, follow up, inspect

```sh
csl light send worker /path/message.txt
csl light followup UUID /path/task.txt
csl light inbox --after 0
```

Use `--from coordinator` when a home has several agents. Preserve the returned
ID and identical content with `--id UUID` on retries. Stream IDs are unique per
(direction, peer); native operation/mailbox checks also prevent cross-peer reuse
in a bridge home. Native acceptance recovery checks the latest 20 turns by client message ID.
A crash after native acceptance but before journal commit, followed by more than
20 newer turns, exceeds that recovery window and can produce a duplicate native
turn. Normal committed delivery/replay remains cached; this limit concerns the
uncertain native-acceptance crash window. Both endpoints commit before
ACK; duplicates reuse the stored result. Progress/final CSL events are pushed over
the same stream and appear in the inbox and existing bridge `wait` interface.
Enrollment opts into registered sender session headers for light.
Mail wakes an explicitly enrolled native recipient; existing bridge mailbox
transport alone does not grant that behavior. Task text is not authority.

Delete `~/.local/share/csl-light/keys/sandbox8` to stop the workstation link.
That does not revoke a copied private key: delete the guest `keys/workstation.pub`
marker or remove its managed `csl-light-sandbox8` authorized-key line too. Marker
deletion closes the stream; a reconnect receives revocation and parks locally until
explicit re-enrollment. Re-enrollment replaces the managed key line and clears that
park marker. Owner-hook identity revocation remains with that hook's owner.

Use `systemctl --user status csl-light` and `journalctl --user -u csl-light` for
health. `loginctl show-user "$USER" -p Linger` must report `yes` for user services
to start before login after reboot. If it does not, the owner must enable linger
(`loginctl enable-linger`) or expect recovery after login; enrollment does not
change this machine policy.

Offline messages remain in `light.sqlite`, including when the local broker is
temporarily down. Repair connectivity/authentication and retry the same ID; no
messaging cohort, publisher receipt or hold extension is a recovery step. Payload
retention is opt-in: set `retention_days` (for example 30) in config to compact
completed stream payloads. Pending messages are never pruned; small permanent hash
and receipt tombstones still refuse ID reuse and replay the cached result. Payload
text is no longer available through the light inbox after compaction. CLI retries
that need the original payload also stop at that retention boundary; an archived
ID cannot create another native delivery. Existing
bridge operation/mail records and enrollment rollback snapshots remain intact;
enrollment audits store source hashes instead of full source copies. Environment
lifecycle, holds and keep-alive publisher remain separate.

Event receipt correlation: a final/progress event is exposed only after the exact
original delegated request has a confirmed native acceptance turn. Light may
recover that original receipt locally from the exact peer/path and immutable RPC
payload; it does not resend the original work. Routed paths retain the logical
origin separately from the authenticated hub hop. Compacted or missing routed
provenance stays deferred rather than supplying authority from event text.

For deprecated watch transports, an event waiting for its original acceptance
keeps that peer's remote cursor unchanged under the existing paced retry. Other
peer workers continue. A legacy inbound gap similarly holds later events for the
same recipient's `wait` cursor until proof arrives; the bounded wait can return
empty. Recording a valid original receipt wakes blocked waiters. Repeating an old
`wait --after` deliberately rereads existing confirmed events. This correlation
check does not implement canonical-final deduplication or exactly-once effects.

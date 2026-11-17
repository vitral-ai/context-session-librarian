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
brokers and connectors reload light entries. `--thread UUID` can additionally
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
ID and identical content with `--id UUID` on retries. Both endpoints commit before
ACK; duplicates reuse the stored result. Progress/final CSL events are pushed over
the same stream and appear in the inbox and existing bridge `wait` interface.
Mail wakes an explicitly enrolled native recipient; existing bridge mailbox
transport alone does not grant that behavior. Task text is not authority.

Delete `~/.local/share/csl-light/keys/sandbox8` to revoke that link, or delete the
guest `keys/workstation.pub` marker. The active stream closes and reconnect stops.
Use `systemctl --user status csl-light` and `journalctl --user -u csl-light` for
health. Offline messages remain in `light.sqlite`, including when the local
broker is temporarily down. Repair connectivity/authentication and retry the same
ID; no messaging cohort, publisher receipt or hold extension is a recovery step.
Environment lifecycle, holds and keep-alive publisher remain separate.

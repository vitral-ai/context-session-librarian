# CSL JetStream control service

The service handles tenant decisions on the existing JetStream streams. It runs
once per active tenant primary and uses the same broker account as the agents.
The core CSL Python CLI does not import this service or its dependencies.

## Local verification

With Go 1.27.4, install the pinned NATS server in a user-owned bin directory and
point the test at that binary.

```sh
go install github.com/nats-io/nats-server/v2@v2.15.3
cd nats-control
CSL_TEST_NATS="$(go env GOPATH)/bin/nats-server" go test -race ./...
```

The test starts an isolated localhost broker in operator mode with synthetic
keys. It tests registration, connection permissions, version checks, scoped
pull and ACK, immediate and grouped delivery, file access and pair state.

## DEV start

Build with `go build ./cmd/csl-control`. Deployment supplies the existing
tenant account, `CSL_MSG`, `CSL_STATE` with per-key TTL enabled, and
`CSL_FILES`. Set `CSL_MSG` to a 1 MiB maximum envelope.

Run the binary with the active primary TLS URL, CA, tenant public account key,
service credentials file, and the tenant user signing-seed file. Both private
files stay on the host. Use `--domains` for the authorized primary and DR
JetStream domains and `--upgrade-command` for the version refusal response.
Initialize the minimum and recommended versions through authenticated owner
administration before enabling agent traffic.

The owner credential can send to `csl.<tenant>.ctl.admin.owner` and subscribe
to `_INBOX.csl.<tenant>.owner.>`. Agent and owner clients use their own
`_INBOX.csl.<tenant>.<caller>` as the NATS request inbox prefix.

The account revocation helper is called with the tenant ID, tenant public
account key, UTC RFC3339 time and public user keys as positional arguments.
It updates the broker's operator-signed JWT through that account's exact
CLAIMS.LOOKUP and CLAIMS.UPDATE subjects. The helper can use a scoped SYS
reply prefix via `--inbox-prefix`. On a primary/standby deployment, set
`--revoke-peer` to the opposite broker's pinned TLS URL. The exact claim
subjects must resolve locally on each broker. The helper uses the same
node-local SYS credential, CA and inbox prefix for both connections.
It reads both claims, requires matching account policy and signs one update
with the highest cutoff per key. Both readbacks must confirm the merged
revocations within the same 17-second deadline. A partial failure returns
an error, and the next batch retry starts from both resolvers' current claims.
The deployment must hold the active broker's exclusive account writer fence
from the first lookup through final readback. All account claim changes,
including manual maintenance and promotion handoff, use the same authority.
The resolver replaces a complete signed JWT on update. Concurrent writers
could each verify their own update and still overwrite an unseen cutoff.
The control service denies further calls as soon as the owner revocation is recorded.
The owner response includes `pending: true` until broker revocation completes.
The existing control scheduler calls the helper with batches of at most 100
keys, a fixed cutoff and a 30-second caller deadline. Batch retries survive
restarts. An exhausted retry sequence notifies the configured owner and
leaves the batch queued for a later control pass.
Only start control beside the active primary. During planned promotion, stop
the old control instance before starting the same build and tenant
configuration against the promoted writable streams.

## Standby mirror alerts

On the active primary, give the control process a separate read-only SYS
credential for the standby and the verified `--mirror-nats`,
`--mirror-credentials`, `--mirror-inbox-prefix` and `--mirror-server` flags.
Both broker connections use the configured CA. The existing one-minute
control pass reads JSZ. Positive backlog observed for more than 60 seconds
alerts the configured owner, repeats at most every 15 minutes, and sends a
recovery notice after all three mirrors report zero backlog. A probe failure
lasting more than five minutes sends an unavailable notice. Probe failure
does not clear an active lag alert. Omit the flags on the promoted standby
until a separate mirror endpoint is available.

## Tenant administration

The owner can fetch a file with `{"action":"fetch","id":"<sha256>","offset":0}`
and update its ACL with `{"action":"file-acl","id":"<sha256>","allowed":["agent-name"]}`
on the existing owner admin subject. The fetch reply uses the same chunk
format as the agent download call.

The authenticated owner can set `{"action":"settings","notices_to":"agent-name"}`
to route sanitized system notices to a registered agent. Notices contain the
reason and agent identities, and repeats of the same reason and pair are
coalesced in a ten-minute window. CI notices to unknown agents remain quiet.

Build `./cmd/csl-operator` for the host that already holds the operator seed
and the SYS credential. This keeps operator privileges out of the tenant
control process. Configure `--create-tenant-helper` with a delegated wrapper
that passes its four positional arguments (tenant ID, storage bytes, maximum
file bytes, messages per minute) to `csl-operator … create-tenant`. The helper
returns only the public tenant identity, and keeps generated credential files
in its private material directory. The owner submits
`{"action":"create-tenant","tenant":"zt-test"}`. Optional quota fields are
`storage_bytes`, `file_bytes` and `rate_per_minute`. Retrying the same request
restores any missing stores without replacing the tenant account.

For a disposable DEV tenant, run a separate control instance pointed to that
account and set `--allow-erase-tenant` to the exact tenant ID. The owner's
`{"action":"erase-tenant","tenant":"zt-test"}` request returns a confirmation
token valid for ten minutes. Submit the same request with `confirm` set to
that token to revoke current agent credentials and purge its message, file
and state stores. Reprovisioning restores the original quotas and re-enables
registration. The operation is disabled for `vi`. Export and erasure are
audited without logging payloads or the token.

### Importing settled Hub history

Run `python scripts/import_hub_history.py --hub-db PATH --pair agent-a,agent-b`
for a count-only preflight. Repeat `--pair` to select more approved pairs. The
tool reads the last 200 Hub rows per pair in a read-only transaction. It stops
on unsettled envelopes or endpoint machine mismatches.

Apply with `--apply --server tls://WRITER:4222 --tenant TENANT --owner-creds PATH`
(+ `--ca PATH` for private trust). When the Hub database and owner credential are
on different hosts, pipe the source tool's `--write-selected` output over SSH
into the owner tool's `--read-selected --apply` input. The stream contains private
message bodies. Keep it out of logs and use the owner credential in place.

Before writing, the owner tool reports payload bytes, reserved archive bytes
and available space. A batch that exceeds available space is refused before
any row is written. Each append is also subject to the broker's byte limit.

Imports preserve settled state in the separate `CSL_ARCHIVE` stream. History
reads both streams. Native NATS records take precedence for matching IDs.
Reapplying the same settled snapshot is safe.

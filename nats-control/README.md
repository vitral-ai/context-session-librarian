# CSL JetStream control service

The service handles tenant decisions on the existing JetStream streams. It runs
once per active tenant primary and uses the same broker account as the agents.
The core CSL Python CLI does not import this service or its dependencies.

## Local verification

With Go 1.27.4, install the pinned NATS server in a user-owned bin directory and
point the test at that binary.

```sh
go install github.com/nats-io/nats-server/v2@v2.12.3
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
It must update the broker's operator-signed account JWT. The control service
denies further calls from a revoked agent even while the broker is updating.
Only start control beside the active primary. During planned promotion, stop
the old control instance before starting the same build and tenant
configuration against the promoted writable streams.

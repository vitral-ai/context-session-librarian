package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	control "github.com/vitral-ai/context-session-librarian/nats-control"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	broker := flag.String("nats", "", "broker URL")
	revokePeer := flag.String("revoke-peer", "", "opposite resolver TLS URL for revocation")
	ca := flag.String("ca", "", "broker CA file")
	operatorFile := flag.String("operator-seed", "", "operator signing seed file")
	inboxPrefix := flag.String("inbox-prefix", "", "custom SYS request inbox prefix")
	systemCreds := flag.String("system-credentials", "", "SYS operator credentials file")
	materialDir := flag.String("material-dir", "", "private tenant provisioning directory")
	allowedAccount := flag.String("revoke-account", "", "account public key authorized for this revoker")
	allowedTenant := flag.String("revoke-tenant", "", "tenant ID authorized for this revoker")
	flag.Parse()
	operation := flag.Arg(0)
	if *broker == "" || *operatorFile == "" || *systemCreds == "" ||
		(operation != "create-tenant" && operation != "revoke") {
		return errors.New("broker, operator seed, SYS credentials and an operator action are required")
	}
	if err := validateBroker(*broker, false); err != nil {
		return err
	}
	if *revokePeer != "" {
		if operation != "revoke" {
			return errors.New("--revoke-peer requires the revoke action")
		}
		if err := validateBroker(*revokePeer, true); err != nil {
			return err
		}
	}
	seed, err := os.ReadFile(*operatorFile)
	if err != nil {
		return errors.New("unable to read operator signing seed")
	}
	defer clear(seed)
	operator, err := nkeys.FromSeed(bytes.TrimSpace(seed))
	if err != nil {
		return errors.New("invalid operator signing seed")
	}
	defer operator.Wipe()
	ctx, cancel := context.WithTimeout(context.Background(), 17*time.Second)
	defer cancel()
	connect := func(endpoint, creds, replyPrefix string) (*nats.Conn, error) {
		deadline, _ := ctx.Deadline()
		timeout := min(5*time.Second, time.Until(deadline))
		if timeout <= 0 {
			return nil, context.DeadlineExceeded
		}
		opts := []nats.Option{nats.UserCredentials(creds), nats.Timeout(timeout), nats.NoReconnect(), nats.IgnoreDiscoveredServers()}
		if *ca != "" {
			opts = append(opts, nats.RootCAs(*ca))
		}
		if replyPrefix != "" {
			opts = append(opts, nats.CustomInboxPrefix(replyPrefix))
		}
		return nats.Connect(endpoint, opts...)
	}
	sys, err := connect(*broker, *systemCreds, *inboxPrefix)
	if err != nil {
		return errors.New("unable to connect as the operator system user")
	}
	defer sys.Close()
	args := flag.Args()[1:]
	if operation == "revoke" {
		if len(args) < 4 || len(args) > 103 || args[0] != *allowedTenant || args[1] != *allowedAccount {
			return errors.New("account or tenant is outside the configured revocation scope")
		}
		at, err := time.Parse(time.RFC3339, args[2])
		if err != nil {
			return errors.New("invalid revocation timestamp")
		}
		if *revokePeer != "" {
			peer, err := connect(*revokePeer, *systemCreds, *inboxPrefix)
			if err != nil {
				return errors.New("unable to connect to the peer resolver as the operator system user")
			}
			defer peer.Close()
			return control.RevokeAccountUsersOnPair(ctx, sys, peer, operator, args[1], args[3:], at)
		}
		return control.RevokeAccountUsers(ctx, sys, operator, args[1], args[3:], at)
	}
	if len(args) != 4 || *materialDir == "" {
		return errors.New("provide the tenant ID, storage bytes, file bytes and rate per minute")
	}
	storage, errStorage := strconv.ParseInt(args[1], 10, 64)
	file, errFile := strconv.ParseInt(args[2], 10, 64)
	rate, errRate := strconv.Atoi(args[3])
	if errStorage != nil || errFile != nil || errRate != nil {
		return errors.New("invalid tenant quotas")
	}
	provision := control.Provisioner{Operator: operator, System: sys, Directory: *materialDir,
		ConnectTenant: func(path string) (*nats.Conn, error) { return connect(*broker, path, "") }}
	info, err := provision.Create(ctx, control.TenantRequest{Tenant: args[0], StorageBytes: storage, FileBytes: file, RatePerMinute: rate})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(info)
}

func validateBroker(endpoint string, peer bool) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || strings.Contains(endpoint, ",") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Hostname() == "" {
		return errors.New("invalid broker URL")
	}
	if parsed.Scheme == "tls" || parsed.Scheme == "wss" {
		return nil
	}
	ip := net.ParseIP(parsed.Hostname())
	if peer || parsed.Scheme != "nats" || (parsed.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback())) {
		return errors.New("broker TLS is required")
	}
	return nil
}

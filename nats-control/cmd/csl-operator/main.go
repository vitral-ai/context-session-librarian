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
	parsed, err := url.Parse(*broker)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Hostname() == "" {
		return errors.New("invalid broker URL")
	}
	if parsed.Scheme != "tls" && parsed.Scheme != "wss" {
		ip := net.ParseIP(parsed.Hostname())
		if parsed.Scheme != "nats" || (parsed.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback())) {
			return errors.New("broker TLS is required outside localhost")
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
	connect := func(creds, replyPrefix string) (*nats.Conn, error) {
		opts := []nats.Option{nats.UserCredentials(creds), nats.Timeout(5 * time.Second)}
		if *ca != "" {
			opts = append(opts, nats.RootCAs(*ca))
		}
		if replyPrefix != "" {
			opts = append(opts, nats.CustomInboxPrefix(replyPrefix))
		}
		return nats.Connect(*broker, opts...)
	}
	sys, err := connect(*systemCreds, *inboxPrefix)
	if err != nil {
		return errors.New("unable to connect as the operator system user")
	}
	defer sys.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	args := flag.Args()[1:]
	if operation == "revoke" {
		if len(args) < 4 || args[0] != *allowedTenant || args[1] != *allowedAccount {
			return errors.New("account or tenant is outside the configured revocation scope")
		}
		at, err := time.Parse(time.RFC3339, args[2])
		if err != nil {
			return errors.New("invalid revocation timestamp")
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
		ConnectTenant: func(path string) (*nats.Conn, error) { return connect(path, "") }}
	info, err := provision.Create(ctx, control.TenantRequest{Tenant: args[0], StorageBytes: storage, FileBytes: file, RatePerMinute: rate})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(info)
}

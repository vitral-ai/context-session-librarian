package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	control "github.com/vitral-ai/context-session-librarian/nats-control"
)

func main() {
	tenant, err := run()
	if err != nil {
		slog.Error("control stopped", "tenant", tenant, "error", err)
		os.Exit(1)
	}
}

func run() (string, error) {
	tenant := flag.String("tenant", "vi", "tenant ID")
	account := flag.String("account", "", "public NATS account key")
	url := flag.String("nats", "", "active primary NATS URL")
	creds := flag.String("credentials", "", "path to csl-control credentials")
	ca := flag.String("ca", "", "trusted DEV CA file")
	signerFile := flag.String("signer-file", "", "path to the tenant user-signing seed")
	domains := flag.String("domains", "", "comma-separated authorized JetStream domains")
	upgrade := flag.String("upgrade-command", "", "command operators provide to upgrade CSL")
	revoke := flag.String("revoke-helper", "", "tenant-scoped account JWT updater")
	flag.Parse()
	if *url == "" || *creds == "" || *signerFile == "" {
		return *tenant, errors.New("nats, credentials and signer-file are required")
	}
	seed, err := os.ReadFile(*signerFile)
	if err != nil {
		return *tenant, errors.New("unable to read the account signing seed")
	}
	defer clear(seed)
	signer, err := nkeys.FromSeed(trim(seed))
	if err != nil {
		return *tenant, errors.New("invalid account signing seed")
	}
	defer signer.Wipe()
	options := []nats.Option{nats.Name("csl-control"), nats.UserCredentials(*creds), nats.Timeout(5 * time.Second)}
	if *ca != "" {
		options = append(options, nats.RootCAs(*ca))
	}
	conn, err := nats.Connect(*url, options...)
	if err != nil {
		return *tenant, err
	}
	defer conn.Close()
	config := control.Config{Tenant: *tenant, Account: *account, Signer: signer, Upgrade: *upgrade}
	if *domains != "" {
		config.Domains = strings.Split(*domains, ",")
	}
	if *revoke != "" {
		config.RevokeUsers = func(ctx context.Context, keys []string, at time.Time) error {
			args := append([]string{*tenant, *account, at.Format(time.RFC3339)}, keys...)
			if err := exec.CommandContext(ctx, *revoke, args...).Run(); err != nil {
				return errors.New("tenant account JWT update failed")
			}
			return nil
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	service, err := control.Open(ctx, conn, config)
	if err != nil {
		return *tenant, err
	}
	subscriptions, err := service.Subscribe()
	if err != nil {
		return *tenant, err
	}
	defer func() {
		for _, sub := range subscriptions {
			_ = sub.Unsubscribe()
		}
	}()
	slog.Info("control ready", "tenant", *tenant)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		tick, cancel := context.WithTimeout(ctx, 45*time.Second)
		if err := service.Tick(tick); err != nil && ctx.Err() == nil {
			slog.Error("control scheduling pass failed", "tenant", *tenant, "error", err)
		}
		cancel()
		select {
		case <-ctx.Done():
			return *tenant, nil
		case <-ticker.C:
		}
	}
}

func trim(seed []byte) []byte {
	// nkeys expects the encoded seed with no trailing newline.
	for len(seed) > 0 && (seed[len(seed)-1] == '\n' || seed[len(seed)-1] == '\r') {
		seed = seed[:len(seed)-1]
	}
	return seed
}

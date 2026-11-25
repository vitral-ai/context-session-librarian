package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/jwt/v2"
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
	broker := flag.String("nats", "", "active primary NATS URL")
	creds := flag.String("credentials", "", "path to csl-control credentials")
	ca := flag.String("ca", "", "trusted DEV CA file")
	signerFile := flag.String("signer-file", "", "path to the tenant user-signing seed")
	domains := flag.String("domains", "", "comma-separated authorized JetStream domains")
	upgrade := flag.String("upgrade-command", "", "command operators provide to upgrade CSL")
	revoke := flag.String("revoke-helper", "", "tenant-scoped account JWT updater")
	create := flag.String("create-tenant-helper", "", "delegated operator tenant provisioner")
	erase := flag.String("allow-erase-tenant", "", "exact disposable tenant ID authorized for erasure")
	flag.Parse()
	if *broker == "" || *creds == "" || *signerFile == "" {
		return *tenant, errors.New("nats, credentials and signer-file are required")
	}
	if err := validateBrokerURL(*broker); err != nil {
		return *tenant, err
	}
	if err := verifyCredentialAccount(*creds, *account); err != nil {
		return *tenant, err
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
	options := []nats.Option{nats.Name("csl-control"), nats.UserCredentials(*creds), nats.Timeout(5 * time.Second),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			slog.Error("broker connection error", "tenant", *tenant, "error", err)
		}),
	}
	if *ca != "" {
		options = append(options, nats.RootCAs(*ca))
	}
	conn, err := nats.Connect(*broker, options...)
	if err != nil {
		return *tenant, err
	}
	defer conn.Close()
	config := control.Config{Tenant: *tenant, Account: *account, Signer: signer, Upgrade: *upgrade, EraseTenant: *erase}
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
	if *create != "" {
		config.CreateTenant = func(ctx context.Context, request control.TenantRequest) (control.TenantInfo, error) {
			out, err := exec.CommandContext(ctx, *create, request.Tenant,
				strconv.FormatInt(request.StorageBytes, 10), strconv.FormatInt(request.FileBytes, 10),
				strconv.Itoa(request.RatePerMinute)).Output()
			if err != nil {
				return control.TenantInfo{}, errors.New("tenant provisioning failed")
			}
			var info control.TenantInfo
			if json.Unmarshal(out, &info) != nil || info.Tenant != request.Tenant ||
				!nkeys.IsValidPublicAccountKey(info.Account) {
				return control.TenantInfo{}, errors.New("tenant provisioner returned an invalid public identity")
			}
			return info, nil
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

func verifyCredentialAccount(path, account string) error {
	material, err := os.ReadFile(path)
	if err != nil {
		return errors.New("unable to read the control credentials")
	}
	defer clear(material)
	token, err := jwt.ParseDecoratedJWT(material)
	if err != nil {
		return errors.New("invalid control credentials")
	}
	claims, err := jwt.DecodeUserClaims(token)
	if err != nil {
		return errors.New("invalid control user claims")
	}
	boundAccount := claims.IssuerAccount
	if boundAccount == "" {
		boundAccount = claims.Issuer
	}
	if boundAccount != account {
		return errors.New("control credentials belong to a different tenant account")
	}
	return nil
}

func validateBrokerURL(raw string) error {
	broker, err := url.Parse(raw)
	if err != nil || broker.Hostname() == "" || broker.User != nil || broker.RawQuery != "" {
		return errors.New("invalid broker URL")
	}
	switch broker.Scheme {
	case "tls", "wss":
		return nil
	case "nats":
		ip := net.ParseIP(broker.Hostname())
		if broker.Hostname() == "localhost" || (ip != nil && ip.IsLoopback()) {
			return nil
		}
	}
	return errors.New("broker TLS is required outside localhost")
}

func trim(seed []byte) []byte {
	// nkeys expects the encoded seed with no trailing newline.
	for len(seed) > 0 && (seed[len(seed)-1] == '\n' || seed[len(seed)-1] == '\r') {
		seed = seed[:len(seed)-1]
	}
	return seed
}

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	control "github.com/vitral-ai/context-session-librarian/nats-control"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	broker := flag.String("nats", "", "public TLS broker URL")
	tenant := flag.String("tenant", "vi", "tenant ID")
	agent := flag.String("agent", "", "recipient agent")
	flag.Parse()
	uri, err := url.Parse(*broker)
	if err != nil || uri.Hostname() == "" || uri.User != nil || uri.RawQuery != "" ||
		(uri.Scheme != "tls" && uri.Scheme != "wss") {
		return errors.New("a public TLS broker URL is required")
	}
	if flag.NArg() != 0 {
		return errors.New("unexpected CI publisher arguments")
	}
	payload, err := io.ReadAll(io.LimitReader(os.Stdin, (1<<20)+1))
	if err != nil || len(payload) > 1<<20 {
		return errors.New("invalid CI notification payload")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(os.Getenv("CSL_NATS_CI_CA_PEM"))) {
		return errors.New("CI broker CA is missing or invalid")
	}
	creds := []byte(os.Getenv("CSL_NATS_CI_CREDS"))
	defer clear(creds)
	token, err := jwt.ParseDecoratedJWT(creds)
	if err != nil {
		return errors.New("CI broker credential is missing or invalid")
	}
	key, err := jwt.ParseDecoratedNKey(creds)
	if err != nil {
		return errors.New("CI broker signing seed is invalid")
	}
	defer key.Wipe()
	conn, err := nats.Connect(*broker, nats.Name("csl-ci-notice"),
		nats.Secure(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}),
		nats.UserJWT(func() (string, error) { return token, nil }, key.Sign),
		nats.Timeout(5*time.Second), nats.NoReconnect())
	if err != nil {
		return errors.New("unable to authenticate to the CI broker")
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := control.PublishCINotice(ctx, conn, *tenant, *agent, payload); err != nil {
		return errors.New("CI broker publication failed")
	}
	return nil
}

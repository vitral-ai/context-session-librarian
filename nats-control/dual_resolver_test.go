package control

import (
	"context"
	"maps"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func TestDualResolverRevocationRetriesPartialUpdate(t *testing.T) {
	fixture := localBroker(t)
	account, _ := fixture.account.PublicKey()
	system, _ := fixture.system.PublicKey()
	operator, _ := fixture.operator.PublicKey()
	now := time.Now().UTC().Truncate(time.Second)
	primaryOnly := newRevocationTestKey(t)
	peerOnly := newRevocationTestKey(t)
	shared := newRevocationTestKey(t)
	agent := fixtureCredentials(t, fixture.account, "revocation-target", nil, nil)
	user, err := jwt.DecodeUserClaims(agent.JWT)
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Unix(user.IssuedAt, 0)
	newerCutoff := cutoff.Add(time.Second)

	encode := func(claims jwt.Claims) string {
		t.Helper()
		token, err := claims.Encode(fixture.operator)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	operatorClaims := jwt.NewOperatorClaims(operator)
	operatorClaims.SystemAccount = system
	peerClaims := *fixture.claims
	peerClaims.Revocations = jwt.RevocationList{peerOnly: now.Unix(), shared: newerCutoff.Unix()}
	peerURL := startFixtureBroker(t, os.Getenv("CSL_TEST_NATS"), encode(operatorClaims),
		system, encode(jwt.NewAccountClaims(system)), account, encode(&peerClaims))

	subject := "$SYS.REQ.ACCOUNT." + account + ".CLAIMS."
	revoker := fixtureCredentials(t, fixture.system, "dual-resolver",
		[]string{subject + "LOOKUP", subject + "UPDATE"}, []string{agentInboxPrefix("vi", "dual-resolver") + ".>"})
	local := fixtureConnect(t, fixture.url, "dual-resolver", revoker)
	peer := fixtureConnect(t, peerURL, "dual-resolver", revoker)
	lookupOnly := fixtureCredentials(t, fixture.system, "lookup-only",
		[]string{subject + "LOOKUP"}, []string{agentInboxPrefix("vi", "lookup-only") + ".>"})
	readOnlyPeer := fixtureConnect(t, peerURL, "lookup-only", lookupOnly)
	primaryClaims := *fixture.claims
	primaryClaims.Revocations = jwt.RevocationList{primaryOnly: now.Unix(), shared: cutoff.Unix()}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := pushAccount(ctx, local, fixture.operator, subject+"UPDATE", &primaryClaims); err != nil {
		t.Fatal(err)
	}
	if err := RevokeAccountUsersOnPair(ctx, local, local, fixture.operator, account, []string{user.Subject}, cutoff); err == nil {
		t.Fatal("same broker accepted as the peer")
	}

	partialCtx, stopPartial := context.WithTimeout(ctx, time.Second)
	err = RevokeAccountUsersOnPair(partialCtx, local, readOnlyPeer, fixture.operator, account, []string{user.Subject, shared}, cutoff)
	stopPartial()
	if err == nil || !strings.Contains(err.Error(), "resolver 2 update") {
		t.Fatalf("expected peer update failure after primary accepted the batch, got %v", err)
	}
	read := func(conn *nats.Conn) *jwt.AccountClaims {
		t.Helper()
		claims, err := lookupAccount(ctx, conn, account)
		if err != nil || claims == nil {
			t.Fatalf("resolver readback failed: %v", err)
		}
		return claims
	}
	if !read(local).Revocations.IsRevoked(user.Subject, cutoff) || read(peer).Revocations.IsRevoked(user.Subject, cutoff) {
		t.Fatal("expected revocation to be present only on primary after the partial failure")
	}
	if err := RevokeAccountUsersOnPair(ctx, local, peer, fixture.operator, account, []string{user.Subject, shared}, cutoff); err != nil {
		t.Fatal(err)
	}
	localClaims, peerClaimsReadback := read(local), read(peer)
	want := jwt.RevocationList{
		primaryOnly: now.Unix(), peerOnly: now.Unix(), shared: newerCutoff.Unix(), user.Subject: cutoff.Unix(),
	}
	if localClaims.ID != peerClaimsReadback.ID || !maps.Equal(localClaims.Revocations, want) || !maps.Equal(peerClaimsReadback.Revocations, want) {
		t.Fatal("both resolvers must store the same claim with the maximum cutoffs")
	}
	for _, endpoint := range []string{fixture.url, peerURL} {
		conn, err := nats.Connect(endpoint, nats.UserJWTAndSeed(agent.JWT, agent.Seed), nats.NoReconnect())
		if err == nil {
			conn.Close()
			t.Fatal("revoked user connected to a broker")
		}
		if !strings.Contains(strings.ToLower(err.Error()), "authorization violation") {
			t.Fatalf("expected broker authentication refusal, got %v", err)
		}
	}

	peerClaimsReadback.Limits.DiskStorage++
	if err := pushAccount(ctx, peer, fixture.operator, subject+"UPDATE", peerClaimsReadback); err != nil {
		t.Fatal(err)
	}
	unwritten := newRevocationTestKey(t)
	if err := RevokeAccountUsersOnPair(ctx, local, peer, fixture.operator, account, []string{unwritten}, cutoff); err == nil ||
		!strings.Contains(err.Error(), "account policy differs") {
		t.Fatalf("expected policy drift refusal, got %v", err)
	}
	if read(local).Revocations.IsRevoked(unwritten, cutoff) || read(peer).Revocations.IsRevoked(unwritten, cutoff) {
		t.Fatal("policy drift must be refused before either write")
	}
}

func newRevocationTestKey(t *testing.T) string {
	t.Helper()
	pair, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer pair.Wipe()
	key, err := pair.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	return key
}

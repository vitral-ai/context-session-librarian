package control

import (
	"slices"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func TestCredentialBindsIdentityAndPullChannels(t *testing.T) {
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer account.Wipe()
	signer, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer signer.Wipe()
	accountKey, _ := account.PublicKey()
	signerKey, _ := signer.PublicKey()
	now := time.Now().UTC().Truncate(time.Second)
	agent := Agent{Role: "worker", CLI: "codex", Machine: "threadripper"}
	result, public, err := issueCredentials(signer, accountKey, "vi", "worker-a", agent, now)
	if err != nil {
		t.Fatal(err)
	}
	key, err := nkeys.FromSeed([]byte(result.Seed))
	if err != nil {
		t.Fatal(err)
	}
	defer key.Wipe()
	keyPublic, _ := key.PublicKey()
	claims, err := jwt.DecodeUserClaims(result.JWT)
	if err != nil {
		t.Fatal(err)
	}
	if keyPublic != public || claims.Subject != public || claims.Issuer != signerKey ||
		claims.IssuerAccount != accountKey || claims.Name != "worker-a" ||
		claims.Expires != now.Add(24*time.Hour).Unix() || result.ExpiresAt.Unix() != claims.Expires {
		t.Fatal("credential identity or expiry mismatch")
	}
	if !slices.Equal(claims.Tags, jwt.TagList{"role:worker", "cli:codex", "machine:threadripper"}) {
		t.Fatalf("tags: %v", claims.Tags)
	}
	wantPub := jwt.StringList{
		"csl.vi.send.worker-a", "csl.vi.ctl.*.worker-a",
		"$JS.API.CONSUMER.INFO.CSL_MSG.agent-worker-a",
		"$JS.API.CONSUMER.MSG.NEXT.CSL_MSG.agent-worker-a",
		"$JS.ACK.CSL_MSG.agent-worker-a.>",
	}
	wantSub := jwt.StringList{"csl.vi.agent.worker-a.wake", "_INBOX.csl.vi.worker-a.>"}
	if !slices.Equal(claims.Pub.Allow, wantPub) || !slices.Equal(claims.Sub.Allow, wantSub) {
		t.Fatalf("unexpected credential permissions: %+v", claims.Permissions)
	}
	if _, _, err := issueCredentials(signer, accountKey, "vi", "owner", agent, now); err == nil {
		t.Fatal("owner subject could be issued as a regular agent")
	}
}

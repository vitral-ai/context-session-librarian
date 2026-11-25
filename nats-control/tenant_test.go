package control

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

func testTenantLifecycle(t *testing.T, ctx context.Context, url string, primaryOwner, primaryControl *nats.Conn, primary jetstream.JetStream, p Provisioner, system nkeys.KeyPair) {
	t.Helper()
	const tenant = "zt-test"
	dir := filepath.Join(p.Directory, tenant)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// A prior process died writing a temporary file before publication.
	if err := os.WriteFile(filepath.Join(dir, ".csl-provision-interrupted"), []byte("truncated staging file"), 0600); err != nil {
		t.Fatal(err)
	}
	request := adminRequest{Action: "create-tenant", Tenant: tenant, StorageBytes: 32 * 1024 * 1024, FileBytes: 1024, RatePerMinute: 3}
	var info TenantInfo
	raw := fixtureRequest(t, ctx, primaryOwner, "csl.vi.ctl.admin.owner", request)
	assertResponseCode(t, raw, "")
	if err := json.Unmarshal(raw, &info); err != nil || info.Tenant != tenant || !nkeys.IsValidPublicAccountKey(info.Account) {
		t.Fatalf("provisioned tenant identity is invalid: %v", err)
	}
	raw = fixtureRequest(t, ctx, primaryOwner, "csl.vi.ctl.admin.owner", request)
	assertResponseCode(t, raw, "")
	var repeated TenantInfo
	if err := json.Unmarshal(raw, &repeated); err != nil || repeated != info {
		t.Fatal("provisioning retry did not preserve account identity")
	}
	assertResponseCode(t, fixtureRequest(t, ctx, primaryOwner, "csl.vi.ctl.admin.owner",
		adminRequest{Action: "erase-tenant", Tenant: "vi"}), "forbidden")

	connect := func(name string, opts ...nats.Option) *nats.Conn {
		t.Helper()
		conn, err := nats.Connect(url, append(opts, nats.UserCredentials(filepath.Join(dir, name+".creds")))...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(conn.Close)
		return conn
	}
	seed, err := os.ReadFile(filepath.Join(dir, "account.nk"))
	if err != nil {
		t.Fatal(err)
	}
	signer, err := nkeys.FromSeed(seed)
	clear(seed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(signer.Wipe)
	resolve := "$SYS.REQ.ACCOUNT." + info.Account + ".CLAIMS."
	revoker := fixtureConnect(t, url, "tenant-revoker", fixtureCredentials(t, system, "tenant-revoker",
		[]string{resolve + "LOOKUP", resolve + "UPDATE"}, []string{"_INBOX.csl.vi.tenant-revoker.>"}))
	config := Config{
		Tenant: tenant, Account: info.Account, Signer: signer, EraseTenant: tenant, Upgrade: "csl self-upgrade",
		RevokeUsers: func(ctx context.Context, users []string, at time.Time) error {
			return RevokeAccountUsers(ctx, revoker, p.Operator, info.Account, users, at)
		},
	}
	misaligned, err := Open(ctx, primaryControl, config)
	if err != nil {
		t.Fatal(err)
	}
	var refusal *apiError
	if _, err := misaligned.eraseTenant(ctx, tenant, ""); !errors.As(err, &refusal) || refusal.Code != "forbidden" {
		t.Fatal("erasure accepted a control connection to the wrong tenant stores")
	}
	service, err := Open(ctx, connect("csl-control"), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Subscribe(); err != nil {
		t.Fatal(err)
	}
	owner := connect("owner", nats.CustomInboxPrefix(agentInboxPrefix(tenant, "owner")))
	admin := func(request adminRequest, code string) []byte {
		t.Helper()
		raw := fixtureRequest(t, ctx, owner, "csl."+tenant+".ctl.admin.owner", request)
		assertResponseCode(t, raw, code)
		return raw
	}
	var exported struct {
		Items []exportRecord `json:"items"`
	}
	if err := json.Unmarshal(admin(adminRequest{Action: "export", Tenant: tenant}, ""), &exported); err != nil ||
		exported.Items == nil || len(exported.Items) != 0 {
		t.Fatal("new tenant did not return an empty export")
	}
	policy, err := service.deliveryPolicy(ctx)
	if err != nil || policy.FileBytes != request.FileBytes || policy.RatePerMinute != request.RatePerMinute {
		t.Fatal("provisioning did not apply requested quotas")
	}
	primaryStream, err := primary.Stream(ctx, "CSL_MSG")
	if err != nil {
		t.Fatal(err)
	}
	before, err := primaryStream.Info(ctx)
	if err != nil || before.State.Msgs == 0 {
		t.Fatalf("primary message witness missing: %v", err)
	}
	admin(adminRequest{Action: "versions", Minimum: "1.0.0", Recommended: "1.0.0"}, "")
	raw = admin(adminRequest{Action: "register", Name: "worker", CLI: "codex", Role: "worker", Machine: "fixture"}, "")
	var cred credentials
	if err := json.Unmarshal(raw, &cred); err != nil {
		t.Fatal(err)
	}
	revoked := make(chan struct{})
	agent, err := nats.Connect(url, nats.UserJWTAndSeed(cred.JWT, cred.Seed), nats.NoReconnect(),
		nats.ClosedHandler(func(*nats.Conn) { close(revoked) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(agent.Close)
	err = service.recordServiceMessage(ctx, serviceEnvelope(randomID(), "worker", "system", "disposable witness", time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.files.PutBytes(ctx, "disposable", []byte("fixture")); err != nil {
		t.Fatal(err)
	}
	erase := adminRequest{Action: "erase-tenant", Tenant: tenant}
	var challenge struct {
		Confirm string `json:"confirm"`
	}
	if err := json.Unmarshal(admin(erase, ""), &challenge); err != nil || challenge.Confirm == "" {
		t.Fatal("erasure did not require an explicit token")
	}
	erase.Confirm = "incorrect"
	admin(erase, "forbidden")
	for _, name := range []string{"CSL_MSG", "OBJ_CSL_FILES", "KV_CSL_STATE"} {
		stream, err := service.js.Stream(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if info, err := stream.Info(ctx); err != nil || info.State.Msgs == 0 {
			t.Fatalf("incorrect token removed %s: %v", name, err)
		}
	}
	erase.Confirm = challenge.Confirm
	admin(erase, "")
	select {
	case <-revoked:
	case <-ctx.Done():
		t.Fatal("tenant erasure did not disconnect the active credential")
	}
	user, err := jwt.DecodeUserClaims(cred.JWT)
	if err != nil {
		t.Fatal(err)
	}
	account, err := lookupAccount(ctx, revoker, info.Account)
	if err != nil || account == nil || !account.Revocations.IsRevoked(user.Subject, time.Unix(user.IssuedAt, 0)) {
		t.Fatal("scoped revocation was not persisted in the tenant resolver")
	}
	if fresh, err := nats.Connect(url, nats.UserJWTAndSeed(cred.JWT, cred.Seed), nats.Timeout(time.Second)); err == nil {
		fresh.Close()
		t.Fatal("the erased tenant's agent credential reconnected")
	}

	for _, name := range []string{"CSL_MSG", "OBJ_CSL_FILES", "KV_CSL_STATE"} {
		stream, err := service.js.Stream(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if info, err := stream.Info(ctx); err != nil || info.State.Msgs != 0 {
			t.Fatalf("disposable tenant %s still has data: %v", name, err)
		}
	}
	admin(erase, "forbidden")
	registration := adminRequest{Action: "register", Name: "after-erase", CLI: "codex", Role: "worker", Machine: "fixture"}
	admin(registration, "forbidden")
	admin(adminRequest{Action: "quotas", Retention: 15, UrgentCap: 5}, "forbidden")
	assertResponseCode(t, fixtureRequest(t, ctx, primaryOwner, "csl.vi.ctl.admin.owner", request), "")
	policy, err = service.deliveryPolicy(ctx)
	if err != nil || policy.FileBytes != request.FileBytes || policy.RatePerMinute != request.RatePerMinute {
		t.Fatal("reprovisioning did not restore approved quotas")
	}
	admin(registration, "")
	after, err := primaryStream.Info(ctx)
	if err != nil || after.State.Msgs != before.State.Msgs || after.State.LastSeq != before.State.LastSeq {
		t.Fatalf("tenant erasure changed the primary stream: %v", err)
	}
}

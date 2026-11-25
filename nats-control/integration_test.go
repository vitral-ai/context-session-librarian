package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

func TestOperatorMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	url, account := localBroker(t)
	public, err := account.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	control := fixtureConnect(t, url, "csl-control", fixtureCredentials(t, account, "csl-control", nil, nil))
	js, err := jetstream.New(control)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "CSL_MSG", Subjects: []string{"csl.vi.agent.*.inbox"}, Storage: jetstream.FileStorage,
		MaxAge: messageAge, Duplicates: 2 * time.Minute, MaxMsgSize: 1024 * 1024,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "CSL_STATE", History: 1, LimitMarkerTTL: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "CSL_FILES", TTL: messageAge}); err != nil {
		t.Fatal(err)
	}
	var revoked []string
	service, err := Open(ctx, control, Config{
		Tenant: "vi", Account: public, Signer: account, Domains: []string{"fixture"},
		Upgrade: "csl self-upgrade",
		RevokeUsers: func(_ context.Context, keys []string, _ time.Time) error {
			revoked = append(revoked, keys...)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Subscribe(); err != nil {
		t.Fatal(err)
	}
	owner := fixtureConnect(t, url, "owner", fixtureCredentials(t, account, "owner",
		[]string{"csl.vi.ctl.*.owner"}, []string{"_INBOX.csl.vi.owner.>"}))
	admin := func(request map[string]any) []byte {
		result := fixtureRequest(t, ctx, owner, "csl.vi.ctl.admin.owner", request)
		assertResponseCode(t, result, "")
		return result
	}
	admin(map[string]any{"action": "versions", "minimum": "1.2.0", "recommended": "1.3.0"})
	register := func(name, role string, opts ...nats.Option) *nats.Conn {
		result := admin(map[string]any{"action": "register", "name": name, "role": role, "cli": "codex", "machine": "fixture"})
		var cred credentials
		if json.Unmarshal(result, &cred) != nil || cred.JWT == "" || cred.Seed == "" {
			t.Fatal("registration did not return credentials")
		}
		return fixtureConnect(t, url, name, cred, opts...)
	}
	violations := make(chan error, 4)
	sender := register("sender", "orchestrator", nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		violations <- err
	}))
	peer := register("worker-a", "worker")
	third := register("worker-b", "worker")
	if err := sender.Publish("csl.vi.agent.worker-a.inbox", []byte("forbidden by account permissions")); err != nil {
		t.Fatal(err)
	}
	if err := sender.FlushWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-violations:
		if !strings.Contains(strings.ToLower(err.Error()), "permissions violation") {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not deny direct inbox publish")
	}
	call := func(conn *nats.Conn, subject string, request any, errorCode string) []byte {
		t.Helper()
		result := fixtureRequest(t, ctx, conn, "csl.vi."+subject, request)
		assertResponseCode(t, result, errorCode)
		return result
	}
	call(sender, "ctl.hello.sender", map[string]string{"client_version": "1.1.0+abc", "status": "working"}, "outdated_client")
	call(sender, "ctl.hello.sender", map[string]string{"client_version": "1.3.0+abc", "status": "working"}, "")
	call(peer, "ctl.status.worker-a", map[string]string{}, "forbidden")
	call(peer, "ctl.admin.worker-a", map[string]string{"action": "register"}, "forbidden")

	text := strings.Repeat("x", maxBody-1) + "\n"
	message := Envelope{V: 1, ID: randomID(), From: "sender", To: "worker-a", Kind: "message",
		Urgency: "normal", CreatedAt: time.Now().UTC(), Body: text, ClientVersion: "1.3.0+abc"}
	call(sender, "send.sender", message, "forbidden")
	admin(map[string]any{"action": "allow", "from": "sender", "to": "worker-a"})
	message.From = "worker-b"
	call(sender, "send.sender", message, "forbidden")
	message.From = "sender"
	call(sender, "send.sender", message, "")
	call(sender, "send.sender", message, "")
	peerJS, err := jetstream.New(peer)
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := peerJS.Consumer(ctx, "CSL_MSG", "agent-worker-a")
	if err != nil {
		t.Fatal(err)
	}
	receive := func(kind string) (Envelope, jetstream.Msg) {
		t.Helper()
		got, err := consumer.Next(jetstream.FetchMaxWait(2 * time.Second))
		if err != nil {
			t.Fatal(err)
		}
		var env Envelope
		if json.Unmarshal(got.Data(), &env) != nil || env.Kind != kind {
			t.Fatalf("expected %s, got %q", kind, env.Kind)
		}
		return env, got
	}
	delivered, msg := receive("message")
	if delivered.Body != text || delivered.From != "sender" || delivered.SessionID == "" || delivered.ID != message.ID {
		t.Fatal("near-limit body or authenticated envelope did not survive")
	}
	call(third, "ctl.receipt.worker-b", sendResult{message.ID, "acked"}, "not_found")
	call(peer, "ctl.receipt.worker-a", sendResult{message.ID, "delivered"}, "")
	if err := msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	call(peer, "ctl.receipt.worker-a", sendResult{message.ID, "acked"}, "")
	call(third, "ctl.history.worker-b", map[string]string{"id": message.ID}, "not_found")
	call(peer, "ctl.history.worker-a", map[string]string{"id": message.ID[:8]}, "")

	admin(map[string]any{"action": "quotas", "retention": 200, "urgent_cap": 1})
	call(peer, "ctl.settings.worker-a", map[string]string{"delivery": "grouped:15"}, "")
	first := Envelope{V: 1, ID: randomID(), To: "worker-a", Kind: "message", Urgency: "normal",
		CreatedAt: time.Now().UTC(), Body: "first", ClientVersion: "1.3.0+abc"}
	second := first
	second.ID, second.Body = randomID(), "second"
	call(sender, "send.sender", first, "")
	call(sender, "send.sender", second, "")
	urgent := first
	urgent.ID, urgent.Urgency, urgent.Reason = randomID(), "urgent", "blocked"
	call(sender, "send.sender", urgent, "")
	immediate, rawUrgent := receive("message")
	if immediate.ID != urgent.ID {
		t.Fatal("urgent message did not bypass grouping")
	}
	if err := rawUrgent.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	limited := urgent
	limited.ID = randomID()
	call(sender, "send.sender", limited, "")
	service.mu.Lock()
	future := time.Now().Add(16 * time.Minute)
	service.clock = func() time.Time { return future }
	service.mu.Unlock()
	if err := service.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	digest, packet := receive("digest")
	if len(digest.Items) != 3 || digest.Items[0].ID != first.ID || digest.Items[1].ID != second.ID ||
		digest.Items[2].ID != limited.ID || digest.Items[2].Urgency != "normal" {
		t.Fatal("digest did not preserve input order")
	}
	if err := packet.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}

	reminder, packet := receive("reminder")
	if !strings.Contains(reminder.Body, "Open sessions: sender.") {
		t.Fatal("eligible worker did not receive its reminder")
	}
	if err := packet.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if pending, err := consumer.Info(ctx); err != nil || pending.NumPending != 0 {
		t.Fatalf("reminder repeated in one slot: %v", err)
	}

	ci := fixtureConnect(t, url, "csl-ci", fixtureCredentials(t, account, "csl-ci", []string{"csl.vi.ci.>"}, nil))
	noticeID := randomID()
	if err := ci.Publish("csl.vi.ci.worker-a", []byte(`{"id":"`+noticeID+`","body":"fixture CI passed"}`)); err != nil {
		t.Fatal(err)
	}
	notice, packet := receive("ci")
	if notice.ID != noticeID || notice.From != "ci-notices" {
		t.Fatal("CI notice was not routed through the control service")
	}
	if err := packet.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}

	content := bytes.Repeat([]byte("file-content"), 24*1024)
	id := fmt.Sprintf("%x", sha256.Sum256(content))
	for offset := 0; offset < len(content); offset += maxChunk {
		call(sender, "ctl.put.sender", fileRequest{ID: id, Name: "fixture.txt", Size: int64(len(content)),
			Allowed: []string{"worker-a"}, Offset: int64(offset), Data: content[offset:min(offset+maxChunk, len(content))]}, "")
	}
	call(third, "ctl.fetch.worker-b", fileRequest{ID: id}, "not_found")
	var received bytes.Buffer
	for offset := int64(0); offset < int64(len(content)); {
		result := call(peer, "ctl.fetch.worker-a", fileRequest{ID: id, Offset: offset}, "")
		var chunk fileResult
		if err := json.Unmarshal(result, &chunk); err != nil {
			t.Fatal(err)
		}
		received.Write(chunk.Data)
		if chunk.NextOffset == nil {
			break
		}
		offset = *chunk.NextOffset
	}
	if !bytes.Equal(content, received.Bytes()) {
		t.Fatal("download failed SHA-256 witness")
	}
	call(peer, "ctl.complete.worker-a", map[string]string{"with": "sender"}, "")
	status := call(sender, "ctl.status.sender", map[string]string{}, "")
	var fleet struct {
		Agents []agentStatus `json:"agents"`
	}
	if json.Unmarshal(status, &fleet) != nil || len(fleet.Agents) != 3 || !fleet.Agents[1].TaskComplete ||
		len(fleet.Agents[1].OpenSessions) != 0 {
		t.Fatal("status did not show the completed pair")
	}
	admin(map[string]any{"action": "revoke", "name": "worker-b"})
	call(third, "ctl.hello.worker-b", map[string]string{"client_version": "1.3.0+abc"}, "forbidden")
	service.mu.Lock()
	defer service.mu.Unlock()
	if len(revoked) != 1 || !nkeys.IsValidPublicUserKey(revoked[0]) {
		t.Fatal("revocation did not request the signed user key")
	}
}

func localBroker(t *testing.T) (string, nkeys.KeyPair) {
	t.Helper()
	server := os.Getenv("CSL_TEST_NATS")
	if server == "" {
		t.Skip("set CSL_TEST_NATS to a local NATS server binary with JetStream TTL support")
	}
	operator, err := nkeys.CreateOperator()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Wipe)
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(account.Wipe)
	operatorKey, _ := operator.PublicKey()
	accountKey, _ := account.PublicKey()
	claims := jwt.NewAccountClaims(accountKey)
	claims.Name = "vitral-internal"
	claims.Limits.DiskStorage = 128 * 1024 * 1024
	claims.Limits.Streams, claims.Limits.Consumer = -1, -1
	accountToken, err := claims.Encode(operator)
	if err != nil {
		t.Fatal(err)
	}
	system, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer system.Wipe()
	systemKey, _ := system.PublicKey()
	systemToken, err := jwt.NewAccountClaims(systemKey).Encode(operator)
	if err != nil {
		t.Fatal(err)
	}
	operatorClaims := jwt.NewOperatorClaims(operatorKey)
	operatorClaims.SystemAccount = systemKey
	operatorToken, err := operatorClaims.Encode(operator)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cfg := fmt.Sprintf(
		"host: 127.0.0.1\nport: -1\nports_file_dir: %q\noperator: %q\nsystem_account: %s\nresolver: MEMORY\nresolver_preload: { %s: %q, %s: %q }\njetstream: {store_dir: %q, domain: fixture}\n",
		dir, operatorToken, systemKey, systemKey, systemToken, accountKey, accountToken, filepath.Join(dir, "data"))
	configPath := filepath.Join(dir, "server.conf")
	if err := os.WriteFile(configPath, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(server, "-c", configPath)
	logPath := filepath.Join(dir, "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logFile.Close() })
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	})
	portsPath := filepath.Join(dir, fmt.Sprintf("%s_%d.ports", filepath.Base(server), cmd.Process.Pid))
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var ports struct {
			Nats []string `json:"nats"`
		}
		if raw, err := os.ReadFile(portsPath); err == nil && json.Unmarshal(raw, &ports) == nil && len(ports.Nats) == 1 {
			return ports.Nats[0], account
		}
		time.Sleep(10 * time.Millisecond)
	}
	logs, _ := os.ReadFile(logPath)
	t.Fatalf("local broker did not become ready: %s", logs)
	return "", nil
}

func fixtureCredentials(t *testing.T, account nkeys.KeyPair, name string, pub, sub []string) credentials {
	t.Helper()
	user, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer user.Wipe()
	public, _ := user.PublicKey()
	seed, _ := user.Seed()
	claims := jwt.NewUserClaims(public)
	claims.Name, claims.Pub.Allow, claims.Sub.Allow = name, pub, sub
	token, err := claims.Encode(account)
	if err != nil {
		t.Fatal(err)
	}
	return credentials{JWT: token, Seed: string(seed)}
}

func fixtureConnect(t *testing.T, url, name string, cred credentials, opts ...nats.Option) *nats.Conn {
	t.Helper()
	opts = append(opts, nats.Name(name), nats.UserJWTAndSeed(cred.JWT, cred.Seed), nats.CustomInboxPrefix(agentInboxPrefix("vi", name)))
	conn, err := nats.Connect(url, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	return conn
}

func fixtureRequest(t *testing.T, ctx context.Context, conn *nats.Conn, subject string, request any) []byte {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	timeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result, err := conn.RequestWithContext(timeout, subject, body)
	if err != nil {
		t.Fatalf("%s: %v", subject, err)
	}
	return result.Data
}

func assertResponseCode(t *testing.T, response []byte, expected string) {
	t.Helper()
	var result apiError
	if err := json.Unmarshal(response, &result); err != nil || result.Code != expected {
		t.Fatalf("response code=%q message=%q, want code=%q", result.Code, result.Message, expected)
	}
}

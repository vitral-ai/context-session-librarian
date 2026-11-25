package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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
	broker := localBroker(t)
	url, account := broker.url, broker.account
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
	archiveStream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: hubArchiveName, Subjects: []string{"csl.vi.archive.>"}, Storage: jetstream.FileStorage,
		Discard: jetstream.DiscardNew, MaxBytes: 2048, MaxMsgSize: 1024 * 1024,
	})
	if err != nil {
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
	sys := fixtureConnect(t, url, "fixture-sys", fixtureCredentials(t, broker.system, "fixture-sys", nil, nil))
	provisioner := Provisioner{
		Operator: broker.operator, System: sys, Directory: t.TempDir(),
		ConnectTenant: func(path string) (*nats.Conn, error) {
			return nats.Connect(url, nats.UserCredentials(path))
		},
	}
	service, err := Open(ctx, control, Config{
		CreateTenant: provisioner.Create,
		Tenant:       "vi", Account: public, Signer: account, Domains: []string{"fixture"},
		Upgrade: "csl self-upgrade",
		RevokeUsers: func(ctx context.Context, keys []string, at time.Time) error {
			for _, key := range keys {
				broker.claims.RevokeAt(key, at)
			}
			return pushFixtureAccount(ctx, sys, broker.operator, broker.claims)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Subscribe(); err != nil {
		t.Fatal(err)
	}
	owner := fixtureConnect(t, url, "owner", fixtureCredentials(t, account, "owner",
		[]string{"csl.vi.ctl.admin.owner"}, []string{"_INBOX.csl.vi.owner.>"}))
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
	revoked := make(chan struct{})
	third := register("worker-b", "worker", nats.NoReconnect(), nats.ClosedHandler(func(_ *nats.Conn) { close(revoked) }))
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

	t.Run("HubHistoryImport", func(t *testing.T) {
		stamp := time.Now().UTC().Add(-time.Hour)
		row := hubHistoryRow{ID: "f0000000-0000-4000-8000-000000000001", From: "worker-a", To: "worker-b",
			FromMachine: "fixture", ToMachine: "fixture", CreatedAt: stamp, State: "delivered", Body: "old Hub body"}
		req := map[string]any{"action": "import-hub-history", "history": &row}
		call(peer, "ctl.admin.worker-a", req, "forbidden")
		row.ToMachine = "another-machine"
		call(owner, "ctl.admin.owner", req, "forbidden")
		row.ToMachine = "fixture"
		row.State = "queued"
		call(owner, "ctl.admin.owner", req, "invalid")
		row.State = "delivered"
		var plan hubHistoryPlan
		if err := json.Unmarshal(admin(map[string]any{"action": "plan-hub-history", "history": &row}), &plan); err != nil ||
			plan.ID != row.ID || plan.PayloadBytes <= 0 || plan.ReserveBytes <= plan.PayloadBytes || plan.Available != 2048 {
			t.Fatalf("archive plan: %#v, %v", plan, err)
		}
		info, err := archiveStream.Info(ctx)
		if err != nil || info.State.Msgs != 0 {
			t.Fatalf("archive plan performed a write: %v, %v", info, err)
		}
		tooLarge := row
		tooLarge.ID, tooLarge.Body = randomID(), strings.Repeat("x", 2048)
		call(owner, "ctl.admin.owner", map[string]any{"action": "import-hub-history", "history": &tooLarge}, "invalid")
		liveSubject := "csl.vi.agent.worker-a.inbox"
		pending, err := js.Publish(ctx, liveSubject, []byte("pending live delivery"))
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"imported", "existing"} {
			var result sendResult
			if err := json.Unmarshal(admin(req), &result); err != nil || result != (sendResult{row.ID, want}) {
				t.Fatalf("import: %#v, %v", result, err)
			}
		}
		row.Body = "mismatched"
		call(owner, "ctl.admin.owner", req, "invalid")
		row.Body = "old Hub body"
		history := map[string]string{"id": row.ID}
		for name, conn := range map[string]*nats.Conn{"worker-a": peer, "worker-b": third} {
			var result struct{ Messages []historyEntry }
			if err := json.Unmarshal(call(conn, "ctl.history."+name, history, ""), &result); err != nil ||
				len(result.Messages) != 1 || result.Messages[0].Body != row.Body ||
				result.Messages[0].State != row.State || !result.Messages[0].CreatedAt.Equal(stamp) {
				t.Fatalf("history: %#v, %v", result, err)
			}
		}
		call(sender, "ctl.history.sender", history, "not_found")
		call(third, "ctl.receipt.worker-b", sendResult{row.ID, "acked"}, "forbidden")
		call(sender, "ctl.receipt.sender", sendResult{row.ID, "acked"}, "not_found")
		unread, err := service.stream.GetMsg(ctx, pending.Sequence)
		if err != nil || !bytes.Equal(unread.Data, []byte("pending live delivery")) {
			t.Fatalf("archive import evicted a pending live message: %v", err)
		}
		if err := service.stream.DeleteMsg(ctx, pending.Sequence); err != nil {
			t.Fatal(err)
		}
		if err := service.deliveryTick(ctx); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"worker-a", "worker-b"} {
			c, err := service.stream.Consumer(ctx, "agent-"+name)
			if err != nil {
				t.Fatal(err)
			}
			info, err := c.Info(ctx)
			if err != nil || info.NumPending != 0 {
				t.Fatalf("history reached %s inbox: %v, %v", name, info, err)
			}
		}
		archive, err := service.archiveMessage(ctx, archiveStream, row.ID)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := readState[messageRecord](ctx, service.state, "message."+row.ID); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("archive modified live history index: %v", err)
		}
		info, err = archiveStream.Info(ctx)
		if err != nil || info.State.Msgs != 1 {
			t.Fatalf("expected one archived copy: %v, %v", info, err)
		}
		live, err := service.stream.Info(ctx)
		if err != nil || live.State.Msgs != 0 {
			t.Fatalf("import wrote to the live stream: %v, %v", live, err)
		}
		admin(map[string]any{"action": "allow", "from": "worker-a", "to": "worker-b"})
		duplicate := Envelope{V: 1, ID: row.ID, To: "worker-b", Kind: "message",
			Urgency: "normal", CreatedAt: stamp, Body: row.Body, ClientVersion: "1.3.0+abc"}
		call(peer, "send.worker-a", duplicate, "invalid")
		duplicate.ID, duplicate.Kind, duplicate.ReplyTo = randomID(), "reply", &row.ID
		call(peer, "send.worker-a", duplicate, "not_found")

		reply := Envelope{V: 1, ID: "f0000000-0000-4000-8000-000000000002", To: "worker-a", Kind: "reply",
			Urgency: "normal", CreatedAt: stamp, Body: "reply to imported Hub message",
			ReplyTo: &row.ID, ClientVersion: "1.3.0+abc"}
		call(third, "send.worker-b", reply, "forbidden")
		admin(map[string]any{"action": "allow", "from": "worker-b", "to": "worker-a"})
		worker, revision, err := readState[Agent](ctx, service.state, "agent.worker-a")
		if err != nil {
			t.Fatal(err)
		}
		rebound := worker
		rebound.Machine = "replacement"
		if err := writeState(ctx, service.state, "agent.worker-a", rebound, revision); err != nil {
			t.Fatal(err)
		}
		call(third, "send.worker-b", reply, "not_found")
		_, revision, err = readState[Agent](ctx, service.state, "agent.worker-a")
		if err != nil {
			t.Fatal(err)
		}
		if err := writeState(ctx, service.state, "agent.worker-a", worker, revision); err != nil {
			t.Fatal(err)
		}
		refused := row
		refused.ID, refused.State = "10000000-0000-4000-8000-000000000001", "refused"
		admin(map[string]any{"action": "import-hub-history", "history": &refused})
		reply.ReplyTo = &refused.ID
		call(third, "send.worker-b", reply, "not_found")
		reply.ReplyTo = &row.ID
		call(third, "send.worker-b", reply, "")
		saved, _, err := readState[messageRecord](ctx, service.state, "message."+reply.ID)
		if err != nil || saved.Envelope.ReplyTo == nil || *saved.Envelope.ReplyTo != row.ID || saved.Sequence == 0 {
			t.Fatalf("reply to archive did not reach the live stream: %#v, %v", saved, err)
		}
		call(peer, "ctl.receipt.worker-a", sendResult{reply.ID, "acked"}, "")
		tied := duplicate
		tied.ID, tied.Kind, tied.ReplyTo = "10000000-0000-4000-8000-000000000002", "message", nil
		queued := tied
		queued.ID = randomID()
		call(third, "ctl.settings.worker-b", map[string]string{"delivery": "grouped:15"}, "")
		call(peer, "send.worker-a", queued, "")
		waiting, _, err := readState[messageRecord](ctx, service.state, "message."+queued.ID)
		if err != nil || waiting.Sequence != 0 {
			t.Fatalf("fixture was not queued: %#v, %v", waiting, err)
		}
		call(third, "ctl.settings.worker-b", map[string]string{"delivery": "immediate"}, "")
		earlier := tied
		earlier.ID, earlier.CreatedAt = randomID(), stamp.Add(-time.Minute)
		for _, message := range []Envelope{tied, earlier} {
			call(peer, "send.worker-a", message, "")
			record, _, err := readState[messageRecord](ctx, service.state, "message."+message.ID)
			if err != nil || record.Sequence == 0 {
				t.Fatalf("fixture did not reach live stream: %#v, %v", record, err)
			}
			call(third, "ctl.receipt.worker-b", sendResult{message.ID, "acked"}, "")
			if err := service.stream.DeleteMsg(ctx, record.Sequence); err != nil {
				t.Fatal(err)
			}
		}
		checkPage := func(before string, last int, next string, want ...string) {
			t.Helper()
			var page struct {
				Messages   []historyEntry
				NextBefore string `json:"next_before"`
			}
			body := call(peer, "ctl.history.worker-a", map[string]any{"with": "worker-b", "last": last, "before": before}, "")
			if err := json.Unmarshal(body, &page); err != nil {
				t.Fatal(err)
			}
			var ids []string
			for _, message := range page.Messages {
				ids = append(ids, message.ID)
			}
			if !slices.Equal(ids, want) || page.NextBefore != next {
				t.Fatalf("history before %q: got %v (next %q), want %v (next %q)", before, ids, page.NextBefore, want, next)
			}
		}
		checkPage("", 1, tied.ID, tied.ID)
		service.mu.Lock()
		clock := service.clock
		service.clock = func() time.Time { return waiting.ReadyAt }
		service.mu.Unlock()
		err = service.deliveryTick(ctx)
		service.mu.Lock()
		service.clock = clock
		service.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		published, _, err := readState[messageRecord](ctx, service.state, "message."+queued.ID)
		if err != nil || published.Sequence == 0 {
			t.Fatalf("fixture did not leave the queue: %#v, %v", published, err)
		}
		checkPage(tied.ID, 2, reply.ID, reply.ID, queued.ID)
		checkPage(reply.ID, 3, "", earlier.ID, row.ID, refused.ID)
		call(third, "ctl.receipt.worker-b", sendResult{queued.ID, "acked"}, "")
		if err := service.stream.DeleteMsg(ctx, published.Sequence); err != nil {
			t.Fatal(err)
		}
		call(peer, "ctl.complete.worker-a", map[string]string{"with": "worker-b"}, "")
		call(third, "ctl.complete.worker-b", map[string]string{"with": "worker-a"}, "")
		if err := service.stream.DeleteMsg(ctx, saved.Sequence); err != nil {
			t.Fatal(err)
		}
		refusedMessage, err := service.archiveMessage(ctx, archiveStream, refused.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := archiveStream.DeleteMsg(ctx, refusedMessage.Sequence); err != nil {
			t.Fatal(err)
		}
		if err := archiveStream.DeleteMsg(ctx, archive.Sequence); err != nil {
			t.Fatal(err)
		}
	})

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
	// A restored stream has retained messages but no durable consumers.
	if err := service.stream.DeleteConsumer(ctx, "agent-worker-a"); err != nil {
		t.Fatal(err)
	}
	call(third, "ctl.ensure_consumer.worker-b", map[string]string{"name": "worker-a"}, "invalid")
	peerJS, err := jetstream.New(peer)
	if err != nil {
		t.Fatal(err)
	}
	call(peer, "ctl.ensure_consumer.worker-a", struct{}{}, "")
	consumer, err := peerJS.Consumer(ctx, "CSL_MSG", "agent-worker-a")
	if err != nil {
		t.Fatal(err)
	}
	config := consumer.CachedInfo().Config
	if config.Durable != "agent-worker-a" || config.FilterSubject != "csl.vi.agent.worker-a.inbox" ||
		config.DeliverSubject != "" || config.AckPolicy != jetstream.AckExplicitPolicy ||
		config.DeliverPolicy != jetstream.DeliverAllPolicy || config.AckWait != 5*time.Minute || config.MaxDeliver != 20 {
		t.Fatalf("unexpected restored consumer config: %+v", config)
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
	call(peer, "ctl.ensure_consumer.worker-a", struct{}{}, "")
	info, err := consumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.AckFloor.Stream == 0 {
		t.Fatal("repeated ensure reset the consumer's acknowledged position")
	}
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
	pendingUrgent := func(expected int) {
		t.Helper()
		raw := call(sender, "ctl.status.sender", map[string]string{}, "")
		var status struct {
			Agents []agentStatus `json:"agents"`
		}
		if json.Unmarshal(raw, &status) != nil || len(status.Agents) != 3 ||
			status.Agents[1].Name != "worker-a" || status.Agents[1].PendingUrgent != expected {
			t.Fatalf("status did not report %d pending urgent item(s) for worker-a", expected)
		}
	}
	pendingUrgent(1)
	call(peer, "ctl.receipt.worker-a", sendResult{urgent.ID, "delivered"}, "")
	pendingUrgent(1)
	if err := rawUrgent.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	call(peer, "ctl.receipt.worker-a", sendResult{urgent.ID, "acked"}, "")
	pendingUrgent(0)

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
	// Loss of item bookkeeping after an accepted digest must not publish a
	// differently grouped digest on the following scheduling pass.
	partial, revision, err := readState[messageRecord](ctx, service.state, "message."+first.ID)
	if err != nil {
		t.Fatal(err)
	}
	partial.Sequence = 0
	if _, err := service.writeExpiring(ctx, "message."+first.ID, partial, revision, messageAge); err != nil {
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
	recovered, _, err := readState[messageRecord](ctx, service.state, "message."+first.ID)
	if err != nil || recovered.Sequence == 0 {
		t.Fatal("digest item bookkeeping did not recover")
	}

	// Simulate JetStream acceptance followed by a crash before saving the
	// publish sequence. A later permission change cannot undo the accepted write.
	saved, revision, err := readState[messageRecord](ctx, service.state, "message."+digest.ID)
	if err != nil || saved.PublishFrom == 0 || saved.Sequence == 0 {
		t.Fatal("digest lacked publication intent and sequence")
	}
	expectedSequence := saved.Sequence
	saved.Sequence = 0
	if _, err := service.writeExpiring(ctx, "message."+digest.ID, saved, revision, messageAge); err != nil {
		t.Fatal(err)
	}
	recovered.Sequence = 0
	_, revision, err = readState[messageRecord](ctx, service.state, "message."+first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.writeExpiring(ctx, "message."+first.ID, recovered, revision, messageAge); err != nil {
		t.Fatal(err)
	}
	admin(map[string]any{"action": "deny", "from": "sender", "to": "worker-a"})
	if err := service.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{digest.ID, first.ID} {
		got, _, err := readState[messageRecord](ctx, service.state, "message."+id)
		if err != nil || got.Sequence != expectedSequence {
			t.Fatalf("accepted digest recovery for %s: sequence=%d err=%v", id, got.Sequence, err)
		}
	}
	admin(map[string]any{"action": "allow", "from": "sender", "to": "worker-a"})

	call(peer, "ctl.settings.worker-a", map[string]string{"delivery": "immediate"}, "")
	ci := fixtureConnect(t, url, "csl-ci", fixtureCredentials(t, account, "csl-ci", []string{"csl.vi.ci.>"}, nil))
	noticeID := randomID()
	if err := PublishCINotice(ctx, ci, "vi", "worker-a", []byte(`{"id":"`+noticeID+`","body":"fixture CI passed"}`)); err != nil {
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
	var ownerChunk fileResult
	if err := json.Unmarshal(admin(map[string]any{"action": "fetch", "id": id, "offset": 7}), &ownerChunk); err != nil ||
		!bytes.Equal(ownerChunk.Data, content[7:7+maxChunk]) {
		t.Fatal("scoped owner could not fetch a file through admin")
	}
	admin(map[string]any{"action": "file-acl", "id": id, "allowed": []string{"worker-a", "worker-b"}})
	call(third, "ctl.fetch.worker-b", fileRequest{ID: id}, "")
	admin(map[string]any{"action": "file-acl", "id": id, "allowed": []string{"worker-a"}})
	call(third, "ctl.fetch.worker-b", fileRequest{ID: id}, "not_found")
	call(third, "ctl.admin.worker-b", map[string]any{"action": "fetch", "id": id}, "forbidden")

	exported := admin(map[string]any{"action": "export", "tenant": "vi"})
	if !bytes.Contains(exported, []byte("file."+id)) || !bytes.Contains(exported, []byte("message."+message.ID)) {
		t.Fatal("owner export did not contain message and file metadata")
	}
	assertResponseCode(t, fixtureRequest(t, ctx, owner, "csl.vi.ctl.admin.owner",
		map[string]any{"action": "export", "tenant": "foreign"}), "forbidden")

	testTenantLifecycle(t, ctx, url, owner, control, js, provisioner, broker.system)

	if contents := admin(map[string]any{"action": "export", "tenant": "vi"}); !bytes.Contains(contents, []byte("file."+id)) {
		t.Fatal("erasing the disposable tenant changed vi data")
	}

	expiring := []byte("resume after checkpoint expiration")
	expireID := fmt.Sprintf("%x", sha256.Sum256(expiring))
	upload := fileRequest{ID: expireID, Name: "resume.txt", Size: int64(len(expiring)), Data: expiring[:5]}
	call(sender, "ctl.put.sender", upload, "")
	service.mu.Lock()
	future = future.Add(uploadTTL + time.Second)
	service.mu.Unlock()
	resumed := upload
	resumed.Offset, resumed.Data = 5, expiring[5:]
	call(sender, "ctl.put.sender", resumed, "invalid")
	call(sender, "ctl.put.sender", upload, "")
	call(sender, "ctl.put.sender", resumed, "")

	call(peer, "ctl.complete.worker-a", map[string]string{"with": "sender"}, "")
	status := call(sender, "ctl.status.sender", map[string]string{}, "")
	var fleet struct {
		Agents []agentStatus `json:"agents"`
	}
	if json.Unmarshal(status, &fleet) != nil || len(fleet.Agents) != 3 || !fleet.Agents[1].TaskComplete ||
		len(fleet.Agents[1].OpenSessions) != 0 {
		t.Fatal("status did not show the completed pair")
	}
	call(peer, "ctl.settings.worker-a", map[string]string{"delivery": "grouped:15"}, "")
	big := first
	big.Body = strings.Repeat("\x01", maxBody)
	for i := range 2 {
		big.ID = randomID()
		big.CreatedAt = future.Add(time.Duration(i+1) * time.Second)
		call(sender, "send.sender", big, "")
	}
	history := call(peer, "ctl.history.worker-a", map[string]any{"with": "sender", "last": 2}, "")
	var page struct {
		Messages []historyEntry `json:"messages"`
		Next     string         `json:"next_before"`
	}
	if err := json.Unmarshal(history, &page); err != nil || len(page.Messages) != 1 ||
		page.Messages[0].Body != big.Body || page.Next != big.ID || len(history) >= maxDigest {
		t.Fatal("history page did not retain a complete near-limit body or cursor")
	}
	history = call(peer, "ctl.history.worker-a", map[string]any{"with": "sender", "last": 1, "before": page.Next}, "")
	if err := json.Unmarshal(history, &page); err != nil || len(page.Messages) != 1 || page.Messages[0].Body != big.Body {
		t.Fatal("history continuation lost a complete body")
	}

	admin(map[string]any{"action": "allow", "from": "sender", "to": "worker-b"})
	call(third, "ctl.settings.worker-b", map[string]string{"delivery": "grouped:15"}, "")
	pending := first
	pending.ID, pending.To = randomID(), "worker-b"
	call(sender, "send.sender", pending, "")
	admin(map[string]any{"action": "revoke", "name": "worker-b"})
	if err := service.ApplyRevocations(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-revoked:
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not disconnect the revoked active credential")
	}
	service.mu.Lock()
	future = future.Add(16 * time.Minute)
	service.mu.Unlock()
	if err := service.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	dropped, _, err := readState[messageState](ctx, service.state, "msg."+pending.ID)
	if err != nil || dropped.State != "dropped" {
		t.Fatalf("revoked recipient digest state=%q err=%v", dropped.State, err)
	}
	admin(map[string]any{"action": "register", "name": "worker-a", "role": "worker", "cli": "codex", "machine": "fixture"})

	foreignAccount, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer foreignAccount.Wipe()
	foreignKey, _ := foreignAccount.PublicKey()
	foreignClaims := jwt.NewAccountClaims(foreignKey)
	foreignClaims.Limits.DiskStorage = 4 * 1024 * 1024
	foreignClaims.Limits.Streams, foreignClaims.Limits.Consumer = -1, -1
	if err := pushFixtureAccount(ctx, sys, broker.operator, foreignClaims); err != nil {
		t.Fatal(err)
	}
	foreign := fixtureConnect(t, url, "foreign-owner", fixtureCredentials(t, foreignAccount, "foreign-owner", nil, nil))
	foreignJS, err := jetstream.New(foreign)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := foreignJS.Stream(ctx, "CSL_MSG"); err == nil {
		t.Fatal("foreign tenant discovered the primary tenant stream")
	}
	if _, err := foreignJS.ObjectStore(ctx, "CSL_FILES"); err == nil {
		t.Fatal("foreign tenant discovered the primary tenant files")
	}
	if _, err := foreign.Request("csl.vi.ctl.admin.owner", []byte(`{"action":"versions"}`), 500*time.Millisecond); err == nil {
		t.Fatal("foreign tenant reached the primary control subjects")
	}
	assertResponseCode(t, fixtureRequest(t, ctx, owner, "csl.vi.ctl.admin.owner",
		map[string]any{"action": "settings", "notices_to": "unregistered"}), "not_found")
	admin(map[string]any{"action": "settings", "notices_to": "sender"})
	noticeRequest := first
	noticeRequest.ID, noticeRequest.Body, noticeRequest.To = randomID(), "private text", "unregistered"
	for range 2 {
		call(sender, "send.sender", noticeRequest, "not_found")
	}
	admin(map[string]any{"action": "deny", "from": "sender", "to": "worker-a"})
	noticeRequest.To = "worker-a"
	for range 2 {
		call(sender, "send.sender", noticeRequest, "forbidden")
		call(sender, "ctl.hello.sender", map[string]string{"client_version": "1.1.0+abc"}, "outdated_client")
	}
	admin(map[string]any{"action": "allow", "from": "sender", "to": "worker-a"})
	noticeRequest.Urgency, noticeRequest.Reason = "urgent", "blocked"
	for range 3 {
		noticeRequest.ID = randomID()
		call(sender, "send.sender", noticeRequest, "")
	}
	ciUnknown := []byte(`{"id":"` + randomID() + `","body":"quiet CI drop"}`)
	if err := ci.Publish("csl.vi.ci.unregistered", ciUnknown); err != nil {
		t.Fatal(err)
	}
	if err := ci.FlushWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	entries, err := service.messages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantNotices := map[string]bool{
		"Unknown target (sender to unregistered).": false,
		"Unapproved pair (sender to worker-a).":    false,
		"Outdated client (sender).":                false,
		"Urgent cap reached (sender to worker-a).": false,
	}
	for _, entry := range entries {
		env := entry.record.Envelope
		if env.To != "sender" || env.Kind != "system" {
			continue
		}
		seen, expected := wantNotices[env.Body]
		if seen || !expected || entry.record.Sequence == 0 {
			t.Fatalf("unexpected or repeated sanitized system notice: %q", env.Body)
		}
		wantNotices[env.Body] = true
	}
	for body, seen := range wantNotices {
		if !seen {
			t.Fatalf("missing system notice: %q", body)
		}
	}
	admin(map[string]any{"action": "quotas", "retention": 1, "urgent_cap": 1})
	call(peer, "ctl.settings.worker-a", map[string]string{"delivery": "immediate"}, "")
	unattached := []byte("unattached file")
	unattachedID := fmt.Sprintf("%x", sha256.Sum256(unattached))
	call(sender, "ctl.put.sender", fileRequest{ID: unattachedID, Name: "unattached.txt",
		Size: int64(len(unattached)), Data: unattached}, "")
	reference := first
	reference.ID = randomID()
	reference.Attachments = []attachment{{ID: id, Name: "fixture.txt", Size: int64(len(content))}}
	call(sender, "send.sender", reference, "")
	replacement := first
	replacement.ID = randomID()
	call(sender, "send.sender", replacement, "")
	if err := service.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	call(peer, "ctl.fetch.worker-a", fileRequest{ID: id}, "")
	call(peer, "ctl.receipt.worker-a", sendResult{reference.ID, "acked"}, "")
	if err := service.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	call(peer, "ctl.fetch.worker-a", fileRequest{ID: id}, "not_found")
	call(sender, "ctl.fetch.sender", fileRequest{ID: unattachedID}, "")

	verifyMirrorNotices(t, ctx, service)
	verifyQueuedRevocations(t, ctx, service, owner)

}

type fixtureBroker struct {
	url                       string
	operator, account, system nkeys.KeyPair
	claims                    *jwt.AccountClaims
}

func pushFixtureAccount(ctx context.Context, conn *nats.Conn, operator nkeys.KeyPair, claims *jwt.AccountClaims) error {
	token, err := claims.Encode(operator)
	if err != nil {
		return err
	}
	msg, err := conn.RequestWithContext(ctx, "$SYS.REQ.CLAIMS.UPDATE", []byte(token))
	if err != nil {
		return err
	}
	var response struct {
		Data struct {
			Account string `json:"account"`
		} `json:"data"`
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &response); err != nil || len(response.Error) != 0 || response.Data.Account != claims.Subject {
		return fmt.Errorf("resolver rejected the signed fixture account")
	}
	return nil
}

func localBroker(t *testing.T) fixtureBroker {
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
	t.Cleanup(system.Wipe)
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
	endpoint := startFixtureBroker(t, server, operatorToken, systemKey, systemToken, accountKey, accountToken)
	return fixtureBroker{endpoint, operator, account, system, claims}
}

func startFixtureBroker(t *testing.T, server, operatorToken, systemKey, systemToken, accountKey, accountToken string) string {
	t.Helper()
	dir := t.TempDir()
	cfg := fmt.Sprintf(
		"host: 127.0.0.1\nport: -1\nports_file_dir: %q\noperator: %q\nsystem_account: %s\nresolver: { type: full, dir: %q }\nresolver_preload: { %s: %q, %s: %q }\njetstream: {store_dir: %q, domain: fixture}\n",
		dir, operatorToken, systemKey, filepath.Join(dir, "resolver"), systemKey, systemToken, accountKey, accountToken, filepath.Join(dir, "data"))
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
			return ports.Nats[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	logs, _ := os.ReadFile(logPath)
	t.Fatalf("local broker did not become ready: %s", logs)
	return ""
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

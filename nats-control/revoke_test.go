package control

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func verifyQueuedRevocations(t *testing.T, ctx context.Context, service *Service, owner *nats.Conn) {
	t.Helper()
	const name = "revocation-worker"
	admin := func(action string, fields map[string]any) []byte {
		t.Helper()
		fields["action"] = action
		reply := fixtureRequest(t, ctx, owner, "csl.vi.ctl.admin.owner", fields)
		assertResponseCode(t, reply, "")
		return reply
	}
	admin("register", map[string]any{"name": name, "cli": "codex", "role": "worker", "machine": "fixture"})
	now := time.Now().UTC()
	for range 205 {
		user, err := nkeys.CreateUser()
		if err != nil {
			t.Fatal(err)
		}
		key, err := user.PublicKey()
		user.Wipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := writeState(ctx, service.state, "credential."+name+"."+key, now.Add(time.Hour), 0); err != nil {
			t.Fatal(err)
		}
	}
	var calls [][]string
	var cutoff time.Time
	originalRevoke := service.config.RevokeUsers
	service.config.RevokeUsers = func(ctx context.Context, keys []string, at time.Time) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > revokeDeadline {
			t.Fatal("revoker had no bounded deadline")
		}
		if cutoff.IsZero() {
			cutoff = at
		}
		if !cutoff.Equal(at) || len(keys) > revokeBatchLimit {
			t.Fatal("revocation retries changed cutoff or exceeded the helper limit")
		}
		calls = append(calls, slices.Clone(keys))
		if len(calls) <= 4 {
			return context.DeadlineExceeded
		}
		return nil
	}
	service.mu.Lock()
	originalClock := service.clock
	service.clock = func() time.Time { return now }
	service.mu.Unlock()
	defer func() {
		service.mu.Lock()
		service.clock = originalClock
		service.config.RevokeUsers = originalRevoke
		service.mu.Unlock()
	}()
	var queued struct {
		Revoked bool `json:"revoked"`
		Pending bool `json:"pending"`
	}
	if err := json.Unmarshal(admin("revoke", map[string]any{"name": name}), &queued); err != nil || !queued.Revoked || !queued.Pending {
		t.Fatal("admin did not report pending broker revocation")
	}
	if err := service.activeAgent(ctx, name); err == nil {
		t.Fatal("control still accepted a queued revoked agent")
	}
	restart, err := Open(ctx, service.conn, service.config)
	if err != nil {
		t.Fatal(err)
	}
	restart.clock = service.clock
	run := func(delay time.Duration) {
		t.Helper()
		service.mu.Lock()
		now = now.Add(delay)
		service.mu.Unlock()
		if err := restart.ApplyRevocations(ctx); err != nil {
			t.Fatal(err)
		}
	}
	run(0)
	run(4 * time.Second)
	if len(calls) != 1 {
		t.Fatal("revocation retry ignored its backoff")
	}
	run(time.Second)
	run(10 * time.Second)
	run(20 * time.Second)
	if len(calls) != 4 {
		t.Fatal("first attempt and three retries did not execute")
	}
	entries, err := service.messages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	noticeCount := 0
	for _, entry := range entries {
		env := entry.record.Envelope
		if env.Body == "Broker credential revocation pending ("+name+")." && env.To == "sender" {
			noticeCount++
		}
	}
	if noticeCount != 1 {
		t.Fatalf("expected one owner failure notice, got %d", noticeCount)
	}
	run(time.Minute)
	run(0)
	run(0)
	if len(calls) != 7 || len(calls[0]) != 100 || len(calls[5]) != 100 || len(calls[6]) != 6 {
		t.Fatal("expected batches of 100, 100 and 6 with the first batch retried")
	}
	for _, retry := range calls[1:5] {
		if !slices.Equal(calls[0], retry) {
			t.Fatal("a restart or timeout changed the pending batch")
		}
	}
	seen := make(map[string]bool)
	for _, batch := range [][]string{calls[0], calls[5], calls[6]} {
		for _, key := range batch {
			if seen[key] {
				t.Fatal("completed batch was added to another batch")
			}
			seen[key] = true
		}
	}
	queued.Pending = false
	if err := json.Unmarshal(admin("revoke", map[string]any{"name": name}), &queued); err != nil || !queued.Revoked || queued.Pending {
		t.Fatal("queue readback did not report revocation")
	}
	done, _, err := readState[revocation](ctx, service.state, "revoked."+name)
	if err != nil || !done.Done {
		t.Fatal("successful batches did not finalize the revocation record")
	}
}

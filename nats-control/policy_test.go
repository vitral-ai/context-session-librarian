package control

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestPairSessionExpiryAndReopen(t *testing.T) {
	key, err := pairKey("worker-z", "worker-a")
	if err != nil || key != "session.worker-a.worker-z" {
		t.Fatalf("key=%q err=%v", key, err)
	}
	now := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	session := (PairSession{}).mention("worker-z", now)
	if !session.isOpen(now.Add(pairIdleLimit-time.Nanosecond)) || session.isOpen(now.Add(pairIdleLimit)) {
		t.Fatal("unexpected expiry boundary")
	}
	updated := session.mention("worker-a", now.Add(time.Minute))
	if updated.OpenedAt != session.OpenedAt || updated.LastFrom != "worker-a" {
		t.Fatalf("unexpected update: %+v", updated)
	}
	updated.Closed = true
	if retry := updated.mention("worker-a", updated.LastMsgAt); retry != updated {
		t.Fatal("duplicate reopened the closed session")
	}
	reopened := updated.mention("worker-z", now.Add(2*time.Minute))
	if !reopened.isOpen(reopened.LastMsgAt) || reopened.OpenedAt != reopened.LastMsgAt {
		t.Fatalf("unexpected replacement session: %+v", reopened)
	}
}

func TestReminderSkipsRecentWritesClosedAndExpiredPairs(t *testing.T) {
	now := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	open := (PairSession{}).mention("worker-a", now.Add(-time.Minute))
	closed := open
	closed.Closed = true
	expired := (PairSession{}).mention("worker-a", now.Add(-pairIdleLimit))
	sessions := []reminderSession{
		{peer: "worker-z", session: open},
		{peer: "worker-b", session: open, lastSentAt: now.Add(-recentWrite)},
		{peer: "worker-c", session: open, lastSentAt: now.Add(-recentWrite + time.Second)},
		{peer: "worker-d", session: closed},
		{peer: "worker-e", session: expired},
	}
	want := "Open sessions: worker-b, worker-z. If you owe an update, send it now. Do not reply to this reminder."
	if got := reminderBody(Agent{Role: "worker"}, sessions, now); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if got := reminderBody(Agent{Role: "orchestrator"}, sessions, now); got != "" {
		t.Fatalf("orchestrator would be reminded: %q", got)
	}
}

func TestVersionPolicyChecksReleaseRatherThanBuild(t *testing.T) {
	policy := VersionPolicy{Minimum: "1.2.0", Recommended: "1.10.0"}
	for _, test := range []struct {
		client   string
		outdated bool
		warn     bool
	}{
		{"1.1.9+abc", true, false},
		{"1.2.0+deadbeef", false, true},
		{"1.10.0+abc", false, false},
		{"2.0.0+def", false, false},
	} {
		outdated, warn, err := policy.check(test.client)
		if err != nil || outdated != test.outdated || warn != test.warn {
			t.Errorf("%s: outdated=%v warn=%v err=%v", test.client, outdated, warn, err)
		}
	}
	if _, _, err := policy.check("01.2.0+abc"); err == nil {
		t.Fatal("invalid client accepted")
	}
	if _, _, err := (VersionPolicy{Minimum: "2.0.0", Recommended: "1.0.0"}).check("2.0.0"); err == nil {
		t.Fatal("inverted policy accepted")
	}
}

func TestDeliveryDefaultsAndIntervals(t *testing.T) {
	if defaultDelivery("worker") != "immediate" || defaultDelivery("orchestrator") != "grouped:15" {
		t.Fatal("unexpected role defaults")
	}
	for _, name := range []string{"grouped:0", "grouped:015", "grouped:+15", "grouped:1000000000000", "digest"} {
		if _, err := deliveryInterval(name); err == nil {
			t.Errorf("accepted %q", name)
		}
	}
	if got, err := deliveryInterval("grouped:15"); err != nil || got != 15*time.Minute {
		t.Fatalf("duration=%s err=%v", got, err)
	}
}

func TestDigestPartsRespectEncodedBudgetAndKeepMessagesWhole(t *testing.T) {
	body := strings.Repeat("\x01", maxBody)
	items := []Envelope{{ID: "one", Body: body}, {ID: "two", Body: body}, {ID: "three", Body: body}}
	parts, err := digestParts(items)
	if err != nil || len(parts) != 3 {
		t.Fatalf("part count=%d err=%v", len(parts), err)
	}
	for i, part := range parts {
		envelope := serviceEnvelope(randomID(), "worker-a", "digest", "", time.Now())
		envelope.Items = part
		envelope.Part = "1/3"
		raw, err := json.Marshal(envelope)
		if err != nil || len(raw) >= maxDigest || len(part) != 1 || part[0].ID != items[i].ID || part[0].Body != body {
			t.Fatalf("invalid digest part %d", i)
		}
	}
}

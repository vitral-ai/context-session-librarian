package control

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const pairHold = 24 * time.Hour

type approvalRequest struct {
	At         time.Time `json:"at"`
	From       string    `json:"from"`
	To         string    `json:"to"`
	Recipients []string  `json:"recipients"`
}

type pendingPair struct {
	From      string    `json:"from"`
	To        string    `json:"to"`
	Count     int       `json:"count"`
	ExpiresAt time.Time `json:"expires_at"`
}

func pairDenialKey(a, b string) string {
	return "pair-denial." + min(a, b) + "." + max(a, b)
}

func (s *Service) requestPairApproval(ctx context.Context, record messageRecord, now time.Time) error {
	if !now.Before(record.HeldUntil) {
		return nil
	}
	env := record.Envelope
	key := "pair-request." + min(env.From, env.To) + "." + max(env.From, env.To)
	request, _, err := readState[approvalRequest](ctx, s.state, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		request = approvalRequest{At: now, From: env.From, To: env.To}
		for _, name := range []string{env.From, env.To} {
			agent, _, err := readState[Agent](ctx, s.state, "agent."+name)
			if err != nil {
				return err
			}
			if agent.Role == "orchestrator" {
				request.Recipients = append(request.Recipients, name)
			}
			request.Recipients = append(request.Recipients, agent.Coordinators...)
		}
		owner, _, lookupErr := readState[string](ctx, s.state, noticeRecipientKey)
		if lookupErr != nil && !errors.Is(lookupErr, jetstream.ErrKeyNotFound) {
			return lookupErr
		}
		if owner != "" {
			request.Recipients = append(request.Recipients, owner)
		}
		slices.Sort(request.Recipients)
		request.Recipients = slices.Compact(request.Recipients)
		_, err = s.writeExpiring(ctx, key, request, 0, pairHold)
	}
	if err != nil {
		return err
	}
	body := fmt.Sprintf("Pair %s -> %s is awaiting approval. Approve with: csl agents allow %s %s", request.From, request.To, request.From, request.To)
	for _, recipient := range request.Recipients {
		active, err := s.deliveryAgent(ctx, recipient)
		if err != nil {
			return err
		}
		if !active {
			continue
		}
		id := stableID(key + "." + request.At.Format(time.RFC3339Nano) + "." + recipient)
		notice := serviceEnvelope(id, recipient, "system", body, request.At)
		notice.Urgency = "urgent"
		if err := s.recordServiceMessage(ctx, notice); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) listHeldPairs(ctx context.Context, caller string) ([]pendingPair, error) {
	entries, err := s.messages(ctx)
	if err != nil {
		return nil, err
	}
	now := s.clock()
	pending := make([]pendingPair, 0)
	index := make(map[[2]string]int)
	unmanaged := make(map[[2]string]bool)
	for _, entry := range entries {
		if !now.Before(entry.record.HeldUntil) {
			continue
		}
		env := entry.record.Envelope
		key := [2]string{env.From, env.To}
		if unmanaged[key] {
			continue
		}
		if position, ok := index[key]; ok {
			pending[position].Count++
			continue
		}
		managed, err := s.managesPair(ctx, caller, env.From, env.To)
		if err != nil {
			return nil, err
		}
		if !managed {
			unmanaged[key] = true
			continue
		}
		index[key] = len(pending)
		pending = append(pending, pendingPair{env.From, env.To, 1, entry.record.HeldUntil})
	}
	return pending, nil
}

func (s *Service) advanceHeldPairs(ctx context.Context, entries []messageEntry, now time.Time, a, b string) error {
	if s.config.VerifyPairs == nil {
		return nil
	}
	held := make([]messageEntry, 0)
	for _, entry := range entries {
		env := entry.record.Envelope
		if !entry.record.HeldUntil.IsZero() && (a == "" || (env.From == a && env.To == b) || (env.From == b && env.To == a)) {
			held = append(held, entry)
		}
	}
	// Entries already have an acceptance/ID tie break. Approval order uses
	// the envelope timestamp, including sends submitted out of order.
	slices.SortStableFunc(held, func(a, b messageEntry) int {
		return a.record.Envelope.CreatedAt.Compare(b.record.Envelope.CreatedAt)
	})
	confirmed := make(map[string]bool)
	for _, entry := range held {
		if err := s.advanceHeldPair(ctx, entry, now, confirmed); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) advanceHeldPair(ctx context.Context, entry messageEntry, now time.Time, confirmed map[string]bool) error {
	record := entry.record
	env := record.Envelope
	deniedAt, _, err := readState[time.Time](ctx, s.state, pairDenialKey(env.From, env.To))
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	stateKey := "msg." + env.ID
	state, rev, err := readState[messageState](ctx, s.state, stateKey)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	eligible, err := s.managesPair(ctx, "owner", env.From, env.To)
	if err != nil {
		return err
	}
	if state.State == "dropped" || !eligible || !now.Before(record.HeldUntil) || !deniedAt.Before(record.AcceptedAt) {
		if state.State != "dropped" {
			if _, err := s.writeExpiring(ctx, stateKey, messageState{"dropped", now}, rev, messageAge); err != nil {
				return err
			}
		}
		active, err := s.deliveryAgent(ctx, env.From)
		if err != nil {
			return err
		}
		if active {
			body := fmt.Sprintf("Held message %s for %s -> %s was dropped because approval was denied or expired.", env.ID, env.From, env.To)
			notice := serviceEnvelope(stableID("held.dropped."+env.ID), env.From, "system", body, now)
			if err := s.recordServiceMessage(ctx, notice); err != nil {
				return err
			}
		}
		record.HeldUntil = time.Time{}
		_, err = s.writeExpiring(ctx, "message."+env.ID, record, entry.revision, messageAge)
		return err
	}
	pairKey := "pair." + env.From + "." + env.To
	allowed, pairRev, err := s.pairAllowed(ctx, env.From, env.To)
	if err != nil {
		return err
	}
	if !allowed {
		if err := s.ensureMessageState(ctx, record); err != nil {
			return err
		}
		return s.requestPairApproval(ctx, record, now)
	}
	if !confirmed[pairKey] {
		if err := s.config.VerifyPairs(ctx, []PairReadback{{pairKey, pairRev, true}}); err != nil {
			return err
		}
		confirmed[pairKey] = true
	}
	if state.State != "queued" {
		if _, err := s.writeExpiring(ctx, stateKey, messageState{"queued", record.AcceptedAt}, rev, messageAge); err != nil {
			return err
		}
	}
	record.HeldUntil = time.Time{}
	record.ReadyAt = record.AcceptedAt
	revision, err := s.writeExpiring(ctx, "message."+env.ID, record, entry.revision, messageAge)
	if err != nil {
		return err
	}
	if err := s.mentionPair(ctx, record); err != nil {
		return err
	}
	return s.publishRecord(ctx, record, revision)
}

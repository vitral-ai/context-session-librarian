package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type messageEntry struct {
	record   messageRecord
	revision uint64
}

func (s *Service) messages(ctx context.Context) ([]messageEntry, error) {
	keys, err := stateKeys(ctx, s.state, "message.*")
	if err != nil {
		return nil, err
	}
	entries := make([]messageEntry, 0, len(keys))
	for _, key := range keys {
		record, revision, err := readState[messageRecord](ctx, s.state, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, messageEntry{record, revision})
	}
	slices.SortFunc(entries, func(a, b messageEntry) int {
		if order := a.record.AcceptedAt.Compare(b.record.AcceptedAt); order != 0 {
			return order
		}
		return strings.Compare(a.record.Envelope.ID, b.record.Envelope.ID)
	})
	return entries, nil
}

var messagePrefix = regexp.MustCompile(`^[a-f0-9]{8,12}$`)

type historyEntry struct {
	Envelope
	State string `json:"state"`
}

func (s *Service) history(ctx context.Context, caller string, body []byte) (any, error) {
	var request struct {
		With string `json:"with,omitempty"`
		Last int    `json:"last,omitempty"`
		ID   string `json:"id,omitempty"`
	}
	if err := decode(body, &request); err != nil {
		return nil, err
	}
	retention, err := s.retention(ctx)
	if err != nil {
		return nil, err
	}
	if request.Last == 0 {
		request.Last = min(5, retention)
	}
	entries, err := s.messages(ctx)
	if err != nil {
		return nil, err
	}
	if request.ID != "" {
		if request.With != "" || (!messageID.MatchString(request.ID) && !messagePrefix.MatchString(request.ID)) {
			return nil, invalid("provide one message ID or pair name")
		}
		var matched string
		for _, entry := range entries {
			envelope := entry.record.Envelope
			if !strings.HasPrefix(envelope.ID, request.ID) || (caller != envelope.To && caller != envelope.From) ||
				(envelope.Kind != "message" && envelope.Kind != "reply") {
				continue
			}
			if matched != "" {
				return nil, invalid("ambiguous message prefix")
			}
			matched = envelope.ID
			request.With = envelope.From
			if request.With == caller {
				request.With = envelope.To
			}
		}
		request.ID = matched
		if request.With == "" {
			return nil, missing("message unavailable")
		}
	}
	if _, err := pairKey(caller, request.With); err != nil || request.Last < 1 || request.Last > retention {
		return nil, invalid("invalid pair or history limit")
	}
	matches := make([]messageEntry, 0, retention)
	for _, entry := range entries {
		env := entry.record.Envelope
		if (env.From == caller && env.To == request.With) || (env.To == caller && env.From == request.With) {
			matches = append(matches, entry)
		}
	}
	matches = matches[max(0, len(matches)-retention):]
	if request.ID == "" {
		matches = matches[max(0, len(matches)-request.Last):]
	}
	result := make([]historyEntry, 0, len(matches))
	for _, entry := range matches {
		if request.ID != "" && request.ID != entry.record.Envelope.ID {
			continue
		}
		state, _, err := readState[messageState](ctx, s.state, "msg."+entry.record.Envelope.ID)
		if err != nil {
			return nil, err
		}
		result = append(result, historyEntry{entry.record.Envelope, state.State})
	}
	if request.ID != "" && len(result) == 0 {
		return nil, missing("message unavailable")
	}
	return struct {
		Messages  []historyEntry `json:"messages"`
		Retention int            `json:"retention"`
	}{result, retention}, nil
}

func (s *Service) receipt(ctx context.Context, caller string, body []byte) (any, error) {
	var request sendResult
	if err := decode(body, &request); err != nil {
		return nil, err
	}
	if !messageID.MatchString(request.ID) || (request.State != "delivered" && request.State != "acked") {
		return nil, invalid("invalid receipt")
	}
	entry, _, err := readState[messageRecord](ctx, s.state, "message."+request.ID)
	if errors.Is(err, jetstream.ErrKeyNotFound) || (err == nil && entry.Envelope.To != caller) {
		return nil, missing("message unavailable")
	}
	if err != nil {
		return nil, err
	}
	if entry.Sequence == 0 {
		return nil, forbidden("message has not reached the inbox")
	}
	state, revision, err := readState[messageState](ctx, s.state, "msg."+request.ID)
	if err != nil {
		return nil, err
	}
	if state.State == "acked" || state.State == request.State || state.State == "dropped" {
		return sendResult{request.ID, state.State}, nil
	}
	now := s.clock().UTC()
	ttl := max(time.Second, entry.AcceptedAt.Add(messageAge).Sub(now))
	_, err = s.writeExpiring(ctx, "msg."+request.ID, messageState{request.State, now}, revision, ttl)
	return request, err
}

type tenantPolicy struct {
	Retention int `json:"retention"`
	UrgentCap int `json:"urgent_cap"`
}

func (s *Service) deliveryPolicy(ctx context.Context) (tenantPolicy, error) {
	policy, _, err := readState[tenantPolicy](ctx, s.state, "policy.delivery")
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return tenantPolicy{defaultRetain, 10}, nil
	}
	return policy, err
}

func (s *Service) retention(ctx context.Context) (int, error) {
	policy, err := s.deliveryPolicy(ctx)
	return policy.Retention, err
}

func (s *Service) markNewWork(ctx context.Context, env Envelope) error {
	items := env.Items
	if env.Kind == "message" || env.Kind == "reply" {
		items = []Envelope{env}
	}
	if len(items) == 0 {
		return nil
	}
	recipient, revision, err := readState[Agent](ctx, s.state, "agent."+env.To)
	if err != nil || !recipient.TaskComplete {
		return err
	}
	for _, item := range items {
		sender, _, err := readState[Agent](ctx, s.state, "agent."+item.From)
		if err != nil {
			return err
		}
		if sender.Role == "orchestrator" {
			recipient.TaskComplete = false
			return writeState(ctx, s.state, "agent."+env.To, recipient, revision)
		}
	}
	return nil
}

func (s *Service) recordServiceMessage(ctx context.Context, envelope Envelope) error {
	key := "message." + envelope.ID
	record, revision, err := readState[messageRecord](ctx, s.state, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		record = messageRecord{Envelope: envelope, AcceptedAt: envelope.CreatedAt, ReadyAt: envelope.CreatedAt}
		revision, err = s.writeExpiring(ctx, key, record, 0, messageAge)
	}
	if err != nil {
		return err
	}
	if record.Sequence != 0 {
		return nil
	}
	if err := s.ensureMessageState(ctx, record); err != nil {
		return err
	}
	return s.publishRecord(ctx, record, revision)
}

func digestParts(items []Envelope) ([][]Envelope, error) {
	// Reserve room for the outer envelope and its part marker.
	const budget = maxDigest - 1024
	var parts [][]Envelope
	start, size := 0, 0
	for i, item := range items {
		raw, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		n := len(raw) + 1
		if n > budget {
			return nil, invalid("message does not fit in a digest part")
		}
		if size+n > budget {
			parts = append(parts, items[start:i])
			start, size = i, 0
		}
		size += n
	}
	if start < len(items) {
		parts = append(parts, items[start:])
	}
	return parts, nil
}

// Tick recovers accepted sends, flushes due grouped messages, and sends one
// reminder per worker per 20-minute interval.
func (s *Service) Tick(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := s.messages(ctx)
	if err != nil {
		return err
	}
	now := s.clock().UTC()
	groups := map[string][]messageEntry{}
	var dueRecipients []string
	for _, entry := range entries {
		env := entry.record.Envelope
		if entry.record.Sequence != 0 || entry.record.ReadyAt.After(now) {
			continue
		}
		state, _, err := readState[messageState](ctx, s.state, "msg."+env.ID)
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return err
		}
		if state.State == "dropped" || state.State == "refused" {
			continue
		}
		if err := s.ensureMessageState(ctx, entry.record); err != nil {
			return err
		}
		if env.Kind == "message" || env.Kind == "reply" {
			allowed, _, err := readState[bool](ctx, s.state, "pair."+env.From+"."+env.To)
			if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
				return err
			}
			if !allowed {
				if err := s.markDropped(ctx, env.ID, now); err != nil {
					return err
				}
				continue
			}
			if err := s.mentionPair(ctx, entry.record); err != nil {
				return err
			}
		}
		if entry.record.ReadyAt.Equal(entry.record.AcceptedAt) {
			if err := s.publishRecord(ctx, entry.record, entry.revision); err != nil {
				return err
			}
			continue
		}
		if groups[env.To] == nil {
			dueRecipients = append(dueRecipients, env.To)
		}
		groups[env.To] = append(groups[env.To], entry)
	}
	for _, name := range dueRecipients {
		if err := s.flushGroup(ctx, name, groups[name], now); err != nil {
			return err
		}
	}
	return s.sendReminders(ctx, now)
}

func (s *Service) markDropped(ctx context.Context, id string, now time.Time) error {
	key := "msg." + id
	_, revision, err := readState[messageState](ctx, s.state, key)
	if err != nil {
		return err
	}
	_, err = s.writeExpiring(ctx, key, messageState{"dropped", now}, revision, messageAge)
	return err
}

func (s *Service) flushGroup(ctx context.Context, recipient string, entries []messageEntry, now time.Time) error {
	items := make([]Envelope, 0, len(entries))
	revisions := make(map[string]messageEntry, len(entries))
	for _, entry := range entries {
		env := entry.record.Envelope
		items = append(items, env)
		revisions[env.ID] = entry
	}
	parts, err := digestParts(items)
	if err != nil {
		return err
	}
	for i, part := range parts {
		var identity strings.Builder
		identity.WriteString("digest." + recipient)
		for _, env := range part {
			identity.WriteString("." + env.ID)
		}
		env := serviceEnvelope(stableID(identity.String()), recipient, "digest", "", now)
		env.Items = part
		if len(parts) > 1 {
			env.Part = fmt.Sprintf("%d/%d", i+1, len(parts))
		}
		if err := s.recordServiceMessage(ctx, env); err != nil {
			return err
		}
		record, _, err := readState[messageRecord](ctx, s.state, "message."+env.ID)
		if err != nil {
			return err
		}
		for _, item := range part {
			entry := revisions[item.ID]
			entry.record.Sequence = record.Sequence
			if _, err := s.writeExpiring(ctx, "message."+item.ID, entry.record, entry.revision, messageAge); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Service) sendReminders(ctx context.Context, now time.Time) error {
	keys, err := stateKeys(ctx, s.state, "session.*.*")
	if err != nil {
		return err
	}
	open := map[string][]reminderSession{}
	for _, key := range keys {
		session, _, err := readState[PairSession](ctx, s.state, key)
		if err != nil {
			return err
		}
		if !session.isOpen(now) {
			continue
		}
		names := strings.Split(key, ".")[1:]
		for i, name := range names {
			peer := names[1-i]
			last, _, err := readState[time.Time](ctx, s.state, "lastsent."+name+"."+peer)
			if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
				return err
			}
			open[name] = append(open[name], reminderSession{peer, session, last})
		}
	}
	for name, sessions := range open {
		agent, _, err := readState[Agent](ctx, s.state, "agent."+name)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		body := reminderBody(agent, sessions, now)
		if body == "" {
			continue
		}
		slot := now.Truncate(reminderInterval)
		id := stableID("reminder." + name + "." + slot.Format(time.RFC3339))
		if err := s.recordServiceMessage(ctx, serviceEnvelope(id, name, "reminder", body, now)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) completePairs(ctx context.Context, caller, peer string) error {
	var keys []string
	if peer != "" {
		key, err := pairKey(caller, peer)
		if err != nil {
			return invalid(err.Error())
		}
		keys = []string{key}
	} else {
		var err error
		keys, err = stateKeys(ctx, s.state, "session.*.*")
		if err != nil {
			return err
		}
	}
	for _, key := range keys {
		names := strings.Split(key, ".")
		if names[1] != caller && names[2] != caller {
			continue
		}
		pair, revision, err := readState[PairSession](ctx, s.state, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if pair.isOpen(s.clock()) {
			pair.Closed = true
			if err := writeState(ctx, s.state, key, pair, revision); err != nil {
				return err
			}
		}
	}
	return nil
}

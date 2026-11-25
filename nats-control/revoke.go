package control

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const revokeBatchLimit = 100
const revokeDeadline = 30 * time.Second

type revocation struct {
	Blocked    bool      `json:"blocked"`
	At         time.Time `json:"at,omitzero"`
	Cursor     string    `json:"cursor,omitempty"`
	Batch      []string  `json:"batch,omitempty"`
	Failures   int       `json:"failures,omitempty"`
	RetryAt    time.Time `json:"retry_at,omitzero"`
	NoticeSent bool      `json:"notice_sent,omitempty"`
	Done       bool      `json:"done,omitempty"`
}

func (r *revocation) UnmarshalJSON(data []byte) error {
	// The previous control binary persisted a boolean. Import true as a
	// pending revoke so an earlier helper failure can be reconciled too.
	if string(data) == "true" || string(data) == "false" {
		*r = revocation{Blocked: string(data) == "true"}
		return nil
	}
	type record revocation
	return json.Unmarshal(data, (*record)(r))
}

func (s *Service) activeAgent(ctx context.Context, name string) error {
	if !isRegisteredName(name) {
		return forbidden("invalid agent identity")
	}
	revoked, _, err := readState[revocation](ctx, s.state, "revoked."+name)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if revoked.Blocked {
		return forbidden("agent credentials are revoked")
	}
	return nil
}

func (s *Service) deliveryAgent(ctx context.Context, name string) (bool, error) {
	var failure *apiError
	err := s.activeAgent(ctx, name)
	if errors.As(err, &failure) {
		return false, nil
	}
	return err == nil, err
}

func (s *Service) revoke(ctx context.Context, name string) (any, error) {
	if s.config.RevokeUsers == nil {
		return nil, forbidden("account revocation is not configured")
	}
	if !isRegisteredName(name) {
		return nil, invalid("invalid agent name")
	}
	if _, _, err := readState[Agent](ctx, s.state, "agent."+name); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, missing("unknown agent")
		}
		return nil, err
	}
	key := "revoked." + name
	record, revision, err := readState[revocation](ctx, s.state, key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, err
	}
	if !record.Blocked {
		record = revocation{Blocked: true, At: s.clock().UTC()}
		if err := writeState(ctx, s.state, key, record, revision); err != nil {
			return nil, err
		}
	}
	if !record.Done {
		s.signalRevocation()
	}
	return struct {
		Revoked bool `json:"revoked"`
		Pending bool `json:"pending,omitempty"`
	}{true, !record.Done}, nil
}

func (s *Service) RevocationsReady() <-chan struct{} {
	return s.revocationWake
}

func (s *Service) signalRevocation() {
	select {
	case s.revocationWake <- struct{}{}:
	default:
	}
}

// ApplyRevocations is invoked by the existing control scheduler on wake or
// Tick. Each persisted batch is retried with the same keys and cutoff.
func (s *Service) ApplyRevocations(ctx context.Context) error {
	if s.config.RevokeUsers == nil {
		return nil
	}
	keys, err := stateKeys(ctx, s.state, "revoked.*")
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.applyRevocation(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) applyRevocation(ctx context.Context, key string) error {
	record, revision, err := s.prepareRevocation(ctx, key)
	if err != nil || len(record.Batch) == 0 {
		return err
	}
	batchErr := s.revokeBatch(ctx, record.Batch, record.At)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock().UTC()
	name := strings.TrimPrefix(key, "revoked.")
	switch {
	case batchErr == nil:
		record.Cursor = record.Batch[len(record.Batch)-1]
		record.Done = len(record.Batch) < revokeBatchLimit
		record.Batch = nil
		record.Failures, record.RetryAt, record.NoticeSent = 0, time.Time{}, false
	case record.Failures < 3:
		record.Failures++
		record.RetryAt = now.Add(5 * time.Second << (record.Failures - 1))
	default:
		if !record.NoticeSent {
			id := "revocation.failed." + name + "." + record.At.Format(time.RFC3339Nano) + "." + record.Batch[0]
			sent, err := s.sendOwnerNotice(ctx, id, "Broker credential revocation pending ("+name+").", now)
			if err != nil {
				return err
			}
			record.NoticeSent = sent
		}
		record.Failures = 0
		record.RetryAt = now.Add(time.Minute)
		s.config.Logger.Warn("broker revocation queued for retry", "tenant", s.config.Tenant, "agent", name)
	}
	if err := writeState(ctx, s.state, key, record, revision); err != nil {
		return err
	}
	if batchErr == nil && !record.Done {
		s.signalRevocation()
	}
	return nil
}

func (s *Service) prepareRevocation(ctx context.Context, key string) (revocation, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, revision, err := readState[revocation](ctx, s.state, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return revocation{}, 0, nil
	}
	if err != nil {
		return revocation{}, 0, err
	}
	now := s.clock().UTC()
	if !record.Blocked || record.Done || now.Before(record.RetryAt) {
		return revocation{}, 0, nil
	}
	if len(record.Batch) != 0 {
		return record, revision, nil
	}
	if record.At.IsZero() {
		record.At = now
	}
	prefix := "credential." + strings.TrimPrefix(key, "revoked.") + "."
	keys, err := stateKeys(ctx, s.state, prefix+"*")
	if err != nil {
		return revocation{}, 0, err
	}
	slices.Sort(keys)
	for _, credential := range keys {
		publicKey := strings.TrimPrefix(credential, prefix)
		if publicKey > record.Cursor {
			record.Batch = append(record.Batch, publicKey)
			if len(record.Batch) == revokeBatchLimit {
				break
			}
		}
	}
	record.Done = len(record.Batch) == 0
	revision, err = s.writeExpiring(ctx, key, record, revision, 0)
	return record, revision, err
}

func (s *Service) revokeBatch(ctx context.Context, keys []string, at time.Time) error {
	s.revocationHelperMu.Lock()
	defer s.revocationHelperMu.Unlock()
	call, stop := context.WithTimeout(ctx, revokeDeadline)
	defer stop()
	return s.config.RevokeUsers(call, keys, at)
}

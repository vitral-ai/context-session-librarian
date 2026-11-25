package control

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const noticeRecipientKey = "policy.notices_to"

func (s *Service) setNoticeRecipient(ctx context.Context, recipient string) (any, error) {
	if !isRegisteredName(recipient) {
		return nil, invalid("notices_to must be a registered agent")
	}
	_, _, err := readState[Agent](ctx, s.state, "agent."+recipient)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, missing("notice recipient is unavailable")
	}
	if err != nil {
		return nil, err
	}
	enabled, err := s.deliveryAgent(ctx, recipient)
	if err != nil {
		return nil, err
	}
	if !enabled {
		return nil, missing("notice recipient is unavailable")
	}
	_, revision, err := readState[string](ctx, s.state, noticeRecipientKey)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, err
	}
	return map[string]string{"notices_to": recipient}, writeState(ctx, s.state, noticeRecipientKey, recipient, revision)
}

func (s *Service) notice(ctx context.Context, reason, caller, target string) {
	if err := s.deliverNotice(ctx, reason, caller, target); err != nil {
		s.config.Logger.Warn("control notice unavailable", "tenant", s.config.Tenant, "reason", reason)
	}
}

func (s *Service) deliverNotice(ctx context.Context, reason, caller, target string) error {
	recipient, _, err := readState[string](ctx, s.state, noticeRecipientKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	enabled, err := s.deliveryAgent(ctx, recipient)
	if err != nil || !enabled {
		return err
	}
	now := s.clock().UTC()
	pair := caller
	if target != "" {
		pair += " to " + target
	}
	slot := now.Truncate(10 * time.Minute).Format(time.RFC3339)
	id := stableID("notice." + recipient + "." + reason + "." + pair + "." + slot)
	return s.recordServiceMessage(ctx, serviceEnvelope(id, recipient, "system", reason+" ("+pair+").", now))
}

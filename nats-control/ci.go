package control

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func (s *Service) handleCI(msg *nats.Msg) {
	recipient := strings.TrimPrefix(msg.Subject, s.subject("ci."))
	if !isRegisteredName(recipient) {
		return
	}
	var notice struct {
		ID   string `json:"id"`
		Body string `json:"body"`
	}
	if !utf8.Valid(msg.Data) || decode(msg.Data, &notice) != nil ||
		!messageID.MatchString(notice.ID) || notice.Body == "" || len(notice.Body) > maxBody {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	agent, _, err := readState[Agent](ctx, s.state, "agent."+recipient)
	if err != nil {
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			s.config.Logger.Error("CI recipient lookup failed", "tenant", s.config.Tenant, "error", err)
		}
		return
	}
	enabled, err := s.deliveryAgent(ctx, recipient)
	if err != nil {
		s.config.Logger.Error("CI eligibility lookup failed", "tenant", s.config.Tenant, "error", err)
		return
	}
	if !enabled {
		return
	}
	envelope := serviceEnvelope(notice.ID, recipient, "ci", notice.Body, s.clock())
	envelope.From = "ci-notices"
	if err := s.queueCI(ctx, envelope, agent); err != nil {
		s.config.Logger.Error("CI notice routing failed", "tenant", s.config.Tenant, "error", err)
	}
}

func (s *Service) queueCI(ctx context.Context, envelope Envelope, agent Agent) error {
	interval, err := deliveryInterval(agent.Delivery)
	if err != nil {
		return err
	}
	if interval == 0 {
		return s.recordServiceMessage(ctx, envelope)
	}
	key := "message." + envelope.ID
	record, _, err := readState[messageRecord](ctx, s.state, key)
	if err == nil {
		return s.ensureMessageState(ctx, record)
	}
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	now := s.acceptanceTime()
	record = messageRecord{Envelope: envelope, AcceptedAt: now, ReadyAt: now.Truncate(interval).Add(interval)}
	if _, err := s.writeExpiring(ctx, key, record, 0, messageAge); err != nil {
		return err
	}
	return s.ensureMessageState(ctx, record)
}

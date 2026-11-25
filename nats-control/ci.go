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
	if _, _, err := readState[Agent](ctx, s.state, "agent."+recipient); err != nil {
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			s.config.Logger.Error("CI recipient lookup failed", "tenant", s.config.Tenant, "error", err)
		}
		return
	}
	envelope := serviceEnvelope(notice.ID, recipient, "ci", notice.Body, s.clock())
	envelope.From = "ci-notices"
	if err := s.recordServiceMessage(ctx, envelope); err != nil {
		s.config.Logger.Error("CI notice routing failed", "tenant", s.config.Tenant, "error", err)
	}
}

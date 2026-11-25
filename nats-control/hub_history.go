package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type hubHistoryRow struct {
	ID          string    `json:"id"`
	From        string    `json:"from"`
	To          string    `json:"to"`
	FromMachine string    `json:"from_machine"`
	ToMachine   string    `json:"to_machine"`
	CreatedAt   time.Time `json:"created_at"`
	State       string    `json:"state"`
	Body        string    `json:"body"`
	ReplyTo     *string   `json:"reply_to"`
}

func (row *hubHistoryRow) valid() bool {
	return row != nil && messageID.MatchString(row.ID) &&
		isRegisteredName(row.From) && isRegisteredName(row.To) && row.From != row.To &&
		!row.CreatedAt.IsZero() && len(row.Body) <= maxBody &&
		(row.State == "delivered" || row.State == "refused" || row.State == "dropped") &&
		(row.ReplyTo == nil || (*row.ReplyTo != row.ID && messageID.MatchString(*row.ReplyTo)))
}

func (s *Service) importHubHistory(ctx context.Context, row *hubHistoryRow) (any, error) {
	if !row.valid() || row.CreatedAt.After(s.clock().Add(time.Minute)) {
		return nil, invalid("invalid settled Hub history row")
	}
	for name, machine := range map[string]string{row.From: row.FromMachine, row.To: row.ToMachine} {
		agent, _, err := readState[Agent](ctx, s.state, "agent."+name)
		if errors.Is(err, jetstream.ErrKeyNotFound) || (err == nil && agent.Machine != machine) {
			return nil, forbidden("Hub history endpoint does not match its registration")
		}
		if err != nil {
			return nil, err
		}
		if err := s.activeAgent(ctx, name); err != nil {
			return nil, err
		}
	}

	kind := "message"
	if row.ReplyTo != nil {
		kind = "reply"
	}
	envelope := Envelope{
		V: 1, ID: row.ID, From: row.From, To: row.To, Kind: kind, Urgency: "normal",
		CreatedAt: row.CreatedAt.UTC(), Body: row.Body, ReplyTo: row.ReplyTo,
		Attachments: []attachment{}, Items: []Envelope{},
	}
	key := "message." + row.ID
	existing, _, err := readState[messageRecord](ctx, s.state, key)
	if err == nil {
		prior := existing.Envelope
		if prior.From != row.From || prior.To != row.To || prior.Body != row.Body {
			return nil, invalid("message ID was already used for different content")
		}
		if existing.HubHistoryState != "" &&
			(existing.HubHistoryState != row.State || !prior.CreatedAt.Equal(row.CreatedAt) ||
				!sameHistoryParent(prior.ReplyTo, row.ReplyTo)) {
			return nil, invalid("message ID was already used for different Hub history")
		}
		return sendResult{row.ID, "existing"}, nil
	}
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, err
	}

	// Registered names cannot contain an underscore. No agent durable can
	// receive this subject. Its per-ID address also makes retry safe after a
	// stream publish succeeds and the history index write does not.
	subject := s.subject("agent._history-" + row.ID + ".inbox")
	raw, err := json.Marshal(historyEntry{envelope, row.State})
	if err != nil {
		return nil, err
	}
	var sequence uint64
	archived, err := s.stream.GetLastMsgForSubject(ctx, subject)
	switch {
	case err == nil:
		if !bytes.Equal(archived.Data, raw) {
			return nil, invalid("message ID was already used for different Hub history")
		}
		sequence = archived.Sequence
	case errors.Is(err, jetstream.ErrMsgNotFound):
		ack, err := s.js.Publish(ctx, subject, raw, jetstream.WithExpectStream("CSL_MSG"),
			jetstream.WithExpectLastSequencePerSubject(0))
		if err != nil {
			return nil, err
		}
		sequence = ack.Sequence
	default:
		return nil, err
	}

	record := messageRecord{
		Envelope: envelope, HubHistoryState: row.State, AcceptedAt: envelope.CreatedAt, Sequence: sequence,
	}
	if _, err := s.writeExpiring(ctx, key, record, 0, messageAge); err != nil {
		return nil, err
	}
	return sendResult{row.ID, "imported"}, nil
}

func sameHistoryParent(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

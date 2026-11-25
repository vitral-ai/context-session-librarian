package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
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

const hubArchiveName = "CSL_ARCHIVE"

type archiveEntry struct {
	historyEntry
	FromMachine string `json:"from_machine"`
	ToMachine   string `json:"to_machine"`
}

type hubHistoryPlan struct {
	ID           string `json:"id"`
	PayloadBytes int64  `json:"payload_bytes"`
	ReserveBytes int64  `json:"reserve_bytes"`
	Available    int64  `json:"available_bytes"`
}

func (s *Service) archiveStream(ctx context.Context) (jetstream.Stream, error) {
	return s.js.Stream(ctx, hubArchiveName)
}

func (s *Service) archiveMessage(ctx context.Context, archive jetstream.Stream, id string) (*jetstream.RawStreamMsg, error) {
	return archive.GetMsg(ctx, 1, jetstream.WithGetMsgSubject(s.subject("archive."+id+".*.*")))
}

func (s *Service) checkArchiveID(ctx context.Context, id string) error {
	archive, err := s.archiveStream(ctx)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = s.archiveMessage(ctx, archive, id)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil
	}
	if err == nil {
		return invalid("message ID was already used")
	}
	return err
}

func (s *Service) importHubHistory(ctx context.Context, row *hubHistoryRow, preview bool) (any, error) {
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
	archive, err := s.archiveStream(ctx)
	if err != nil {
		return nil, err
	}
	info, err := archive.Info(ctx)
	if err != nil {
		return nil, err
	}
	config := info.Config
	if !slices.Equal(config.Subjects, []string{s.subject("archive.>")}) ||
		config.Discard != jetstream.DiscardNew || config.Storage != jetstream.FileStorage ||
		config.MaxAge != 0 || config.MaxBytes <= 0 || config.Mirror != nil {
		return nil, invalid("archive must be a writable isolated discard-new file stream")
	}
	available := max(0, config.MaxBytes-int64(info.State.Bytes))
	plan := hubHistoryPlan{ID: row.ID, Available: available}

	existing, _, err := readState[messageRecord](ctx, s.state, "message."+row.ID)
	if err == nil {
		prior := existing.Envelope
		if prior.From != row.From || prior.To != row.To || prior.Body != row.Body ||
			!sameHistoryParent(prior.ReplyTo, row.ReplyTo) {
			return nil, invalid("message ID was already used for different content")
		}
		if preview {
			return plan, nil
		}
		return sendResult{row.ID, "existing"}, nil
	}
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, err
	}

	names := []string{row.From, row.To}
	slices.Sort(names)
	subject := s.subject("archive." + row.ID + "." + names[0] + "." + names[1])
	raw, err := json.Marshal(archiveEntry{historyEntry{envelope, row.State}, row.FromMachine, row.ToMachine})
	if err != nil {
		return nil, err
	}
	archived, err := s.archiveMessage(ctx, archive, row.ID)
	if err == nil {
		if archived.Subject != subject || !bytes.Equal(archived.Data, raw) {
			return nil, invalid("message ID was already used for different Hub history")
		}
		if preview {
			return plan, nil
		}
		return sendResult{row.ID, "existing"}, nil
	}
	if !errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, err
	}

	plan.PayloadBytes = int64(len(raw))
	// Reserve space for the subject and stream record overhead. The broker
	// also enforces its exact byte limit atomically with every publish.
	plan.ReserveBytes = plan.PayloadBytes + int64(len(subject)) + 1024
	if preview {
		return plan, nil
	}
	if plan.ReserveBytes > available {
		return nil, invalid("archive has insufficient space for this history row")
	}
	if _, err := s.js.Publish(ctx, subject, raw, jetstream.WithExpectStream(hubArchiveName),
		jetstream.WithExpectLastSequencePerSubject(0)); err != nil {
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

package control

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	maxBody       = 48 * 1024
	maxDigest     = 512 * 1024
	messageAge    = 30 * 24 * time.Hour
	defaultRetain = 200
)

var messageID = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)
var fileID = regexp.MustCompile(`^[a-f0-9]{64}$`)

type attachment struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type Envelope struct {
	V             int          `json:"v"`
	ID            string       `json:"id"`
	From          string       `json:"from"`
	To            string       `json:"to"`
	Kind          string       `json:"kind"`
	Urgency       string       `json:"urgency"`
	Reason        string       `json:"reason,omitempty"`
	ReplyTo       *string      `json:"reply_to"`
	SessionID     string       `json:"session_id,omitempty"`
	CreatedAt     time.Time    `json:"created_at"`
	Body          string       `json:"body"`
	Attachments   []attachment `json:"attachments"`
	ClientVersion string       `json:"client_version"`
	Items         []Envelope   `json:"items"`
	Part          string       `json:"part,omitempty"`
}

type messageRecord struct {
	Envelope    Envelope  `json:"envelope"`
	Fingerprint string    `json:"fingerprint,omitempty"`
	AcceptedAt  time.Time `json:"accepted_at"`
	ReadyAt     time.Time `json:"ready_at"`
	Sequence    uint64    `json:"sequence,omitempty"`
	PublishFrom uint64    `json:"publish_from,omitempty"`
}

type messageState struct {
	State string    `json:"state"`
	At    time.Time `json:"at"`
}

type sendResult struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

func randomID() string {
	var id [16]byte
	rand.Read(id[:])
	id[6] = id[6]&15 | 64
	id[8] = id[8]&63 | 128
	return formatID(id[:])
}

func formatID(id []byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:16])
}

func stableID(seed string) string {
	id := sha256.Sum256([]byte(seed))
	id[6] = id[6]&15 | 80
	id[8] = id[8]&63 | 128
	return formatID(id[:16])
}

func (s *Service) handleSend(msg *nats.Msg) {
	caller := strings.TrimPrefix(msg.Subject, s.subject("send."))
	if !isRegisteredName(caller) || !strings.HasPrefix(msg.Reply, agentInboxPrefix(s.config.Tenant, caller)+".") {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.mu.Lock()
	response, err := s.send(ctx, caller, msg.Data)
	s.mu.Unlock()
	s.respond(msg, "send", response, err)
}

func (s *Service) send(ctx context.Context, caller string, body []byte) (sendResult, error) {
	var envelope Envelope
	if !utf8.Valid(body) {
		return sendResult{}, invalid("message must be UTF-8")
	}
	if err := decode(body, &envelope); err != nil {
		return sendResult{}, err
	}
	if err := s.activeAgent(ctx, caller); err != nil {
		return sendResult{}, err
	}
	if envelope.From != "" && envelope.From != caller {
		return sendResult{}, forbidden("sender does not match the authenticated subject")
	}
	if err := validateSend(caller, envelope); err != nil {
		return sendResult{}, err
	}
	envelope.From = caller
	envelope.SessionID = ""
	envelope.CreatedAt = envelope.CreatedAt.UTC()
	if envelope.Attachments == nil {
		envelope.Attachments = []attachment{}
	}
	envelope.Items = []Envelope{}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return sendResult{}, err
	}
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(raw))
	warn, err := s.checkVersion(ctx, caller, envelope.To, envelope.ClientVersion)
	if err != nil {
		return sendResult{}, err
	}
	agent, rev, err := readState[Agent](ctx, s.state, "agent."+caller)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return sendResult{}, missing("unknown sender")
	}
	if err != nil {
		return sendResult{}, err
	}
	if err := s.activeAgent(ctx, envelope.To); err != nil {
		return sendResult{}, err
	}
	recipient, _, err := readState[Agent](ctx, s.state, "agent."+envelope.To)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		s.notice(ctx, "Unknown target", caller, envelope.To)
		return sendResult{}, missing("unknown recipient")
	}
	if err != nil {
		return sendResult{}, err
	}
	allowed, _, err := readState[bool](ctx, s.state, "pair."+caller+"."+envelope.To)
	if errors.Is(err, jetstream.ErrKeyNotFound) || (err == nil && !allowed) {
		s.notice(ctx, "Unapproved pair", caller, envelope.To)
		return sendResult{}, forbidden(fmt.Sprintf(
			"pair permission is required for %s -> %s. Ask the owner via nats-control to approve this direction.",
			caller, envelope.To,
		))
	}
	if err != nil {
		return sendResult{}, err
	}
	if envelope.ReplyTo != nil {
		parent, _, err := readState[messageRecord](ctx, s.state, "message."+*envelope.ReplyTo)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			matches, err := s.historyRecords(ctx, caller, envelope.To, *envelope.ReplyTo)
			if err != nil {
				return sendResult{}, err
			}
			if len(matches) == 1 && matches[0].State == "delivered" {
				parent.Envelope = matches[0].Envelope
			}
		} else if err != nil {
			return sendResult{}, err
		}
		if parent.Envelope.To != caller || parent.Envelope.From != envelope.To {
			return sendResult{}, missing("reply target does not belong to this pair")
		}
	}
	if err := s.authorizeAttachments(ctx, envelope); err != nil {
		return sendResult{}, err
	}
	key := "message." + envelope.ID
	record, revision, err := readState[messageRecord](ctx, s.state, key)
	if err == nil {
		if record.Fingerprint != fingerprint {
			return sendResult{}, invalid("message ID was already used")
		}
	} else if errors.Is(err, jetstream.ErrKeyNotFound) {
		if err := s.checkArchiveID(ctx, envelope.ID); err != nil {
			return sendResult{}, err
		}
		now := s.acceptanceTime()
		interval, err := deliveryInterval(recipient.Delivery)
		if err != nil {
			return sendResult{}, err
		}
		pair, _ := pairKey(caller, envelope.To)
		session, _, err := readState[PairSession](ctx, s.state, pair)
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return sendResult{}, err
		}
		session = session.mention(caller, now)
		envelope.SessionID = stableID(pair + "." + session.OpenedAt.Format(time.RFC3339Nano))
		if _, err := digestItemSize(envelope); err != nil {
			return sendResult{}, err
		}
		if envelope.Urgency == "urgent" {
			if err := s.admitUrgent(ctx, &envelope, now); err != nil {
				return sendResult{}, err
			}
		}
		if err := s.admitRate(ctx, now); err != nil {
			return sendResult{}, err
		}
		record = messageRecord{Envelope: envelope, Fingerprint: fingerprint, AcceptedAt: now, ReadyAt: now}
		if envelope.Urgency == "normal" && interval != 0 {
			record.ReadyAt = now.Truncate(interval).Add(interval)
		}
		revision, err = s.writeExpiring(ctx, key, record, 0, messageAge)
		if err != nil {
			return sendResult{}, err
		}
	} else {
		return sendResult{}, err
	}
	if err := s.ensureMessageState(ctx, record); err != nil {
		return sendResult{}, err
	}
	if record.Sequence == 0 {
		if err := s.mentionPair(ctx, record); err != nil {
			return sendResult{}, err
		}
		if record.ReadyAt.Equal(record.AcceptedAt) {
			if err := s.publishRecord(ctx, record, revision); err != nil {
				return sendResult{}, err
			}
		}
	}
	now := s.clock().UTC()
	agent.ClientVersion, agent.LastSeen = envelope.ClientVersion, now
	if err := writeState(ctx, s.state, "agent."+caller, agent, rev); err != nil {
		return sendResult{}, err
	}
	if warn {
		if err := s.warnClient(ctx, caller, now); err != nil {
			return sendResult{}, err
		}
	}
	delivery, _, err := readState[messageState](ctx, s.state, "msg."+record.Envelope.ID)
	return sendResult{record.Envelope.ID, delivery.State}, err
}

func validateSend(caller string, envelope Envelope) error {
	if envelope.V != 1 || !messageID.MatchString(envelope.ID) || !isRegisteredName(envelope.To) ||
		caller == envelope.To || envelope.CreatedAt.IsZero() || len(envelope.Body) > maxBody ||
		len(envelope.Items) != 0 || envelope.Part != "" {
		return invalid("invalid message envelope")
	}
	switch envelope.Kind {
	case "message":
		if envelope.ReplyTo != nil {
			return invalid("message cannot have a reply target")
		}
	case "reply":
		if envelope.ReplyTo == nil || !messageID.MatchString(*envelope.ReplyTo) {
			return invalid("reply requires a message ID")
		}
	default:
		return invalid("agents may send messages and replies")
	}
	switch envelope.Urgency {
	case "normal":
		if envelope.Reason != "" {
			return invalid("normal messages do not carry an urgent reason")
		}
	case "urgent":
		switch envelope.Reason {
		case "blocked", "needs_approval", "security", "operation_failed":
		default:
			return invalid("urgent messages require an approved reason")
		}
	default:
		return invalid("urgency must be normal or urgent")
	}
	for _, attachment := range envelope.Attachments {
		if !fileID.MatchString(attachment.ID) || attachment.Name == "" || len(attachment.Name) > 255 || attachment.Size < 0 {
			return invalid("invalid attachment metadata")
		}
	}
	return nil
}

func (s *Service) admitRate(ctx context.Context, now time.Time) error {
	policy, err := s.deliveryPolicy(ctx)
	if err != nil {
		return err
	}
	key := "rate." + now.Truncate(time.Minute).Format("200601021504")
	count, revision, err := readState[int](ctx, s.state, key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	if count >= policy.RatePerMinute {
		return forbidden("tenant message rate limit reached")
	}
	_, err = s.writeExpiring(ctx, key, count+1, revision, time.Minute)
	return err
}

type urgentUse struct {
	ID string    `json:"id"`
	At time.Time `json:"at"`
}

func (s *Service) admitUrgent(ctx context.Context, envelope *Envelope, now time.Time) error {
	key := "urgent." + envelope.From
	uses, revision, err := readState[[]urgentUse](ctx, s.state, key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	uses = slices.DeleteFunc(uses, func(use urgentUse) bool { return !use.At.After(now.Add(-time.Hour)) })
	for _, use := range uses {
		if use.ID == envelope.ID {
			return nil
		}
	}
	policy, err := s.deliveryPolicy(ctx)
	if err != nil {
		return err
	}
	if len(uses) >= policy.UrgentCap {
		envelope.Urgency, envelope.Reason = "normal", ""
		s.config.Logger.Warn("urgent message downgraded", "tenant", s.config.Tenant,
			"agent", envelope.From, "message_id", envelope.ID)
		s.notice(ctx, "Urgent cap reached", envelope.From, envelope.To)
		return nil
	}
	_, err = s.writeExpiring(ctx, key, append(uses, urgentUse{envelope.ID, now}), revision, time.Hour)
	return err
}

func (s *Service) writeExpiring(ctx context.Context, key string, value any, revision uint64, ttl time.Duration) (uint64, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	if revision == 0 {
		version, err := s.state.Create(ctx, key, body, jetstream.KeyTTL(ttl))
		if err != nil {
			return 0, fmt.Errorf("create %s: %w", key, err)
		}
		return version, nil
	}
	ack, err := s.js.Publish(ctx, "$KV.CSL_STATE."+key, body,
		jetstream.WithExpectStream("KV_CSL_STATE"), jetstream.WithExpectLastSequencePerSubject(revision),
		jetstream.WithMsgTTL(ttl))
	if err != nil {
		return 0, fmt.Errorf("update %s at %d: %w", key, revision, err)
	}
	return ack.Sequence, nil
}

func (s *Service) ensureMessageState(ctx context.Context, record messageRecord) error {
	key := "msg." + record.Envelope.ID
	_, _, err := readState[messageState](ctx, s.state, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		_, err = s.writeExpiring(ctx, key, messageState{"queued", record.AcceptedAt}, 0, messageAge)
	}
	return err
}

func (s *Service) mentionPair(ctx context.Context, record messageRecord) error {
	env := record.Envelope
	key, err := pairKey(env.From, env.To)
	if err != nil {
		return err
	}
	session, revision, err := readState[PairSession](ctx, s.state, key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	if updated := session.mention(env.From, record.AcceptedAt); updated != session {
		if err := writeState(ctx, s.state, key, updated, revision); err != nil {
			return err
		}
	}
	key = "lastsent." + env.From + "." + env.To
	lastSent, revision, err := readState[time.Time](ctx, s.state, key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	if record.AcceptedAt.After(lastSent) {
		_, err = s.writeExpiring(ctx, key, record.AcceptedAt, revision, pairIdleLimit)
	} else {
		err = nil
	}
	return err
}

func (s *Service) publish(ctx context.Context, envelope Envelope) (uint64, error) {
	body, err := json.Marshal(envelope)
	if err != nil {
		return 0, err
	}
	ack, err := s.js.Publish(ctx, s.subject("agent."+envelope.To+".inbox"), body, jetstream.WithMsgID(envelope.ID))
	if err != nil {
		return 0, err
	}
	return ack.Sequence, nil
}

func (s *Service) publishRecord(ctx context.Context, record messageRecord, revision uint64) error {
	if record.Envelope.Kind == "digest" && record.PublishFrom == 0 {
		info, err := s.stream.Info(ctx)
		if err != nil {
			return err
		}
		record.PublishFrom = info.State.LastSeq + 1
		revision, err = s.writeExpiring(ctx, "message."+record.Envelope.ID, record, revision, messageAge)
		if err != nil {
			return err
		}
	}
	sequence, err := s.publish(ctx, record.Envelope)
	if err != nil {
		return err
	}
	return s.finishPublication(ctx, record, revision, sequence)
}

func (s *Service) finishPublication(ctx context.Context, record messageRecord, revision, sequence uint64) error {
	if err := s.markNewWork(ctx, record.Envelope); err != nil {
		return err
	}
	record.Sequence = sequence
	if _, err := s.writeExpiring(ctx, "message."+record.Envelope.ID, record, revision, messageAge); err != nil {
		return err
	}
	return s.wake(record.Envelope.To, 1)
}

func (s *Service) wake(name string, count int) error {
	body, err := json.Marshal(struct {
		Count int `json:"count"`
	}{count})
	if err != nil {
		return err
	}
	return s.conn.Publish(s.subject("agent."+name+".wake"), body)
}

func (s *Service) warnClient(ctx context.Context, caller string, now time.Time) error {
	key := "warning." + caller
	_, _, err := readState[time.Time](ctx, s.state, key)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	envelope := serviceEnvelope(stableID(key+"."+now.Format("2006-01-02")), caller, "system",
		"Your CSL client is below the recommended version. Upgrade with: "+s.config.Upgrade, now)
	if err := s.recordServiceMessage(ctx, envelope); err != nil {
		return err
	}
	if _, err := s.writeExpiring(ctx, key, now, 0, 24*time.Hour); err != nil {
		return err
	}
	return nil
}

func serviceEnvelope(id, to, kind, body string, now time.Time) Envelope {
	return Envelope{V: 1, ID: id, From: "csl-control", To: to, Kind: kind, Urgency: "normal",
		CreatedAt: now.UTC(), Body: body, Attachments: []attachment{}, Items: []Envelope{}}
}

package control

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
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

func (s *Service) pendingUrgent(ctx context.Context) (map[string]int, error) {
	entries, err := s.messages(ctx)
	if err != nil {
		return nil, err
	}
	pending := make(map[string]int)
	for _, entry := range entries {
		envelope := entry.record.Envelope
		if envelope.Urgency != "urgent" {
			continue
		}
		state, _, err := readState[messageState](ctx, s.state, "msg."+envelope.ID)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if state.State == "queued" || state.State == "delivered" {
			pending[envelope.To]++
		}
	}
	return pending, nil
}

func validMessagePrefix(id string) bool {
	const format = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
	if len(id) < 8 || len(id) > len(format) {
		return false
	}
	for i, ch := range []byte(id) {
		if format[i] == '-' {
			if ch != '-' {
				return false
			}
		} else if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}

type historyEntry struct {
	Envelope
	State string `json:"state"`
}

type historyRecord struct {
	historyEntry
	archived   bool
	sequence   uint64
	acceptedAt time.Time
}

func compareHistoryRecords(a, b historyRecord) int {
	if order := a.CreatedAt.Compare(b.CreatedAt); order != 0 {
		return order
	}
	if a.archived != b.archived {
		if a.archived {
			return -1
		}
		return 1
	}
	if a.archived {
		if order := cmp.Compare(a.sequence, b.sequence); order != 0 {
			return order
		}
	} else {
		// Live publication sequence can change when a queued message is flushed.
		if order := a.acceptedAt.Compare(b.acceptedAt); order != 0 {
			return order
		}
	}
	return strings.Compare(a.ID, b.ID)
}

const maxUnreadIDs = 1000

type unreadResult struct {
	IDs   []string `json:"ids"`
	Count int      `json:"count"`
}

func (s *Service) unread(ctx context.Context, caller string, body []byte) (any, error) {
	var request struct {
		With string `json:"with"`
	}
	if err := decode(body, &request); err != nil {
		return nil, err
	}
	if !isRegisteredName(request.With) || request.With == caller {
		return nil, invalid("unread requires a distinct peer")
	}
	entries, err := s.messages(ctx)
	if err != nil {
		return nil, err
	}
	var unread []historyRecord
	for _, entry := range entries {
		envelope := entry.record.Envelope
		if envelope.From != request.With || envelope.To != caller {
			continue
		}
		state, _, err := readState[messageState](ctx, s.state, "msg."+envelope.ID)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if state.State == "queued" || state.State == "delivered" {
			unread = append(unread, historyRecord{historyEntry: historyEntry{Envelope: envelope}, acceptedAt: entry.record.AcceptedAt})
		}
	}
	slices.SortFunc(unread, compareHistoryRecords)
	ids := make([]string, min(maxUnreadIDs, len(unread)))
	for i := range ids {
		ids[i] = unread[i].ID
	}
	return unreadResult{IDs: ids, Count: len(unread)}, nil
}

func (s *Service) history(ctx context.Context, caller string, body []byte) (any, error) {
	var request struct {
		With   string     `json:"with,omitempty"`
		Last   int        `json:"last,omitempty"`
		ID     string     `json:"id,omitempty"`
		Before string     `json:"before,omitempty"`
		Since  *time.Time `json:"since,omitempty"`
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
	if request.Last < 1 || request.Last > retention {
		return nil, invalid("invalid pair or history limit")
	}
	if request.ID != "" {
		if request.With != "" || request.Before != "" || request.Since != nil ||
			(!messageID.MatchString(request.ID) && !validMessagePrefix(request.ID)) {
			return nil, invalid("provide one message ID or pair name")
		}
	} else if _, err := pairKey(caller, request.With); err != nil {
		return nil, invalid("invalid pair or history limit")
	}
	matches, err := s.historyRecords(ctx, caller, request.With, request.ID)
	if err != nil {
		return nil, err
	}
	if request.ID != "" {
		switch len(matches) {
		case 0:
			return nil, missing("message unavailable")
		case 1:
			match := matches[0]
			request.ID = match.ID
			request.With = match.From
			if request.With == caller {
				request.With = match.To
			}
		default:
			return nil, invalid("ambiguous message prefix")
		}
		matches, err = s.historyRecords(ctx, caller, request.With, "")
		if err != nil {
			return nil, err
		}
	}
	slices.SortFunc(matches, compareHistoryRecords)
	matches = matches[max(0, len(matches)-retention):]
	if request.Before != "" {
		index := slices.IndexFunc(matches, func(entry historyRecord) bool {
			return entry.ID == request.Before
		})
		if index < 0 {
			return nil, missing("history cursor unavailable")
		}
		matches = matches[:index]
	}
	if request.Since != nil {
		index := sort.Search(len(matches), func(i int) bool {
			return matches[i].CreatedAt.After(*request.Since)
		})
		matches = matches[index:]
	}
	requested := min(request.Last, len(matches))
	result := make([]historyEntry, 0, requested)
	encoded := 0
	for i := len(matches) - 1; i >= 0 && len(result) < requested; i-- {
		item := matches[i].historyEntry
		if request.ID != "" && item.ID != request.ID {
			continue
		}
		if item.State == "" {
			current, _, err := readState[messageState](ctx, s.state, "msg."+item.ID)
			if err != nil {
				return nil, err
			}
			item.State = current.State
		}
		raw, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		if len(result) > 0 && encoded+len(raw)+1 >= maxDigest-1024 {
			break
		}
		encoded += len(raw) + 1
		result = append(result, item)
	}
	if request.ID != "" && len(result) == 0 {
		return nil, missing("message unavailable")
	}
	slices.Reverse(result)
	var next string
	if request.ID == "" && len(result) < len(matches) && len(result) != 0 {
		next = result[0].ID
	}
	return struct {
		Messages   []historyEntry `json:"messages"`
		Retention  int            `json:"retention"`
		NextBefore string         `json:"next_before,omitempty"`
	}{result, retention, next}, nil
}

type latestEntry struct {
	With    string    `json:"with"`
	ID      string    `json:"id"`
	At      time.Time `json:"at"`
	From    string    `json:"from"`
	Preview string    `json:"preview"`
}

func (s *Service) latest(ctx context.Context, caller string, body []byte) (any, error) {
	if err := decode(body, &struct{}{}); err != nil {
		return nil, err
	}
	records, err := s.historyRecords(ctx, caller, "", "")
	if err != nil {
		return nil, err
	}
	slices.SortFunc(records, func(a, b historyRecord) int { return compareHistoryRecords(b, a) })
	seen := make(map[string]bool)
	latest := make([]latestEntry, 0)
	for _, record := range records {
		peer := record.From
		if peer == caller {
			peer = record.To
		}
		if seen[peer] {
			continue
		}
		seen[peer] = true
		preview := record.Body
		characters := 0
		for offset := range preview {
			if characters == 120 {
				preview = preview[:offset]
				break
			}
			characters++
		}
		latest = append(latest, latestEntry{With: peer, ID: record.ID, At: record.CreatedAt,
			From: record.From, Preview: preview})
	}
	return latest, nil
}

func (s *Service) historyRecords(ctx context.Context, caller, peer, id string) ([]historyRecord, error) {
	var live []messageEntry
	if messageID.MatchString(id) {
		record, revision, err := readState[messageRecord](ctx, s.state, "message."+id)
		switch {
		case err == nil:
			live = []messageEntry{{record, revision}}
		case !errors.Is(err, jetstream.ErrKeyNotFound):
			return nil, err
		}
	} else {
		var err error
		live, err = s.messages(ctx)
		if err != nil {
			return nil, err
		}
	}
	matches := make([]historyRecord, 0)
	liveIDs := make(map[string]bool, len(live))
	for _, item := range live {
		record := item.record
		liveIDs[record.Envelope.ID] = true
		if historyMatches(record.Envelope, caller, peer, id) {
			matches = append(matches, historyRecord{
				historyEntry: historyEntry{Envelope: record.Envelope},
				acceptedAt:   record.AcceptedAt,
			})
		}
	}
	if messageID.MatchString(id) && len(matches) != 0 {
		return matches, nil
	}
	archive, err := s.archiveStream(ctx)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return matches, nil
	}
	if err != nil {
		return nil, err
	}
	machines := make(map[string]string)
	machine := func(name string) (string, error) {
		if value, ok := machines[name]; ok {
			return value, nil
		}
		agent, _, err := readState[Agent](ctx, s.state, "agent."+name)
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return "", err
		}
		machines[name] = agent.Machine
		return agent.Machine, nil
	}
	add := func(raw *jetstream.RawStreamMsg) error {
		var entry archiveEntry
		if err := json.Unmarshal(raw.Data, &entry); err != nil {
			return err
		}
		if !messageID.MatchString(entry.ID) || !isRegisteredName(entry.From) || !isRegisteredName(entry.To) ||
			entry.From == entry.To || entry.CreatedAt.IsZero() ||
			(entry.State != "delivered" && entry.State != "refused" && entry.State != "dropped") ||
			raw.Subject != s.subject("archive."+entry.ID+"."+min(entry.From, entry.To)+"."+max(entry.From, entry.To)) {
			return errors.New("invalid archived history record")
		}
		if liveIDs[entry.ID] || !historyMatches(entry.Envelope, caller, peer, id) {
			return nil
		}
		from, err := machine(entry.From)
		if err != nil {
			return err
		}
		to, err := machine(entry.To)
		if err != nil {
			return err
		}
		if from != "" && to != "" && from == entry.FromMachine && to == entry.ToMachine {
			matches = append(matches, historyRecord{historyEntry: entry.historyEntry, archived: true, sequence: raw.Sequence})
		}
		return nil
	}
	if messageID.MatchString(id) {
		raw, err := s.archiveMessage(ctx, archive, id)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return matches, nil
		}
		if err != nil {
			return nil, err
		}
		if err := add(raw); err != nil {
			return nil, err
		}
	} else {
		root := s.subject("archive.*.")
		filters := []string{root + caller + ".*", root + "*." + caller}
		if peer != "" {
			filters = []string{root + min(caller, peer) + "." + max(caller, peer)}
		}
		for _, filter := range filters {
			for next := uint64(1); ; {
				raw, err := archive.GetMsg(ctx, next, jetstream.WithGetMsgSubject(filter))
				if errors.Is(err, jetstream.ErrMsgNotFound) {
					break
				}
				if err != nil {
					return nil, err
				}
				if raw.Sequence < next {
					return nil, errors.New("archive stream returned a non-advancing sequence")
				}
				if err := add(raw); err != nil {
					return nil, err
				}
				next = raw.Sequence + 1
			}
		}
	}
	return matches, nil
}

func historyMatches(env Envelope, caller, peer, id string) bool {
	if id != "" {
		return (env.From == caller || env.To == caller) && strings.HasPrefix(env.ID, id) &&
			(env.Kind == "message" || env.Kind == "reply")
	}
	if peer == "" {
		return (env.From == caller && isRegisteredName(env.To)) ||
			(env.To == caller && isRegisteredName(env.From))
	}
	return (env.From == caller && env.To == peer) || (env.To == caller && env.From == peer)
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
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		matches, err := s.historyRecords(ctx, caller, "", request.ID)
		if err != nil {
			return nil, err
		}
		if len(matches) == 1 && matches[0].To == caller {
			return nil, forbidden("message has not reached the inbox")
		}
		return nil, missing("message unavailable")
	}
	if err != nil {
		return nil, err
	}
	if entry.Envelope.To != caller {
		return nil, missing("message unavailable")
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
	Retention     int   `json:"retention"`
	UrgentCap     int   `json:"urgent_cap"`
	FileBytes     int64 `json:"file_bytes,omitempty"`
	RatePerMinute int   `json:"rate_per_minute,omitempty"`
}

func (s *Service) deliveryPolicy(ctx context.Context) (tenantPolicy, error) {
	policy, _, err := readState[tenantPolicy](ctx, s.state, "policy.delivery")
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		if s.config.Tenant != "vi" {
			return tenantPolicy{}, forbidden("tenant must be provisioned before use")
		}
		return tenantPolicy{Retention: defaultRetain, UrgentCap: 10, FileBytes: maxFile, RatePerMinute: 600}, nil
	}
	if policy.FileBytes == 0 {
		policy.FileBytes = maxFile
	}
	if policy.RatePerMinute == 0 {
		policy.RatePerMinute = 600
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
		if item.Kind != "message" && item.Kind != "reply" {
			continue
		}
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

// Reserve room for the outer envelope, its part marker, and item separators.
const digestBudget = maxDigest - 1024

func digestItemSize(item Envelope) (int, error) {
	raw, err := json.Marshal(item)
	if err != nil {
		return 0, err
	}
	size := len(raw) + 1
	if size > digestBudget {
		return 0, invalid("message does not fit in a digest part")
	}
	return size, nil
}

func digestParts(items []Envelope) ([][]Envelope, error) {
	var parts [][]Envelope
	start, size := 0, 0
	for i, item := range items {
		n, err := digestItemSize(item)
		if err != nil {
			return nil, err
		}
		if size+n > digestBudget {
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
	return errors.Join(s.monitorMirrors(ctx), s.deliveryTick(ctx), s.ApplyRevocations(ctx))
}

func (s *Service) deliveryTick(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := s.messages(ctx)
	if err != nil {
		return err
	}
	now := s.clock().UTC()
	heldErr := s.advanceHeldPairs(ctx, entries, now, "", "")
	// Digest records list the original message IDs. Recover their membership
	// before regrouping originals after a crash between publish and bookkeeping.
	batched := map[string]uint64{}
	deferred := map[string]bool{}
	for _, entry := range entries {
		env := entry.record.Envelope
		if env.Kind != "digest" {
			continue
		}
		if entry.record.Sequence == 0 {
			sequence, err := s.recoverPublication(ctx, entry)
			if err != nil {
				return err
			}
			entry.record.Sequence = sequence
		}
		if entry.record.Sequence == 0 {
			state, _, err := readState[messageState](ctx, s.state, "msg."+env.ID)
			if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
				return err
			}
			if state.State == "dropped" {
				continue
			}
			allowed, pending, err := s.canDeliver(ctx, env)
			if err != nil {
				return err
			}
			if pending {
				for _, item := range env.Items {
					deferred[item.ID] = true
				}
				continue
			}
			if !allowed {
				if err := s.ensureMessageState(ctx, entry.record); err != nil {
					return err
				}
				if err := s.markDropped(ctx, env.ID, now); err != nil {
					return err
				}
				continue
			}
			if err := s.recordServiceMessage(ctx, env); err != nil {
				return err
			}
			record, _, err := readState[messageRecord](ctx, s.state, "message."+env.ID)
			if err != nil {
				return err
			}
			entry.record.Sequence = record.Sequence
		}
		for _, item := range env.Items {
			batched[item.ID] = entry.record.Sequence
		}
	}
	groups := map[string][]messageEntry{}
	var dueRecipients []string
	for _, entry := range entries {
		env := entry.record.Envelope
		if entry.record.Sequence != 0 || !entry.record.HeldUntil.IsZero() || entry.record.ReadyAt.After(now) {
			continue
		}
		if sequence, exists := batched[env.ID]; exists {
			entry.record.Sequence = sequence
			if _, err := s.writeExpiring(ctx, "message."+env.ID, entry.record, entry.revision, messageAge); err != nil {
				return err
			}
			continue
		}
		if env.Kind == "digest" || deferred[env.ID] {
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
		allowed, pending, err := s.canDeliver(ctx, env)
		if err != nil {
			return err
		}
		if pending {
			continue
		}
		if !allowed {
			if err := s.markDropped(ctx, env.ID, now); err != nil {
				return err
			}
			continue
		}
		if env.Kind == "message" || env.Kind == "reply" {
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
	if err := s.sendReminders(ctx, now); err != nil {
		return err
	}
	return errors.Join(heldErr, s.advanceWatches(ctx, entries, now), s.pruneFiles(ctx, entries))
}

// The publish intent records the earliest possible stream sequence. A
// confirmed broker write remains authoritative even after pair policy changes.
func (s *Service) recoverPublication(ctx context.Context, entry messageEntry) (uint64, error) {
	env := entry.record.Envelope
	next := entry.record.PublishFrom
	if next == 0 {
		return 0, nil
	}
	subject := s.subject("agent." + env.To + ".inbox")
	for {
		msg, err := s.stream.GetMsg(ctx, next, jetstream.WithGetMsgSubject(subject))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		if msg.Header.Get("Nats-Msg-Id") == env.ID {
			return msg.Sequence, s.finishPublication(ctx, entry.record, entry.revision, msg.Sequence)
		}
		next = msg.Sequence + 1
	}
}

func (s *Service) canDeliver(ctx context.Context, env Envelope) (allowed, pending bool, err error) {
	enabled, err := s.deliveryAgent(ctx, env.To)
	if !enabled || err != nil {
		return enabled, false, err
	}
	items := env.Items
	if env.Kind == "message" || env.Kind == "reply" {
		items = []Envelope{env}
	}
	for _, item := range items {
		if item.Kind != "message" && item.Kind != "reply" {
			continue
		}
		enabled, err := s.deliveryAgent(ctx, item.From)
		if !enabled || err != nil {
			return enabled, false, err
		}
		allowed, _, pairPending, err := s.pairAllowed(ctx, item.From, item.To)
		if err != nil || (!allowed && !pairPending) {
			return false, false, err
		}
		pending = pending || pairPending
	}
	return !pending, pending, nil
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
	inactive := map[string]bool{}
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
			if _, checked := inactive[name]; !checked {
				enabled, err := s.deliveryAgent(ctx, name)
				if err != nil {
					return err
				}
				inactive[name] = !enabled
			}
			peer := names[1-i]
			last, _, err := readState[time.Time](ctx, s.state, "lastsent."+name+"."+peer)
			if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
				return err
			}
			open[name] = append(open[name], reminderSession{peer, session, last})
		}
	}
	for name, sessions := range open {
		if inactive[name] {
			continue
		}
		sessions = slices.DeleteFunc(sessions, func(entry reminderSession) bool {
			return inactive[entry.peer]
		})
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

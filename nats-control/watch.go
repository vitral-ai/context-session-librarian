package control

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go/jetstream"
)

const maxPromiseWatches = 50

type promiseWatch struct {
	ID        string    `json:"id"`
	Agent     string    `json:"agent"`
	Note      string    `json:"note"`
	CreatedAt time.Time `json:"created_at"`
	DueAt     time.Time `json:"due_at"`
}

func (s *Service) watch(ctx context.Context, caller string, agent Agent, body []byte) (any, error) {
	if agent.Role != "orchestrator" {
		return nil, forbidden("watch requires the orchestrator role")
	}
	var request struct {
		Action       string `json:"action"`
		Agent        string `json:"agent"`
		AfterSeconds int64  `json:"after_seconds"`
		Note         string `json:"note"`
		ID           string `json:"id"`
	}
	if err := decode(body, &request); err != nil {
		return nil, err
	}
	prefix := "watch." + caller + "."
	switch request.Action {
	case "cancel":
		if !messageID.MatchString(request.ID) {
			return nil, invalid("watch cancel requires an ID")
		}
		_, revision, err := readState[promiseWatch](ctx, s.state, prefix+request.ID)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, missing("watch unavailable")
		}
		if err != nil {
			return nil, err
		}
		if err := s.state.Delete(ctx, prefix+request.ID, jetstream.LastRevision(revision)); err != nil {
			return nil, err
		}
		return struct {
			ID        string `json:"id"`
			Cancelled bool   `json:"cancelled"`
		}{request.ID, true}, nil
	case "list":
		keys, err := stateKeys(ctx, s.state, prefix+"*")
		if err != nil {
			return nil, err
		}
		watches := make([]promiseWatch, 0, len(keys))
		for _, key := range keys {
			watch, _, err := readState[promiseWatch](ctx, s.state, key)
			if err != nil {
				return nil, err
			}
			watches = append(watches, watch)
		}
		slices.SortFunc(watches, func(a, b promiseWatch) int {
			if order := a.DueAt.Compare(b.DueAt); order != 0 {
				return order
			}
			return strings.Compare(a.ID, b.ID)
		})
		return struct {
			Watches []promiseWatch `json:"watches"`
		}{watches}, nil
	case "create":
		if !isRegisteredName(request.Agent) || request.Agent == caller ||
			request.AfterSeconds <= 0 || request.AfterSeconds > math.MaxInt64/int64(time.Second) ||
			request.AfterSeconds%60 != 0 || strings.TrimSpace(request.Note) == "" ||
			len(strconv.Quote(request.Note)) > maxBody-512 || !utf8.ValidString(request.Note) {
			return nil, invalid("watch requires a distinct agent, positive whole minutes, and a note")
		}
		if err := s.activeAgent(ctx, request.Agent); err != nil {
			return nil, err
		}
		if _, _, err := readState[Agent](ctx, s.state, "agent."+request.Agent); err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				return nil, missing("watch agent unavailable")
			}
			return nil, err
		}
		keys, err := stateKeys(ctx, s.state, prefix+"*")
		if err != nil {
			return nil, err
		}
		if len(keys) >= maxPromiseWatches {
			return nil, invalid("at most 50 open watches are allowed")
		}
		now := s.clock().UTC()
		watch := promiseWatch{randomID(), request.Agent, request.Note, now,
			now.Add(time.Duration(request.AfterSeconds) * time.Second)}
		return watch, writeState(ctx, s.state, prefix+watch.ID, watch, 0)
	default:
		return nil, invalid("unknown watch action")
	}
}

func (s *Service) advanceWatches(ctx context.Context, entries []messageEntry, now time.Time) error {
	keys, err := stateKeys(ctx, s.state, "watch.*.*")
	if err != nil {
		return err
	}
	for _, key := range keys {
		caller, _, _ := strings.Cut(strings.TrimPrefix(key, "watch."), ".")
		watch, revision, err := readState[promiseWatch](ctx, s.state, key)
		if err != nil {
			return err
		}
		reported := slices.ContainsFunc(entries, func(entry messageEntry) bool {
			message := entry.record.Envelope
			at := entry.record.AcceptedAt
			return message.From == watch.Agent && message.To == caller &&
				(message.Kind == "message" || message.Kind == "reply") &&
				at.After(watch.CreatedAt) && !at.After(watch.DueAt)
		})
		if !reported && now.Before(watch.DueAt) {
			continue
		}
		if !reported {
			active, err := s.deliveryAgent(ctx, caller)
			if err != nil {
				return err
			}
			if active {
				minutes := int64(watch.DueAt.Sub(watch.CreatedAt) / time.Minute)
				elapsed := fmt.Sprintf("%dm", minutes)
				if minutes%60 == 0 {
					elapsed = fmt.Sprintf("%dh", minutes/60)
				}
				body := fmt.Sprintf("%s promised %q %s ago and has not reported; ping it.", watch.Agent, watch.Note, elapsed)
				message := serviceEnvelope(stableID(key), caller, "system", body, now)
				if err := s.recordServiceMessage(ctx, message); err != nil {
					return err
				}
			}
		}
		if err := s.state.Delete(ctx, key, jetstream.LastRevision(revision)); err != nil {
			return err
		}
	}
	return nil
}

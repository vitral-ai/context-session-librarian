package control

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func (s *Service) activeAgent(ctx context.Context, name string) error {
	if !isRegisteredName(name) {
		return forbidden("invalid agent identity")
	}
	revoked, _, err := readState[bool](ctx, s.state, "revoked."+name)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if revoked {
		return forbidden("agent credentials are revoked")
	}
	return nil
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
	prefix := "credential." + name + "."
	keys, err := stateKeys(ctx, s.state, prefix+"*")
	if err != nil {
		return nil, err
	}
	at := s.clock().UTC()
	var publicKeys []string
	for _, key := range keys {
		expires, _, err := readState[time.Time](ctx, s.state, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if expires.After(at) {
			publicKeys = append(publicKeys, strings.TrimPrefix(key, prefix))
		}
	}
	key := "revoked." + name
	blocked, revision, err := readState[bool](ctx, s.state, key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, err
	}
	if !blocked {
		if err := writeState(ctx, s.state, key, true, revision); err != nil {
			return nil, err
		}
	}
	if len(publicKeys) != 0 {
		if err := s.config.RevokeUsers(ctx, publicKeys, at); err != nil {
			return nil, err
		}
	}
	return struct {
		Revoked bool `json:"revoked"`
	}{true}, nil
}

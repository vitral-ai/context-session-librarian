package control

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type pairRequest struct {
	Action  string `json:"action"`
	A       string `json:"a,omitempty"`
	B       string `json:"b,omitempty"`
	Pending bool   `json:"pending,omitempty"`
}

type pairApproval struct {
	A       string `json:"a"`
	B       string `json:"b"`
	Allowed bool   `json:"allowed"`
}

type PairReadback struct {
	Key      string
	Revision uint64
	Allowed  bool
}

func pairWriteKey(a, b string) string {
	return "pair-write." + min(a, b) + "." + max(a, b)
}

func (s *Service) pairAllowed(ctx context.Context, from, to string) (bool, uint64, error) {
	pending, _, err := readState[bool](ctx, s.state, pairWriteKey(from, to))
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, 0, err
	}
	if pending {
		return false, 0, nil
	}
	allowed, revision, err := readState[bool](ctx, s.state, "pair."+from+"."+to)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, 0, nil
	}
	return allowed, revision, err
}

func (s *Service) validateCoordinators(ctx context.Context, name string, coordinators []string) error {
	if !isRegisteredName(name) {
		return invalid("invalid agent name")
	}
	seen := make(map[string]bool)
	for _, coordinator := range coordinators {
		if !isRegisteredName(coordinator) || coordinator == name || seen[coordinator] {
			return invalid("coordinators must be distinct orchestrator names")
		}
		seen[coordinator] = true
		agent, _, err := readState[Agent](ctx, s.state, "agent."+coordinator)
		if errors.Is(err, jetstream.ErrKeyNotFound) || (err == nil && agent.Role != "orchestrator") {
			return invalid("each coordinator must be a registered orchestrator")
		}
		if err != nil {
			return err
		}
		if err := s.activeAgent(ctx, coordinator); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) setCoordinators(ctx context.Context, name string, coordinators []string) (Agent, error) {
	if err := s.validateCoordinators(ctx, name, coordinators); err != nil {
		return Agent{}, err
	}
	record, rev, err := readState[Agent](ctx, s.state, "agent."+name)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return Agent{}, missing("unknown agent")
	}
	if err != nil {
		return Agent{}, err
	}
	record.Coordinators = coordinators
	return record, writeState(ctx, s.state, "agent."+name, record, rev)
}

func (s *Service) managesEndpoint(ctx context.Context, caller, endpoint string) (bool, error) {
	agent, _, err := readState[Agent](ctx, s.state, "agent."+endpoint)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	active, err := s.deliveryAgent(ctx, endpoint)
	if err != nil || !active {
		return false, err
	}
	return caller == "owner" || caller == endpoint || slices.Contains(agent.Coordinators, caller), nil
}

func (s *Service) managesPair(ctx context.Context, caller, a, b string) (bool, error) {
	for _, endpoint := range []string{a, b} {
		ok, err := s.managesEndpoint(ctx, caller, endpoint)
		if !ok || err != nil {
			return false, err
		}
	}
	return true, nil
}

func (s *Service) pairs(ctx context.Context, caller, role string, request pairRequest) (response any, err error) {
	if caller != "owner" && role != "orchestrator" {
		return nil, forbidden("pairs requires a coordinator or owner")
	}
	if request.Action == "list" {
		if request.A != "" || request.B != "" {
			return nil, invalid("pair list does not take endpoint arguments")
		}
		return s.listPairs(ctx, caller, request.Pending)
	}
	defer func() {
		s.config.Logger.Info("control pair decision", "tenant", s.config.Tenant, "actor", caller,
			"action", request.Action, "a", request.A, "b", request.B, "success", err == nil)
	}()
	if (request.Action != "allow" && request.Action != "deny") || request.Pending ||
		!isRegisteredName(request.A) || !isRegisteredName(request.B) || request.A == request.B {
		return nil, invalid("allow or deny requires two distinct agents")
	}
	managed, err := s.managesPair(ctx, caller, request.A, request.B)
	if err != nil {
		return nil, err
	}
	if !managed {
		return nil, forbidden("the pair is outside the caller's coordinator scope")
	}
	if s.config.VerifyPairs == nil {
		return nil, invalid("pair mirror verification is unavailable")
	}
	if err := s.config.VerifyPairs(ctx, nil); err != nil {
		return nil, err
	}
	gate := pairWriteKey(request.A, request.B)
	pending, gateRevision, err := readState[bool](ctx, s.state, gate)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, err
	}
	if !pending {
		gateRevision, err = s.writeExpiring(ctx, gate, true, gateRevision, 0)
		if err != nil {
			return nil, err
		}
	}
	allowed := request.Action == "allow"
	now := s.clock().UTC()
	if !allowed {
		key := pairDenialKey(request.A, request.B)
		_, revision, err := readState[time.Time](ctx, s.state, key)
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, err
		}
		if _, err := s.writeExpiring(ctx, key, now, revision, pairHold); err != nil {
			return nil, err
		}
	}
	revisions := make([]PairReadback, 0, 2)
	for _, key := range []string{"pair." + request.A + "." + request.B, "pair." + request.B + "." + request.A} {
		value, rev, err := readState[bool](ctx, s.state, key)
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, err
		}
		if rev == 0 || value != allowed {
			rev, err = s.writeExpiring(ctx, key, allowed, rev, 0)
			if err != nil {
				return nil, err
			}
		}
		read, localRev, err := readState[bool](ctx, s.state, key)
		if err != nil {
			return nil, err
		}
		if read != allowed || rev != localRev {
			return nil, errors.New("local pair revision did not match")
		}
		revisions = append(revisions, PairReadback{key, rev, allowed})
	}
	if err := s.config.VerifyPairs(ctx, revisions); err != nil {
		return nil, err
	}
	if err := writeState(ctx, s.state, gate, false, gateRevision); err != nil {
		return nil, err
	}
	entries, err := s.messages(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.advanceHeldPairs(ctx, entries, now, request.A, request.B); err != nil {
		return nil, err
	}
	return pairApproval{request.A, request.B, allowed}, nil
}

func (s *Service) listPairs(ctx context.Context, caller string, pendingOnly bool) (any, error) {
	pending, err := s.listHeldPairs(ctx, caller)
	if err != nil {
		return nil, err
	}
	result := struct {
		Pairs   []pairApproval `json:"pairs"`
		Pending []pendingPair  `json:"pending"`
	}{[]pairApproval{}, pending}
	if pendingOnly {
		return result, nil
	}
	keys, err := stateKeys(ctx, s.state, "pair.*.*")
	if err != nil {
		return nil, err
	}
	slices.Sort(keys)
	scope := make(map[string]bool)
	for _, key := range keys {
		parts := strings.Split(key, ".")
		if len(parts) != 3 {
			continue
		}
		for _, endpoint := range parts[1:] {
			if _, known := scope[endpoint]; known {
				continue
			}
			managed, err := s.managesEndpoint(ctx, caller, endpoint)
			if err != nil {
				return nil, err
			}
			scope[endpoint] = managed
		}
		if !scope[parts[1]] || !scope[parts[2]] {
			continue
		}
		allowed, _, err := s.pairAllowed(ctx, parts[1], parts[2])
		if err != nil {
			return nil, err
		}
		if allowed {
			result.Pairs = append(result.Pairs, pairApproval{parts[1], parts[2], allowed})
		}
	}
	return result, nil
}

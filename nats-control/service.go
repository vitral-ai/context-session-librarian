package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

type Config struct {
	Tenant       string
	Account      string
	Signer       nkeys.KeyPair
	Upgrade      string
	Logger       *slog.Logger
	Domains      []string
	RevokeUsers  func(context.Context, []string, time.Time) error
	CreateTenant func(context.Context, TenantRequest) (TenantInfo, error)
	EraseTenant  string
	MirrorProbe  func(context.Context) (map[string]uint64, error)
	VerifyPairs  func(context.Context, []PairReadback) error
}

type Service struct {
	config Config
	conn   *nats.Conn
	js     jetstream.JetStream
	state  jetstream.KeyValue
	stream jetstream.Stream
	files  jetstream.ObjectStore
	clock  func() time.Time

	// Only the active primary runs control. Serialize its mutations across
	// subscriptions, including administration and the scheduled delivery pass.
	mu                 sync.Mutex
	lastAccepted       time.Time
	revocationWake     chan struct{}
	revocationHelperMu sync.Mutex
}

func Open(ctx context.Context, conn *nats.Conn, config Config) (*Service, error) {
	if !tenantID.MatchString(config.Tenant) || !nkeys.IsValidPublicAccountKey(config.Account) ||
		config.Signer == nil || config.Upgrade == "" {
		return nil, errors.New("tenant, account, signer and upgrade command are required")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, err
	}
	kv, err := js.KeyValue(ctx, "CSL_STATE")
	if err != nil {
		return nil, fmt.Errorf("open CSL_STATE: %w", err)
	}
	stream, err := js.Stream(ctx, "CSL_MSG")
	if err != nil {
		return nil, fmt.Errorf("open CSL_MSG: %w", err)
	}
	files, err := js.ObjectStore(ctx, "CSL_FILES")
	if err != nil {
		return nil, fmt.Errorf("open CSL_FILES: %w", err)
	}
	return &Service{config: config, conn: conn, js: js, state: kv, stream: stream, files: files, clock: time.Now, revocationWake: make(chan struct{}, 1)}, nil
}

type apiError struct {
	Code    string `json:"error"`
	Message string `json:"message"`
}

func (e *apiError) Error() string { return e.Message }

func invalid(message string) error   { return &apiError{"invalid", message} }
func forbidden(message string) error { return &apiError{"forbidden", message} }
func missing(message string) error   { return &apiError{"not_found", message} }

func decode(body []byte, value any) error {
	body = bytes.TrimSpace(body)
	if len(body) == 0 || body[0] != '{' {
		return invalid("request must be a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return invalid("invalid request JSON")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return invalid("invalid request JSON")
	}
	return nil
}

func (s *Service) acceptanceTime() time.Time {
	now := s.clock().UTC()
	if !now.After(s.lastAccepted) {
		now = s.lastAccepted.Add(time.Nanosecond)
	}
	s.lastAccepted = now
	return now
}

func (s *Service) subject(tail string) string {
	return "csl." + s.config.Tenant + "." + tail
}

func (s *Service) Subscribe() ([]*nats.Subscription, error) {
	first, err := s.conn.Subscribe(s.subject("ctl.*.*"), s.handleControl)
	if err != nil {
		return nil, err
	}
	second, err := s.conn.Subscribe(s.subject("send.*"), s.handleSend)
	if err != nil {
		_ = first.Unsubscribe()
		return nil, err
	}
	third, err := s.conn.Subscribe(s.subject("ci.*"), s.handleCI)
	if err != nil {
		_ = first.Unsubscribe()
		_ = second.Unsubscribe()
		return nil, err
	}
	if err := s.conn.FlushTimeout(5 * time.Second); err != nil {
		_ = first.Unsubscribe()
		_ = second.Unsubscribe()
		_ = third.Unsubscribe()
		return nil, err
	}
	return []*nats.Subscription{first, second, third}, nil
}

func (s *Service) handleControl(msg *nats.Msg) {
	tokens := strings.Split(msg.Subject, ".")
	caller, verb := tokens[4], tokens[3]
	if !agentName.MatchString(caller) ||
		!strings.HasPrefix(msg.Reply, agentInboxPrefix(s.config.Tenant, caller)+".") {
		return
	}
	timeout := 10 * time.Second
	if caller == "owner" && verb == "admin" && s.config.EraseTenant == s.config.Tenant {
		var request struct {
			Action string `json:"action"`
		}
		if json.Unmarshal(msg.Data, &request) == nil && request.Action == "erase-tenant" {
			timeout = 2 * time.Minute
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	s.mu.Lock()
	response, err := s.control(ctx, caller, verb, msg.Data)
	s.mu.Unlock()
	s.respond(msg, verb, response, err)
}

func (s *Service) respond(msg *nats.Msg, verb string, response any, err error) {
	if err != nil {
		var rejection *apiError
		if !errors.As(err, &rejection) {
			s.config.Logger.Error("control operation failed", "tenant", s.config.Tenant, "operation", verb, "error", err)
			rejection = &apiError{"invalid", "control operation unavailable"}
		}
		response = rejection
	}
	body, err := json.Marshal(response)
	if err == nil {
		err = msg.Respond(body)
	}
	if err != nil {
		s.config.Logger.Error("control response failed", "tenant", s.config.Tenant, "operation", verb, "error", err)
	}
}

func (s *Service) control(ctx context.Context, caller, verb string, body []byte) (any, error) {
	if verb == "admin" {
		if caller != "owner" {
			return nil, forbidden("owner credentials are required")
		}
		return s.admin(ctx, body)
	}
	if caller == "owner" {
		switch verb {
		case "status", "versions":
			return s.fleet(ctx, verb, body)
		case "fetch", "file-acl":
			return s.file(ctx, caller, verb, body)
		}
		return nil, forbidden("owner must use ctl.admin.owner")
	}
	if err := s.activeAgent(ctx, caller); err != nil {
		return nil, err
	}
	agent, revision, err := readState[Agent](ctx, s.state, "agent."+caller)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, missing("unknown agent")
	}
	if err != nil {
		return nil, err
	}
	switch verb {
	case "history":
		return s.history(ctx, caller, body)
	case "pairs":
		var request pairRequest
		if err := decode(body, &request); err != nil {
			return nil, err
		}
		return s.pairs(ctx, caller, agent.Role, request)
	case "unread":
		return s.unread(ctx, caller, body)
	case "receipt":
		return s.receipt(ctx, caller, body)
	case "put", "fetch", "file-acl":
		return s.file(ctx, caller, verb, body)
	case "hello", "renew":
		return s.clientVersion(ctx, caller, verb, body, agent, revision)
	case "ensure_consumer":
		if err := decode(body, &struct{}{}); err != nil {
			return nil, err
		}
		if err := s.ensureConsumer(ctx, caller); err != nil {
			return nil, err
		}
		return struct{}{}, nil
	case "status", "versions":
		if verb == "status" && agent.Role != "orchestrator" {
			return nil, forbidden("status requires the orchestrator role")
		}
		return s.fleet(ctx, verb, body)
	case "settings":
		var request struct {
			Delivery string `json:"delivery"`
		}
		if err := decode(body, &request); err != nil {
			return nil, err
		}
		if _, err := deliveryInterval(request.Delivery); err != nil {
			return nil, invalid(err.Error())
		}
		agent.Delivery = request.Delivery
		if err := writeState(ctx, s.state, "agent."+caller, agent, revision); err != nil {
			return nil, err
		}
		return request, nil
	case "close":
		var request struct {
			With string `json:"with"`
		}
		if err := decode(body, &request); err != nil {
			return nil, err
		}
		key, err := pairKey(caller, request.With)
		if err != nil {
			return nil, invalid(err.Error())
		}
		session, revision, err := readState[PairSession](ctx, s.state, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, missing("pair session does not exist")
		}
		if err != nil {
			return nil, err
		}
		session.Closed = true
		if err := writeState(ctx, s.state, key, session, revision); err != nil {
			return nil, err
		}
		return session, nil
	case "complete":
		var request struct {
			Task string `json:"task,omitempty"`
			With string `json:"with,omitempty"`
		}
		if err := decode(body, &request); err != nil {
			return nil, err
		}
		if err := s.completePairs(ctx, caller, request.With); err != nil {
			return nil, err
		}
		agent.TaskComplete = true
		agent.Status = "idle"
		agent.LastSeen = s.clock().UTC()
		if err := writeState(ctx, s.state, "agent."+caller, agent, revision); err != nil {
			return nil, err
		}
		return agent, nil
	default:
		return nil, missing("unknown control operation")
	}
}

func (s *Service) clientVersion(ctx context.Context, name, verb string, body []byte, agent Agent, revision uint64) (any, error) {
	var request struct {
		ClientVersion string `json:"client_version"`
		Status        string `json:"status,omitempty"`
	}
	if err := decode(body, &request); err != nil {
		return nil, err
	}
	if request.Status != "" && request.Status != "idle" && request.Status != "working" {
		return nil, invalid("status must be idle or working")
	}
	warn, err := s.checkVersion(ctx, name, "", request.ClientVersion)
	if err != nil {
		return nil, err
	}
	agent.ClientVersion, agent.LastSeen = request.ClientVersion, s.clock().UTC()
	if request.Status != "" {
		agent.Status = request.Status
	}
	if err := writeState(ctx, s.state, "agent."+name, agent, revision); err != nil {
		return nil, err
	}
	if warn {
		if err := s.warnClient(ctx, name, s.clock().UTC()); err != nil {
			return nil, err
		}
	}
	if verb == "renew" {
		return s.issue(ctx, name, agent)
	}
	request.Status = agent.Status
	return request, nil
}

func (s *Service) checkVersion(ctx context.Context, caller, target, client string) (bool, error) {
	policy, _, err := readState[VersionPolicy](ctx, s.state, "policy.versions")
	if err != nil {
		return false, err
	}
	if !strings.Contains(client, "+") {
		return false, invalid("client_version must include the build identity")
	}
	outdated, warn, err := policy.check(client)
	if err != nil {
		return false, invalid(err.Error())
	}
	if outdated {
		s.notice(ctx, "Outdated client", caller, target)
		return false, &apiError{"outdated_client", "Upgrade with: " + s.config.Upgrade}
	}
	return warn, nil
}

type agentStatus struct {
	Name string `json:"name"`
	Agent
	OpenSessions  []string `json:"open_sessions"`
	PendingUrgent int      `json:"pending_urgent"`
}

func (s *Service) fleet(ctx context.Context, verb string, body []byte) (any, error) {
	if err := decode(body, &struct{}{}); err != nil {
		return nil, err
	}
	keys, err := stateKeys(ctx, s.state, "agent.*")
	if err != nil {
		return nil, err
	}
	slices.Sort(keys)
	agents := make([]agentStatus, 0, len(keys))
	for _, key := range keys {
		agent, _, err := readState[Agent](ctx, s.state, key)
		if err != nil {
			return nil, err
		}
		agents = append(agents, agentStatus{Name: strings.TrimPrefix(key, "agent."), Agent: agent, OpenSessions: []string{}})
	}
	if verb == "versions" {
		policy, _, err := readState[VersionPolicy](ctx, s.state, "policy.versions")
		return struct {
			Agents []agentStatus `json:"agents"`
			Policy VersionPolicy `json:"policy"`
		}{agents, policy}, err
	}
	pairs, err := stateKeys(ctx, s.state, "session.*.*")
	if err != nil {
		return nil, err
	}
	slices.Sort(pairs)
	now := s.clock()
	for _, key := range pairs {
		session, _, err := readState[PairSession](ctx, s.state, key)
		if err != nil {
			return nil, err
		}
		if !session.isOpen(now) {
			continue
		}
		names := strings.Split(key, ".")
		for i := range agents {
			switch agents[i].Name {
			case names[1]:
				agents[i].OpenSessions = append(agents[i].OpenSessions, names[2])
			case names[2]:
				agents[i].OpenSessions = append(agents[i].OpenSessions, names[1])
			}
		}
	}
	pending, err := s.pendingUrgent(ctx)
	if err != nil {
		return nil, err
	}
	for i := range agents {
		agents[i].PendingUrgent = pending[agents[i].Name]
	}
	return struct {
		Agents []agentStatus `json:"agents"`
	}{agents}, nil
}

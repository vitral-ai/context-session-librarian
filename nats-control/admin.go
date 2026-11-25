package control

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type adminRequest struct {
	ID            string   `json:"id,omitempty"`
	Offset        int64    `json:"offset,omitempty"`
	Allowed       []string `json:"allowed,omitempty"`
	From          string   `json:"from,omitempty"`
	To            string   `json:"to,omitempty"`
	Action        string   `json:"action"`
	Tenant        string   `json:"tenant,omitempty"`
	After         string   `json:"after,omitempty"`
	NoticesTo     string   `json:"notices_to,omitempty"`
	Name          string   `json:"name,omitempty"`
	Machine       string   `json:"machine,omitempty"`
	CLI           string   `json:"cli,omitempty"`
	Role          string   `json:"role,omitempty"`
	Delivery      string   `json:"delivery,omitempty"`
	Minimum       string   `json:"minimum,omitempty"`
	Recommended   string   `json:"recommended,omitempty"`
	Retention     int      `json:"retention,omitempty"`
	UrgentCap     int      `json:"urgent_cap,omitempty"`
	StorageBytes  int64    `json:"storage_bytes,omitempty"`
	FileBytes     int64    `json:"file_bytes,omitempty"`
	RatePerMinute int      `json:"rate_per_minute,omitempty"`
	Confirm       string   `json:"confirm,omitempty"`
}

func (s *Service) admin(ctx context.Context, body []byte) (response any, err error) {
	var request adminRequest
	if err := decode(body, &request); err != nil {
		return nil, err
	}
	defer func() {
		s.config.Logger.Info("control admin action", "tenant", s.config.Tenant, "actor", "owner",
			"action", request.Action, "agent", request.Name, "target_tenant", request.Tenant, "notices_to", request.NoticesTo, "success", err == nil, "at", s.clock().UTC())
	}()
	switch request.Action {
	case "status":
		return s.fleet(ctx, "status", []byte("{}"))
	case "export":
		return s.export(ctx, request.Tenant, request.After)
	case "fetch", "file-acl":
		return s.performFileOperation(ctx, "owner", request.Action, fileRequest{
			ID: request.ID, Offset: request.Offset, Allowed: request.Allowed,
		})
	case "create-tenant":
		if s.config.CreateTenant == nil {
			return nil, forbidden("tenant provisioning is unavailable")
		}
		config, err := normalizedTenant(TenantRequest{request.Tenant, request.StorageBytes, request.FileBytes, request.RatePerMinute})
		if err != nil {
			return nil, err
		}
		return s.config.CreateTenant(ctx, config)
	case "erase-tenant":
		return s.eraseTenant(ctx, request.Tenant, request.Confirm)
	case "revoke":
		return s.revoke(ctx, request.Name)
	case "register":
		return s.register(ctx, request)
	case "versions":
		if request.Minimum == "" && request.Recommended == "" {
			return s.fleet(ctx, "versions", []byte("{}"))
		}
		policy := VersionPolicy{Minimum: request.Minimum, Recommended: request.Recommended}
		if _, _, err := policy.check(request.Minimum); err != nil {
			return nil, invalid(err.Error())
		}
		_, revision, err := readState[VersionPolicy](ctx, s.state, "policy.versions")
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, err
		}
		return policy, writeState(ctx, s.state, "policy.versions", policy, revision)
	case "allow", "deny":
		if !isRegisteredName(request.From) || !isRegisteredName(request.To) || request.From == request.To {
			return nil, invalid("two distinct agents are required")
		}
		if _, _, err := readState[Agent](ctx, s.state, "agent."+request.From); err != nil {
			return nil, missing("unknown sender")
		}
		if _, _, err := readState[Agent](ctx, s.state, "agent."+request.To); err != nil {
			return nil, missing("unknown recipient")
		}
		key := "pair." + request.From + "." + request.To
		_, revision, err := readState[bool](ctx, s.state, key)
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, err
		}
		return struct {
				Allowed bool `json:"allowed"`
			}{request.Action == "allow"},
			writeState(ctx, s.state, key, request.Action == "allow", revision)
	case "quotas":
		if _, err := s.deliveryPolicy(ctx); err != nil {
			return nil, err
		}
		if request.Retention < 1 || request.Retention > 10000 || request.UrgentCap < 1 {
			return nil, invalid("positive retention and urgent cap are required")
		}
		policy, revision, err := readState[tenantPolicy](ctx, s.state, "policy.delivery")
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, err
		}
		if request.FileBytes < 0 || request.FileBytes > maxFile || request.RatePerMinute < 0 {
			return nil, invalid("invalid file or message quota")
		}
		policy.Retention, policy.UrgentCap = request.Retention, request.UrgentCap
		if request.FileBytes > 0 {
			policy.FileBytes = request.FileBytes
		}
		if request.RatePerMinute > 0 {
			policy.RatePerMinute = request.RatePerMinute
		}
		return policy, writeState(ctx, s.state, "policy.delivery", policy, revision)
	case "settings":
		if request.NoticesTo != "" {
			if request.Name != "" || request.Role != "" || request.Delivery != "" {
				return nil, invalid("notice configuration is a tenant setting")
			}
			return s.setNoticeRecipient(ctx, request.NoticesTo)
		}
		return s.adminSettings(ctx, request)
	default:
		return nil, missing("unknown administrator operation")
	}
}

func (s *Service) register(ctx context.Context, request adminRequest) (any, error) {
	if _, err := s.deliveryPolicy(ctx); err != nil {
		return nil, err
	}
	agent := Agent{Role: request.Role, CLI: request.CLI, Machine: request.Machine,
		Delivery: defaultDelivery(request.Role), Status: "idle"}
	if !isRegisteredName(request.Name) || !validAgent(agent) {
		return nil, invalid("invalid agent registration")
	}
	if err := s.activeAgent(ctx, request.Name); err != nil {
		return nil, err
	}
	key := "agent." + request.Name
	existing, _, err := readState[Agent](ctx, s.state, key)
	if err == nil && (existing.Role != agent.Role || existing.CLI != agent.CLI || existing.Machine != agent.Machine) {
		return nil, forbidden("agent already has a different registration")
	}
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, err
	}
	if _, err := s.stream.CreateOrUpdateConsumer(ctx, s.agentConsumerConfig(request.Name)); err != nil {
		return nil, err
	}
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		if err := writeState(ctx, s.state, key, agent, 0); err != nil {
			return nil, err
		}
	} else {
		agent = existing
	}
	return s.issue(ctx, request.Name, agent)
}

func (s *Service) ensureConsumer(ctx context.Context, name string) error {
	config := s.agentConsumerConfig(name)
	if _, err := s.stream.Consumer(ctx, config.Durable); !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return err
	}
	_, err := s.stream.CreateConsumer(ctx, config)
	return err
}

func (s *Service) agentConsumerConfig(name string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable: "agent-" + name, FilterSubject: s.subject("agent." + name + ".inbox"),
		DeliverPolicy: jetstream.DeliverAllPolicy, AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: 5 * time.Minute, MaxDeliver: 20,
	}
}

func (s *Service) issue(ctx context.Context, name string, agent Agent) (any, error) {
	cred, key, err := issueCredentials(s.config.Signer, s.config.Account, s.config.Tenant, name, agent, s.clock(), s.config.Domains...)
	if err != nil {
		return nil, err
	}
	if _, err := s.writeExpiring(ctx, "credential."+name+"."+key, cred.ExpiresAt, 0, credentialLifetime); err != nil {
		return nil, err
	}
	return cred, nil
}

func (s *Service) adminSettings(ctx context.Context, request adminRequest) (any, error) {
	if !isRegisteredName(request.Name) || (request.Role == "" && request.Delivery == "") {
		return nil, invalid("agent and at least one setting are required")
	}
	agent, revision, err := readState[Agent](ctx, s.state, "agent."+request.Name)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, missing("unknown agent")
	}
	if err != nil {
		return nil, err
	}
	if request.Role != "" {
		if request.Role != "worker" && request.Role != "orchestrator" {
			return nil, invalid("invalid agent role")
		}
		agent.Role = request.Role
	}
	if request.Delivery != "" {
		if _, err := deliveryInterval(request.Delivery); err != nil {
			return nil, invalid(err.Error())
		}
		agent.Delivery = request.Delivery
	}
	return agent, writeState(ctx, s.state, "agent."+request.Name, agent, revision)
}

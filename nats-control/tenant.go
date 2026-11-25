package control

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type TenantRequest struct {
	Tenant        string `json:"tenant"`
	StorageBytes  int64  `json:"storage_bytes,omitempty"`
	FileBytes     int64  `json:"file_bytes,omitempty"`
	RatePerMinute int    `json:"rate_per_minute,omitempty"`
}

type TenantInfo struct {
	Tenant  string `json:"tenant"`
	Account string `json:"account"`
}

func normalizedTenant(request TenantRequest) (TenantRequest, error) {
	if !tenantID.MatchString(request.Tenant) || request.Tenant == "vi" {
		return TenantRequest{}, invalid("a new tenant ID is required")
	}
	if request.StorageBytes == 0 {
		request.StorageBytes = 512 * 1024 * 1024
	}
	if request.FileBytes == 0 {
		request.FileBytes = maxFile
	}
	if request.RatePerMinute == 0 {
		request.RatePerMinute = 600
	}
	if request.FileBytes < 1 || request.FileBytes > maxFile ||
		request.StorageBytes < 4*request.FileBytes || request.StorageBytes < 16*1024*1024 ||
		request.RatePerMinute < 1 {
		return TenantRequest{}, invalid("invalid tenant storage, file or message quota")
	}
	return request, nil
}

func (s *Service) eraseTenant(ctx context.Context, tenant, confirm string) (any, error) {
	if tenant == "" || tenant == "vi" || tenant != s.config.Tenant || tenant != s.config.EraseTenant {
		return nil, forbidden("erasure is unavailable for this tenant")
	}
	if s.config.RevokeUsers == nil {
		return nil, forbidden("account revocation is required before erasure")
	}
	info, err := s.stream.Info(ctx)
	if err != nil {
		return nil, err
	}
	subjects := info.Config.Subjects
	if len(subjects) != 1 || subjects[0] != s.subject("agent.*.inbox") || info.Config.Mirror != nil {
		return nil, forbidden("the broker store does not match the erase tenant")
	}
	const key = "admin.erasure"
	previous, revision, err := readState[[32]byte](ctx, s.state, key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, err
	}
	if confirm == "" {
		token := randomID()
		digest := sha256.Sum256([]byte(token))
		if _, err := s.writeExpiring(ctx, key, digest, revision, 10*time.Minute); err != nil {
			return nil, err
		}
		return struct {
			Confirm string `json:"confirm"`
		}{token}, nil
	}
	got := sha256.Sum256([]byte(confirm))
	if revision == 0 || subtle.ConstantTimeCompare(previous[:], got[:]) != 1 {
		return nil, forbidden("erasure confirmation is invalid or expired")
	}
	keys, err := stateKeys(ctx, s.state, "credential.*.*")
	if err != nil {
		return nil, err
	}
	active := make([]string, 0, len(keys))
	now := s.clock().UTC()
	for _, key := range keys {
		expires, _, err := readState[time.Time](ctx, s.state, key)
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, err
		}
		if expires.After(now) {
			active = append(active, key[strings.LastIndexByte(key, '.')+1:])
		}
	}
	if len(active) != 0 {
		if err := s.config.RevokeUsers(ctx, active, now); err != nil {
			return nil, err
		}
	}
	objects, err := s.js.Stream(ctx, "OBJ_CSL_FILES")
	if err != nil {
		return nil, err
	}
	kv, err := s.js.Stream(ctx, "KV_CSL_STATE")
	if err != nil {
		return nil, err
	}
	// Keep the broker resources and erase only the selected tenant's data. The
	// state and confirmation are removed last so an interrupted purge can retry.
	for _, stream := range []jetstream.Stream{s.stream, objects, kv} {
		if err := stream.Purge(ctx); err != nil {
			return nil, err
		}
	}
	return struct {
		Erased bool `json:"erased"`
	}{true}, nil
}

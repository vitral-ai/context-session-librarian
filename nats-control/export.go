package control

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

type exportRecord struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

func (s *Service) export(ctx context.Context, tenant, after string) (any, error) {
	if tenant != "" && tenant != s.config.Tenant {
		return nil, forbidden("export is scoped to the authenticated tenant")
	}
	keys, err := stateKeys(ctx, s.state, "message.*")
	if err != nil {
		return nil, err
	}
	objects, err := s.files.List(ctx)
	if err != nil {
		return nil, err
	}
	byKey := map[string]*jetstream.ObjectInfo{}
	for _, object := range objects {
		key := "file." + object.Name
		keys = append(keys, key)
		byKey[key] = object
	}
	slices.Sort(keys)
	items := []exportRecord{}
	used := 0
	var next string
	for _, key := range keys {
		if key <= after {
			continue
		}
		var raw []byte
		if strings.HasPrefix(key, "file.") {
			meta, err := readFileInfo(byKey[key])
			if err != nil {
				return nil, err
			}
			raw, err = json.Marshal(meta)
			if err != nil {
				return nil, err
			}
		} else {
			message, _, err := readState[messageRecord](ctx, s.state, key)
			if err != nil {
				return nil, err
			}
			raw, err = json.Marshal(message.Envelope)
			if err != nil {
				return nil, err
			}
		}
		item := exportRecord{key, raw}
		cost := len(raw) + len(key) + 32
		if len(items) > 0 && used+cost >= maxDigest-1024 {
			next = items[len(items)-1].Key
			break
		}
		used += cost
		items = append(items, item)
	}
	return struct {
		Tenant    string         `json:"tenant"`
		Items     []exportRecord `json:"items"`
		NextAfter string         `json:"next_after,omitempty"`
	}{s.config.Tenant, items, next}, nil
}

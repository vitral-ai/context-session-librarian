package control

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
)

func readState[T any](ctx context.Context, kv jetstream.KeyValue, key string) (T, uint64, error) {
	var value T
	entry, err := kv.Get(ctx, key)
	if err != nil {
		return value, 0, err
	}
	err = json.Unmarshal(entry.Value(), &value)
	return value, entry.Revision(), err
}

func writeState[T any](ctx context.Context, kv jetstream.KeyValue, key string, value T, revision uint64) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if revision == 0 {
		_, err = kv.Create(ctx, key, body)
	} else {
		_, err = kv.Update(ctx, key, body, revision)
	}
	return err
}

func stateKeys(ctx context.Context, kv jetstream.KeyValue, filter string) ([]string, error) {
	list, err := kv.ListKeysFiltered(ctx, filter)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer list.Stop()
	var keys []string
	for key := range list.Keys() {
		keys = append(keys, key)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}

package control

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const mirrorJSZ = "$SYS.REQ.SERVER.PING.JSZ"
const mirrorContactLimit = time.Minute

var mirroredStreams = []string{"CSL_MSG", "KV_CSL_STATE", "OBJ_CSL_FILES"}

func MirrorProbe(conn *nats.Conn, server, account string) func(context.Context) (map[string]uint64, error) {
	body, _ := json.Marshal(map[string]any{"accounts": true, "streams": true, "account": account})
	return func(ctx context.Context) (map[string]uint64, error) {
		msg, err := conn.RequestWithContext(ctx, mirrorJSZ, body)
		if err != nil {
			return nil, err
		}
		return mirrorLag(msg.Data, server, account)
	}
}

func mirrorLag(body []byte, server, account string) (map[string]uint64, error) {
	var response struct {
		Server struct {
			Name string `json:"name"`
		} `json:"server"`
		Data struct {
			Accounts []struct {
				ID      string `json:"id"`
				Streams []struct {
					Mirror *struct {
						Name   string           `json:"name"`
						Lag    *uint64          `json:"lag"`
						Active *time.Duration   `json:"active"`
						Error  *json.RawMessage `json:"error"`
					} `json:"mirror"`
				} `json:"stream_detail"`
			} `json:"account_details"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil || response.Server.Name != server {
		return nil, errors.New("unexpected mirror JSZ response")
	}
	lag := make(map[string]uint64, len(mirroredStreams))
	for _, item := range response.Data.Accounts {
		if item.ID != account {
			continue
		}
		for _, stream := range item.Streams {
			mirror := stream.Mirror
			if mirror == nil || !slices.Contains(mirroredStreams, mirror.Name) {
				continue
			}
			if _, exists := lag[mirror.Name]; exists || mirror.Error != nil ||
				mirror.Lag == nil || mirror.Active == nil || *mirror.Active < 0 ||
				*mirror.Active > mirrorContactLimit {
				return nil, errors.New("mirror JSZ reported an unavailable stream")
			}
			lag[mirror.Name] = *mirror.Lag
		}
	}
	if len(lag) != len(mirroredStreams) {
		return nil, errors.New("mirror JSZ did not report the tenant's expected mirrors")
	}
	return lag, nil
}

type mirrorWatch struct {
	LagSince            map[string]time.Time `json:"lag_since,omitempty"`
	LagNotifiedAt       time.Time            `json:"lag_notified_at,omitzero"`
	UnavailableSince    time.Time            `json:"unavailable_since,omitzero"`
	UnavailableNotified bool                 `json:"unavailable_notified,omitempty"`
}

func (s *Service) monitorMirrors(ctx context.Context) error {
	if s.config.MirrorProbe == nil {
		return nil
	}
	probe, cancel := context.WithTimeout(ctx, 3*time.Second)
	lag, probeErr := s.config.MirrorProbe(probe)
	cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	const key = "monitor.mirrors"
	state, revision, err := readState[mirrorWatch](ctx, s.state, key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	now := s.clock().UTC()
	if probeErr != nil {
		if state.UnavailableSince.IsZero() {
			state.UnavailableSince = now
			return writeState(ctx, s.state, key, state, revision)
		}
		if state.UnavailableNotified || now.Sub(state.UnavailableSince) <= 5*time.Minute {
			return nil
		}
		id := "mirror.unavailable." + state.UnavailableSince.Format(time.RFC3339Nano)
		sent, err := s.sendOwnerNotice(ctx, id, "Mirror monitoring unavailable.", now)
		if err != nil || !sent {
			return err
		}
		state.UnavailableNotified = true
		return writeState(ctx, s.state, key, state, revision)
	}
	state.UnavailableSince = time.Time{}
	state.UnavailableNotified = false
	lagSince := make(map[string]time.Time)
	var overdue []string
	for _, name := range mirroredStreams {
		if lag[name] == 0 {
			continue
		}
		since := state.LagSince[name]
		if since.IsZero() {
			since = now
		}
		lagSince[name] = since
		if now.Sub(since) > time.Minute {
			overdue = append(overdue, name)
		}
	}
	if len(lagSince) == 0 {
		if !state.LagNotifiedAt.IsZero() {
			id := "mirror.recovered." + state.LagNotifiedAt.Format(time.RFC3339Nano)
			if _, err := s.sendOwnerNotice(ctx, id, "Mirror replication recovered.", now); err != nil {
				return err
			}
		}
		state.LagNotifiedAt = time.Time{}
	} else if len(overdue) != 0 && now.Sub(state.LagNotifiedAt) >= 15*time.Minute {
		id := "mirror.lag." + now.Truncate(15*time.Minute).Format(time.RFC3339)
		for _, name := range overdue {
			id += "." + name + "." + lagSince[name].Format(time.RFC3339Nano)
		}
		sent, err := s.sendOwnerNotice(ctx, id, "Mirror replication has lagged more than 60 seconds: "+strings.Join(overdue, ", ")+".", now)
		if err != nil {
			return err
		}
		if sent {
			state.LagNotifiedAt = now
		}
	}
	state.LagSince = lagSince
	return writeState(ctx, s.state, key, state, revision)
}

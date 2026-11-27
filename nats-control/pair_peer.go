package control

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// PairPeer verifies the independent peer mirror and the original KV revisions.
// Direct subject lookup lets the broker restrict this credential to pair keys.
func PairPeer(conn *nats.Conn, peerDomain, writerDomain string) func(context.Context, []PairReadback) error {
	const stream = "KV_CSL_STATE"
	api := "$JS." + peerDomain + ".API"
	writer := "$JS." + writerDomain + ".API"
	return func(ctx context.Context, expected []PairReadback) error {
		check, cancel := context.WithTimeout(ctx, 4*time.Second)
		defer cancel()
		reply, err := conn.RequestWithContext(check, api+".STREAM.INFO."+stream, nil)
		if err != nil {
			return err
		}
		var info jetstream.StreamInfo
		if err := json.Unmarshal(reply.Data, &info); err != nil {
			return err
		}
		config, actual := info.Config.Mirror, info.Mirror
		if info.Config.Name != stream || !info.Config.MirrorDirect || config == nil || actual == nil ||
			config.Name != stream || actual.Name != stream || config.External == nil || actual.External == nil ||
			config.External.APIPrefix != writer || actual.External.APIPrefix != writer {
			return errors.New("opposite broker is not the expected state mirror")
		}
		for _, value := range expected {
			subject := "$KV.CSL_STATE." + value.Key
			for {
				msg, err := conn.RequestWithContext(check, api+".DIRECT.GET."+stream+"."+subject, nil)
				if err != nil {
					return err
				}
				revision, _ := strconv.ParseUint(msg.Header.Get("Nats-Sequence"), 10, 64)
				if revision == value.Revision && msg.Header.Get("Nats-Stream") == stream &&
					msg.Header.Get("Nats-Subject") == subject && string(msg.Data) == strconv.FormatBool(value.Allowed) {
					break
				}
				if revision >= value.Revision {
					return errors.New("opposite broker pair value or revision differs")
				}
				select {
				case <-check.Done():
					return check.Err()
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
		return nil
	}
}

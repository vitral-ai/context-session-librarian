package control

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDecodeMirrorLag(t *testing.T) {
	body := `{"server":{"name":"standby"},"data":{"account_details":[
		{"id":"foreign","stream_detail":[{"mirror":{"name":"CSL_MSG","lag":99,"active":12}}]},
		{"id":"tenant","stream_detail":[{"mirror":{"name":"CSL_MSG","lag":5,"active":12}},
		{"mirror":{"name":"KV_CSL_STATE","lag":0,"active":0}},{"mirror":{"name":"OBJ_CSL_FILES","lag":1,"active":12}}]}
	]}}`
	got, err := mirrorLag([]byte(body), "standby", "tenant")
	if err != nil || got["CSL_MSG"] != 5 || got["KV_CSL_STATE"] != 0 || got["OBJ_CSL_FILES"] != 1 {
		t.Fatalf("tenant mirror response was not decoded: %v", err)
	}
	withArchive := strings.Replace(body, `{"mirror":{"name":"OBJ_CSL_FILES","lag":1,"active":12}}`,
		`{"mirror":{"name":"OBJ_CSL_FILES","lag":1,"active":12}},{"name":"CSL_ARCHIVE","mirror":{"name":"CSL_ARCHIVE","lag":3,"active":12}}`, 1)
	if lag, err := mirrorLag([]byte(withArchive), "standby", "tenant"); err != nil || lag[hubArchiveName] != 3 {
		t.Fatalf("archive mirror response was not decoded: %v", err)
	}
	wrongDestination := strings.Replace(withArchive, `"name":"CSL_ARCHIVE","mirror":`, `"name":"OLD_ARCHIVE_COPY","mirror":`, 1)
	wrongDestination = strings.Replace(wrongDestination, `"lag":3`, `"lag":0`, 1)
	wrongSource := strings.Replace(withArchive, `"mirror":{"name":"CSL_ARCHIVE"`, `"mirror":{"name":"CSL_MSG"`, 1)
	for _, response := range []string{wrongDestination, wrongSource} {
		lag, err := mirrorLag([]byte(response), "standby", "tenant")
		if _, present := lag[hubArchiveName]; err != nil || present || lag["CSL_MSG"] != 5 {
			t.Fatalf("wrong archive destination or source affected mirror health: %v, %v", lag, err)
		}
	}
	for _, invalid := range []string{
		strings.Replace(body, "standby", "unexpected", 1),
		strings.Replace(body, "OBJ_CSL_FILES", "irrelevant", 1),
		strings.Replace(body, `"lag":5`, `"lag":5,"error":{"description":"inactive"}`, 1),
		strings.Replace(body, `"lag":5,`, "", 1),
		strings.Replace(body, `"lag":5`, `"lag":null`, 1),
		strings.Replace(body, `,"active":0`, "", 1),
		strings.Replace(body, `"active":0`, `"active":null`, 1),
		strings.Replace(body, `"active":0`, `"active":-1`, 1),
		strings.Replace(body, `"active":0`, `"active":61000000000`, 1),
		strings.Replace(withArchive, `"OBJ_CSL_FILES"`, `"irrelevant"`, 1),
		strings.Replace(withArchive, `"lag":3`, `"lag":null`, 1),
	} {
		if _, err := mirrorLag([]byte(invalid), "standby", "tenant"); err == nil {
			t.Fatal("invalid or unavailable mirror response was treated as healthy")
		}
	}
}

func verifyMirrorNotices(t *testing.T, ctx context.Context, service *Service) {
	t.Helper()
	now := time.Now().UTC()
	var lag uint64
	hasArchive := true
	service.mu.Lock()
	originalClock := service.clock
	service.clock = func() time.Time { return now }
	service.config.MirrorProbe = func(ctx context.Context) (map[string]uint64, error) {
		result := map[string]uint64{"CSL_MSG": 0, "KV_CSL_STATE": 0, "OBJ_CSL_FILES": 0}
		if hasArchive {
			result[hubArchiveName] = lag
		}
		return result, nil
	}
	service.mu.Unlock()
	tick := func(delay time.Duration) {
		t.Helper()
		service.mu.Lock()
		now = now.Add(delay)
		service.mu.Unlock()
		if err := service.monitorMirrors(ctx); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		service.mu.Lock()
		service.clock = originalClock
		service.config.MirrorProbe = nil
		service.mu.Unlock()
	}()
	assertNotices := func(stalled, unavailable, recovered int) {
		t.Helper()
		entries, err := service.messages(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var lagNotices, errors, recovery int
		for _, entry := range entries {
			env := entry.record.Envelope
			switch env.Body {
			case "Mirror replication has lagged more than 60 seconds: CSL_ARCHIVE.":
				lagNotices++
			case "Mirror monitoring unavailable.":
				errors++
			case "Mirror replication recovered.":
				recovery++
			default:
				continue
			}
			if env.Kind != "system" || env.To != "sender" || entry.record.Sequence == 0 {
				t.Fatal("monitor notice did not reach the configured owner")
			}
		}
		if lagNotices != stalled || errors != unavailable || recovery != recovered {
			t.Fatalf("notice counts: lag=%d unavailable=%d recovered=%d", lagNotices, errors, recovery)
		}
	}
	lag = 1
	tick(0)
	tick(time.Minute)
	assertNotices(0, 0, 0)
	tick(time.Second)
	tick(time.Minute)
	assertNotices(1, 0, 0)
	tick(14 * time.Minute)
	assertNotices(2, 0, 0)
	hasArchive = false
	tick(time.Minute)
	tick(5 * time.Minute)
	assertNotices(2, 0, 0)
	tick(time.Second)
	tick(15 * time.Minute)
	assertNotices(2, 1, 0)
	hasArchive, lag = true, 0
	tick(time.Minute)
	tick(time.Minute)
	assertNotices(2, 1, 1)
}

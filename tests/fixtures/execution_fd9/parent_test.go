package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReadyPIDWaitsForPublishedBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		time.Sleep(25 * time.Millisecond)
		result <- os.WriteFile(path, []byte("12345"), 0600)
	}()
	pid, err := waitReadyPID(path, time.Now().Add(time.Second))
	if writeErr := <-result; writeErr != nil {
		t.Fatal(writeErr)
	}
	if err != nil || pid != 12345 {
		t.Fatalf("pid=%d err=%v", pid, err)
	}
}

func TestReadyPIDTimeoutIsClear(t *testing.T) {
	for _, value := range []string{"", "not-a-pid", "0", "-1"} {
		t.Run(value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ready")
			if err := os.WriteFile(path, []byte(value), 0600); err != nil {
				t.Fatal(err)
			}
			pid, err := waitReadyPID(path, time.Now().Add(30*time.Millisecond))
			if pid != 0 || err == nil || !strings.Contains(err.Error(), "child ready timeout: PID file is empty or invalid") {
				t.Fatalf("pid=%d err=%v", pid, err)
			}
		})
	}
	pid, err := waitReadyPID(filepath.Join(t.TempDir(), "absent"), time.Now().Add(30*time.Millisecond))
	if pid != 0 || err == nil || !strings.Contains(err.Error(), "PID file not published") {
		t.Fatalf("pid=%d err=%v", pid, err)
	}
}

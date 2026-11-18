// Synthetic Go/Python process fixture. No native Access or provider publication.
package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func bootNow() int64 {
	var ts syscall.Timespec
	_, _, errno := syscall.Syscall(syscall.SYS_CLOCK_GETTIME, 7, uintptr(unsafe.Pointer(&ts)), 0)
	if errno != 0 {
		panic(errno)
	}
	return ts.Sec*1e9 + ts.Nsec
}
func encoded(v any) []byte { b, e := json.Marshal(v); must(e); return b }
func writeFrame(w io.Writer, b []byte) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(b)))
	_, e := w.Write(header[:])
	must(e)
	_, e = w.Write(b)
	must(e)
}
func readFrame(r io.Reader, limit uint32) ([]byte, error) {
	var header [4]byte
	if _, e := io.ReadFull(r, header[:]); e != nil {
		return nil, e
	}
	n := binary.BigEndian.Uint32(header[:])
	if n == 0 || n > limit {
		return nil, fmt.Errorf("frame limit: %d", n)
	}
	b := make([]byte, n)
	_, e := io.ReadFull(r, b)
	return b, e
}
func opened(path string) *os.File { f, e := os.Open(path); must(e); return f }

func deathSupervisor(args []string) {
	// Own the orphan we deliberately create; reap exactly its PID, not other jobs.
	_, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, 36, 1, 0, 0, 0, 0)
	if errno != 0 {
		panic(errno)
	}
	defer syscall.Syscall6(syscall.SYS_PRCTL, 36, 0, 0, 0, 0, 0)
	inner := exec.Command(os.Args[0], append(args, "inner")...)
	inner.Stderr = os.Stderr
	must(inner.Start())
	defer func() { _ = inner.Process.Kill(); _ = inner.Wait() }()
	root := args[3]
	var childPID int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b, e := os.ReadFile(filepath.Join(root, "ready"))
		if e == nil {
			childPID, e = strconv.Atoi(string(b))
			must(e)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if childPID == 0 {
		panic("child ready timeout")
	}
	must(inner.Process.Kill())
	_ = inner.Wait()
	var status syscall.WaitStatus
	for time.Now().Before(deadline) {
		pid, e := syscall.Wait4(childPID, &status, syscall.WNOHANG, nil)
		must(e)
		if pid == childPID {
			_, e = os.Stat(filepath.Join(root, "effect"))
			if !status.Signaled() || status.Signal() != syscall.SIGKILL || !os.IsNotExist(e) {
				panic("parent death fence failed")
			}
			fmt.Println(string(encoded(map[string]any{"reaped": true, "signal": int(status.Signal()), "effect": false})))
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = syscall.Kill(childPID, syscall.SIGKILL)
	_, _ = syscall.Wait4(childPID, &status, 0, nil)
	panic("parent-death reap timeout")
}

func main() {
	if len(os.Args) < 6 {
		panic("python child repo root scenario required")
	}
	args := os.Args[1:]
	python, child, repo, root, scenario := args[0], args[1], args[2], args[3], args[4]
	if strings.HasPrefix(scenario, "death-") && len(args) == 5 {
		deathSupervisor(args)
		return
	}
	must(os.WriteFile(filepath.Join(root, "package"), []byte("package"), 0600))
	must(os.Mkdir(filepath.Join(root, "codex"), 0700))
	boot, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
	must(e)
	now := bootNow()
	duration := int64(2 * time.Second)
	if strings.Contains(scenario, "pressure") || scenario == "prehello-silence" || scenario == "partial-deny" || scenario == "setup-stall" {
		duration = int64(400 * time.Millisecond)
	}
	authority := map[string]any{"boot_id": strings.TrimSpace(string(boot)), "start_boottime_ns": strconv.FormatInt(now, 10), "end_boottime_ns": strconv.FormatInt(now+duration, 10)}
	info, e := os.Stat(root)
	must(e)
	st := info.Sys().(*syscall.Stat_t)
	digest := sha256.Sum256([]byte("package"))
	req := map[string]any{"schema_version": 1, "operation": "import", "execution_id": "re_" + strings.Repeat("1", 64), "import_key": "ri_" + strings.Repeat("2", 64), "provider": "codex", "source_id": "synthetic", "package_size": 7, "package_sha256": hex.EncodeToString(digest[:]), "owner": strings.Repeat("1", 24), "host_id": "ses_fixture", "assignment_id": "33333333-3333-4333-8333-333333333333", "generation": "1", "boot_id": authority["boot_id"], "cwd": root, "cwd_device": strconv.FormatUint(uint64(st.Dev), 10), "cwd_inode": strconv.FormatUint(st.Ino, 10), "authority": authority}
	raw := encoded(req)
	commitment := sha256.Sum256(raw)
	rr, rw, e := os.Pipe()
	must(e)
	sr, sw, e := os.Pipe()
	must(e)
	// A real small result pipe guarantees pressure before a 16KiB result ends.
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, sw.Fd(), syscall.F_SETPIPE_SZ, 4096)
	if errno != 0 {
		panic(errno)
	}
	pair, e := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	must(e)
	parentFile := os.NewFile(uintptr(pair[0]), "parent")
	childFile := os.NewFile(uintptr(pair[1]), "child")
	conn, e := net.FileConn(parentFile)
	must(e)
	must(parentFile.Close())
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	extras := []*os.File{rr, sw, opened(filepath.Join(root, "package")), opened(root), opened(filepath.Join(root, "codex")), opened("/dev/null"), childFile}
	cmd := exec.Command(python, "-I", "-B", child, repo, root, scenario, base64.StdEncoding.EncodeToString(raw))
	cmd.ExtraFiles = extras
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL, Setpgid: true}
	// Pdeathsig is tied to the spawning OS thread, not Go's abstract goroutine.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	must(cmd.Start())
	// The original deadline supervises even Python startup before Channel exists.
	// Poll BOOTTIME itself: a Go timer's monotonic clock alone excludes suspend.
	var expired atomic.Bool
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			if bootNow() >= now+duration {
				expired.Store(true)
				_ = cmd.Process.Kill()
				_ = conn.Close()
				return
			}
			select {
			case <-watchDone:
				return
			case <-ticker.C:
			}
		}
	}()
	for _, f := range extras {
		must(f.Close())
	} // includes every inherited peer copy
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	defer rw.Close()
	defer sr.Close()
	if scenario != "death-prehello" && scenario != "prehello-silence" {
		writeFrame(conn, encoded(map[string]any{"type": "hello", "request_sha256": hex.EncodeToString(commitment[:]), "authority": authority}))
	}
	// Request fits the fixture pipe; truncation keeps its original advertised size.
	if scenario == "request-oversize" {
		var h [4]byte
		binary.BigEndian.PutUint32(h[:], 16385)
		_, e = rw.Write(h[:])
		must(e)
	} else if scenario == "request-pressure" {
		_, e = rw.Write([]byte{0, 0, 4, 0, '{'})
		must(e)
	} else if scenario == "request-header-truncated" {
		_, e = rw.Write([]byte{0, 0, 4})
		must(e)
	} else if scenario == "request-truncated" {
		var h [4]byte
		binary.BigEndian.PutUint32(h[:], uint32(len(raw)))
		_, e = rw.Write(h[:])
		must(e)
		_, e = rw.Write(raw[:len(raw)-1])
		must(e)
	} else {
		if scenario == "request-mismatch" {
			req["source_id"] = "other"
			raw = encoded(req)
		}
		writeFrame(rw, raw)
		if scenario == "request-trailing" {
			_, e = rw.Write([]byte{'x'})
			must(e)
		}
	}
	if scenario != "request-pressure" {
		must(rw.Close())
	}
	resultCh := make(chan []byte, 1)
	if scenario != "result-pressure" {
		go func() { b, _ := readFrame(sr, 16384); resultCh <- b }()
	}
	captures := 0
	for {
		b, err := readFrame(conn, 1024)
		if err != nil {
			break
		}
		var message map[string]any
		must(json.Unmarshal(b, &message))
		if message["type"] != "check" {
			panic("non-check child message")
		}
		// This fixture checker is deliberately synthetic, but its original
		// BOOTTIME is still enforced on BOTH sides; progress never renews it.
		if bootNow() >= now+duration {
			writeFrame(conn, encoded(map[string]any{"type": "deny", "reason": "original_expired"}))
			break
		}
		if scenario == "death-effect" && message["phase"] == "effect" {
			select {}
		}
		if scenario == "deny" {
			writeFrame(conn, encoded(map[string]any{"type": "deny", "reason": "fixture_revoked"}))
			break
		}
		if scenario == "close" {
			must(conn.Close())
			break
		}
		if scenario == "partial-deny" {
			_, e = conn.Write([]byte{0})
			must(e)
			time.Sleep(time.Second)
			break
		}
		response := map[string]any{"type": "ok", "seq": message["seq"], "authority": authority}
		if scenario == "wrong-seq" {
			response["seq"] = "999"
		}
		if scenario == "wrong-bound" {
			response["authority"] = map[string]any{}
		}
		if message["phase"] == "captured" {
			must(os.WriteFile(filepath.Join(root, "package"), []byte("changed"), 0600))
		}
		if message["phase"] == "capture" {
			captures++
			if scenario == "capture-tamper" && captures == 2 {
				must(os.WriteFile(filepath.Join(root, "package"), []byte("changed"), 0600))
			}
		}
		writeFrame(conn, encoded(response))
	}
	childErr := cmd.Wait()
	if childErr != nil {
		if !expired.Load() {
			panic(childErr)
		}
		// A killed in-flight child has no completed result. Report the actual
		// parent fence, never manufacture a child's successful response.
		fmt.Println(string(encoded(map[string]any{"status": "refused", "error": "parent_original_bound_lost", "parent_killed": true})))
		return
	}
	report, e := os.ReadFile(filepath.Join(root, "report.json"))
	must(e)
	if scenario == "success" {
		select {
		case result := <-resultCh:
			if string(result) != string(report) {
				panic("result differs")
			}
		case <-time.After(time.Second):
			panic("missing result")
		}
	}
	fmt.Println(string(report))
}

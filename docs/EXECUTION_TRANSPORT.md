# Private execution transport

`csl_execution_transport` is an unwired Linux library. No command, production
launcher, Access serialization, production native import/publication, target
nonce adapter or model Start uses it. A passing fixture is source evidence,
not native authorization or imported-session qualification.

`Channel` consumes the sole child FD9 Unix stream endpoint and verifies actual
`SO_PEERCRED` PID/UID against the independently supplied expected parent and
`getppid()`. It closes on context exit and on failed construction. The launcher
must close every inherited peer copy and own the original pre-hello context,
Pdeathsig, process group, cancellation and bounded kill/reap. Request metadata
does not establish any of those facts. There is no fallback checker or pathname
socket. A closed channel cannot reconnect or renew its original bound.

The separately provided, validated launch request supplies the immutable original
Linux boot/start/end and canonical request SHA. It must be available before
framing begins; do not read an unbounded request first to discover its deadline.
The parent sends one `hello` containing exactly `type`, `request_sha256` and
`authority`. Admission requires the exact original values. The commitment binds
canonical request bytes (not incidental JSON whitespace). It belongs to this
attempt; the persistent import key/mapping remains in the existing journal.
A separately authorized lookup gets its own channel without refreshing an old
reservation's publication authority.

Before each effect the child sends `check` with a positive, increasing uint64
decimal-string `seq` and bounded phase. Parent replies with exactly `ok`, the
same sequence and original `authority`, only after checking its real original
Access plus assignment and cwd. Alternatively it sends `deny` with a bounded
literal reason. This module has no parent authority implementation; the Go
fixture's synthetic checker is explicitly not that production integration.
One check is in flight; failures are sticky. Local checks reject wrong boot,
backwards time and expiry equality. Observed denial keeps its reason.

Frames use four-byte big-endian lengths. Control is capped at 1024 bytes before
payload allocation; request and result are capped at 16 KiB. Reads/writes are
incremental and nonblocking. Every selector includes FD9 and waits at most the
lesser of original remaining BOOTTIME and 50 ms. That polling quantum grants no
extension. Queued bytes outside an expected response, including partial denial,
duplicate frames, unsolicited OK, malformed JSON, EOF and changed bounds fence
the next effect. A partial expected frame can wait only until the original end.
The final nonblocking liveness read follows the clock sample, including when
that sample expires or fails: loss queued during a blocking boot-ID read cannot
survive into the next write or successful guard return. Known denial keeps its
exact reason ahead of clock diagnostics. Framing/selector completion and capture
callbacks use the same ordering.
An in-flight syscall may already commit; this is a pre-dispatch guard, not an
atomic transaction with the identity authority. Retain uncertainty and intents.

`read_request` borrows the sole FD3 read-pipe endpoint, requires one complete
bounded frame and writer EOF, and compares the parsed commitment. `write_result`
borrows the sole FD4 write-pipe endpoint; a full pipe cannot extend the bound.
The caller closes those borrowed endpoints. Nonblocking changes affect the open
file description, so a launcher must not retain alias copies that it still uses.
FD5 capture remains child-owned, whole-size/SHA qualified and sealed before
parsing; its checker is `Channel.check`. Regular-file I/O is not selector-
timeoutable; the launcher's killable child and post-return check remain required.

## Reproducible synthetic qualification

`tests/fixtures/execution_fd9/parent.go` launches a fresh Python interpreter with
actual ExtraFiles 3..9: request pipe, result pipe, read-only package, journal dir,
Codex dir, unused-provider `/dev/null`, control endpoint. Python measures this
table rather than asserting only a proposed Go slice. Go locks the spawning OS
thread through Wait, sets Pdeathsig SIGKILL and Setpgid, and closes all inherited
endpoint copies in the parent. No production home or histories are read.

An original-BOOTTIME parent watcher also kills and reaps a deliberately stalled
Python bootstrap before its guard exists. This is a fixture supervisor, not a
production native stop policy or a replacement for its original context.
The fixture checks real framing/request EOF, capture seals/offset/hash and stable
bytes after the original inode changes. It exercises request silence/truncation/
oversize, wrong sequence/bound, denial/EOF/partial denial and a deliberately full
4 KiB result pipe. Original-clock checks run on both sides. Unit socketpair
cases add every control-frame cut, duplicate keys/frames, control-write pressure,
bad peer and sticky failure. Unit-only same-process peer tests explicitly mock
getppid; the actual Go/Python tests do not.

For parent-death cuts a private Go supervisor becomes a child subreaper, kills
its own Go launcher before hello or before an effect check returns, then reaps
the exact Python PID and checks SIGKILL with no effect marker. It creates no
daemon, unit, privileged path or new container. The separate private
[publisher tests](EXECUTION_PUBLISHER.md) now add synthetic keeper publication,
cold SIGKILL recovery and an actual receipt round-trip on this channel. They
also kill/reap the real parent before its first link, with zero target files.
No production Access or native launcher is supplied by either fixture.

Run `python3 -B -m unittest -v tests.test_execution_transport` with Go on PATH.
Without Go/Linux the language-pair tests report SKIP; set
`CSL_REQUIRE_EXECUTION_PROCESS_TESTS=1` to require them. The transfer CI job pins
Go 1.27.4 and sets that requirement. Core runtime remains standard-library-only.
Shared exact canonical request preimages cover HTML characters, Unicode line
separators, non-ASCII and ordinary escapes (including literal backslash-u text).
The Go fixture emits the existing CSL canonical bytes rather than default Go
JSON escaping; incidental wire whitespace still does not change semantic identity.
Separate real-fork/pipe regressions queue deny, partial control or EOF during the
last clock call and require zero result/control bytes and zero capture creation.
Production cancellation, parent-death setup and endpoint closure must still be
qualified with the real native launcher before any positive caller is wired.

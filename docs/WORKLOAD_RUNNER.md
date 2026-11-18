# Bounded Workload Runner Source

This adds an opt-in Python source runner and literal-argument job wrapper. It
reuses the reviewed workload identity, sanitized providers and existing CSL
Collector/journal/uploader. It does not build or activate a workload image,
register credentials, approve a person, deploy a route or qualify A8.
Existing desktop/raw CLI commands stay unchanged.

The source entry point is `python3 -S csl_workload_runner.py`. With no arguments
it watches the configured transcript-only mount. Arguments after `--` are one
literal producer command, launched once without a shell, retries, version probes
or implicit agent selection. No new installed `vitral` command is claimed here;
the reviewed image/entry-point assembly is a separate contribution.

## Configuration And State

The packaging ABI is the merged chart/ACA interface from #43. Required provider
and profile pairs come from `packaging/workload-collector/profiles.json`, checked
against the actual codec constants. Missing, auto, unknown or mismatched profiles
refuse. There is no version inference by executing a harness or reading homes.
The future image must include that exact matrix at the code-relative path.

Configuration reads only the documented `VITRAL_COLLECTOR_*` and
`VITRAL_LOCATION_*` inputs. Redaction must be `required`; there is no raw/off
fallback. One canonical HTTPS origin, fixed public client, frozen metadata,
bounded flush/spool values and disjoint absolute input/state mounts are checked
inside the runtime API as well as the environment parser.

The mount root in `VITRAL_COLLECTOR_STATE` is left untouched, including an fsGroup
0770 root. The runner owns private 0700 children: `identity`, `auth`, `sync`,
`projection`, `runtime` and `worker-home`. `WorkloadKey` uses STATE/identity, not
the volume root. The optional public `VITRAL_COLLECTOR_TRUST_CA` is explicitly
forwarded to HTTPS with certificate and hostname validation still enabled.
Kubernetes' normal public-CA ConfigMap symlink projection is supported.

Credentials must already have been obtained through the separately reviewed
person-approved workload flow in these private children. Watching never starts
approval, a browser, a refresh-less fallback or a new enrollment. It may use the
existing client's bound refresh protocol. A missing/revoked/uncertain identity
cannot fall back to desktop credentials. There is no harness credential access.

The exclusive runner claim binds provider/profile, origin, transcript root and
workload metadata. Its private receipt directory cannot be adopted or cleaned
without that claim. The sync journal is additionally bound to the approved
account/device. A populated unbound journal refuses before registration/upload.
Changing that context is not an automatic cache reset. Receipts are private,
bounded, run-ID/scope checked, never authoritative instead of durable uploader
ACKs. Restart removes only generated, owned regular receipt/orphan files from
its claimed directory, not keys, credentials or unrelated data.

## Collection And Limits

Only the explicitly mounted transcript root is walked. The scan is capped at
4096 entries, eight directory levels, 1024-byte relative paths and 32 JSONL
files. It uses no-follow directory descriptors and refuses symlinks/special
files. It never searches a harness home, project registry, DB/WAL, referenced
tool-result path or credential store. Files must remain available until ACK;
observed paths are journaled and a disappeared source keeps the run pending.
The first source rotates durably before each pass, so repeated network failure
or a time cut in an early source cannot permanently hide the later sources.
Malformed/unqualified inputs still refuse the workload, never a raw fallback or
a silently skipped source. Large-input throughput remains an image qualification.

Every pass uses the merged qualified codecs, deterministic redaction and
sanitized coordinates. Registry device IDs come from the approved identity,
not the desktop journal's random default. Packages remain unavailable. Source
cursors/hashes appear only in private local receipts, never in upload metadata
or stdout. Metadata/content/view delivery still uses the existing durable
plans, exact range/hash validation and idempotency keys.

The configured sanitized spool budget has a 1 MiB runtime minimum. Half is
reserved for projection, including atomic replacements. Per-source output/view
limits are at most budget/16 (and at most 16 MiB). One raw snapshot plus JSON
view snapshots, including up to 3x UTF-8-to-ASCII expansion, then needs at most
another quarter. Input record limits tighten with that output bound. Fixed
identity/ACK journals are separate private state; packaging's overall volume
limit remains necessary. This is not a measured container memory guarantee.

Passes run in separate processes with a minimal environment: trusted PATH,
LANG, a private HOME and bytecode suppression. No cloud, kube or harness secrets
are copied into that environment. Configuration contains only explicit public
paths/metadata/settings in a private file; tokens are not in argv or env. Child
stdout/stderr and producer stdout/stderr are suppressed rather than collecting
transcripts or secrets in logs. The producer inherits its own environment
without the collector inspecting it. Only exit/count/boolean evidence is printed.

## Stop And Completion

The watch cadence is 100 ms, with 250 ms between finished passes. A live pass
has a 60-second ceiling. Its Linux PDEATHSIG binding prevents it from continuing
uploads after its supervisor dies; parent identity is rechecked to close the
fork-to-prctl race. OpenSSL helpers are in the worker's process group. A stalled
worker is killed, not abandoned in a background thread or replaced while alive.

On producer exit or SIGTERM/SIGINT, a supervised producer group gets a bounded
five-second TERM/KILL attempt. The final drain then gets at most the configured
1-20 seconds, using CLOCK_BOOTTIME. The final half second is reserved for killing
and reaping. Both the worker launch and every HTTPS opener check that same
deadline; proof-based auth also observes it. Suspend cannot admit later writes.
An unreaped worker is reported as such and can never make a completed close.

A fresh final worker runs after the producer stop attempt. It checks complete
EOF and an unchanged input set after BOTH content and view ACKs. Partial tails,
source append/rewrite during ACK, deleted inputs, outage, lost ACK, deadline or
identity uncertainty remain pending. Unknown remote results are recovered by
the existing journal/replay rules, never by retrying the producer command.
The parent does not hash large sources outside the killable worker.

`complete` describes delivery/close, not command success. A known producer exit
code is preserved when delivery completes; pending delivery exits 2. Reports
contain `complete`, `pending`, `producer_quiesced`, `writer_exit`, `synced` and
`worker_reaped`, not transcripts, paths, source hashes, tokens or account emails.

**Limit:** command-mode quiescence covers the declared sole producer's process
group, not hostile same-UID code or writers that escape it. Such commands are
not qualified, and no general native harness/containment claim is made. Bare
watch mode has no authoritative external-producer-stop witness, so it can flush
ACKed prefixes but deliberately reports pending rather than a completed close.
Packaging/external-producer finalization and actual harness lifecycle proof
remain required before image/end-to-end acceptance. At least 30 seconds of
packaging termination grace is still required; this code does not alter it.

## Local Evidence Only

Synthetic stdlib tests exercise actual subprocesses, literal argv, real watch
updates, SIGTERM final drain, unknown ACK/restart, parent death, source races,
private modes, scope binding, bounded scans and clock jumps. An owned HTTPS peer
and OpenSSL key exercise the real sanitizer/uploader and trusted test CA.
The peer checks a synthetic token value; it is NOT real W7/Mongo authorization
or a registry-server qualification. Healthy synthetic markers are measured to
ACK within three seconds; no UI visibility or production timing is claimed.

No actual Codex/Claude/Gemini invocation, provider call, harness login, personal
transcript, real identity enrollment/refresh, image, kind/ACA, UI, cloud, new
access or spend is exercised. The three profile formats remain exactly their
previously qualified synthetic subsets, not expanded by the job wrapper.

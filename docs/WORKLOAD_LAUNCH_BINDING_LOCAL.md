# Private Fresh Codex Binding

Local source integration, default off. This is not source clearance, an image,
native/model run, delivery authorization or A8 evidence. Existing `AcceptanceDriver`
still requires an independently selected UUID; this separate private driver does
not set `OBSERVE_SESSION` in the product worker or change its CLI/profile.

## Owner Interface

```python
lease = PreparedLaunch()
driver = LaunchAcceptanceDriver(source=reviewed_runtime_fingerprint,
    scope=scope(config), account_id=expected_approved_account,
    device_id=expected_approved_device, prepared=lease)
report = packaged_prepare.prepare(literal_arguments, own_environment, launch=driver)
evidence = driver.result
```

Import these classes from `csl_workload_launch_binding`. `packaged_prepare` is
the reviewed `/opt/vitral/codex/prepare.py` module in the sole-producer artifact.
The caller must be the independently reviewed private PID1/UID10001 owner driver,
not a replacement product CLI or an arbitrary preparation callback. The expected
account/device and runtime contents come from its approved independent inputs,
never helper output, a discovered filename, another run or copied host journals.
The private `prepare(..., launch=driver)` return is Runner's report, not an exit
code; ordinary `prepare()`/main behavior is unchanged. `driver.result` retains the
existing three fixed surfaces/booleans/matched UUIDs or `None`, not a PASS flag.
Omit `OBSERVE_SESSION` in this mode; never guess or persist a selector to enable
it. The actual accepted native header supplies the UUID through private binding.

This path uses the **actual** packaged validator, rejects a substituted validator
or `run` callback, and selects the existing fixed Python/native-exec/Codex argv.
It applies no configuration/settings from native records. Literal native args,
preparation BOOTTIME cap, USR1/TERM behavior, kernel guardian and final flush are
the existing mechanisms. Preparation cancellation closes the optional lease and
helper even if Runner was never reached. Reusing a driver/lease is unsupported.

## Trusted Facts Before The First Observed PUT

The env4 carrier input is exact tree `1bc19adea745852c1e029e5119a78927c4404d12`
(attributed head `c3dd35c8e9fb06fcdfdf3a7c7ef5ae26df35960b`, base8180399).
Its original exported five files and full patch were verified separately. This
integration extends those facts rather than pretending the tuple was already a
lifetime witness. Actual empty dual-view validation now also holds an opened
root descriptor until the run ends. At USR1 the original dev/inode tuple must
match; the existing native-exec child independently checks the empty aliases and
same tuple again. A held inode cannot silently become a newly reused inode.

The original Producer samples its boot and run once. Its guardian samples its
own and gated writer's `/proc/PID/stat` field22 start ticks with parent checks,
persists the original identities, and optionally writes the same six closed
fields to an owned nonblocking anonymous pipe before gate release. Parent-side
binding reads only this pipe, never disk-reading `launch_identity()` or `/proc`
on the stop/request path. Missing sideband, changed run/boot/guardian/writer,
unavailable start ticks and mismatched scope cannot authorize a binding. The
original disk-reading carrier API remains available off-path, not used here.

Only the actual provider's successful **complete batch** supplies a native UUID.
Full pinned-profile projection, redaction/provenance/history/media rules and
prefix verification run first. Binding additionally requires one source and one
canonical UUID, explicit `session_meta.payload.source=exec`, declared0.169.0,
nonpartial complete records, and a stable native file inode and policy. No first
row, missing source declaration, unknown record, filename UUID or synthetic
callback can select it. The worker duplicates the root descriptors from its
existing anchored read and prefix-check opens; there is no rescan/reprojection
or optional path stat on product control. Descriptor passing is bounded private
AF_UNIX IPC, not a listener or filesystem service.

The parent generates run/worker/seal/launch nonces independently and retains the
original Producer object/identity. Its off-control binder matches the actual
worker admission message and both opened root descriptors against the held
preparation root. Exactly one native birth may be assigned for the run; later
passes must confirm the same session, relative source, inode and policy. The
helper never supplies a UUID or expected nonce. Parent approval selects only the
helper's private observation configuration after independently checked auth/source
preparation. The product worker configuration stays session-unselected.

Before the first eligible session PUT, the worker checks the parent grant once,
nonblockingly. It matches the original worker/seal/deadline/launch and its own
validated birth before attaching the existing immutable Request-body observer.
Absent, stale, late or mismatched grant loses evidence; the ORIGINAL request
proceeds. Cold or slow binding can therefore make a legitimate run unqualified.
There is no wait, replay, reupload, deadline renewal, prefix adoption or retry to
make evidence qualify. Earlier failed coverage cannot be healed by a later pass.

Before any birth, a worker can find the intentionally empty source. Only that
actual return before any Collector/registry call emits a zero-session-PUT notice
and a matching private receipt bound to the current run/worker/seal/launch and
owner/source/scope. Both must be received and checked before a new assignment.
This keeps pre-birth eligibility, not upload evidence; its result stays `None`.
Missing notices/receipts, worker failure, deadline expiry, nonzero PUTs, identity
mismatch and any post-birth coverage gap still permanently invalidate evidence.
The parent never waits for this notice or starts an extra pass to obtain it.

Final worker receipts retain the parent-approved birth and kernel fact privately.
The reader matches them against parent memory, then uses merged67's original
product status, persisted ACK/plan/proof/EOF and same-deadline checks before
cleanup. It cannot adopt a native UUID, launch or worker from a helper seal.
Existing content/view coverage, package/snapshot body scope and fixed canary
checks are unchanged. No bearer/header/native body or exception is logged.

## Bounds And Independence

Admission/grant messages cap at4096 bytes and two passed descriptors; capture
retains at most two worker root FDs. One binder exists at a time; a stalled
previous binder cannot spawn a replacement. Parent stop never joins it. All
optional source/auth/journal work remains in the killable helper; only descriptor
metadata checks run in the daemon binder. Producer/HTTP/control paths use bounded
nonblocking IPC. Socket/thread/descriptor failures invalidate evidence rather
than changing product admission or completion. FD lifetime is single-owner for
the sideband; stop closes the product lifetime pipe, not a concurrently read FD.

Same-UID forged code/current-nonce state, an untrusted caller manufacturing a
lease, unauthorized second writers/remounts, whole-host/cgroup resource failure
and shared-device scheduling remain outside the trusted deployment boundary.
This is not cryptographic tamper resistance, global historical UUID uniqueness,
or proof against an administrator. The reviewed wrapper/Job must provide the
sole real producer, empty approved mounts and independent own sign-in/state.

## Evidence And Artifact Limits

Owned tests exercise actual guardian/worker/provider/reader/TLS code with safe
native-shaped inputs, not an installed real agent. Positive fixtures explicitly
pad readiness independently of production; negative controls do not grant that
padding to a stalled optional binder. Source tests alone do not select a runtime.

Module inventory/COPY/context include `csl_workload_launch_binding.py`. Profiles
and frozen input hashes/base pins are unchanged. The existing sole-producer
overlay still pins old collector manifest8691a8fd: it does **not** contain this
module or merged67 observation source. A fresh reviewed collector build and an
actual verified manifest are an owner prerequisite for repinning the overlay;
no digest is invented or silently substituted here. Owner-private PID1 driver
wiring must then consume this reviewed API. No native/model/device flow, image
build/import, Pod, cloud operation or new access is authorized by this source.

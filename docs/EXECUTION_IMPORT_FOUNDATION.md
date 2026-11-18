# Private execution-import foundations

This is an unwired library for Linux. It adds no CLI flags, package download,
provider publication, native indexing, process launch, model call or automatic
cleanup. Ordinary import still allocates fresh IDs exactly as before. No
`reserved` response means a session was imported, running or remembered.

`csl_execution.request` parses a closed version-1 request of at most16KiB. A
request binds the W4 execution/import key, provider/source, exact whole package
size/SHA, owner/host/assignment/generation/boot, cwd identity and original Linux
BOOTTIME start/end. Host IDs use W7's ses_ syntax; generation is a positive
uint64 string. Unknown/duplicate keys, booleans as integers, malformed paths and
noncanonical numbers refuse. These fields are metadata, not authentication.

Callers must inject a real original-authority checker. `Guard` composes it with
the unchanged same-boot bound; failure/backwards clock refuses, known revocation
is checked before expiry. Nothing derives another deadline from progress or
serializes native `Access`. There is no default always-allow production caller.
The actual native inherited-FD/framed FD9/Pdeathsig template is NOT implemented
or qualified here. That remains a prerequisite to any child/CLI wiring.

`capture` borrows only an offset-zero read-only regular FD with a positive caller
cap. Bounded `pread` never changes that FD's offset. It checks the original
checker around each read/write and requires exact expected length/outer SHA and
stable input metadata. It creates an owner0600 Linux memfd with all WRITE/GROW/
SHRINK/SEAL seals, then hands only that stable object to ZIP validation. A
same-inode same-size rewrite to another valid archive refuses. Later mutation
of the original does not change the capture; writes to the capture get EPERM.
The memfd is volatile, has nlink0, is closed by its owner, and proves no durable
staging or cold-publication outcome. No network or fallback pathname is read. The original checker repeats after
sealing and final qualification before the owned capture is exposed; every failed
completion closes its raw or wrapped memfd.

`plan` runs the existing bundle validators against that sealed FD and requires
one raw Codex or Claude source. For referenced Codex histories it reuses the
existing unpack/lineage validator in owned scratch and allocates the COMPLETE
rollout/owner map. It does not invoke today's fresh-UUID importer. The optional
`mapping` argument to `csl_codex_history.restore` repeats those IDs while rewriting
into caller staging only; the ordinary absent-map behavior is unchanged.

`csl_durable` holds qualified directory FDs, checks private root ownership/mode
and every non-writable ancestor, uses a nonblocking exclusive lock and writes
exclusive canonical metadata relative to that FD. It reuses backup's canonical
encoder; it does NOT blindly reuse helpers that chmod caller parents. Existing
backup APIs are unchanged. No foreign directory is repaired or adopted.

`reserve` retains original authority and the first complete mapping under one
`ri_`-keyed row. Exact repeats return it; changed original bounds conflict.
`lookup` never creates anything and may read the original reservation under a
separately checked current metadata-read bound. It returns only a validated
`reserved` row, no imported result. Unknown fsync acknowledgement retains its
exact final reference rather than rolling it back or manufacturing a new child.
Only a dedicated missing-at-initial-open result can admit first creation.
Disappearance after observing a row remains uncertain and cannot allocate another
map. All metadata opens are nonblocking before regular-file/mode/owner/link
qualification; a FIFO cannot hold a lookup hostage. Replaced/linked/insecure
records refuse. There is no TTL or actor.

Still required: reviewed identity-owned target proof and person authority;
actual Go/Python FD launch/lifetime/EOF/revocation/backpressure qualification;
keeper-backed destination publication with real SIGKILL cuts; exact result and
provenance receipts; qualified manifest-scoped erasure; the native loading/model
and A7/A10 tests. Existing harness state and credentials are never fixtures.

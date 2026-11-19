# Private Codex publication and cold recovery

Current R3 behavior and author evidence are recorded in
[EXECUTION_PUBLISHER_R3.md](EXECUTION_PUBLISHER_R3.md). External Claude/Fable
round2 remained CHANGES REQUIRED; these author corrections are not clearance.

`csl_import_execution` is a Linux library, deliberately absent from the CLI and
all production callers. It installs synthetic/test-authorized Codex raw singleton
and uncompressed complete history_base graphs through one member engine. Claude
publication explicitly refuses before effects; existing reservation and read-only
lookup remain unchanged. Ordinary fresh-ID import/receive/backup behavior remains
unchanged. There is no index/cache refresh, native model, Start, child sync or
positive W4/A7/A10 claim.

The private callable is
`publish(journal_fd, codex_home_fd, request, captured, check, *, roots, fault=None)`.
Descriptors are borrowed. `roots` contains independently qualified held `journal`
and `home` `{device,inode}` strings; `identity(fd)` only reads metadata and grants
no authority. The caller supplies the real original nonrenewing checker, covering
native Access, current assignment, cwd and clock/channel. Missing checker or root
context refuses. Request fields/16KiB bound and sealed capture are unchanged.
There is no native constructor or production launcher for this callable.

Before effects, held roots, non-writable ancestors and current exact cwd identity
are checked repeatedly. Destination directories are traversed relative to the
held provider root with O_NOFOLLOW and strict owned0700 checks; existing foreign
directories are never chmod-repaired. The original checker surrounds writes,
fsync/link/unlink and also checks syscall-error/unknown paths. Ownership intent
survives a dispatched unknown; filesystem effects are not a transaction with a
remote identity authority. Inode replacement after the final pre-dispatch check
can still produce an unknown effect, which is refused and retained.

The existing `ri_` record is deliberately versioned to v2 for publisher states.
Existing strict v1 reserved records can continue with their exact map/original
bound; no planning/reserved row is imported. The complete mapping and fixed
singleton rewrite time precede preparation of published bytes; initial planning
may validate owned raw scratch before that mapping is committed. Graph rewrite uses the existing complete strict map and
source timestamps/offset validator, with optional per-record original checks.
Only the child-owned size/SHA-qualified sealed capture is parser input.

R3 has closed internal schema3 keys `v, revision, intent, plan, state, roots,
rewrite_at, planning, workspace, members, receipt`. The earlier unpublished
publisher shapes are refused rather than silently adopted. Legacy v1 reservation
semantics remain unchanged. Internal metadata is capped at256KiB and at33 graph members, distinct
from the16KiB request/result cap. Current file2GiB/total4GiB/graph-depth32 caps
remain ceilings; the execution/capture caller cap is never increased. Singleton
v3 companions and compressed graphs explicitly refuse. Member records bind the
complete source/child rollout and owner IDs, canonical relative path/role/order,
size/SHA and exact keeper inode. All bytes must validate and fsync before prepared.

Private metadata publication uses Linux renameat2(RENAME_NOREPLACE) for first
create; unavailable support fails closed. Under the qualified operation lock,
expected exact prior bytes/revision are re-read before atomic replacement and
parent fsync. Metadata stays nlink1 through process death. A former link-create
record left with nlink2 remains refused by unchanged `csl_durable.regular`.
Uncommitted temporary metadata or keeper custody stays pending; it is never
adopted/removed by basename or SHA. A missing final row plus an unknown first
metadata stage does not allocate another mapping.

The operation lock now yields a held-FD/name lifetime check. Every publisher
guard repeats it after blocking authority checks and through each effect. Replacing
or removing that lock fences the original actor even if a second actor acquired a
new named lock. Metadata additionally stays pinned by an owned FD: same-content
replacement is not accepted. Only the exact old/new inodes of this actor's own
in-flight rename are admitted, then current custody moves to its new pinned
record. Old pins close promptly; record bytes and inode are rechecked before
advancement/readback. Active destination parents are walked again from the held
home through O_NOFOLLOW components and must equal their original held FDs.

Scratch is explicitly owned, not repeatedly randomized or retained forever.
An initial durable `planning` intent precedes the complete reserved mapping.
Fixed `.planning-<import_key>` and `.publisher-<import_key>` roots each carry a
capped512-entry path/kind/inode ledger. Every root/component is created explicitly
0700 and file0600, independent of umask002/027; no foreign chmod repair occurs.
Inodes are acknowledged before data writes. Known owned partial work can reset
under the same original guard/lock, after preflight checks every object against
that ledger. Unknown created-before-ack, extra/replaced/linked/symlinked objects
remain bounded Pending and are never adopted by name/hash. The complete mapping
remains immutable once reserved; no duplicate child is allocated during rewrite
recovery. A completed rewrite fsyncs members before preparing.

After the durable receipt and keeper retirement, checked ledger files/directories
are removed with write-ahead deleting/deleted and retiring/retired progress. Cold
cleanup reconciles only those exact owned identities, including an unlink whose
acknowledgement was lost. Cleanup failure preserves the receipt, never regresses
import or deletes an unrelated object. Success removes both raw scratch trees.
Maximum in-flight graph scratch remains one4GiB planning graph plus one4GiB
unpacked graph and one4GiB rewritten graph (each file2GiB); guarded rewriting
enforces the output caps while writing. Native staging quota and actual account
erasure/post-Start integration remain separate requirements, not an A10 clearance.

Members advance planned→allocating→writing→kept→publishing→published, with durable
write-ahead intent and owned same-filesystem keepers. Ancestors publish first,
root last. Existing target adoption requires that exact keeper inode, exact bytes
and expected nlink2; identical foreign bytes and nlink3 refuse. Partial writes to
an acknowledged owned writing inode can be repeated only under the same original
authorized attempt. Keeper creation before inode acknowledgement stays pending.
An allocating intent with proven absence can repeat exclusive creation; an
existing unacknowledged keeper remains Pending even if its bytes match.
All custody remains on failure; there is no exception rollback of destinations.

After all installed members are proven, a durable closed receipt binds:
`schema_version=1`, `kind=execution_import_result`, `state=imported`,
`validation=native_unverified`, execution/import key, immutable business
`request_sha256` (canonical stored intent excluding attempt authority), package
size/SHA, provider, imported root ID, full mapping digest, target binding digest,
cwd identity digest and installed inventory/provenance digest. Digests are not
native authority. Lookup verifies exact receipt semantics and filesystem custody;
JSON booleans cannot stand in for typed integer fields.

The durable `retiring_keepers` state precedes keeper unlink. Each member records
retiring/retired, allowing cold recovery to distinguish legitimate nlink2→1
without accepting nlink3. Receipt does not regress on cleanup failure. Complete
provenance remains after keeper retirement. Normal revoke preserves imported
copies; no broad purge API is introduced. Post-Start changes and account-erasure
producer integration require separately reviewed native update/cleanup provenance.

`lookup(journal_fd, home_fd, request, check, *, roots)` is read-only: no lock/stage
creation, publication, receipt advancement or UUID/time allocation. Missing row
returns not_prepared; otherwise it returns last proven state and receipt only if
durably present and validated. A fresh metadata-read bound may read the same
business identity but cannot renew publication. Import retry requires the exact
stored original bound. Files alone never create an imported receipt.
If verification races legitimate publisher progress, lookup can re-read and
reconcile at most three verification attempts under the SAME read bound. It
requires a strictly newer validated revision, immutable request/target, the same
complete mapping once reserved, the same receipt once committed, and monotonic
member phases. This also covers qualified pre-receipt progress. Same-content replacement without progress,
unchanged corruption or unproved state remains a failure/Pending; lookup never
creates a lock, publishes, repairs or extends an action deadline.

Correction provenance: the external Claude review commissioned by Fable found
CHANGES REQUIRED against the original freeze. Its four confirmed findings and
suspected retirement race drove these author corrections/regressions. No review
clearance follows from this document. The interrupted Codex review remains
incomplete and stopped; no retry/replacement review was conducted here.
The same external reviewer found three R2 regressions (umask-dependent scratch,
owned midwork recovery and retained raw scratch copies). R3 addresses those with
the explicit private components, per-entry custody/reset and post-receipt cleanup
above. R3 is author-tested source, pending root's separate external process.

Tests install only in owned isolated synthetic homes. Real fork/SIGKILL cuts
cover metadata rename/fsync, keeper creation/fsync, each graph link, receipt and
keeper retirement; a separate cold process reads the surviving state. The Go/Python
FD9 fixture round-trips the actual receipt and proves parent death before link
with real Pdeathsig, retained spawning thread and exact child reaping. Its native
checker remains synthetic; physical suspend and real Access/assignment integration
are not qualified by these source tests. The fixture measures inherited FDs before
loading ctypes because this Python3.14 host loader retains its own libffi FD.

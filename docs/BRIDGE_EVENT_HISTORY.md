# Canonical event history and consumers

The history substrate supplies canonical final identity to the shared
`EventDelivery` backend. Fallback ingress, light event dispatch, Native recovery
and local wait use that backend. This source integration does not authorize a
private deployment. Cutover must stop old per-wire writers for each home before
opening it with the canonical consumer; mixed old/new writers are unsupported.

The helper classifies existing inbound rows in 128-row transactions and commits
its watermark with catalog fingerprints and any malformed-history barrier. A
known peer bounds the suppression scope; an unknown scope blocks the home. Unknown
recipient history also holds the existing wait cursor. Originals are not rewritten.
Unknown schema versions cannot authorize a claim or catalog progression.
Cataloging also retains each final key's earliest sequence per recipient in an
indexed gap table. A never-seeded or partially seeded earlier final holds the
cursor ahead of later progress or finals, including after restart. Aliases do
not become additional automatic publications.

Before a final can become claimable, its entire old key is seeded in 64-alias
batches. All pending/uncertain/delivered combinations are inspected, including
late delivered rows. Old delivered state conservatively suppresses another effect;
old uncertainty retains the actual wire IDs and an independently recorded local
thread when available. Missing historical thread information never comes from
today's registration. Uncertain legacy wire/thread evidence is captured in the
catalog transaction, so a subsequently restored original cannot retrospectively
supply an unknown historical attempt's thread. Already duplicated effects cannot be undone or described as
exactly once. Conflicting historical projections select no fresh winner.

New aliases join the existing immutable body. Changed same-ID bodies, cross-kind
IDs, changed original request/route identity and conflicting final proposals are
refused with durable fingerprints. A terminal refusal binds that peer/wire ID:
identical retries replay the refusal, and changed proposals retain collision
evidence instead of becoming aliases. The first accepted/refused decision is
stored separately from later rejected-proposal audits: an alternate body cannot
poison replay of the originally accepted raw identity. Decision, alias, route and
cursor writes use the same transaction. Transient absence of independent acceptance
remains retryable and creates no terminal refusal. Alias admission and a supplied fallback cursor
commit together. Persistence is not Native acceptance. An in-flight canonical
attempt is never reset to pending by a later alias or a resumed seed.

The indexed publication projection uses the existing inbound sequence: one final
representative and distinct progress entries. Deliberate old-cursor inspection
remains possible. Deferred and uncertain gaps cannot be silently consumed. Optional
headers normalize only when the independent original Native receipt establishes
the sender thread; otherwise presence/value remain exact. Headers do not supply
missing original authority.

Known original authority is frozen at the first independently known catalog/seed
binding, before any partial seed may complete. Every catalog-observed proof
contradiction retains refusal evidence and a proof fingerprint with the watermark,
including invalid hashes or malformed originals/receipts. Evidence fingerprints
use bounded, explicit null/integer/real/text/BLOB tags; corrupt SQLite metadata
cannot escape classification through JSON encoding or be coerced into authority.
The original's expected text storage classes are checked before using it. Repair
cannot erase that observation. A different known binding records
a conflict without replacing the signature, cursor, thread or projection. Unknown
binding is represented separately, not by a digest of absence: an independently
restored original may trigger a bounded alias re-evaluation while preserving all
old delivered/uncertain suppression and attempt evidence. A known binding whose
original temporarily disappears before the first seed stays unsealed until its
proof returns; pending is never published with a NULL frozen signature. Original
ID, reversed actors and valid source-session syntax must agree before an original
can establish a historical attempted local thread. No restoration authorizes
an original-work resend.

Settled canonical history retains independent original acceptance evidence. Its
representative/raw identity, projection and recipient are still checked, but an
explicit old-cursor read or settled claim shortcut does not query the original
operation or Native history again. Fresh claims and uncertain promotions still
require current independent correlation. A contradictory later proposal cannot
replace a settled canonical outcome.

A confirmed original request can retain its exact authenticated route and native
receipt for later local recovery. The immutable original hash, logical peer,
physical hop, origin/destination and successful receipt must still match. This
retains independently acquired evidence; it never infers acceptance from an event,
queries an owner/controller, resends work or holds another database's lock.

The final publisher freezes the first exact body/status and finalizes its task in
one transaction. A later callback/recovery narrative conflict is inspectable and
cannot replace the first publication. A completed turn is not evidence that the
narrative's technical action succeeded.

A task-specific publication contradiction parks that task, with one durable
first/last fingerprint record, while independently valid sibling tasks and homes
continue. Missing original proof retains one bounded, secret-screened completion
proposal. Existing maintenance loops retry up to 32 such proposals on a rotating
fixed horizon, using only bridge metadata and fresh original/turn/thread checks;
this neither reads Native history nor sends user input. Permanent refusals do not
re-enter Native recovery. Schema/home gates remain home scoped, and a storage
failure is not converted into acceptance or a task-specific skip.

The shared state primitives now accept the exact Store connection's already-open
transaction plus a pure database correlation guard. Consumers couple claims and reservations without nested transactions or holding
SQLite across Native I/O. The guard and claim are tested together under failures.

Consumers reserve every ordinary, direct-light, mail and event Native client ID
inside the recipient home's bridge transaction. A fresh final claim reserves a
separate random Native client ID, marks uncertainty and records the actual attempt
before releasing SQLite for the user-input call. An uncertain retry performs only
an evidence lookup on the recorded thread/client ID; a missing or aged history
match never permits reinjection. Only exact, unique typed user-message matches
can settle an attempt. Matching legacy aliases are rechecked after history I/O.

Wait observation and Native delivery compete for the same canonical claim. An
explicit old cursor returns retained settled history without another effect.
Client-only recipients and fallback recipients outside their existing activation
policy remain persisted, and an empty narrative is
receipt metadata rather than a Native acceptance. Progress remains a separate
per-wire purpose and cannot borrow an ordinary operation's Native client ID.

Local classification uses the 128-row catalog, 64-alias seed and rotating 32-job
windows. A client-only wait continues known local batches at a paced 10 ms
interval within its original deadline, reserving a seed slot for its recipient's
earliest gap. Unknown originals, home barriers and uncertain acceptance still
hold the cursor and wait for external evidence; they do not trigger busy polling.
Light recovery shares the existing 32 total/4 per-peer worker pool and
monotonic retry backoff; transport frame queues and 20/200-turn history ceilings
are unchanged. Native calls never hold a bridge transaction or another home's
lock. Tests use private SQLite and synthetic Native endpoints, not live agents.

The existing peer-scoped `receipt` operation reads the current canonical outcome
without driving Native recovery. Its `result.delivery` distinguishes pending,
uncertain, wait delivery, actual Native delivery and legacy suppression;
`result.native_accepted` is true only for actual typed Native acceptance. A
`native_receipt` records the local attempted client ID/thread/accepted turn, not
the delegated task's remote accepted turn. An RPC persistence ACK is not that
receipt. Existing peer receipt ACLs still apply, including routed logical aliases.

Progress retains independent original proof and historical thread snapshots too.
An unknown old attempted thread is never filled from a later registration or a
restored original. Light recovery only schedules peers owned by configured light
links; legacy-only work retains the connector's existing activation rules.

Qualification uses private real SQLite and fake app-server endpoints: actual
fallback/light/wait adapters, same/new wire aliases, aged 20/200-turn histories,
missing proof, typed/ambiguous acceptance, source/route/ID-purpose conflicts,
client-only/empty results, concurrent claims, post-input write failure and four
subprocess exit boundaries. No real model/runtime/provider deployment is tested.

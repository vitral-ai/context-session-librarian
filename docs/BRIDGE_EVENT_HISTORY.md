# Bounded event history helpers (inactive)

This contribution builds on the event-state primitives. It remains unused by
production consumers: no Native, transport or CLI path imports this module.
It does not complete the event-delivery runtime fix or authorize private adoption.

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
evidence instead of becoming aliases. Transient absence of independent acceptance
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
binding, before any partial seed may complete. A different known binding records
a conflict without replacing the signature, cursor, thread or projection. Unknown
binding is represented separately, not by a digest of absence: an independently
restored original may trigger a bounded alias re-evaluation while preserving all
old delivered/uncertain suppression and attempt evidence. No restoration authorizes
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

The shared state primitives now accept the exact Store connection's already-open
transaction plus a pure database correlation guard. This lets the next consumer
contribution couple claims and reservations without nested transactions or holding
SQLite across Native I/O. The guard and claim are tested together under failures.

Still required before runtime activation: every ordinary/direct-light/mail Native
ID-purpose hook, bounded retry/recovery driving, separate new-effect versus
evidence-only Native adapters, and actual fallback/light/wait consumer integration.
Those paths must preserve the 32 total/4 per-peer workers, existing backoff, 64-frame
queues and 20/200-turn Native history ceilings. This helper performs no Native read
or effect. Source tests use private real SQLite and synthetic records, including
concurrency, interrupted batches, disk-full and write failures.

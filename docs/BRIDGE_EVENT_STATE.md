# Event effect store primitives (inactive)

`csl_bridge_event_state.py` is a first, inert contribution to the event-identity
fix. No production consumer imports it. It creates its additive tables only when
explicitly constructed with an existing `Store`; opening a normal bridge home
does not activate it. There is no CLI, transport change or runtime guarantee in
this contribution.

The primitives provide conditional Native claims, an atomic wait claim and
observation, exact-home Native ID-purpose reservations, durable historical
suppression barriers, typed attempt outcomes and a rotating bounded recovery
selection. Native and transport calls happen outside their transactions. The
recovery selector performs no history read. A fixed sweep horizon prevents
continuous arrivals or permanently uncertain keys from starving later attempts.

The canonical identity rows are *trusted preconditions*, not remote input. An
integration must independently establish immutable correlation, the canonical
projection, a complete historical seed, the representative sequence and the
recipient binding before setting `seed_complete` and `pending`. Tests explicitly
seed those preconditions to exercise transaction races and failure behavior; they
do not qualify a historical upgrade or real Native endpoint.

Native ID checks must run inside the same recipient `bridge.sqlite` transaction
as the ordinary operation, mail or progress reservation, including direct light
writes. A method call only during final allocation would be insufficient.
`reserve_id` refuses cross-purpose reuse and checks existing operation, inbound
and optional mailbox records without creating a mailbox schema. Its namespace
does not cover another bridge home or arbitrary external Native writers.

An uncertain attempt remains bound to its recorded thread and client ID. A typed
Native acceptance settles it; a timeout or availability-only result does not.
Only independently definite nonacceptance permits another claim under the
existing activation policy. Wait and Native compete for one claim. A settled
representative can be deliberately inspected with an old cursor without reopening
that claim. Empty receipt observation is metadata, not narrative acceptance.

Malformed history barriers commit with their eventual catalog watermark. A known
peer scope blocks that peer; an unknown scope blocks the home. Original raw-body
fingerprints remain intact. There is no body-derived narrowing or deletion API.

The separate consumer integration must still implement and qualify:

- Exact canonical admission and immutable alias fingerprints across both
  transports; complete bounded 128-row catalog / 64-alias seed and old-state merge.
- A single original-sequence final publication representative, distinct progress
  entries and deferred-gap wait behavior, with atomic task-finalize/publication.
- All ordinary Native-entry purpose checks in the same reservation transaction,
  shared final claims, actual Native result handling and evidence-only recovery
  in the attempted thread, retaining the 20/200-turn history ceilings.
- Receipt/publication/recovery correlation, promotion revalidation, and the full
  concurrency, crash, retention, tombstone, malformed-history, busy/disk-full and
  old-home regression matrix. No old and new effect consumers may share a home
  during eventual activation.

Identity, suppression and attempt evidence are append-only; this module adds no
TTL, quota, pruning, lifecycle owner, controller access or scheduler. Source
integration, review and an explicitly authorized future adoption remain separate.

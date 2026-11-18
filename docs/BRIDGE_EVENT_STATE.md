# Shared event effect transactions (development)

`csl_bridge_event_state.py` provides the per-home identity and effect transactions
used by the development consumer integration. `Store` initializes its additive
ID namespace on existing and new homes; `EventDelivery` establishes original
correlation and historical classification before claims. The source adapters are
tested with synthetic endpoints. No private/native deployment or compatibility
claim for an untested provider follows from that integration.

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

The shared consumer integration and its bounds are documented in
[canonical event history](BRIDGE_EVENT_HISTORY.md). Native and wait share one
claim; progress retains separate per-wire identity. Recovery revalidates actual
recorded attempts and uses typed user-message evidence. The ordinary request
recovery behavior remains separate and retains its existing limits.

A future deployment must coordinate every writer on a home. Old binaries cannot
honor this namespace: never run old per-wire and new canonical effect consumers
simultaneously against the same bridge.sqlite. A source PR is not that deployment.

Identity, suppression and attempt evidence are append-only; this module adds no
TTL, quota, pruning, lifecycle owner, controller access or scheduler. Source
integration, review and an explicitly authorized future adoption remain separate.

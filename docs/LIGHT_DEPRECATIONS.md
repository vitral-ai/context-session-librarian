# Messaging transport retirement

New pool peers default to `light`. Existing peer configurations are preserved;
explicit `bridge peer --ssh-alias` and `--operator-helper` select the deprecated
per-request fallback. There is no automatic fallback that can resend a message
under a new ID. The old restricted receiver remains for those explicit users.

The light broker replaces per-request SSH watches with pushed CSL progress/final
events. It does not import a pool SDK, grant publisher, receipt follower, renewal
supervisor, cohort controller or worker lease into message delivery. Owner SSH
hooks remain outside the message path and run only on installation/reconnect.

## Dead-weight inventory

| Old piece | Disposition |
| --- | --- |
| Single-run return workers, worker launch/rotation recipes, bounded messaging runtime packages | Retired site tooling; not tracked in this CSL repository and intentionally not imported into this PR. Replace with the persistent light service. |
| Messaging renewal cohorts, T0/ACK switch windows and worker-replacement timers | Retired site tooling; absent from the public transport and enrollment path. |
| Publisher-receipt checks used to gate messaging | Removed from the transport design; no receipt-publisher dependency in the broker or portable enrollment. |
| Per-request SSH/operator bridge exchange | Deprecated explicit fallback retained in `csl_bridge_transport.py`; not selected by new pool peers. |
| SSH watch polling for light peers | Removed from the default connector path; existing event envelopes are pushed durably. |
| Message ACKs, message-ID receipts, and replay SQLite | Retained: these are delivery/idempotency data, not lifecycle or publisher receipts. |
| Lifecycle holds, environment keep-alive publisher and owner-access bookkeeping | Retained outside CSL messaging. Do not delete or stop them as transport cleanup. |
| Private audit snapshots, historical inboxes or credentials | Not repository content and not cleanup candidates for this PR. |

The private one-shot packages are not public source files, so no public PR can
remove those artifacts. Their execution paths were retired independently; safe
site cleanup preserves lifecycle consumers, audit/user data and rollback history.
This inventory does not authorize cloud-resource or user-data deletion.

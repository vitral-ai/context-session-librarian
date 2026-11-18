# Container Apps workload collector packaging

Build the collector layer with the reviewed collector image digest as
`COLLECTOR_IMAGE`. The input image must implement the workload entrypoint and
configuration described in [the packaging contract](../README.md). The current
raw CSL image is not a valid input.

For an explicitly opted-in replica, mount a dedicated replica-scoped transcript
volume read-write into the writer and read-only at `/transcripts` in the collector.
Mount separate private replica-scoped state at `/collector-state` only in the
collector. The optional init layer checks that explicitly mounted volumes are
available to UID 10001. It needs no identity input, network request or device
approval. When using the init layer, remove its mounts at the end of init and
mount the transcript input read-only in the actual collector.

Supply a stable workload ID and environment or cluster ID in replica
configuration. Supply a distinct replica ID per instance. Feed those values
through the same workload metadata as Kubernetes. Device approval still
belongs to each replica, with no automatic state handoff on replacement.

Configure the writer and collector termination budgets for a bounded
writer-exit and 20-second collector drain, with at least 30 seconds total and
extra time for writer shutdown. Measure scheduling and platform termination
behavior before relying on it. This source recipe has no cloud deployment.

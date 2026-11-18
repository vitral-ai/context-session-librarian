# Workload collector packaging

This is a source integration for the Phase 1 workload collector. Rendering the
chart with default values creates no workloads. Enable it only with a reviewed,
digest-pinned workload collector image. Current raw CSL sync and its PKCE
callback are outside this packaging interface.

## Collector image interface

The separate [image source recipe](collector-image/README.md) assembles the
reviewed runner, helper and exact runtime inventory. Building it is distinct
from publishing a digest, activating packaging or qualifying end-to-end use.

The image provides `/usr/local/bin/csl-workload-collector` and runs as a
non-root user. Kubernetes supplies the documented input environment in the
chart. The executable validates the exact configured HTTPS origin throughout
discovery, device enrollment, refresh and registry traffic. An optional
ConfigMap supplies a public test CA. TLS still checks the hostname. The human
approval page may use the normal browser origin. The device code must be
exposed through the approved access-controlled interaction, never shared logs.

The workload client is `vitral-workload-collector`. Use W7 person approval
and independent credentials per replica. Only a restart with the same
replica's private state can reuse approval. A replaced replica enrolls again.
The collector owns its key, refresh state, sanitized spool and durable ACKs
under `/collector-state`. The wrapper creates its own `0700` identity child
under that mount without changing the mount root's permissions. Nothing mounts
a harness home or a credential store.

Set `transcripts.provider` and `transcripts.profile` explicitly. Kubernetes
passes them as `VITRAL_COLLECTOR_PROVIDER` and `VITRAL_COLLECTOR_PROFILE`.
The [reviewed profile matrix](workload-collector/profiles.json) defines the
only accepted pairs. Enabled workloads reject absent, automatic, or mismatched
profiles before launch. The packaging does not execute the harness or inspect
its home to infer a version or transcript layout.
The transcript mount `/transcripts` contains only the selected provider's
declared source layout. For Gemini CLI, export only the approved chats
subtree for the qualified native format. Do not mount its home or project
registry to discover transcripts. Gemini CLI and Antigravity are distinct.
The image owns path confinement, complete-record
sanitization, deterministic ACK offsets and bounded stop behavior. It refuses
unsupported formats or endpoint changes instead of falling back to raw sync.

## Kubernetes

The opt-in chart provides a native restartable init sidecar and a writer that
shares a transcript-only pod volume. The sidecar has a read-only mount and the
writer has no collector-state mount. The digest-pinned Linux writer image supplies Python 3 with CLOCK_BOOTTIME.
The mounted supervisor runs the writer in its own process group and forwards
TERM. It terminates a group that exceeds `writer.shutdownSeconds` so the
kubelet can stop the sidecar within the remaining pod grace. The sidecar's
own flush is at most 20 seconds. Pod grace is at least 30 seconds and includes
the writer's shutdown budget and overhead. Kubernetes termination ordering
and the end-to-end ACK result still require an in-pod run.

The DaemonSet is disabled by default. An operator must supply an existing
same-owner transcript-only PVC and explicitly attest the narrow export scope.
Use an export backed by storage that supports read-only mounts on the
selected nodes. The DaemonSet neither creates storage nor discovers host
paths. Omit this mode if namespace or storage policy cannot enforce the input
boundary. The chart creates no RBAC, inbound service or ingress.

A workload's configured stable ID, deployment provider and cluster ID combine
with the namespace to group replicas for the verified owner. A pod UID is
replica provenance, not a stable workload ID or authorization.

## Local verification

Render the chart with Helm and a digest-pinned fixture image reference.
Run the local fixtures with Python unittest and OpenSSL. They exercise the
package settings and single-origin HTTPS fixture, not the authentic CSL
workload client or Sessions UI. For the in-pod run, use a disposable kind or
k3d cluster configured with a run-local kubeconfig, a private fixture CA and
one exact test hostname. Use only the reviewed workload collector image,
the local identity and registry fixtures, and synthetic transcript data.
Measure complete-record write to ACK (3 seconds) and foreground UI visibility
after ACK (2 seconds) separately. Keep a final harness demonstration and
Azure Container Apps qualification as separate evidence.

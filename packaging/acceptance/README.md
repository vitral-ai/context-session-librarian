# Collector local acceptance preflight

Run the synthetic checker tests from the checkout root with Python 3.11+,
Helm 3.19.0 and PyYAML installed in the test environment.

```sh
VITRAL_FIXTURE_HELM=/absolute/path/to/verified/helm \
  python3 -m unittest discover -s packaging/acceptance -v
```

These checkers validate supplied evidence. They do not start infrastructure,
enroll devices, issue tokens, upload transcripts, or produce runtime evidence.
Keep run artifacts outside Git. Collect only synthetic markers, sanitized
receipts, opaque IDs and timing observations. Never save device codes, bearer
values or transcripts in the evidence.

## Operator image and chart preflight

1. Use the authorized workstation's original rootless Podman/kind route with
   the qualified node image and resource limits. Record the provider, node
   image digest, local API address and run ownership. The guest is for source
   checks. Build the Collector at the reviewed merge
   `e00fd7eea427debcb1f987c78ef8ab09fd8a27f0` with the
   [image recipe](../collector-image/README.md). Record the actual platform,
   image manifest digest, archive SHA-256 and runtime smoke results. Use the
   approved synthetic writer's image and digest too.
2. Have the workstation build/export step produce single-platform,
   uncompressed OCI archives for the target node architecture. Verify both
   archives against the independently recorded image manifest digests. The
   archive checksum is transport evidence and differs from the image digest.

   ```sh
   python3 packaging/acceptance/check_oci.py "$collector_archive" "$collector_digest"
   python3 packaging/acceptance/check_oci.py "$writer_archive" "$writer_digest"
   KIND_EXPERIMENTAL_PROVIDER=podman kind load image-archive \
     --name "$owned_cluster" "$collector_archive" "$writer_archive"
   ```

   Inspect the node's k8s.io containerd image inventory after import. Each
   rendered repository@digest must resolve in the node store to its verified
   image manifest. If the importer only creates a local name, use the
   workstation's local containerd image tagging operation on that verified
   record, then recheck digest resolution. Set both fixture container imagePullPolicy values to Never for the
   fixture so a missing image fails locally. Record the running CRI image IDs
   and their resolution to the expected manifests. Stop on unresolved
   references or transformed manifest digests. No registry push or registry
   credential is required.
3. Render the merged chart including the numeric serialization fix from
   `cea101c3f0674cb223660e48b5a50c83355dae2c`. Supply exact collector/writer
   image references, single fixture HTTPS origin and test CA ConfigMap,
   explicitly selected provider/profile, stable workload fields, and literal
   writer argv. Use a namespace and volumes belonging to this run. Set both fixture pull policies to `Never`. Convert the
   final Helm YAML documents to a JSON array with a YAML parser such as PyYAML
   `safe_load_all`. Supply a separate trusted pins JSON object containing
   `collector_image`, `writer_image`, `origin`, `provider`, `ca_configmap`,
   `writer_argv` (the exact literal array including the writer shutdown budget and
   `--`), `supervisor_name`, `supervisor_sha256`, `grace_seconds`, and
   `location` (label, provider, cluster, workload). For nondefault collector
   budgets, supply `flush_seconds` and `spool_limit_bytes`. Hash the script bytes
   from the reviewed source checkout. Compute the name from the reviewed Helm
   naming rule and intended release/namespace, independently of the input being
   checked. This preflight expects the image UID, default volume limits, and
   chart collector resource bounds.

   ```sh
   python3 packaging/acceptance/check_rendered_abi.py rendered.json reviewed-pins.json
   ```

   Submit the checked objects. Independently compare the admission result and
   actual Pod against the checked execution boundary, accounting for API default
   fields. Reject mutation of the checked inputs and any injected execution path.
   Record actual digest, mounts, process boundary and CA.
   Do not substitute a tag or the source preflight for an image result.

## Run and capture observations

Use the reviewed joined identity/registry/browser fixture on one HTTPS
hostname with the owned CA. Record all counterpart source and artifact pins.
Use the normal human-approved device flow with independent replica-private
state. Observe two pod UIDs under the same workload and deliberately use the
same synthetic native session ID in both. Check distinct opaque registry
records, one visible occurrence of every expected marker and view-only
capabilities. Measure complete write, registry acknowledgment and foreground
visibility using one observer's monotonic clock. Observe each marker and
preserve event ordering; enforce 3 seconds to ACK, 2 seconds from ACK to UI,
and 5 seconds end to end per marker.

Exercise opt-out, denied approval, an unrelated fixture account and off-origin
refusal. Count synthetic canary appearances across outbound content and
metadata, view, package, UI and sanitized fixture logs. Capture counts only.
Drop a reply after a committed upload while preserving the replica's private
state. Restart and verify the final content revision and view generation
converge with no duplicate or missing marker. Exercise content and view lost
replies independently. Make an unavailable-registry shutdown attempt and
verify it reports pending or incomplete within the drain budget.

For the healthy final-complete case, run the exact image with its literal
sole-producer wrapper separately from the default external-writer chart mode.
The reviewed runner must witness its producer group stopped. Observe EOF after
stop for the same source generation, then verify fresh exact content and view
ACKs for that capture within the original 20-second BOOTTIME budget. The
`check_producer_stop.py` predicate exercises those correlations with synthetic
typed observations. Construct the runtime stop witness only inside the owned
fixture supervisor. Serialize the original BOOTTIME drain start and deadline,
the observed stop and EOF, and typed content and view receipts with their
capture identifiers and timestamps. The summary revalidates the timeline with
the same predicate and cross-checks its elapsed time. Serialized observations
are review evidence, so collect them from the trusted observer rather than the
writer or transcript.

Default native sidecar, DaemonSet and ACA external-watch remain pending on
shutdown. Kubelet's delayed sidecar SIGTERM does not communicate which writer
invocation stopped or prove final EOF. The concrete supported completion route
is to put the sole producer under the existing runner in one private container
PID namespace, preserving the independent identity/state and transcript-only
input contract for the chart's external mode. Arbitrary external producer
completion would require a separate trusted, invocation-bound platform witness.
The current chart has no source for one. No shared marker, listener, service
account, RBAC, or shared PID namespace is used.

Use the shape in `test_check_fixture.py` for the sanitized observation summary.
Populate it from the actual observers and retain the separate stop/EOF/ACK
evidence, Pod metadata, negative-case observations and hashes. The sample data
only tests validator behavior. Supply an independent JSON array of marker IDs
from the producer scenario, covering every expected marker. Preserve this
inventory separately from telemetry collected by the browser and registry.

```sh
python3 packaging/acceptance/check_fixture.py expected-markers.json < observations.json
```

Report source checker results separately from OCI, actual kind, UI and ACA
results. Until the workstation performs the joined run, A8 remains pending.

# Fixture-only replica enrollment overlay

Use this only on the authorized workstation's owned local fixture after
independent source clearance of the enrollment adapter. The composer pins the
exact bytes of `scripts/fixtures/workload-replica/enroll.py` from reviewed merge
`5fcc61b8a5df6e6cf1ba1859bb26c86051721bd4`, SHA-256
`e5c29e2fa11d048ebb5444d3a9ddaa1b29bada2c2d745b42561fa8c77ddd6981`. Keep it separate from the
reviewed OCI manifest digests and the fixture's public CA pin. Verify the
actual artifacts and CRI resolution using the baseline acceptance runbook.

Run the baseline with the reviewed namespace, CA and replica metadata. Keep
the original baseline and pin files for evidence. Compose an overlay from
those checked inputs and the exact adapter bytes.

```sh
python3 packaging/acceptance/compose_enrollment.py \
  rendered.json reviewed-pins.json "$reviewed_helper" 90 \
  > enrollment-plan.json
```

Pass `enrollment-plan.json` as a fifth argument to verify a candidate plan
against the exact composition. The verifier checks the full manifest and exec
plan. It does not inspect the running Pod or the exec transport.

The resulting `manifests` list adds only one immutable ConfigMap and a
collector-only read-only mount with an explicit filename and mode. Save those
objects as the reviewed submission. The separate `exec` object is a plan for
the operator's existing local fixture exec transport. It is not a Kubernetes
API object or a command string. Do not apply the whole JSON as a manifest.
Review the exact admitted Pod against the composed spec, including the
namespace-local helper ConfigMap and its bytes, Pod UID, container image IDs,
private state and unchanged writer mounts. Admission-injected execution paths
invalidate the fixture evidence. The baseline checker is intentionally strict
and refuses this additional mount when run against the composed deployment.

Inside the selected collector replica, use the transport's private non-TTY
exec stream and pass the emitted argv as literal arguments. The fixture
controller accepts public consent records only on that dedicated pipe and
requires a ready record and exit zero. Confirm FD 1 actually arrives as a
pipe in the image. If the transport presents a socket, terminal, or other
unsupported FD, treat enrollment as pending. Do not make the helper accept
another FD type or route its consent through shared logs. Bound the exec
lifetime and keep records off disk and out of controller debug output. Match
the rendezvous to the expected replica and use the existing synthetic W7
consent fixture. Journal and key creation take place in that replica's private
state using exactly its runner configuration. A missing result or state
ambiguity leaves that replica pending.

Prove the pod reaches only the configured HTTPS origin with exact hostname
validation and the fixture CA before attempting the joined test. Reuse the
original rootless kind route. External watch remains pending on stop. Use
a separate sole-supervised-producer case for final completion and require
post-stop EOF and fresh, distinct content and view ACKs within the original
BOOTTIME window.

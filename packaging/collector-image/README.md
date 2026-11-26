# Workload collector image source

Build from a clean, reviewed CSL checkout, using BuildKit (Docker 23+):

```sh
docker build --file packaging/collector-image/Containerfile --tag csl-workload:fixture .
docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges --tmpfs /tmp:rw,nosuid,nodev,size=64m \
  --entrypoint /usr/local/bin/python3 csl-workload:fixture \
  -E -s -S /opt/vitral/collector/check-runtime.py
```

This is an opt-in source recipe, not a released/published artifact, enrollment,
cutover, Helm value update or A8 proof. Record the exact CSL commit, resulting
image digest, platform, Python/TLS and helper versions when it is built. Tags in
the commands above are local only; packaging requires a reviewed image digest.
No image push or deployment is performed by the workflow.

## Exact-source Lane F handoff

The recipe already copies the codec containing merged #57, #58 and #59; no
runtime inventory, provider/profile, base or helper pin change is needed for
these records. Its in-image smoke now projects a bounded synthetic paginated
Codex stream with catalog, world state, usage and thread settings records. It
requires all four fixed omission notices, sanitized coordinates and no metadata
or canary leakage. It neither reads a transcript nor runs a harness. The existing
build step and no-network image CI lane execute this smoke. A successful import
alone, the unchanged profile name, or an earlier e00 image is not this evidence.

The authorized build operator starts in the exact **reviewed clean commit** for
this contribution (record the full SHA from the PR, not a moving branch). Keep
`EVIDENCE` outside the checkout, empty and owner-private; do not put credentials,
transcripts or collector STATE there. For a Linux/amd64 qualification:

```sh
SOURCE=$(git rev-parse --verify HEAD)
test -z "$(git status --porcelain --untracked-files=all)"
git merge-base --is-ancestor 79fcf62a55ce4740db36c61e33ff449b9ab822f1 "$SOURCE"
TAG="csl-workload:lanef-$SOURCE"
python3 -B -I packaging/collector-image/assemble.py "$PWD" "$EVIDENCE/expected-runtime"
python3 -B -S scripts/check_workload_build_context.py
docker build --platform linux/amd64 --progress plain \
  --file packaging/collector-image/Containerfile --tag "$TAG" \
  --label "org.opencontainers.image.revision=$SOURCE" \
  --iidfile "$EVIDENCE/image-id" . >"$EVIDENCE/build.log" 2>&1
IMAGE=$(cat "$EVIDENCE/image-id")
docker image inspect "$IMAGE" >"$EVIDENCE/image-inspect.json"
docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges --memory 256m --cpus 1 --pids-limit 128 \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m --entrypoint /bin/cat "$IMAGE" \
  /opt/vitral/collector/runtime-manifest.json >"$EVIDENCE/runtime-manifest.json"
cmp "$EVIDENCE/expected-runtime/runtime-manifest.json" "$EVIDENCE/runtime-manifest.json"
docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges --memory 256m --cpus 1 --pids-limit 128 \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m --entrypoint /usr/local/bin/python3 "$IMAGE" \
  -B -E -s -S /opt/vitral/collector/check-runtime.py >"$EVIDENCE/runtime-smoke.json"
docker save --output "$EVIDENCE/collector-image.tar" "$TAG"
sha256sum "$EVIDENCE/collector-image.tar" >"$EVIDENCE/archive.sha256"
```

Run these commands fail-fast (for example in a shell with `set -eu`). Do not
continue after a failed context/build/manifest/smoke check or substitute a
previous artifact. Record `SOURCE`, inspection `.Os`, `.Architecture`, `.Id`,
`.RepoDigests`, revision label, source manifest and the smoke's Python, machine,
helper and TLS versions. Compare entry-point and check-script bytes against the
same source too: they are explicit recipe inputs outside the assembled source
manifest. Export them using the same no-network `/bin/cat` container pattern
from `/usr/local/bin/csl-workload-collector` and
`/opt/vitral/collector/check-runtime.py`, and use `cmp` against this checkout.
Record the helper binary's SHA-256 separately from its pinned source archive.

The iidfile is a **local image/config identity**, not automatically a registry
manifest digest. A saved archive's SHA-256 is only that archive's receipt.
`.RepoDigests` may be empty. Never fill a digest-required Helm image value with
either of those substitutes. The existing authorized kind operator can import
the exact archive into the existing cluster, inspect its immutable manifest
reference in that runtime and bind the fixture to it. No new cluster, daemon,
privilege workaround or public registry push follows from these commands.
Cloud tests likewise need an actually available reviewed manifest for the
target architecture; missing distribution/descriptor evidence remains pending.
Pinned inputs make the build procedure repeatable, not a promise of identical
OCI digests across builders or architectures. Keep e00's receipts unchanged.

## Real producer and marker procedure

Env4 remains the sole enrollment/collector executor. Reuse its person-approved
replica-private STATE and exact configured one-origin HTTPS route, with normal
hostname/CA verification. Do not import keys, tokens or journals into a new
volume, reset STATE, mount a harness home, or start a second collector. A fresh
replica needs its own existing person-approval flow. Record the selected source,
artifact, profile and private-volume binding separately from browser evidence.

For an already authorized future real producer scenario, use a predeclared
public run label and an exact expected marker list, such as
`VITRAL_A8_LANEF_R1_0001`, `VITRAL_A8_LANEF_R1_0002`,
`VITRAL_A8_LANEF_R1_FINAL`. The synthetic redaction canary is `sk-proj-` followed
by 32 `Q` characters, never a real secret. The real agent writes native complete
records normally; do not append/edit/filter its rollout or substitute a synthetic
writer and call it real-agent evidence. This procedure commissions no new model
turn: current parsing and collection reuse the already-created source.

Observe each marker's complete-record, registry ACK and foreground visibility
times independently; targets remain 3 seconds to ACK plus 2 seconds to UI.
Check canary absence across sanitized content, metadata, view, package refusal,
UI and retained logs, without dumping the corpus. Keep the expected marker
inventory independent from observed rows. Real paths/IDs/text are not debug
output. No screenshots of login, profiles or credentials.

Final completion requires the **actual declared writer** under the existing
literal-argv guardian, not an inert command launched against someone else's
source. Witness that group stopped, then complete EOF, then fresh distinct
content and view ACKs for that exact final capture within the original at-most
20-second BOOTTIME drain; reserve at least 30 seconds termination grace and
satisfy the chart's writer-stop + flush + overhead arithmetic. Repeated
signals cannot renew the budget. Outage, partial EOF, missing/mismatched ACK or
unknown writer outcome is pending/incomplete, with durable state preserved.

The collector image deliberately contains no Codex executable or harness login.
The chart's separate external writer is not owned by the collector guardian,
and the current runner accepts no external stop-witness input. Thus this
image's bare-watch lane can prove live collection and bounded final ACK drain,
but **must remain `complete:false,pending:true`** even if an external writer
appears idle or stopped. `-- python3 -c pass` cannot close that gap. A real
sole-producer completion qualification needs an actual supported producer
environment under the guardian or separately reviewed external-witness wiring;
do not silently add credentials, install a harness or invent containment.
Keep plain-process source, synthetic container, real kind and cloud receipts
separate. None alone establishes integrated A8 acceptance.

## Inputs and entry point

- Official `python:3.12.16-bookworm` build index:
  `sha256:4209fae09a3e20afd863e528bfddc235da5fd5977983e1996cd54cc9a72e6267`.
- Official `python:3.12.16-slim-bookworm` runtime/assembly index:
  `sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df`.
- OpenSSL helper 3.5.9 public source:
  [archive](https://www.openssl.org/source/openssl-3.5.9.tar.gz),
  [upstream digest](https://www.openssl.org/source/openssl-3.5.9.tar.gz.sha256)
  `603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a`.
  Build checks the hash before extraction. The helper uses static libcrypto/
  libssl, no loadable modules, and the matching Debian runtime libc. Python HTTPS
  uses the pinned base's separate system TLS library and normal CA bundle, not
  this helper. The helper's upstream license is retained at
  `/usr/local/share/licenses/workload-openssl/LICENSE.txt`.
  No runtime dependency download or package manager invocation.

`runtime.json` is the exact local Python import closure plus LICENSE and the
merged #43 code-relative `packaging/workload-collector/profiles.json`. Tests
compare the closure, explicit COPY operands and deny-by-default context allowlist.
The Dockerfile-specific ignore file prevents transcript/state, `.git`, tests,
homes and arbitrary checkout files from entering the build context. Only those
declared inputs are copied into the runtime. An image-local manifest records
SHA-256 for the assembled source/data files; it is evidence, not a server trust
or approval substitute. The external entry-point/check scripts and helper/base
pins remain visible build inputs in the same reviewed commit.

Directory exceptions require an immediate recursive descendant exclusion before
the individual file exceptions: opening a parent alone can admit unlisted
children under Docker's actual matching rules. The hosted image lane executes
the filter, not just its spelling:

```sh
python3 -S scripts/check_workload_build_context.py
```

This uses unchanged filter bytes with temporary synthetic public files and real
BuildKit scratch COPY/local-export probes. Three positive controls must succeed;
ten unlisted root, nested, hidden and filename-suffix siblings must refuse with
a terminal BuildKit checksum error naming that exact source, not an echoed COPY
argument plus an unrelated "not found". Legacy/unknown diagnostics, canceled
builds, nonstandard exit codes and partial exports fail rather than count as a
policy refusal. Four
old-filter controls reproduce the historical directory-reopening bug. No real
state/secret is used, no checkout file is mutated, and no probe image is pushed
or tagged. Temporary exports are removed; ordinary BuildKit caches remain.
The earlier filter admitted those paths to COPY probes, but the real recipe's
explicit COPY operands did not put unexpected files in its final image. That
does not establish that actual secrets were transferred by the unchanged build.

Shared modules include dormant desktop/media/Antigravity parsing helpers needed
by existing imports; their CLI launchers and optional media worker/dependencies
are absent. No extra provider/profile or raw fallback is enabled.

The non-root UID/GID is `10001:10001`. `/usr/local/bin/csl-workload-collector`
executes the fixed runner with `-E -s -S`: no PYTHONPATH, Python user site or
site startup injection. It preserves literal arguments and signals; no shell
expansion, enrollment, command retry, wrapper daemons, implicit producer or
default Python REPL. The base's inherited CMD is explicitly cleared.

Runtime configuration is exactly the merged packaging ABI. No provider,
profile, location, identity origin or redaction opt-out is baked into the image.
`VITRAL_COLLECTOR_TRUST_CA` remains explicitly forwarded with hostname checking.
STATE is the mounted root, left unchanged; `identity` and the other private
children are collector-owned. An approved per-replica token journal must already
exist; the image does not automate person approval or read harness credentials.
Do not mount a harness home, credential store, host runtime socket or service
account token. Read-only transcript and private writable state mounts, resource/
volume bounds and termination grace remain the packager's responsibility.

## Scope and qualification

No arguments means bare watch, which deliberately remains pending without an
external producer-stop witness. `-- <literal argv>` is the previously reviewed
sole-producer wrapper, not arbitrary escaped-writer containment. Completion
still requires stopped producer, complete EOF and fresh exact content + view
ACKs inside the original BOOTTIME drain budget (at most 20 seconds). Packaging's
external stop/EOF/ACK integration is separate; the image never invents it.

The build runs the no-network smoke as UID10001: exact imports, three profiles,
BOOTTIME, system CA availability and actual helper Ed25519 sign/verify. Hosted
CI additionally runs it with network disabled, a read-only root and bounded
tmpfs, then verifies an unconfigured entry point exits pending/refused rather
than starting a producer or enrollment. A separate configured literal
`python3 -S -c pass` smoke proves the producer is stopped and reaped but the unapproved
collector still reports pending, with no enrollment or upload possible. This does not test real W7 authority,
registry, UI, in-pod termination, workload timing, harnesses or ACA. BuildKit is
required so the recipe-specific context allowlist is honored. Architecture
coverage is only what was actually built; multi-platform base indexes are not
qualification of multiple architectures. Image/base/helper security updates
require new explicit pins and review, not mutable runtime updates.

On the author guest, Docker has no daemon and Podman cannot create its runtime
directory. No privilege or namespace workaround is used. Local Python assembly
tests and an actual OCI build/run must be reported separately; a passing source
test is not an image qualification.

## Managed pool enrollment

With the same reviewed workload configuration, the pool supervisor invokes
`--managed-identity` to obtain the collector key thumbprint. It then passes the
signed collector assertion on standard input to `--enroll-managed`. Both
commands print only the workload thumbprint and local enrollment state. The
collector keeps the resulting registry credential in its private state and uses
the regular runner for collection and refresh.

### Registro del runtime en Vitral Connect

Ejecuta una vez `csl-workload-collector --connect-runtime` con la configuración
habitual del collector, `VITRAL_TUNNEL_HOME` para el estado del host y estas
variables del entorno:

- `VITRAL_CONNECT_ORGANIZATION` contiene el ID de la organización.
- `VITRAL_CONNECT_METHOD` es `oidc` o `join_token`.
- `VITRAL_CONNECT_CREDENTIAL_FILE` apunta al archivo privado con el token.
- `VITRAL_HOST_BINARY` puede indicar el binario de Vitral Host ya verificado.
- `VITRAL_TUNNEL_KIND` es `container` para Pods o `ssh` para VMs.

El comando requiere `ssh-keygen` y usa HTTPS en el puerto 443. Si devuelve
`enrolled: true`, ambos registros están listos. Conserva los dos directorios de
estado para reintentar la misma solicitud tras un fallo transitorio. El archivo
del token se monta solo durante el registro.

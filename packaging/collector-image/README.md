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

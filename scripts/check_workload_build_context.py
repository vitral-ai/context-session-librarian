"""Real BuildKit context admission, using only temporary synthetic public files.

No image push, base pull, secret, transcript or checkout mutation. Scratch COPY
probes export one sentinel locally, not an OCI image. Build caches are not pruned.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
EXCLUSIONS = ('packaging/collector-image/**', 'packaging/workload-collector/**')
ALLOWED = ('packaging/collector-image/runtime.json',
           'packaging/workload-collector/profiles.json', 'csl_workload_producer.py')
REGRESSION = ('packaging/collector-image/review-private.sqlite',
              'packaging/collector-image/README.md',
              'packaging/workload-collector/writer-supervisor.py',
              'packaging/workload-collector/.local/review-private.sqlite')
SIBLINGS = ('packaging/collector-image/.local/public-canary.env',
            'packaging/workload-collector/notes/public-canary.env',
            'packaging/collector-image/runtime.json.bak',
            'packaging/workload-collector/profiles.json.bak',
            'packaging/.local/public-canary.sqlite', 'README.md')
CODEX_ALLOWED = tuple('packaging/codex-sole-producer/'+name for name in (
    'Containerfile', 'Containerfile.dockerignore', 'inputs.json', 'install.py', 'prepare.py', 'csl-codex-prepare'))
CODEX_DENIED = ('packaging/codex-sole-producer/README.md',
    'packaging/codex-sole-producer/build.py',
    'packaging/codex-sole-producer/.local/private.sqlite',
    'packaging/codex-sole-producer/inputs.json.bak', 'csl_workload_auth.py',
    'packaging/collector-image/Containerfile', 'LICENSE')


def old_filter(current):
    lines = current.splitlines()
    if any(lines.count(value) != 1 for value in EXCLUSIONS):
        raise ValueError('missing unique descendant exclusions')
    return '\n'.join(line for line in lines if line not in EXCLUSIONS)+'\n'


def verify_result(result, candidate, allowed, output, expected):
    if allowed:
        if result.returncode or not output.is_file() or output.read_bytes() != expected:
            raise RuntimeError('allowed context control failed: '+candidate)
    else:
        diagnostic = result.stdout.decode('utf-8', errors='replace')
        terminal = diagnostic.splitlines()[-1] if diagnostic.splitlines() else ''
        # Plain BuildKit output echoes COPY arguments too. Their presence and an
        # unrelated "not found" are not source exclusion. Unknown diagnostics
        # fail closed; this probe deliberately does not recognize legacy output.
        missing_source = re.fullmatch(
            r'ERROR: (?:failed to build: )?failed to solve: failed to compute cache key: '
            r'failed to calculate checksum of ref [^\r\n]+: '
            +re.escape(json.dumps('/'+candidate))+r': not found', terminal)
        if (result.returncode != 1 or output.exists() or output.is_symlink() or not missing_source):
            raise RuntimeError('missing context exclusion: '+candidate)


def check():
    current = (ROOT/'packaging/collector-image/Containerfile.dockerignore').read_text()
    previous = old_filter(current)
    codex = (ROOT/'packaging/codex-sole-producer/Containerfile.dockerignore').read_text()
    results = []
    with tempfile.TemporaryDirectory(prefix='csl-context-admission-') as temporary:
        root = Path(temporary)
        context = root/'context'
        context.mkdir()
        for candidate in set(ALLOWED+REGRESSION+SIBLINGS+CODEX_ALLOWED+CODEX_DENIED):
            target = context/candidate
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(('public synthetic COPY control: '+candidate+'\n').encode())
        recipe = context/'ContextProbe'
        ignores = context/'ContextProbe.dockerignore'
        # All positives run first. The unchanged fixed filter must reject every
        # forbidden sibling. Old-filter controls deliberately reproduce F1 using
        # public bytes only, proving this is an executable before/after regression.
        cases = ([(current, name, True, 'fixed') for name in ALLOWED]
                 + [(current, name, False, 'fixed') for name in REGRESSION+SIBLINGS]
                 + [(previous, name, True, 'old-filter') for name in REGRESSION]
                 + [(codex, name, True, 'codex-overlay') for name in CODEX_ALLOWED]
                 + [(codex, name, False, 'codex-overlay') for name in CODEX_DENIED])
        for index, (policy, candidate, allowed, version) in enumerate(cases):
            recipe.write_text('FROM scratch\nCOPY '+json.dumps([candidate, '/sentinel'])+'\n')
            ignores.write_text(policy)
            destination = root/('export-'+str(index))
            result = subprocess.run(['docker', 'build', '--network=none', '--no-cache',
                '--progress=plain', '--file', str(recipe), '--output',
                'type=local,dest='+str(destination), str(context)],
                env={**os.environ, 'DOCKER_BUILDKIT': '1'},
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=45)
            verify_result(result, candidate, allowed, destination/'sentinel',
                          (context/candidate).read_bytes())
            results.append({'filter': version, 'candidate': candidate, 'allowed': allowed})
    print(json.dumps({'buildkit_context_cases': results, 'synthetic_only': True,
                      'image_push': False}, separators=(',', ':')))


if __name__ == '__main__': check()

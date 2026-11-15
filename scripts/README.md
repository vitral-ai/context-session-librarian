# Run isolated tests

From the repository root, with Python 3.11 or newer:

```sh
python3 scripts/run_isolated_tests.py
```

This runs the base suite in fresh HOME, Codex, Claude and XDG directories, discarding
inherited authentication, provider-selection flags and private configuration.
Optional native/media/migration checks may be skipped. A private result directory
contains `report.json` and `tests.log`; its path is printed. No account login or
paid model generation is performed. Fresh homes are not a general OS sandbox.

For the full synthetic suite on Linux, first install Codex CLI, Claude Code and
`bubblewrap` using their supported installers, then check availability:

```sh
python3 scripts/run_isolated_tests.py --full --check
python3 scripts/run_isolated_tests.py --full
```

Full mode requires **Codex CLI 0.169.0 or newer** for this native validation
profile, plus working bubblewrap network/user namespaces, and records installed
CLI versions. Confirm `codex --version` and PATH order: an independent run with
0.118.0 first on PATH failed native checks; the same candidate with 0.169.0 passed.
`--full --check` rejects the older CLI before running tests. This test-profile
floor is not a minimum version claim for all CSL operations; newer native versions
still require compatibility checks. Claude Code 2.1.325 was used in validation.
The runner does not change host namespace policy or authenticate
either CLI. It creates a temporary test virtualenv and installs the directly pinned
requirements from `scripts/isolated-test-requirements.txt`; dependency installation
uses network access but makes no model calls. `--wheelhouse /path/to/wheels` selects
an offline wheel source instead. Prerequisite or dependency failure exits with code
2 before the suite rather than quietly presenting an incomplete full run.

An existing **dedicated test** virtualenv with those exact requirements can be
selected instead, without changing its installation:

```sh
python3 scripts/run_isolated_tests.py --full --media-python /path/to/test-venv/bin/python
```

Do not supply an authenticated personal environment as a test home. The runner
always makes its own fresh homes. `--output /path/to/new-results` preserves results
at a chosen location and refuses to overwrite an existing directory. Read the log
to distinguish executed checks, skips and failures; passing structural/native
tests does not establish real-model recall or cross-machine compatibility.

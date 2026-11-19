# CI scheduling source preparation

This local candidate keeps Tests' Python3.11/3.13 transfer and media matrices, execution-process requirement, pinned Go/Helm inputs and complete unittest commands unchanged. Main pushes and every PR still run; non-main push duplicates no longer run separately. Superseded PR revisions cancel within the same workflow/PR key, without changing unrelated runs.

Workload image builds retain their exact commands and pinned parent/tool inputs. All PRs receive the existing collector-image check via an always gate. A pinned changed-file selector decides whether the image execution is applicable; failure/unknown detection, failed/cancelled execution or skipped applicable execution cannot pass. Source filters conservatively include all root CSL Python, packaging, LICENSE, the build-context checker, image source tests and workflow. Main pushes always execute the image lane. This is not an image-build receipt.

The public repository must not route arbitrary PR code onto credential-bearing private review hosts. Existing observed labels do not establish an isolated lane or allowed public-repository group policy. No self-host routing, new runner permission, provisioning, native CLI login, actual image build or model operation is authorized by this source candidate. The separate isolated native-validation dispatch workflow is unchanged. Unmerged composition375ac is excluded; source base is3a6847b8586ebe8f006783fbdb563e582e075f26.

No PR, push or merge until billing/hosted CI is restored and fresh required checks and review complete. The<800 hosted-minute/day goal needs observed run counts/cancellation and a separately qualified lane; it is not proven by these source edits. No paid fallback is introduced.

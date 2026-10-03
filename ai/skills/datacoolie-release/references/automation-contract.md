# Upload automation contract

Project automation consumes the installed DataCoolie CLI. The public [project
workflow and release handoff](https://datacoolie.github.io/datacoolie/guide/cli/project/#release-handoff-external-upload-workflow)
owns build output, manifest fields, validation scope, and the exact upload
mapping. This reference keeps only the release-agent boundary and evidence
gates.

## Required boundary

- Validate the project and the selected artifact with the installed CLI and
  require the JSON result's `ok` field, alongside the process exit code.
- Pin one exact retained `build_id` and environment before invoking an upload.
- Follow the public handoff's ordered phases and mapping; do not advance a
  dependent phase until its prerequisite succeeds.
- Pass structured arguments to the project/target adapter. It does not rebuild
  the project, execute a runner, activate a target, install packages, or create
  a scheduler job.

For local contract tests, `scripts/upload_local.py` is a local adapter only;
cloud targets use their official upload command. Keep one pinned source path for
all phases and do not re-resolve the selected build halfway through a transfer.

## Credentials and records

Use the CI platform's secret store, workload identity, or target CLI. Never put
tokens in `datacoolie.yml`, manifests, generated runners, command logs, or
records. A small `.releases/<env>/<release_id>.json` record may contain the
build ID, environment, deployment path, source path, phase statuses, file
counts, and redacted errors. It must not claim remote verification, activation,
rollback, authorization, or job health.

## Retry and outcome evidence

Retry the same pinned build at the relative-file level. A prerequisite phase
failure prevents its dependent phase; a later-phase failure is
`partial_failure`. Retain earlier successful output and retry the same pinned
bytes without deleting or inventing rollback behavior.

Report the exact validation results, pinned build/environment, adapter commands
and phase outcomes, skipped remote checks, and unresolved questions. Author/build
checks do not require workload execution; workload output, activation, and target
health checks belong to the project/platform workflow and are outside upload-only
Release evidence.

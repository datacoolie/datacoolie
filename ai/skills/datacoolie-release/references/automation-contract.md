# Upload automation contract

Release automation is project-owned and consumes the installed DataCoolie CLI.
It must not import or vendor a skill validator. The automation may wrap local
validation and an official target upload command, but it does not rebuild the
project or execute a runner.

## Required sequence

```text
dc validate --project-dir <project> --format json
dc validate --artifact-path <project>/.builds/current --format json
pin manifest.build_id
upload retained artifact to <deployment_path>/artifacts/<build_id>/
upload the same retained artifact to <deployment_path>/current/
```

The second upload is conditional on the first succeeding. Keep one immutable
source path for both operations. Do not read or upload the moving `current`
directory after pinning. Pass paths as structured process arguments so spaces
and special characters remain safe.

For local contract tests, `scripts/upload_local.py` implements this same
sequence against a local directory. It is a test/local adapter only; cloud
targets must use their official upload command.

## Credentials and records

Use the CI platform's secret store or workload identity. Never place tokens in
`datacoolie.yml`, manifests, generated runners, command logs, or records. A
small `.releases/<env>/<release_id>.json` record may contain the build ID,
environment, deployment path, source path, phase statuses, file
counts, and redacted errors. It must not claim remote verification, activation,
rollback, authorization, or job health.

## Retry and failure

Uploading a previously published build is idempotent at the relative-file level.
Retry the same pinned build after a failure. Artifact-phase failure means no
current upload is attempted. Current-phase failure is reported as
`partial_failure`; do not delete the artifact or attempt an unplanned rollback.

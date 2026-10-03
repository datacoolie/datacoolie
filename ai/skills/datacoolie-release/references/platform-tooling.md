# Platform upload tooling

Use this reference only after the project has selected its target storage and
official copy mechanism. The public [project workflow and release handoff](https://datacoolie.github.io/datacoolie/guide/cli/project/#release-handoff-external-upload-workflow)
owns the DataCoolie artifact mapping and phase order. This reference keeps
command hygiene, not target provisioning, activation, scheduler creation, or
workload execution.

## Selection

Prefer the existing project-owned adapter or the target platform's official
file/object upload CLI. Examples include AWS CLI for S3, Databricks/Fabric
file APIs where the target actually exposes a file store, and a local copy for
local deployment paths. Do not invent a generic cloud abstraction when the
target's path semantics differ.

Check current official documentation for the installed tool and record its
version. Authentication is supplied by the operator, CI secret store, or
workload identity. Never write tokens, secret values, or raw provider responses
to project files or release records.

## Command safety

- Pass an argument vector instead of a concatenated shell string.
- Quote/escape paths with spaces and preserve Unicode.
- Map every source relative path to the selected destination prefix.
- Use overwrite/update semantics for matching objects only.
- Do not pass delete, purge, clean, activation, package-install, or execute
  flags.
- Capture exit status and a redacted, bounded error message.

Follow the public release handoff for the two ordered upload phases. Remote
state observation or checksum comparison is outside this upload-only contract
and must not be reported as performed by a successful copy command.

For author/build-only requests, perform CLI/artifact checks without invoking a
runner. For authorized uploads, report local validation and transfer commands
with their performed, skipped, and unavailable checks. Route target/runtime
verification to the project/platform owner; Release does not execute a runner
or target workload.

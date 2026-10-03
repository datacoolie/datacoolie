# Upload contract

The public [project workflow and release handoff](https://datacoolie.github.io/datacoolie/guide/cli/project/#release-handoff-external-upload-workflow)
owns artifact layout, manifest fields, target configuration, and the exact
upload mapping. This reference defines the release agent's upload-only boundary,
source pinning gate, and evidence requirements.

## Source pinning

Resolve the selected build once, validate it with the installed CLI, and pin its
`build_id` and environment before transfer. Use the retained environment
projection named by the public handoff; do not re-resolve a selector or read
moving `current` bytes halfway through an upload. If project/current comparison
was not supplied, report that limited scope explicitly.

## Transfer gate

Use the target's official file/object copy command or a checked-in project-owned
adapter. The public handoff owns phase layout and relative-path mapping. Release
must pin one retained byte source, must not advance a dependent phase until its
prerequisites succeed, and must retry the same pinned bytes after a failure.
Record each phase's command outcome and classify a later-phase failure as
partial; retain the earlier successful phase for retry.

The release workflow does not delete or purge unknown target files, activate a
resource, install a package, create a scheduler, invoke a job, compare remote
hashes, or claim remote integrity from a successful copy command.

## Adapter obligations

The adapter receives an argument list, preserves spaces and Unicode paths,
exposes exit status and a bounded redacted error, and records attempted source
file counts without credentials or raw provider responses. Target identity and
activation state remain the platform workflow's concern.

## Outcome-scoped verification

For author/build-only requests, validate and pin the artifact without executing
a runner. For an authorized upload, retain local source validation and upload
phase evidence only. Workload output, activation, import, and target health
checks route to the project/platform workflow; Release does not execute them.
Retain validation results, phase outcomes, skipped/unknown remote checks, and
unresolved questions.

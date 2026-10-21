---
name: datacoolie-release
description: Upload one locally verified DataCoolie environment artifact using the public project handoff. Owns upload authorization, adapters and evidence; never activates, executes, installs, deletes, or provisions a workload.
---

# DataCoolie release

## Outcome and boundary

Release is an upload-only handoff. Follow the public
[release handoff](https://datacoolie.github.io/datacoolie/guide/cli/project/#release-handoff-external-upload-workflow)
for build selection, validation scope, destination ownership, transfer order
and retry behavior. Read [CLI commands](https://datacoolie.github.io/datacoolie/guide/cli/commands/)
for parameters and responses. This Skill owns the contextual authorization,
adapter choice, transfer evidence and local upload record.

Release ends at upload results. Target activation, package installation,
scheduler/job creation, workload execution, monitoring and recovery belong to
the target workflow. Do not create a job, run a runner, delete/purge remote
files, compare remote hashes or claim remote integrity from local validation.

## Inputs and preflight

Require an exact project or retained artifact, environment, build selection,
target and explicit upload authorization appropriate to that target. Use
credentials supplied by its external mechanism; keep them out of authored files
and evidence.

1. Read the public handoff and choose the documented validation checks for the
   selected scope. For a project, use `dc validate --project-dir <project>
   --format json` and the matching artifact validation command.
2. Interpret the JSON results using the CLI response documentation. Require
   the checks appropriate to that target; retain limited/skipped-check evidence.
   Stop on failed or insufficient required validation rather than claiming
   history/integrity verification beyond the report's scope.
3. Pin the validated build/environment source before transfer. Resolve the
   target through the documented project contract; never infer it from an
   unrelated artifact or moving projection. Keep input bytes fixed across phases.
4. If resources or permissions are missing, return the evidenced gap to
   Provision. Local validation does not authorize upload or activation.

## Transfer contract

Read `references/deployment-contract.md` and the relevant tooling reference.
Use an official copy/upload command or existing checked-in project adapter.
Construct argument vectors, handle paths with spaces and fail closed on an
empty/ambiguous destination. Follow the public handoff's phase mapping and
completion/retry rules without inventing another publication protocol.

Route the selected handoff details directly to the focused references:

| Release scope | Reference |
|---|---|
| Environment runner mapping and target/runtime boundary | `references/runner-deployment-mapping.md` |
| Retained Python functions roots, packaging and host import boundary | `references/python-functions-deployment.md` |
| Adapter sequencing, credentials and transfer evidence | `references/deployment-contract.md` and `references/automation-contract.md` |

Retain per-phase commands, process status, file counts where available and safe
errors. Stop before the next phase when its prerequisite fails; report
`partial_failure` when the documented later phase fails. Retry only the pinned
build. Retain target-only files and perform only the authorized upload.

`scripts/upload_local.py` is the local/fake-target adapter for this boundary;
it does not qualify cloud transfer. Automation consumes existing local reports
and uses `references/automation-contract.md` for agent-specific checks.

## Local release record

If persisting a record, use the existing `templates/upload-record.json.example`
and `schemas/upload-record.schema.json`; validate the exact record with
`scripts/validate_upload_record.py`. The schema keeps a v1 branch for existing
records and a v2 branch for the local adapter's preflight and record-persistence
stages. The validator owns only phase relationships after JSON Schema has
validated shape and types.

The local adapter writes v2 records. A failed preflight records both upload
phases as `skipped` with zero files and marks the overall transfer `failed`.
Once preflight passes, artifact/current phase semantics remain unchanged. The
identity fields may be `null` only before preflight succeeds. A requested
record is marked `written` only after its atomic write completes; a persistence
failure leaves the transfer and preflight outcomes on stdout, marks the record
stage `failed` with a bounded safe error, and exits nonzero.
Return its exact path rather than discovering a latest receipt.

The record observes transfer commands. It does not establish activation,
authorization, target health or remote integrity. Keep secrets, raw provider
responses and credentials out of records. Reuse validated CLI evidence instead
of creating a second manifest, checksum sidecar or release validator.

## Routing

| Need | Route |
|---|---|
| Project edits, metadata, runners/functions or local validation/build | `datacoolie-build` and its public docs |
| Missing target resources or access | `datacoolie-provision` / target owner |
| Material release-policy change | `datacoolie-design` |
| Target activation, scheduler jobs, package installation or rollback operation | project/platform workflow |

## Verification

Shared CLI response interpretation, authorization boundaries and credential
handling come from `datacoolie/ai/AGENTS.md`; this section keeps only the
release-specific handoff result that must be reported.

Report selected input/build/environment, documented destination mapping,
authorization reference, validation results and scope, transfer-phase results,
record path if present, skipped checks and unresolved questions. Success requires
all checks/phases required by the selected public handoff; explicitly preserve
partial failures and unverified remote/runtime claims.

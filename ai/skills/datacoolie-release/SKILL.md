---
name: datacoolie-release
description: Upload one locally verified DataCoolie environment artifact to the deployment path declared in datacoolie.yml. This skill never activates, executes, installs, deletes, or provisions a workload.
---

# DataCoolie release

Use the public [release handoff](https://datacoolie.github.io/datacoolie/guide/cli/project/#release-handoff-external-upload-workflow)
for the shared project/build contract. This Skill adds the approval, adapter,
receipt, and evidence procedure; it does not redefine CLI behavior.

## Outcome and boundary

Release is an upload-only handoff for a project build. It consumes an exact,
locally verified `.builds/current` (or an explicitly selected retained build),
pins its `build_id`, and copies the selected environment projection to the
target path configured by `environments.<env>.deployment_path` in
`datacoolie.yml`.

The transfer order is:

```text
<deployment_path>/artifacts/<build_id>/   # immutable history first
<deployment_path>/current/                # only after artifact upload succeeds
```

Overwrite matching paths, retain unrelated target files, and report a partial
failure if the second upload fails. Do not delete, purge, activate, install a
package, create a job, run a runner, compare remote hashes, or infer a target
platform operation. A platform-specific upload command is an external tool
owned by the project/target; this skill records the exact command and result.
For local/fake-target verification, `scripts/upload_local.py` implements the
same two-phase copy without pretending to support cloud URIs.

## Inputs and preflight

Require:

- project path (or a direct retained build path for a portable handoff);
- exact environment name;
- optional build selector (`current` by default or a validated build ID);
- an explicit upload authorization appropriate to the target;
- credentials supplied by the platform CLI/environment, never in metadata or
  a receipt.

For a project selector:

1. Run `dc validate --project-dir <project> --format json` and require `ok`.
2. Run `dc validate --artifact-path <project>/.builds/current --format json` and
   require `ok` plus `details.current_comparison.ok`.
3. Read the root `manifest.json` once and pin its `build_id`.
4. Select `.builds/artifacts/<build_id>/<env>` as the upload source. Do not read
   moving `current` bytes after the pin.
5. Read only `environments.<env>.deployment_path` from `datacoolie.yml`. A
   missing value is a release error, not a reason to consult `manifest.json`.

For a direct retained build, run `dc validate --artifact-path
<build>/<env> --format json`; the result is limited-scope unless the enclosing
project/current comparison is also supplied. Never claim project-history
verification when it was not performed.

The source must contain the environment `manifest.json`, metadata, the
configured SQL roots, functions outputs, and matching `runners/<env>` files as
the build produced them. The root build manifest is the sole inventory and
hash descriptor; there is no `build.json` pointer or `SHA256SUMS` sidecar.

## Transfer contract

Use the target's official copy/upload command or a checked-in project-owned
adapter. Construct argument vectors rather than shell-concatenated strings;
quote paths with spaces; fail closed on an empty or ambiguous destination.

The adapter must:

- upload every source file below the retained environment directory;
- map the root of that directory to `<deployment_path>/artifacts/<build_id>/`;
- repeat the same mapping to `<deployment_path>/current/` only after the first
  operation succeeds;
- overwrite a destination object with the same relative path;
- never issue delete, purge, recursive-clean, activation, install, or execute
  operations;
- return per-phase command, exit status, and safe error text (without tokens or
  provider response bodies).

Re-running the same build is a safe retry. A target may retain files from an
older build because release intentionally does not delete unknown paths. A
consumer needing an exact version should address `artifacts/<build_id>` rather
than the mutable `current` projection.

## Local release record

If an adapter persists a record, use `.releases/<env>/<release_id>.json` with
only:

```json
{
  "schema_version": 1,
  "release_id": "...",
  "build_id": "...",
  "environment": "dev",
  "deployment_path": "...",
  "source": ".../artifacts/<build_id>/dev",
  "status": "success|partial_failure|failed",
  "uploads": {
    "artifact": {"status": "success", "files": 0},
    "current": {"status": "success", "files": 0}
  }
}
```

This is an observation of local transfer commands, not proof of remote
integrity, activation, authorization, or workload health. Never store secrets,
raw cloud responses, or credentials. There is no requirement for build-runtime
receipts, provision receipts, runner qualification, target activation, or
rollback chains in an upload-only release.

## Routing

| Need | Route |
|---|---|
| Project/config/build validation | `datacoolie-build` and the installed CLI |
| Target existence, permissions, or infrastructure | `datacoolie-provision` / platform owner |
| Metadata, runner, or function changes | `datacoolie-build` |
| Target activation, scheduler/job creation, package installation, rollback policy | target/platform workflow outside this skill |

## Verification

Before reporting success, retain the JSON results from both local validation and
both upload phases, the pinned build/environment, the exact destination mapping,
and any skipped/unknown remote checks. If artifact upload fails, do not attempt
current. If current upload fails after artifact succeeds, report
`partial_failure` and retry the same pinned build; never auto-delete or roll
back remote files.

---
title: DataCoolie CLI preparation walkthrough
description: Prepare, inspect, build, and verify a DataCoolie artifact project with the CLI.
---

# CLI preparation walkthrough

This walkthrough takes a complete DataCoolie project from a clean checkout to a
validated build artifact. It uses the canonical **Artifact SQL project** so the
commands exercise project validation, inspection, dry-run planning, build
publication, and artifact integrity checks together.

The CLI prepares inputs and artifacts; it does not execute a dataflow or a
project runner. After the final validation step, continue with [Run a
stage](../operations/run-stage.md) when you are ready to choose an engine,
platform, state path, and runner-owned execution configuration.
To execute this walkthrough's downloaded project locally, use the
[Artifact project recipe](../../examples/dataflows.md#artifact-project-recipe),
including its `polars-sql` dependency profile and project-owned runner.

## Prerequisites

Install the CLI extra in the environment that will run the commands:

~~~bash
python -m pip install "datacoolie[cli]"
~~~

Download [Artifact SQL project](../../examples/downloads/artifact.zip) from the
examples catalog and extract it into a new working directory. The archive has a
top-level artifact/ directory. For example, the following commands work in a
POSIX shell even when the parent directory contains spaces:

~~~bash
curl -fsSL https://datacoolie.github.io/datacoolie/examples/downloads/artifact.zip -o artifact.zip
mkdir "cli tutorial"
unzip artifact.zip -d "cli tutorial"
cd "cli tutorial/artifact"
~~~

On PowerShell, the equivalent extraction is:

~~~powershell
Invoke-WebRequest -Uri https://datacoolie.github.io/datacoolie/examples/downloads/artifact.zip -OutFile .\artifact.zip
New-Item -ItemType Directory -Path "cli tutorial" -Force | Out-Null
Expand-Archive -Path .\artifact.zip -DestinationPath ".\cli tutorial" -Force
Set-Location ".\cli tutorial\artifact"
~~~

For an offline checkout, use the same canonical source directory instead:
docs/examples/files/projects/artifact. Run the commands below from that
directory, or keep another working directory and pass its path with
--project-dir.

## 1. Confirm the CLI and project

Help and version are informational text commands. They do not use the JSON
envelope:

~~~bash
dc --version
dc --help
~~~

The project-aware commands below use --project-dir . so the selected project
is explicit and does not depend on the current-directory discovery walk. A
successful JSON response has the shared envelope
{schema_version, datacoolie_version, ok, data}. Check the process exit code
and then the top-level ok field before reading data.

## 2. Validate the authored project

~~~bash
dc --format json validate --project-dir .
~~~

The canonical project should return exit code 0, "ok": true,
data.scope: "project", and an empty data.details.not_checked list. This
check reads configuration, metadata, resources, and SQL references. It does
not start a Driver, connect to a source or destination, execute SQL, or run
the project runner.

## 3. Inspect the resolved inputs

Inspect configuration and a full dataflow item when you need to understand
what the project declares:

~~~bash
dc --format json inspect config --project-dir .
dc --format json inspect metadata --project-dir . \
  --section dataflows --full
~~~

The first response places the effective configuration under data, including
data.project_dir and data.resolved_components. The second response lists
data.documents, counts, and the redacted or full data.items selected by the
filters. Inspection is an inventory view; it does not prove schema/model
validity or runtime readiness.

## 4. Preview the build without writing .builds

~~~bash
dc --format json build --project-dir . --dry-run
~~~

Expect exit code 0, data.status: "dry_run", a secret-free data.plan, and
data.not_performed entries for serialization/round-trip, function packaging,
assembled-artifact verification, and publication. A dry-run may calculate an
input digest and validate the same local inputs as a normal build, but it does
not create .builds, staging files, locks, package outputs, or runtime state.

## 5. Create the immutable build

~~~bash
dc --format json build --project-dir .
~~~

The command builds all declared environments in one operation. A successful
response reports data.status as created or reused, together with the
immutable data.build_id, data.build_path, and data.current_path. Use the
paths returned by this response; do not construct a build ID from the clock or
from a fixed example value. The normal build writes .builds/ and may invoke
a configured Python wheel backend, but it never executes a runner or dataflow.

## 6. Inspect and verify the published artifact

First inspect the root manifest:

~~~bash
dc --format json inspect artifact --project-dir .
~~~

For the build root or .builds/current, a successful inspection reports
data.artifact_type: "datacoolie_build" and data.limited_scope: false.
Then validate the mutable current projection:

~~~bash
dc --format json validate --artifact-path .builds/current
~~~

Require exit code 0, top-level ok: true, and
data.details.current_comparison.performed: true with
data.details.current_comparison.ok: true when the release gate needs proof
that current matches its retained build. A retained
.builds/artifacts/<build_id> directory has inventory and hash checks but does
not require a comparison with current. An environment-only directory such
as .builds/current/dev is useful for a portable handoff, but its successful
report remains limited scope (data.details.limited_scope: true).

## What this workflow changes

- Validation, inspection, and build --dry-run do not modify authored inputs.
- The normal build creates or reuses .builds/artifacts/<build_id>/ and then
  updates .builds/current/ only after the artifact is verified.
- The CLI does not upload deployment_path, execute runners/<env>, resolve
  secrets, open provider connections, or run SQL.
- Keep the returned build ID and artifact paths in the handoff receipt. The
  external release workflow decides how a selected environment is uploaded;
  see [Release handoff](project.md#release-handoff-external-upload-workflow).

## Recipe for an AI agent

Use this sequence when automating the walkthrough:

1. Invoke every command with --format json except --help and --version.
2. Treat a non-zero process exit code as failure. On exit code 0, require the
   envelope's top-level ok value before consuming data.
3. Ignore unknown additive fields, but do not assume omitted required fields
   are successful. Read data.details.not_checked, warnings, and
   limited_scope according to the selected target.
4. Pass the build path or ID returned by build to later checks. Do not use a
   hard-coded build ID or parse human-readable text.
5. For a release decision, validate the exact selected artifact. Require the
   current/history comparison only for .builds/current; retain the
   limited-scope marker for an environment-only or manifest-less handoff.

## Common recovery

| Symptom | Action |
|---|---|
| dc is not found or an optional dependency is missing | Activate the intended Python environment and install datacoolie[cli]; install the extra named by a conversion or packaging error. |
| Project not found | Run from the directory containing datacoolie.yml or pass --project-dir <project>. |
| Validation warning for no dataflows | Add a dataflow before building a runtime artifact; warnings keep exit code 0 but do not create runtime behavior. |
| Metadata or resource validation fails | Read data.errors and data.details.not_checked, then fix the named configuration, metadata, or path before retrying. |
| current_comparison.performed is false | Validate the root .builds/current; a retained build or environment directory has a different, documented scope. |
| Build output already exists | Use the reported build_id and inspect the input digest. Do not delete or overwrite an existing artifact to hide a collision. |
| init or agents update cannot download guidance | Check network access and retry the command; these operations do not silently fabricate the canonical AGENTS.md. |

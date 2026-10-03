# DataCoolie runner contract

Read this reference when authoring or reviewing a Python or notebook entrypoint.
The runner is project-owned execution code: it constructs framework components
and calls one framework operation. The public [project workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/),
[runtime configuration](https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/),
[provider guide](https://datacoolie.github.io/datacoolie/guide/providers/),
[run-stage guide](https://datacoolie.github.io/datacoolie/guide/operations/run-stage/),
and [published runner examples](https://datacoolie.github.io/datacoolie/examples/runners/)
own layout, constructor fields, defaults, host setup, and recipe details.

## Identity and location

Derive runner location, environment mapping, host setup, and copy behavior from the public project
workflow and the selected [public example](public-examples.md). For a newly authored host-specific
runner, keep platform, engine, provider, and operation identity in its descriptive filename and use
separate files for normal run, replay, and maintenance. When reusing a maintained example, preserve
its canonical filename and fixed host/operation identity; a generic filename does not permit runtime
implementation selectors. The selected project/environment remains the source of truth.

## Construction boundary

Each runner may only:

1. Decode execution-host parameters, including one received stage value and any caller-supplied job
   context defined by the public recipe.
2. Construct the selected platform, engine, and metadata provider.
3. Perform required engine-local setup through public APIs, such as qualified Delta/Iceberg
   registration for Polars SQL.
4. Construct `DataCoolieRunConfig` and pass caller-owned runtime paths and `run_attributes`.
5. Construct `DataCoolieDriver` and call exactly one selected operation.

The runner does not discover or merge metadata, resolve query files, install packages, create
scheduler jobs, mutate target resources, or execute another runner. Preparation, including SQL-file
and secret resolution, belongs to the framework execution context; authored metadata is not
rewritten by a runner.

## Paths and context

Use the public [runtime path ownership guide](https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/)
and the selected provider page for the exact component roots, precedence, fallback, and caller
context. Pass project-selected roots unchanged; do not invent a fixed metadata or SQL root or a
second path alias. The public runtime guide owns the run-attribute shape and persistence boundary;
pass caller-owned scheduler context through the documented run configuration without introducing
another session identity or placing scheduler fields in project metadata. Secrets are resolved by
the configured provider and never written to metadata, artifacts, or logs.

## Stage, replay, and maintenance

Pass one received stage value through one `driver.run(stage=stage)` call. Do not split a selection,
create stage plans, hardcode layer names, or call the driver repeatedly inside a normal runner.
Use the dedicated operation reference and public guides for replay/maintenance safety parameters.
External orchestration may invoke the same runner again for another stage.

## Functions

Use the public [function project recipe](https://datacoolie.github.io/datacoolie/examples/dataflows/#function-project-recipe)
and sibling [function guidance](python-functions-contract.md) for build/host boundaries. The runner does not
package or install functions. Preserve the recipe's existing project-owned import/bootstrap
adaptation and fixed prefixes against the selected project/build; do not turn them into runtime
selectors, arbitrary search paths, or a new runner-owned `sys.path` bootstrap.

## Outcome-scoped verification

For author/build-only requests, inspect the runner source, validate the project, build the requested
artifact, and confirm that the built runner bytes remain the authored bytes. Do not execute a
workload merely to certify authoring/build success. For an explicitly requested safe runtime check,
follow the public [build and execute workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/#build-and-execute)
and matching recipe for the exact copy, command and path contract. Confirm the CLI-selected retained
environment is the one being checked, read output/state independently and preserve the documented
rerun and retained/current validation evidence. Identify legitimate empty or skipped results,
unavailable host dependencies, and any unperformed cloud check.

Report exact input/output paths or IDs, the CLI/result evidence, checks performed and skipped,
blockers/next owner, and unresolved questions. No package installation, remote upload, activation,
or workload discovery belongs in a Build runner.

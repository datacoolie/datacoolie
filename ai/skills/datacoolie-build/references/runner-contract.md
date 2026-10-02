# DataCoolie runner contract

Read this reference when authoring or reviewing a Python or notebook entrypoint.
The runner is project-owned execution code. It constructs framework components
and calls one framework operation; it does not become a second project/build
system.

Use the public [runtime configuration guide](https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/)
and [project workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/)
for the full path/build contract. The checks below are the agent-facing
adaptation and handoff rules that remain specific to runners.

## Identity and location

Runners are stored below `runners/<environment>/` and copied to the same
environment directory in a build. For newly authored host-specific runners,
keep implementation identity in the filename:

```text
run_<platform>_<engine>[_{provider}].py|ipynb
replay_<platform>_<engine>[_{provider}].py|ipynb
maintenance_<platform>_<engine>[_{provider}].py|ipynb
```

When reusing a maintained [public example](public-examples.md), preserve its
canonical filename, including generic project-owned names. Verify the fixed
platform, engine, provider and operation from its source and usage guide; a
generic filename does not permit runtime implementation selectors.

The environment is selected by the directory, not by a runtime flag. Platform,
engine, provider, and operation are fixed by the entrypoint. Use separate files
for normal run, replay, and maintenance rather than a large mode switch.

## Construction boundary

Each runner may do only the following:

1. Decode its execution-host parameters (including one optional `stage` scalar
   and optional integer `job_num`/`job_index`, default `1`/`0`).
2. Construct the selected platform, engine, and metadata provider.
3. Perform required engine-local setup through public APIs (for example,
   register qualified Delta/Iceberg tables for Polars SQL).
4. Construct `DataCoolieRunConfig` and pass caller-owned runtime paths.
5. Construct `DataCoolieDriver` and call exactly one selected operation.

The runner does not discover or merge metadata, resolve query files, install
packages, create scheduler jobs, mutate target resources, or execute another
runner. Preparation—including SQL-file and secret resolution—belongs to the
framework execution context before a reader is created. Authored metadata is
not rewritten by a runner.

## Paths and context

Use the path that matches the chosen runtime mode:

| Mode | Required/optional input |
|---|---|
| Project artifact | `artifact_base_path`; metadata defaults to `<artifact>/metadata` unless `metadata_base_path` is explicit |
| Standalone metadata | `metadata_path` (one file) or `metadata_base_path` (metadata directory) |
| SQL files | one or more `sql_base_path` values on the metadata provider or Driver; provider roots are preferred and Driver roots are the session fallback |
| Runtime state | `state_base_path`; provider may derive missing component roots |
| File-provider watermark | optional `watermark_base_path`; it takes precedence over derivation |
| Log output | `log_base_path`; do not use `base_log_path` |

Pass paths explicitly and unchanged. `FileProvider` may be constructed without
a platform, but the platform is required once it performs I/O. Provider-specific
attributes remain behind `BaseMetadataProvider`; the Driver validates only
cross-component conflicts and startup readiness.

Use the public [metadata provider guide](https://datacoolie.github.io/datacoolie/guide/providers/)
and [runtime path ownership guide](https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/)
for provider/Driver precedence and SQL-root examples.

External scheduler/job identifiers are caller-owned `run_attributes`, one strict
JSON object passed to `DataCoolieRunConfig`. Do not introduce another session ID
or place scheduler fields in project metadata. Secrets are resolved by the
configured provider and never written to metadata, artifacts, or logs.

Example Python construction:

```python
platform = LocalPlatform()
engine = PolarsEngine(platform=platform)
metadata = FileProvider(
    metadata_base_path=args.metadata_base_path,
    platform=platform,
    watermark_base_path=args.watermark_base_path,
    sql_base_path=args.sql_base_path or None,
)
config = DataCoolieRunConfig(
    job_num=args.job_num,
    job_index=args.job_index,
    run_attributes=args.run_attributes,
    dry_run=args.dry_run,
)
with DataCoolieDriver(
    engine=engine,
    metadata_provider=metadata,
    artifact_base_path=args.artifact_base_path,
    sql_base_path=args.sql_base_path,
    state_base_path=args.state_base_path,
    log_base_path=args.log_base_path,
    config=config,
) as driver:
    result = driver.run(stage=args.stage)
```

When an artifact root is provided with no metadata provider, the Driver creates
the file provider for `<artifact>/metadata`. If a provider is supplied, an
explicit `metadata_base_path` must agree with the provider's configured root;
conflicting explicit SQL roots also fail during construction. A provider may
retain SQL roots without a platform; Driver preparation uses its execution
platform to read the selected file. A project runner should pass exact
component roots rather than reading `manifest.json`; the framework intentionally
ignores build manifests.

## Stage, replay, and maintenance

Pass one received stage value unchanged to one call:

```python
driver.run(stage=stage)
```

Do not split comma-delimited values, create stage plans, hardcode medallion
names, or call the driver repeatedly inside the runner. An external orchestrator
may invoke the same runner again for another stage. Replay and maintenance use
their dedicated operation APIs and their own safety parameters; read
`operations-contract.md` for those details.

## Functions

Build may package each configured functions root as a wheel, root-init ZIP, or
copied source. The runner does not package or install it. Pass a fixed
`allowed_function_prefixes` list rendered from the authored project/build, or
`[]` when no function source is used. Never accept the import prefix as a runtime
selector. Do not add a new `sys.path` bootstrap. When reusing the maintained
[Function project recipe](https://datacoolie.github.io/datacoolie/examples/dataflows/#function-project-recipe),
preserve only its existing project-owned bootstrap: the root is derived from
the entrypoint layout, and the import path is the authored source root or its
deterministic CLI-built functions ZIP. Keep the fixed import prefixes; this
exception does not permit runtime-selected module roots, arbitrary search
paths, package installation or runner-owned packaging.

## Verification checklist

- Runner path is `runners/<env>`; implementation identity is fixed by the
  descriptive filename or verified canonical example source and guide.
- It has no `--env`, platform, engine, provider, or operation selector.
- `log_base_path` is used; `base_log_path` is absent.
- Metadata, SQL, artifact, state, watermark, and run-attribute values are passed
  to framework APIs without runner-side reinterpretation.
- Stage reaches one framework call unchanged; job shard values reach
  `DataCoolieRunConfig`.
- No package installation, remote upload, activation, or workload discovery is
  performed by the runner.
- The exact built runner bytes remain the authored bytes.

---
name: datacoolie-build
description: Build, modify, run, and verify DataCoolie projects through the public CLI and framework APIs. Owns metadata, runners, functions, local verification and requested project automation; route discovery, material design, infrastructure and upload to their owning skills.
---

# DataCoolie Build

## Outcome and boundary

Turn project intent into a valid, runnable DataCoolie project and an immutable
all-environment build. The canonical contract is `datacoolie.yml`; the installed
`dc`/`datacoolie` CLI owns deterministic operations while this skill supplies
authoring decisions, runner guidance and local execution checks.

Build does not create a `run` CLI command, deploy or provision target resources,
discover source facts, make material architecture decisions, or execute a
workload without a project-owned runner/script.

## Route only the needed resource

| Need | First action |
|---|---|
| Project bootstrap | `dc init [PATH]` |
| Project/config/metadata/resource checks | `dc validate --project-dir <PATH>` |
| Standalone metadata or artifact check | `dc validate --metadata-path ...` or `--artifact-path ...` |
| Config, metadata, capability or artifact inventory | `dc inspect ... --format json` |
| Metadata representation conversion | `dc metadata convert ...` |
| Build all environments | `dc build --project-dir <PATH>` |
| Latest project guidance | `dc agents update --project-dir <PATH>` |
| Framework fields and Driver behavior | Public [runtime configuration](https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/) and [metadata document](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#metadata-document); use `references/runner-contract.md` for agent checks |
| Replay or maintenance | `references/operations-contract.md` and the matching runner |
| Public runner/source examples | `references/public-examples.md` and the published examples URL |
| Polars qualified SQL/table registration | `references/polars-qualified-sql.md` |
| Unsupported boundary | `references/framework-boundary.md` |
| Design approval handoff | `datacoolie-design/scripts/design_approval.py` |

Use `--format json` for agents/automation, inspect exit code/`ok`, and never copy skill scripts into a project as runtime dependencies.

The project-owned metadata contract is at
`https://datacoolie.github.io/datacoolie/schema/index.json`; the stable current
authoring alias is `https://datacoolie.github.io/datacoolie/schema/latest/metadata.schema.json`.
For a specific framework, use the index to choose the greatest compatible
versioned schema and `dc validate` offline. `latest` is resolved locally, not
fetched; pin a versioned URL for reproducible artifacts. This Skill carries no
competing schema/validator.

When this Skill summary differs from a public page, follow the public page for
framework behavior; this Skill is for sequencing, gates, evidence and
project-specific adaptation, not authoritative fields or defaults.

## Docs-first metadata routing

Read the public [Metadata Guide](https://datacoolie.github.io/datacoolie/guide/metadata/)
before drafting/revising metadata; it is the user-facing source of truth. Follow
the workflow through these direct routes:

1. [Build your first metadata file](https://datacoolie.github.io/datacoolie/guide/metadata/first-metadata-file/)
2. [Connections](https://datacoolie.github.io/datacoolie/guide/metadata/connections/)
3. [Dataflows](https://datacoolie.github.io/datacoolie/guide/metadata/dataflows/)
4. [Source patterns](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/)
5. [Transform patterns](https://datacoolie.github.io/datacoolie/guide/metadata/transform-patterns/)
6. [Destination and load patterns](https://datacoolie.github.io/datacoolie/guide/metadata/destination-and-load-patterns/)
7. [Datatypes and schema hints](https://datacoolie.github.io/datacoolie/guide/metadata/data-types/)
8. [Validation checklist](https://datacoolie.github.io/datacoolie/guide/metadata/validation-checklist/)

Use the [Metadata guide](https://datacoolie.github.io/datacoolie/guide/metadata/#metadata-document),
[API source configuration](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/#api-source-configuration)
and [incremental windows](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/#incremental-windows-and-look-back)
for complete configuration. For combinations, use [window replacement](https://datacoolie.github.io/datacoolie/guide/metadata/watermark-window-replacement/),
[paginated API](https://datacoolie.github.io/datacoolie/guide/metadata/api-advanced/),
[late files](https://datacoolie.github.io/datacoolie/guide/metadata/late-arriving-files/),
[protected keys](https://datacoolie.github.io/datacoolie/guide/metadata/stable-keys-and-protected-output/)
and [incremental SCD2](https://datacoolie.github.io/datacoolie/guide/metadata/merge-and-scd2/).
Use the exact [metadata schema reference](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#metadata-document)
for fields/anchors. This Skill adds routing, gates and edge cases; it does not
replace or restate the public guide.

## Inputs and gates

- Read `datacoolie.yml`, affected metadata, configured SQL/functions roots and
  the matching `runners/<environment>` directory.
- Require source discovery for a new source and design approval for material
  architecture/data contract/platform/release changes; artifacts inform
  authoring but never become runtime dependencies.
- For a material design, verify the final architecture before relying on it:
  `python <datacoolie-design>/scripts/design_approval.py verify --workspace <project>
  --architecture <project>/architecture/current.md`. The helper must find the
  hash-matching receipt under `.approvals/design/`; a missing, stale or unavailable
  receipt blocks Build. Reuse the helper; do not copy its hash/receipt validator into Build. Compatible
  implementation work with no material design dependency remains allowed.
- Validate the project before build. Resolve every query file reference using
  the configured SQL roots or artifact root; do not silently treat a missing
  `.sql` path as inline SQL.
- Keep secrets in platform/environment providers. Never place credentials in
  metadata, SQL, manifests, runners or logs.

## Project contract

`datacoolie.yml` is the only configuration file. Metadata defaults to
`metadata`; SQL/functions are optional multiple entries with a singular `path`.
Function entries independently select `auto`, `wheel`, `zip`, or `copy`. Runners
live below `runners/<env>` and keep authored bytes. For adaptations, use the
public examples source and pin its revision; runner source remains owned by the
public examples/project, not by this Skill.

`dc build` always builds every declared environment, copies metadata/SQL,
packages functions roots, copies matching runners and validates the artifact; it
does not execute a runner or upload anything.

Functions `auto` selection is deterministic:

1. a valid Python build backend in the root produces a wheel;
2. a root-level `__init__.py` produces a wrapped ZIP;
3. any other source tree is copied as source.

An `__init__.py` nested only below the configured root does not turn the whole
root into a ZIP. Use explicit packaging when a project needs a different
layout. Preserve the actual import prefix and configure
`allowed_function_prefixes` in the project-owned runner.

## Metadata and query preparation

Metadata documents use section wrappers, regardless of filename. The CLI may
build a single file, split files or preserve authored documents according to
`components.metadata.output`. Use `dc metadata convert` for a manual format
change; it does not merge overlays or resolve queries.

`source.query` remains the original declarative string in authored metadata.
It can be inline SQL, a relative `.sql` reference, or an explicit
`artifact:/...` reference. Query classification and file loading happen during
framework preparation before a reader is created. The metadata snapshot and
execution dataflow therefore keep separate roles; runtime logs may retain the
original declaration while `source_action["query"]` records the final SQL sent
to the source.

For qualified Polars SQL, runner code may register tables before calling the
Driver. The CLI does not infer table registration or rewrite SQL.

## Runner contract

Runners are project-owned scripts or notebooks. They construct the engine,
provider and `DataCoolieRunConfig`, then call `driver.run(...)`, replay or
maintenance APIs. They must pass paths explicitly and unchanged:

- artifact mode: `artifact_base_path` plus explicit/default metadata root;
- standalone metadata: `metadata_path` or `metadata_base_path`;
- SQL roots: one or more `sql_base_path` values on the metadata provider, with
  Driver `sql_base_path` available as the session fallback;
- mutable state: `state_base_path`, optional `watermark_base_path` and
  `log_base_path` according to the provider/fallback contract.

`FileProvider` may be created without a platform, but platform is required when
it performs I/O. Provider-specific path binding stays in the provider; the
Driver communicates through the base provider contracts and validates conflicts
when assembling the session.

Expose external scheduler/job context as one strict JSON object when needed,
for example `--run-attributes-json`, and pass it to
`DataCoolieRunConfig(run_attributes=...)`. Do not add a second log/session ID.
Use `log_base_path`; `base_log_path` is retired.

Driver construction starts the session and its JobRuntime lifecycle. Preparation
and query/secret resolution are part of that execution context, while authored
metadata remains unchanged. A startup or preparation exception must propagate
with the Driver's normal teardown and logging behavior.

## Build and local verification

Run:

```text
dc validate --project-dir <project> --format json
dc build --project-dir <project> --format json
dc validate --artifact-path <project>/.builds/current --format json
```

The build output is:

```text
.builds/artifacts/<build_id>/manifest.json
.builds/artifacts/<build_id>/<env>/manifest.json
.builds/current/manifest.json
.builds/current/<env>/manifest.json
```

The current root manifest has the same bytes and `build_id` as its retained
source. `dc validate` compares current with
`.builds/artifacts/<build_id>` when current is selected; a missing history,
manifest difference, missing file, extra file, changed SHA-256 or symlink is a
failure. The root manifest owns the file inventory and content digest. There is
no `build.json`, `SHA256SUMS` or deployment path in build output.

Use `.builds/artifacts/<build_id>/<env>` for an exact historical test. Keep
logs/watermarks under `.runtime/<env>/` or an explicitly isolated test root,
outside immutable builds.

`dc build --dry-run --format json` is a preview only. It reports effective
metadata output, SQL/functions/runners mappings and packaging decisions without
creating `.builds`, staging files, package outputs or runtime state.

## Automation and handoff

Generate project automation only when requested. Generated build automation
must invoke the installed CLI (`dc validate`, `dc build`) and may add project
specific orchestration around it. It must not import scripts from an installed
skill directory or vendor a second config/manifest validator.

Return exact project/artifact paths, build ID, environment coverage, validation
result, executed/skipped local checks and unresolved questions. Route missing
resources to provision and upload/deployment to release.

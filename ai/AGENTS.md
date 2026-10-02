# DataCoolie AI Workflow

This file owns agent routing, safety, approvals and handoffs. Public
documentation owns the shared framework and project contract. Do not copy
those contracts into Skills or into this file.

## Start with the public contract

Use the smallest relevant page before acting:

- [Project and CLI workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/)
  owns `datacoolie.yml`, layout, build output and the external upload handoff.
- [CLI commands](https://datacoolie.github.io/datacoolie/guide/cli/commands/)
  owns commands, options, responses and exit behavior.
- [Runtime configuration](https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/)
  owns paths, fallbacks, lifecycle and provider boundaries.
- [Metadata schema index](https://datacoolie.github.io/datacoolie/schema/index.json)
  owns versioned metadata contracts. Use the stable
  [latest schema alias](https://datacoolie.github.io/datacoolie/schema/latest/metadata.schema.json)
  for current authoring/discovery, and the installed CLI for offline checks.
  Pin a versioned URL for reproducible artifacts.
- [Examples catalog](https://datacoolie.github.io/datacoolie/examples/)
  owns executable sample source and project downloads.

When a Skill summary conflicts with a public contract, the public page owns
framework behavior. Keep the Skill summary only when it adds agent sequencing,
decision gates, evidence requirements or project-specific adaptation.

## Example retrieval protocol

Use the catalog to discover a sample, then use its direct action:

- `source` opens the rendered source page;
- `raw` opens the exact file bytes;
- `project-files` opens the project section in the catalog;
- `download` retrieves the complete project archive.

Pin the published revision when reproducibility matters. Verify the retrieved
source, raw bytes and archive belong to the same revision. If the requested
revision is not published or a file is unavailable, report that fact and stop;
do not silently substitute another revision. Runner source is owned by the
public examples and by each project; Skills do not provide a runtime runner
tree.

## Agent-owned workflow

Use the five lifecycle Skills for their distinct responsibilities:

| Skill | Agent responsibility |
|---|---|
| `datacoolie-discover` | verified source facts and bounded evidence |
| `datacoolie-design` | architecture, data contracts and material decisions |
| `datacoolie-build` | source edits, CLI validation/build, runners and local checks |
| `datacoolie-provision` | required target resources and readiness evidence |
| `datacoolie-release` | local validation and upload of one exact environment artifact |

Do not route ordinary project authoring through a different Skill merely to
repeat a contract. Discovery facts feed design; material design approval feeds
Build; missing target resources feed Provision; an exact locally validated
artifact feeds Release. Release is upload-only and does not activate, install,
execute, monitor or recover a workload.

## Project and runtime boundary

Read and change project files through the public project/CLI documentation.
The CLI prepares and validates a project; it does not run a Driver or user
code. Runners remain project-owned scripts or notebooks and choose the engine,
metadata provider, table registration and explicit component paths.

At runtime, preserve these ownership boundaries:

- metadata providers own metadata access and provider-specific paths;
- framework preparation resolves inline SQL and `.sql`/`artifact:/...` references;
- `DataCoolieRunConfig.run_attributes` carries external scheduler/job context;
- Driver owns session lifecycle and execution;
- logging owns `log_base_path`, snapshot/batch persistence and schema-v3 records;
- the watermark manager reads/saves values through its selected provider boundary.

Callers own explicit `artifact_base_path`, `metadata_base_path`,
`sql_base_path`, `state_base_path`, `watermark_base_path` and `log_base_path`
choices. Do not add `base_log_path`; it is retired. Shared storage may use
`.runtime/<environment>/logs/` and `.runtime/<environment>/watermarks/`, while
an already isolated single-environment root may use `.runtime/logs/` and
`.runtime/watermarks/`. The framework does not infer an environment from a
folder name.

External scheduler context is one strict JSON object passed as
`run_attributes`; do not invent a second session identity or a parallel
workflow-state envelope.

## Safety, evidence and handoff

- Keep credentials and discovery evidence out of metadata, SQL, runners,
  manifests and logs.
- Preserve custom runner bytes; validation/build may copy them but never run or
  rewrite them.
- Use `--format json` for automation and check both process exit code and the
  response `ok` field.
- Report exact input/output paths or IDs, checks performed and skipped,
  blockers/next owner and unresolved questions.
- Reuse CLI reports, manifests and existing approval/provision/upload receipts;
  do not create a second handoff envelope or workflow state store.
- The lifecycle covered by these Skills ends at local validation and upload.
  Monitoring, activation, workload execution, replay, recovery and other
  post-deploy operations remain outside their scope.

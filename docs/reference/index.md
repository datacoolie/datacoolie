---
title: Reference — DataCoolie Contracts & API
description: Precise field-level contracts for DataCoolie metadata, runtime configuration, plugins, CLI and Python APIs.
---

# Reference

Precise, mechanical contracts. Use this section when you need exact field names,
types, defaults, or Python signatures. Metadata and session runtime contracts
are intentionally separate. Prose explanations live under
[Concepts](concepts/index.md); task recipes under the [User guide](../guide/index.md).

This is not the best place to start if you are still learning the framework.
Use the [User guide](../guide/index.md) for runnable examples and guided tasks.

Some reference pages are generated only at docs-build time, so you will not see
their source `.md` files checked into `docs/reference/`.

## Ownership and version scopes

Use the following authority map when two summaries appear to disagree. The
owner listed in the **full definition** column wins; examples and Skills may
adapt that definition, but they must not maintain a competing copy.

| Concern | Full definition | Use the other source for |
|---|---|---|
| Runtime behavior and Python API | `src/datacoolie/` and the generated API pages | Runnable examples and agent routing |
| Authored metadata structure | `src/datacoolie/project/schemas/` and its published [`schema/index.json`](../schema/index.json) | Human authoring guidance and `dc validate` results |
| Project and CLI contract | [`datacoolie.yml` workflow](../guide/cli/project.md), [`CLI reference`](../guide/cli/commands.md), and the CLI implementation | Skill-specific command sequencing |
| Runtime paths and session configuration | [`Runtime configuration`](runtime-configuration.md) and its generated companion | Runner examples and platform-specific setup |
| Example source and inventory | [`Examples`](../examples/index.md) and `docs/examples/files/` | Public source, raw bytes, archives, and Skill retrieval |
| Agent procedure and approvals | `ai/AGENTS.md` and the five `ai/skills/*/SKILL.md` files | Shared framework behavior and field definitions |
| Internal rationale | The linked workspace wiki records | Public usage instructions |

The schema service is bundled with the framework and the CLI invokes it;
runtime metadata Providers hydrate `core.models` directly and do not fetch a
JSON Schema at execution time. The public schema tree is generated from the
bundled resources, so edit `src/datacoolie/project/schemas/`, not `docs/schema/`.

Schema selection is offline and deterministic: choose the greatest bundled
metadata schema version less than or equal to the installed framework version.
Stable releases ignore prerelease schemas; a prerelease may select a matching
prerelease contract but never a future final release. If authored metadata has
`$schema`, it may use the public `latest` alias for non-pinned authoring or the
exact selected public URL for reproducible artifacts. The CLI never fetches or
falls forward to that mutable alias: it resolves the installed framework's
bundled schema offline and reports the exact version used. No compatible schema,
an unknown marker, a checksum mismatch, or an invalid schema is an error.
`dc validate` applies the schema first, then constructs runtime models and checks
applicable resources; a schema failure does not silently continue to later
stages.

Version numbers are scoped contracts, not one shared release counter:

| Version | Owner and meaning |
|---|---|
| Framework/package | `pyproject.toml`; the installed runtime and CLI release |
| Metadata schema | Each versioned JSON Schema under `project/schemas/` |
| Schema index | `index_version` in the bundled/published `schema/index.json` |
| Project config | `schema_version` in `datacoolie.yml` |
| Manifest | `schema_version` in build manifests |
| CLI response | `schema_version` in the machine-readable response envelope |
| Log records | `log_schema_version` in system, job, and dataflow records |

Equal numbers across these rows are coincidental. A schema change follows the
metadata-schema release policy; a CLI, manifest, or log shape change follows
its own owner and compatibility checks.

CLI JSON responses, build manifests, and persisted log records report the
producer package as `datacoolie_version`, obtained from `datacoolie.__version__`.
This is the same installed version shown by `dc --version`. CLI and log
consumers ignore unknown fields; compatible optional additions preserve their
schema version. Removing or renaming fields, changing their types, or changing
their meaning incompatibly requires a new schema version. Historical CLI/log
outputs may omit `datacoolie_version`.

Generated reference pages are available from the published site even when
their source Markdown is not committed. AI callers should use the stable page
URLs in this reference section (for example
`https://datacoolie.github.io/datacoolie/reference/runtime-configuration/`)
or the checked-in source files for pages that do have one. Do not invent a raw
GitHub URL for a generated page.

## Start with the contract you need

- Full authored metadata field definitions: [Metadata reference](metadata-schema.md#metadata-document)
- Driver session, replay and logging fields: [Runtime configuration](runtime-configuration.md)
- Plugin registration names and entry-point groups: [Plugin entry points](plugin-entry-points.md)
- Runtime configuration knobs: [Environment variables](environment-variables.md)
- Programmatic interfaces: the API pages listed below

## Configuration contracts

- [Metadata reference](metadata-schema.md#metadata-document) — the generated authored-field
  reference from the project-owned, versioned JSON Schema. Browse the published
  schema index at [`/schema/index.json`](../schema/index.json), or the current
  [`latest` schema](../schema/latest/metadata.schema.json) for authoring, and use
  `dc validate` for authoritative structural validation. The page explains
  authored paths and reviewed runtime behavior for `Connection`, `DataFlow`,
  `Transform`, schema hints, load strategies, watermark config, and partition
  config; it does not own `DataCoolieRunConfig` or `ReplayConfig`.
- [Runtime configuration](runtime-configuration.md) — **generated** from the
  Driver session models and `LogConfig`; covers `run_attributes`, sharding,
  retries, replay ranges, persistence and console settings.
- [Plugin entry points](plugin-entry-points.md) — **generated** from
  `pyproject.toml`. Lists packaged entry-point declarations; in-process-only
  built-ins are called out separately.
- [Environment variables](environment-variables.md) — runtime overrides that
  DataCoolie reads from the process environment.
- [CLI](../guide/cli/index.md) — portable project preparation, validation, inspection, and builds.
- [Project configuration](../guide/cli/project.md) — `datacoolie.yml`, project workflow, and artifact layout.

## Python API reference

Selected Python contracts are rendered from source via `mkdocstrings`.
Core model fields include runtime-only values; use the metadata reference for
authored JSON. The API pages also expose the protected hooks required by the
extension guides. An exported helper or documented hook does not imply a new
compatibility guarantee; check the installed framework when upgrading plugins.

- [Core](api/core.md) — hydrated metadata models, constants, six registry factories, registry, secrets and shared exceptions. Runtime models are on [Runtime configuration](runtime-configuration.md).
- [Engines](api/engines.md) — `BaseEngine[DF]`, `PolarsEngine`, `SparkEngine`.
- [Platforms](api/platforms.md) — `BasePlatform`, `LocalPlatform`, `AWSPlatform`, `FabricPlatform`, and `DatabricksPlatform`.
- [Sources](api/sources.md) — `BaseSourceReader`, `FileReader`, `APIReader`.
- [Destinations](api/destinations.md) — `BaseDestinationWriter`, `FileWriter`.
- [Transformers](api/transformers.md) — built-in transformers (`ColumnValueTransformer`, `SchemaConverter`, `HashColumnAdder`, `Deduplicator`, `ColumnAdder`, `RowFilter`, `SCD2ColumnAdder`, `SystemColumnAdder`, `PartitionHandler`, `DataMasker`, `ColumnProjector`, `ColumnNameSanitizer`) and `TransformerPipeline`.
- [Orchestration](api/orchestration.md) — `DataCoolieDriver`, `JobDistributor`, `ParallelExecutor`.
- [Metadata](api/metadata.md) — provider classes and `BaseMetadataProvider`.
- [Watermark](api/watermark.md) — `WatermarkManager` and the raw-JSON contract.
- [Logging](api/logging.md) — `ExecutionLogger`, `SystemLogger`, persistence modes, and factories.

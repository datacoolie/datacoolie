---
title: Metadata Guide for New Users — DataCoolie User Guide
description: Metadata authoring guide for new DataCoolie users — connections, sources, destinations, transforms, providers, load types, and validation.
---

# Metadata guide for new users

If you are a new Data Engineer or Data Analyst who just installed DataCoolie
and is not sure how to configure your first pipeline, start here.

DataCoolie is **driven entirely by metadata** — a JSON (or YAML, or Excel)
document that tells the framework what to read, how to transform it, where to
write it, and when to re-run incrementally. You do **not** write Python for
each pipeline; you fill in a structured document.

This guide walks through that document from zero, but it also covers the cases
that usually appear right after the first successful run: incremental loads,
query-based sources, API pagination, function sources, partitioning, merge
strategies, secrets, and metadata-provider differences.

Use the [Metadata reference](../../reference/metadata-schema.md#metadata-document) when you need
the complete authored field contract. The topic pages below explain how fields
work together; each case keeps its JSON configuration beside the explanation.
The runnable projects under [Examples](../../examples/index.md) are separate
projects with runners, SQL and fixtures for cases that need an executable setup.

## Choose your path

!!! tip "New to DataCoolie?"
    Start with [Build your first metadata file](first-metadata-file.md). It
    creates a small JSON document, validates it, and runs the first dataflow.

!!! info "Authoring or reviewing production metadata?"
    Follow the [authoring workflow](#authoring-workflow): begin with reusable
    [connections](connections.md), compose [dataflows](dataflows.md), then
    configure the source, transform, and destination phases.

The first path is optimized for a quick successful run. The second is the
conceptual route for understanding how a larger metadata document is organized.

## Configure features and solve combined cases

**Configure metadata** teaches the supported fields and conditions of each
component, including OAuth authentication, pagination, look-back, partitioning
and SCD2. Start with the page that owns the component. **Advanced** contains
worked situations that combine those already explained components: complete
window replacement, incremental paginated APIs, late files, protected keyed
outputs, and incremental SCD2. It is a task route, not a mandatory step.

The generated [Metadata reference](../../reference/metadata-schema.md#metadata-document)
provides exact field types, values and defaults when a guide case leaves you
needing the precise authored shape.

## Metadata document

Start with one authored document containing `connections` and `dataflows`.
The optional root `schema_hints` groups reusable column types across matching
sources. The [Metadata document](../../reference/metadata-schema.md#metadata-document)
reference lists the exact root fields; the pages for [Connections](connections.md),
[Dataflows](dataflows.md), and [Datatypes and schema hints](data-types.md)
explain how each section is configured.

This is a root-level fragment with no configured flows yet:

```json
{
  "$schema": "https://datacoolie.github.io/datacoolie/schema/latest/metadata.schema.json",
  "connections": [],
  "dataflows": [],
  "schema_hints": [],
  "extensions": {"owner": "analytics"}
}
```

### Choose a schema marker

The optional `$schema` helps editors and validators find the authored
contract. Use the published `latest` alias for current authoring, or pin a
compatible versioned URL when an artifact must remain reproducible. The
installed CLI validates with its bundled compatible schema; it does not fetch
the URL. The Driver does not download it at runtime.

### Keep names readable

Connections always need a nonblank `name`. Keep that name unique in the active
name-lookup scope; distinct explicit connection IDs may share a display name
when every operation uses the ID. Dataflows normally use a unique name, but an
explicit `dataflow_id` can be used without a name for ID-based integrations.
For ordinary authoring, refer to a
connection using `source.connection_name`,
`destination.connection_name`, and root `schema_hints[].connection_name`.
DataCoolie derives connection and dataflow IDs when they are absent. Use an
explicit stable ID only when another system owns that identity; when a
`workspace_id` is present, name scope is that workspace. See
[Connection identity](connections.md#connection-identity) and
[Dataflow identity](dataflows.md#dataflow-identity).

### Add project-owned extensions

The root `extensions` object can hold project annotations, for example owner,
data product, and ticket identifiers:

```json
{
  "extensions": {
    "owner": "analytics",
    "data_product": "orders",
    "change_ticket": "DATA-1234"
  }
}
```

DataCoolie core does not interpret those keys. An external runner or project
extension may read them; they do not configure connections, change loads, or
bypass validation. Put operational settings in their documented fields.

### Prepare an environment variant

An environment overlay is applied to a common metadata snapshot during
project preparation, before validation. It is not a connection or dataflow
field and the Driver does not read overlay files. Follow the
[Environment overlays](../cli/project.md#environment-overlays) procedure,
then validate the effective document. The overlay cannot change identity
fields or delete existing definitions.

## Authoring workflow

!!! tip "Recommended workflow"
    Start with the reusable connections, compose a dataflow, then follow its
    runtime phases from source through transform to destination. This is a
    navigation path, not a requirement to read every advanced page before your
    first run. Validate before executing and repeat the check after changes.

| Order | Concern | What you decide |
|-------|---------|-----------------|
| 1 | [Metadata document](#metadata-document) | Root structure, schema marker and extensions |
| 2 | [Connections](connections.md) | Reusable endpoints, formats, defaults, workspace scope, secrets and authentication |
| 3 | [Dataflows](dataflows.md) | Pipeline identity, scheduling envelope, and phase boundaries |
| 4 | [Source](source-patterns.md) | Table, SQL/query, file, API, function, filter, pagination, watermark and look-back behavior |
| 5 | [Transform](transform-patterns.md) | Cast, normalize, deduplicate, hash, mask, project, and compute columns |
| 6 | [Destination](destination-and-load-patterns.md) | Target, `load_type`, keys, partitions, and write behavior |
| 7 | [Datatypes and schema hints](data-types.md) | Inline or shared type hints, source dialects, decimals, and timestamps |
| 8 | [Validation checklist](validation-checklist.md) | Provider, path, secret, cross-field, and pre-run checks |

The order follows the authored document and then the dataflow phases. Destination
strategy can constrain transform choices, so revisit it before finalizing a
complex transform. For combinations, choose
[window replacement](watermark-window-replacement.md),
[paginated incremental API](api-advanced.md),
[late files](late-arriving-files.md),
[stable protected keys](stable-keys-and-protected-output.md), or
[incremental SCD2](merge-and-scd2.md).

## Coverage map

| Area | Configure it here | Built-in cases |
|------|-------------------|----------------|
| Metadata shape | [Metadata document](#metadata-document), [dataflows](dataflows.md) | `connections[]`, `dataflows[]`, shared `schema_hints[]`, orchestration fields |
| Metadata backends | [Provider chooser](../providers/index.md) | JSON, YAML, Excel, database provider, API provider |
| Connections and secrets | [Connections](connections.md) | File, lakehouse, database, API, function; `configure`, `secrets_ref` |
| Source types | [Source](source-patterns.md) | File, Delta, Iceberg, database table/query/SQL file, REST API, Python function |
| Destination and load | [Destination patterns](destination-and-load-patterns.md) | File outputs, Delta, Iceberg; `append`, `overwrite`, `full_load`, merge, SCD2, window replacement |
| Transform and partition | [Transform patterns](transform-patterns.md), [destination partitioning](destination-and-load-patterns.md#partition_columns-partition-the-output-table) | Normalize, hash, deduplicate, compute, mask, project, partition, system columns |
| Datatypes and hints | [Datatypes and schema hints](data-types.md) | Inline and shared hints, source types, timestamp and decimal semantics |
| Incremental behavior | [Source](source-patterns.md#incremental-windows-and-look-back) | Watermarks, look-back, closing day, API ranges, replay boundaries |
| Identity and preparation | [Metadata document](#metadata-document), [Connections](connections.md#connection-identity), [Dataflows](dataflows.md#dataflow-identity), [Project overlays](../cli/project.md#environment-overlays) | `$schema`, derived/explicit IDs, project-owned `extensions`, environment overlays |
| Validation and safety | [Validation checklist](validation-checklist.md) | Secrets, cross-field checks, smoke tests, common errors |

For every area, the [Metadata reference](../../reference/metadata-schema.md#metadata-document)
is the field lookup: type, schema enum, default, and authored structure. The
linked guide page explains which fields must be combined for a working case.

!!! info "Important edge cases"
  This guide covers the real behavior of the current framework, including:

  - `connection_type` can be derived automatically from `format`
  - Excel is a supported **source** format, not a writable destination
  - flat-file destinations support `append`, `overwrite`, and `full_load`, but not merge or SCD2
  - `connection_type: "streaming"` exists in the model but has no supported formats yet

## Where metadata lives

DataCoolie supports three metadata backends. Choose one:

| Backend | Good for | Operational note | Guide |
|---------|----------|------------------|--------|
| **JSON / YAML / Excel file** | Local dev, small teams, single-machine runs | JSON should stay canonical; YAML/Excel are alternative views or generated siblings | [Configure file metadata](../providers/file.md) |
| **Relational database** | Shared team configuration, multi-workspace governance | Rows are workspace-scoped via `workspace_id` | [Configure database metadata](../providers/database.md) |
| **REST API** | Enterprise ops, Git-backed or approval-gated config | Endpoints are workspace-scoped under `/workspaces/{workspace_id}/...` | [Configure API metadata](../providers/api.md) |

!!! note "Recommendation for beginners"
    Start with a **JSON file**. The file backend requires no database and no
    service — just create a `.json` file and point `FileProvider` at it.
    You can migrate to the database or API backend later while keeping the
    same logical metadata model. Verify provider round-trips before rollout:
    database/API storage has its own schema and payload contract, and API
    `source.filter_expression` is still evaluated locally after the DataFrame
    is created.

## What metadata tells the framework

```
metadata.json
├── connections[]            ← WHERE to read from and write to
│   ├── name / format / configure
│   ├── catalog / database / base_path
│   └── secrets_ref / is_active / workspace_id
└── dataflows[]              ← HOW to move data
  ├── name / stage / description
  ├── group_number / execution_order / processing_mode / is_active
  ├── source               ← which connection + table/query/function to read
  ├── destination          ← which connection + table + load_type to write
  └── transform            ← normalization, schema, hashing, masking, projection (optional)
```

Start with `connections` and `dataflows`. The rest is optional and you can
add it incrementally.

If you are unsure where a field belongs, use this rule:

- `Connection.configure` = reusable endpoint defaults
- `source.configure` / `destination.configure` = per-dataflow overrides
- `transform.configure` = transformer behavior flags

## Quick example (30 seconds)

```json
{
  "connections": [
    {
      "name": "csv_input",
      "connection_type": "file",
      "format": "csv",
      "configure": { "base_path": "data/input" }
    },
    {
      "name": "bronze",
      "format": "delta",
      "configure": { "base_path": "data/output/bronze" }
    }
  ],
  "dataflows": [
    {
      "name": "orders_to_bronze",
      "stage": "ingest",
      "source":      { "connection_name": "csv_input", "table": "orders" },
      "destination": { "connection_name": "bronze", "schema_name": "sales", "table": "orders", "load_type": "append" }
    }
  ]
}
```

This reads `data/input/orders` (a folder of CSV files) and appends to a Delta
table at `data/output/bronze/sales/orders`. In this example DataCoolie derives
`connection_type: "lakehouse"` from `format: "delta"`.

→ **Choose a path**: [Build your first metadata file](first-metadata-file.md) ·
[Connections](connections.md) · [Dataflows](dataflows.md)

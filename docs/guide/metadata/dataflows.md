---
title: Dataflows — DataCoolie User Guide
description: Compose DataCoolie source, transform, and destination blocks into a dataflow with clear execution and scheduling controls.
---

# Dataflows

**Prerequisites** · Reusable [connections](connections.md) are available, or
you are intentionally using an inline connection.  
**End state** · A dataflow that describes one source-to-destination unit and
can be validated before execution.

A dataflow is the unit DataCoolie reads, transforms, and writes. Its three
runtime phases are siblings in the metadata model:

```text
dataflows[]
├── source       read and filter input
├── transform    optionally shape the rows
└── destination  write rows with a load strategy
```

At runtime the phases run as `Source -> Transform -> Destination`. In the
authoring process, choose the destination contract early enough to know whether
the transform needs merge keys, SCD2 columns, partition expressions, or
watermark columns. JSON property order does not change execution order.

## Dataflow at a glance

```json
{
  "dataflows": [
    {
      "name": "orders_to_bronze",
      "stage": "ingest",
      "source": {
        "connection_name": "orders_input",
        "table": "orders"
      },
      "transform": {
        "schema_hints": [
          { "column_name": "order_id", "data_type": "long" }
        ]
      },
      "destination": {
        "connection_name": "bronze",
        "table": "orders",
        "load_type": "append"
      }
    }
  ]
}
```

The [Dataflow](../../reference/metadata-schema.md#dataflow) contract requires
`source` and `destination`. `transform` is optional; when it is
omitted, DataCoolie still applies the driver-managed pipeline behavior that
belongs to every run.

## Define the dataflow envelope

Give every dataflow a stable name or an explicit `dataflow_id`, plus a stage.
Names are the normal authoring identity; add scheduling fields only when the
run needs them.

| Field | Use it for |
|---|---|
| `name` | Recommended human-readable identity for name-based authoring |
| `dataflow_id` | Explicit stable identity for ID-based integrations; it can replace `name` |
| `stage` | Select related dataflows with `driver.run(stage=...)` |
| `description` | Business purpose or operational context |
| `group_number` | Put dependent or related flows in the same scheduling group |
| `execution_order` | Order buckets within a non-null group |
| `processing_mode` | Select the configured processing mode; the normal built-in ETL path is batch-oriented |
| `is_active` | Keep a definition in metadata while preventing its execution |
| `configure` | Pass dataflow-level settings for supported extensions |

```json
{
  "name": "orders_to_silver",
  "description": "Load the current order state",
  "stage": "transform",
  "group_number": 1,
  "execution_order": 20,
  "processing_mode": "batch",
  "is_active": true,
  "source": { "connection_name": "bronze", "table": "orders" },
  "destination": {
    "connection_name": "silver",
    "table": "orders",
    "load_type": "merge_upsert",
    "merge_keys": ["order_id"]
  }
}
```

Use the same non-null `group_number` and increasing `execution_order` when a
combined stage selection has an explicit dependency. Independent dataflows can
omit both fields. See the run and orchestration guides for execution selection
and failure behavior.

Keep `name` as the normal dataflow identity. See
[Dataflow identity](#dataflow-identity) when another system requires an
explicit stable ID.

### Dataflow identity

Use a unique name when the dataflow is authored or selected by name. DataCoolie
derives `dataflow_id` from the name when an explicit ID is absent. An explicit
ID may be used without a name for ID-based integrations; keep that ID unique
and stable across deployments. The document mapper enforces dataflow ID
uniqueness and permits distinct explicit IDs with the same display name, but
any name-based consumer must still have one unambiguous match. When
`workspace_id` is supplied, name scope is that workspace. Watermark and
provider records use dataflow identity, so changing an explicit ID can select a
different state record. For most documents, omit it and use the name. See the
[Dataflow](../../reference/metadata-schema.md#dataflow) contract.

For an externally managed state identity, add the ID to the named dataflow
(dataflow fragment):

```json
{
  "name": "orders_to_bronze",
  "dataflow_id": "flow-orders-bronze-v1",
  "source": {"connection_name": "orders_source", "table": "orders"},
  "destination": {
    "connection_name": "bronze", "table": "orders", "load_type": "append"
  }
}
```

### Activation and selection

A dataflow can run only when its own `is_active`, its source connection's
`is_active`, and its destination connection's `is_active` are all `true`.
All three flags default to `true`. Normal metadata selection omits inactive
dataflows; `get_dataflows(active_only=False)` returns them for inspection.
Connections remain resolvable in the metadata provider even when inactive.

If a selected dataflow references an inactive connection, it finishes as
`SKIPPED` with an activation reason. The same execution check applies when
passing dataflows directly to `driver.run()`, replay or maintenance. A dataflow
filtered out before execution has no run record. Disabling an upstream
dataflow does not automatically disable downstream dataflows; check stage
dependencies yourself. See [Connection activation](connections.md#disable-a-connection-without-removing-it)
and the dataflow is_active field.

## Configure the three phases

### Source

The [Source](../../reference/metadata-schema.md#source) block chooses the read mode:
`table`, `query`, or `python_function`. A query source may
also set `table` as a human-readable logical label. A function source may use
`table` and other Source fields as inputs to the custom function. Add watermarks
and source filters when the source is incremental or needs a bounded read. A
relative or `artifact:/` SQL path still belongs in `source.query`.

Continue with [Source](source-patterns.md) for file, database, SQL
file, API, function, and watermark cases.

### Transform

Use [Transform](../../reference/metadata-schema.md#transform) for casts, value rules, computed columns, deduplication,
hashing, masking, projection, renaming, and row filters. Transform behavior
may depend on the destination strategy: merge and SCD2 need key or audit
columns, and partition expressions must survive the transform pipeline.

See [Transform patterns](transform-patterns.md) for the execution order and
feature-specific examples. Use [Datatypes and schema hints](data-types.md)
when the source datatype or reusable schema metadata needs explicit control.

### Destination

The [Destination](../../reference/metadata-schema.md#destination) block chooses
the target connection, table, `load_type`, and any write modifiers.
`merge_keys`, `partition_columns`, SCD2 settings, and
`replace_by_watermark` are conditional on the load strategy and source
coverage.

See [Destination & load patterns](destination-and-load-patterns.md) for
append, overwrite, merge, SCD2, and window-replacement cases.

## Add shared schema hints when a table is reused

Top-level `schema_hints[]` is an optional reusable layer defined by
[Shared schema hint](../../reference/metadata-schema.md#shared-schema-hint). Match it by source
`connection_name`,
optional `schema_name`,
and `table_name`;
the provider can attach the matching `hints[]` items, shaped as
[Schema hint](../../reference/metadata-schema.md#schema-hint),
to `transform.schema_hints` for a table source.

```json
{
  "schema_hints": [
    {
      "connection_name": "warehouse",
      "schema_name": "sales",
      "table_name": "orders",
      "hints": [
        { "column_name": "order_id", "data_type": "long" },
        { "column_name": "created_at", "data_type": "timestamp" }
      ]
    }
  ]
}
```

This layer is shared by matching table sources; it is not an all-database
datatype override. The source connection must allow schema hints, and inline
`dataflows[].transform.schema_hints` takes precedence. Query and function
sources can also match shared hints when `source.table` identifies their
output. Without it, put their hints inline. See
[Datatypes and schema hints](data-types.md) for matching and precedence.

If the source is incremental, choose the watermark and look-back together;
see [Incremental windows and look-back](source-patterns.md#incremental-windows-and-look-back). If the
destination replaces a watermark window, the source must cover the complete
delete scope and the destination must use the matching load strategy.

## Validate and run

Validate the complete metadata document after composing its connections and
dataflows, and repeat the check after changing a source, transform, or load
strategy. Use the [Validation checklist](validation-checklist.md) before the
first run. Keep SQL-file roots, secrets, merge keys, source coverage, and
destination support in the same review.

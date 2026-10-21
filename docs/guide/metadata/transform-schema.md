---
title: Schema conversion | DataCoolie
description: Configure schema conversion with metadata, examples and backend-specific guidance.
---

# Schema conversion

[All transform recipes](transform-patterns.md).

## SchemaConverter

**When to use:** your source data has weak types (CSV strings, JSON mixed types)
and you need specific types in the destination.

### Cast selected columns in one dataflow

Add `schema_hints` inside `transform`. The [Schema hint](../../reference/metadata-schema.md#schema-hint)
entry defines each item:

```json
"transform": {
  "schema_hints": [
    { "column_name": "order_id",    "data_type": "int" },
    { "column_name": "customer_id", "data_type": "int" },
    { "column_name": "amount",      "data_type": "decimal(18,2)" },
    { "column_name": "order_date",  "data_type": "date" },
    { "column_name": "created_at",  "data_type": "timestamp" },
    { "column_name": "is_active",   "data_type": "boolean" }
  ]
}
```

For source-specific type names, decimals, timezone conversion, and the choice
between global and dataflow hints, use [Datatypes and schema hints](data-types.md).
Reusable root-level `schema_hints` groups follow the
[Shared schema hint](../../reference/metadata-schema.md#shared-schema-hint)
contract and are attached to matching dataflows by the metadata provider.
Less-common hint fields such as `format`, `default_value`, `ordinal_position`,
and `is_active` are listed in the [Schema hint reference](../../reference/metadata-schema.md#schema-hint).

!!! note "How schema hints are applied"
  - Matching is case-insensitive
  - Missing columns are skipped, not fatal
  - `use_schema_hint` in the source [connection configure](../../reference/metadata-schema.md#connectionsconfigure) must be enabled for hint-based casts
  - `timestamp_ntz` conversion happens after hint-based casting when enabled with `timestamp_timezone`; see [Timestamp semantics](data-types.md#timestamp-semantics)

!!! tip "Only list the columns you want to cast"
    You do not need a hint for every column. Columns not listed keep their
    inferred type from the source reader.

### Reuse global hints or override them locally

At the metadata document root, a shared group can describe the source table:

```json
"schema_hints": [
  {
    "connection_name": "erp_postgres",
    "schema_name": "sales",
    "table_name": "orders",
    "hints": [
      {"column_name": "order_id", "data_type": "bigint"},
      {"column_name": "amount", "data_type": "numeric(18,2)"}
    ]
  }
]
```

The provider attaches this group to a source using that connection, schema,
and table. A non-empty `transform.schema_hints` list replaces the whole shared
group for that dataflow. To keep some shared casts, repeat them in the local
list. See [Global hints for a source table](data-types.md#global-hints-for-a-source-table)
for matching rules, including query/function sources.

### Decimal precision and formatted dates

```json
"transform": {
  "schema_hints": [
    {"column_name": "amount", "data_type": "decimal", "precision": 18, "scale": 2},
    {"column_name": "order_date", "data_type": "date", "format": "dd/MM/yyyy"},
    {"column_name": "created_at", "data_type": "timestamp_ntz", "format": "dd/MM/yyyy HH:mm:ss"}
  ]
}
```

`decimal` with `precision: 18, scale: 2` is equivalent to `decimal(18,2)`.
If both forms supply parameters, they must agree. The format examples parse
`"25/09/2026"` and `"25/09/2026 11:10:00"`. Use formats supported by the
selected engine; these common Spark-style patterns are translated by Polars.
Arbitrary vendor format strings are not a portable contract.

### Interpret hints from a source datatype system

When a weak source reuses database type declarations, set the source
[connection configure](../../reference/metadata-schema.md#connectionsconfigure):

```json
"configure": {"schema_hint_type_system": "postgresql"}
```

For example, a PostgreSQL `int8` hint then means a signed 64-bit integer.
Database connections normally supply their dialect through `database_type`.
See [Which type system is read?](data-types.md#which-type-system-is-read)
for all supported systems and precedence.

### Disable one hint or all hint-based casts

Keep a hint in metadata while temporarily leaving its column unchanged:

```json
"transform": {
  "schema_hints": [
    {"column_name": "amount", "data_type": "decimal(18,2)", "is_active": false}
  ]
}
```

To disable both global and inline casts for a source connection, set its
`configure` as follows:

```json
"configure": {"use_schema_hint": false}
```

Missing hinted columns are warned about and skipped; duplicate hint names are
rejected case-insensitively. `default_value` does not fill missing/null data in
the built-in converter; use `ColumnValueTransformer.fill_null` for that.
`ordinal_position` orders shared hint records during provider resolution; it
does not reorder output columns. Use `ColumnProjector.select_columns` to do so.

### Convert timezone-free timestamps

To interpret NTZ values as instants, set `convert_timestamp_ntz` and
`timestamp_timezone` in [`transform.configure`](../../reference/metadata-schema.md#dataflowstransformconfigure):

```json
"transform": {
  "configure": {
    "convert_timestamp_ntz": true,
    "timestamp_timezone": "Asia/Ho_Chi_Minh"
  }
}
```

This conversion runs after hint-based casts and can also apply to NTZ columns
already present in the source schema. The timezone identifies the source
wall-clock time; see [Timestamp semantics](data-types.md#timestamp-semantics)
for supported timezone forms and a conversion example.

To retain timezone-free values, leave the default or set:

```json
"transform": {"configure": {"convert_timestamp_ntz": false}}
```

Hint-based casting and NTZ conversion are separate switches: disabling
`use_schema_hint` does not disable an explicitly enabled NTZ conversion.
Columns already carrying timezone-aware timestamps retain their instant.

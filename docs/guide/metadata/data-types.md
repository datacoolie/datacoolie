---
title: Datatypes and schema hints — DataCoolie User Guide
description: Declare source-aware schema hints and portable output datatypes for Spark and Polars.
---

# Datatypes and schema hints

`transform.schema_hints`
declares the logical type that a column must have
during DataCoolie's transform phase. The authored source datatype remains raw;
the selected engine interprets it with the configured source dialect, so a
PostgreSQL `int8` and a Spark SQL `bigint` can describe the same 64-bit signed
value without making the transformer guess which dialect was intended.

The same metadata is used by Spark and Polars. A dependency-free shared
resolver supplies logical meaning, and each engine constructs its native
datatype. Native datatype objects may differ; supported ranges, values,
null/error behavior, and temporal semantics must agree.

## Choose where to configure hints

| Scope | Where it goes | When to use it |
|---|---|---|
| Global, shared by a source table | Top-level `schema_hints[]`, alongside `connections` and `dataflows`. Each group identifies a source by `connection_name`, `table_name`, and optional `schema_name`, then lists column hints in `hints[]`. | Reuse one source-table type contract across dataflows. This is convenient when you can export many column types from a database catalog such as `information_schema` into metadata. |
| One dataflow | `dataflows[].transform.schema_hints[]`. Each item names a `column_name` and `data_type`; it does not repeat the connection, schema, or table. | Cast a few columns for one source/dataflow, or handle a query/function output without a shared type contract. |

Both forms use the same column-level hint fields. `column_name` and `data_type`
are required; `format`, `precision`, and `scale` are optional.

### Global hints for a source table

Column types collected from a PostgreSQL source catalog can be authored once
at the document root as [Shared schema hint](../../reference/metadata-schema.md#shared-schema-hint)
groups. The example below is a metadata fragment; the complete
document also needs `connections` and `dataflows`. DataCoolie matches the
authored hints but does not query `information_schema` to create them for you.
Typed database readers may already return the desired types; use hints when
normalization or an explicit reusable contract is needed.

```json
{
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
}
```

The provider matches a group by source connection and table, and by
`schema_name`
when supplied. Schema and table matching is case-insensitive;
multiple schema groups for an unqualified source are ambiguous and fail.
Prefer `connection_name` in authored metadata. For provider-managed
`connection_id`, keep the value stable and consistent with the connection's
identity. Use it for provider-managed integration when a name is unsuitable;
see [Connection identity](connections.md#connection-identity).

When a provider supplies `connection_id` instead of a name, the root group
can use it as the matching key (root fragment):

```json
{
  "schema_hints": [
    {
      "connection_id": "conn-orders-prod-v1",
      "table_name": "orders",
      "hints": [{"column_name": "order_id", "data_type": "bigint"}]
    }
  ]
}
```

Keep it equal to the source connection's stable ID; omitting `schema_name`
works only when the table identity is unambiguous among shared groups.

Query and Python function sources usually handle types in SQL or custom code,
so shared hints are uncommon for them. They are supported when a reusable
output-type contract is useful: shared lookup requires `source.table` as a
matching key, with optional `source.schema_name`. For a query, `table` may be
a logical alias; for a function, it may also be an input the function actually
uses. In either case, the hints must describe the output columns, not just an
underlying table. Without `source.table`, or when only a few casts are needed
for one dataflow, use `transform.schema_hints` instead.

### Hints for one dataflow

Put the following [Transform](../../reference/metadata-schema.md#transform) fragment
inside one `dataflows[]` item to cast only its `amount` column. Each entry
follows the [Schema hint](../../reference/metadata-schema.md#schema-hint) shape.
The source identity is already on that dataflow's
`source` block, so it is not repeated in each hint:

```json
{
  "transform": {
    "schema_hints": [
      {"column_name": "amount", "data_type": "decimal(18,2)"}
    ]
  }
}
```

When a dataflow has a non-empty inline list, it replaces the entire matched
shared hint group; hints from the two locations are not merged column by
column. If other shared casts are still needed, include them in the inline
list too. Setting the source connection's
`configure.use_schema_hint`
to `false` disables casting from either location for that connection.

Portable `decimal` hints need precision and scale. When exporting a database
catalog, include those parameters: a bare `decimal`/`number` hint is rejected
rather than assigned a precision from a sample value. Vendor declarations may
define a scale default: `NUMBER(p)`/`NUMERIC(p)` means scale `0` for the
supported Oracle, PostgreSQL, MySQL, and SQL Server dialects. A declaration
without a usable precision (for example bare Oracle `NUMBER`) still fails
because the framework cannot promise an exact target.

### Once bronze is typed Parquet

Suppose the database-to-bronze dataflow uses schema hints to normalize
`orders.amount`, then writes bronze as Parquet. The Parquet file stores the
resulting typed schema. A bronze-to-silver or later dataflow reading that
Parquet normally uses those column types without copying the upstream hints.
Add a hint downstream only when that flow intentionally needs another cast or
its reader does not preserve the required type. Check the persisted schema and
values for the engine and connector in use, especially for decimal and
timestamp columns.

## Which type system is read?

The type-system override belongs to [`connections[].configure`](../../reference/metadata-schema.md#connectionsconfigure);
the connection families and their settings are listed under [Connection settings by endpoint type](../../reference/metadata-schema.md#connection-settings-by-endpoint-type).

Resolution follows one deterministic precedence order for the source
connection:

1. `source.connection.configure.schema_hint_type_system`, when explicitly set;
2. the known `connection.configure.database_type` for a database source;
3. Spark SQL conventions as the neutral fallback.

The execution engine is never used to infer the source dialect. Use the
override when a weakly typed source (for example CSV) carries hints copied
from another system:

```json
{
  "source": {"connection_name": "orders_csv", "table": "orders"},
  "transform": {
    "schema_hints": [
      {"column_name": "created_at", "data_type": "DATE"},
      {"column_name": "amount", "data_type": "NUMBER(18,2)"}
    ]
  }
}
```

Put the override on the source connection when it is needed:

```json
{
  "name": "orders_csv",
  "connection_type": "file",
  "format": "csv",
  "configure": {
    "base_path": "./data/orders",
    "schema_hint_type_system": "oracle"
  }
}
```

Supported names are `spark_sql`, `postgresql`, `mysql`, `mssql`, `oracle`,
and `sqlite`. Accepted spelling aliases include `spark sql` and `spark` for
`spark_sql`, and `sql server` and `sqlserver` for `mssql`.
An unknown type-system name or a datatype not supported by the selected
system is an error; it is not silently retried as another dialect.

## Source spelling to logical semantics

The table describes the shared logical meaning produced by the engine-owned
resolver. It is not a promise that Spark and Polars use identical native
objects, or that Parquet, Delta, and Iceberg use identical physical
encodings. Each format may add its own annotation or promotion rule while
retaining the value semantics.

| Source system | Authored hint | Logical meaning | Reason |
|---|---|---|---|
| PostgreSQL | `int8`, `bigint` | `bigint` | signed 64-bit integer |
| MySQL | `tinyint unsigned` | `smallint` | signed target must hold 0–255 |
| MySQL | `bigint unsigned` | `decimal(20,0)` | no signed 64-bit target holds the full range |
| SQL Server | `tinyint` | `smallint` | SQL Server `tinyint` is unsigned |
| SQL Server | `rowversion` / `timestamp` | `binary` | SQL Server `timestamp` is a binary version token, not a date |
| Oracle | `DATE` | `timestamp_ntz` | Oracle DATE includes time but has no offset |
| Oracle | `TIMESTAMP WITH TIME ZONE` | `timestamp` | value represents an instant |
| SQLite | `INTEGER` | `bigint` | SQLite integer affinity is signed 64-bit at the boundary |

Oracle and PostgreSQL negative decimal scales are normalized to an integral
decimal meaning: `NUMBER(18,-2)` becomes `decimal(20,0)`. The extra
precision retains the range after the source has rounded values to the left of
the decimal point. Negative scales from Spark SQL, MySQL, SQL Server, or
SQLite are rejected as unsupported rather than silently reinterpreted.

Spark SQL names such as `tinyint`, `smallint`, `int`, `bigint`, `float`,
`double`, `string`, `binary`, `date`, `timestamp`, and `timestamp_ntz` can be
used directly. Use `decimal(precision,scale)` for exact numeric values.

Parameterized approximate types keep their source precision semantics before
the engine chooses a native width: MySQL `FLOAT(p)` uses 32-bit semantics up
to the single-precision boundary and 64-bit semantics above it; SQL Server
`float(p)` follows the same 24/53-bit split; Oracle `FLOAT(p)` uses binary
precision and is represented as 32-bit or 64-bit when it crosses the native
adapter boundary. Unsupported precision ranges fail validation instead of
being silently ignored.

## Timestamp semantics

The conversion flags are grouped under [`dataflows[].transform.configure`](../../reference/metadata-schema.md#dataflowstransformconfigure).

`timestamp_ntz` is a wall-clock value. DataCoolie does not silently attach
UTC. The `convert_timestamp_ntz`
setting defaults to `false`. When conversion to an instant is intentional,
provide `timestamp_timezone`:

```json
{
  "transform": {
    "configure": {
      "convert_timestamp_ntz": true,
      "timestamp_timezone": "Asia/Ho_Chi_Minh"
    },
    "schema_hints": [
      {"column_name": "created_at", "data_type": "timestamp_ntz"}
    ]
  }
}
```

`timestamp_timezone` is the source timezone to assume for the timezone-free
wall-clock value. DataCoolie uses it to identify the instant, then normalizes
that instant to UTC; it is not a requested display timezone. For example,
`2026-09-23 11:10:00` with `Asia/Ho_Chi_Minh` (UTC+07:00) represents
`2026-09-23T04:10:00Z`. With `UTC`, the same wall-clock fields represent
`2026-09-23T11:10:00Z`.

Use `UTC` or an IANA region name in `Area/Location` form, such as
`Asia/Ho_Chi_Minh`, for consistent Spark and Polars behavior. Spark also
documents fixed offsets such as `+07:00`, but offset-string support can differ
between engines and versions. Avoid abbreviations or forms such as `GMT+7`;
they are ambiguous and are not the portable format. The metadata model accepts
a non-empty string and the selected engine validates the timezone when it
performs the conversion.

Polars represents the result with a UTC timezone. Spark's `timestamp` type
represents an instant but does not retain a timezone per value; Spark displays
it using the session timezone. Thus that same instant can display as
`04:10:00` in a UTC session or `11:10:00` in an `Asia/Ho_Chi_Minh` session.

Missing timezone information is an error when an NTZ column is actually
converted. Aware timestamps retain their instant; a timezone setting does not
rewrite them. Date and NTZ values do not need a timezone when they remain in
their original logical form.

## Values, overflow, and parameters

- `null` remains `null`.
- Invalid text and integer overflow fail during the cast; they are not turned
  into strings or silently clipped.
- Decimal hints must carry matching precision and scale. If the same values
  appear both in `data_type` and in `precision`/`scale`, conflicting values
  fail.
- A type hint applies only to its matching column. Inactive hints remain
  inactive, and a missing hinted column follows the existing warning/skip
  policy; an unsupported type itself is never skipped.

## No hint and format boundary

Typed database and lakehouse readers should preserve the source result schema.
Hints are the explicit normalization step for weak inputs such as CSV or
mixed JSON, and are applied only by `SchemaConverter` after the source read.
A custom SQL query or Python function must return values whose logical types
already satisfy the same contract; arbitrary custom code is not rewritten by
the framework. Watermark filtering and watermark calculation use the source
reader's own result types and configuration; transform hints never alter that
behavior.

Schema hints are cast by the selected engine during transformation. The final
native frame is then inspected at each write boundary, so the same format rule
also covers unhinted, renamed, projected, and derived columns. This keeps
destination-format policy out of `SchemaConverter` and prevents a format
promotion from hiding a source-range error: a value that cannot fit its
canonical hint fails before a safe output widening is applied. Parquet, Delta,
and Iceberg can represent the same logical value with different physical
annotations, field IDs, or small-integer promotion rules. Compare logical
schema and values when validating a pipeline; do not compare file bytes or
assume that a Parquet physical `INT64` alone identifies a decimal or timestamp
semantic.

No-hint parity is an evidence-backed compatibility expectation, not a promise
that every reader's inference algorithm is identical. Typed Parquet, CSV,
JSON, and JSONL cases are qualified independently for the supported Spark and
Polars versions. JSON and JSONL inference remains engine-specific for values
outside the qualified matrix; use an explicit schema hint or a deliberate
projection when a weak source must have a stable target contract. Reader
options are also engine-specific: Polars translates the supported JSON schema
and inference options, while Spark keeps its native JSON options. A successful
CLI metadata validation cannot prove connector inference or persisted
round-trip parity.

### Output-format mapping

The format adapter inspects the final native schema, then applies only the
format rule required by that format. This is why one metadata definition can
be written by Spark or Polars without making the engines own different
source-dialect rules:

| Logical meaning | Parquet | Delta | Iceberg |
|---|---|---|---|
| `tinyint` | `tinyint` logical contract | `tinyint` logical contract | `int` (Iceberg has no byte type in the Spark-compatible mapping) |
| `smallint` | `smallint` logical contract | `smallint` logical contract | `int` |
| `int` / `bigint` | same logical width | same logical width | same logical width |
| `decimal(p,s)` | `decimal(p,s)` | `decimal(p,s)` | `decimal(p,s)` |
| `timestamp` | instant timestamp annotation | instant timestamp annotation | instant timestamp annotation |
| `timestamp_ntz` | wall-clock timestamp annotation | wall-clock timestamp annotation | wall-clock timestamp annotation |

The table describes logical compatibility, not byte-for-byte equality. The
actual writer and reader must still support the selected precision, temporal
parameters, nested fields, and existing-target load mode. A runtime
qualification is required before claiming a particular engine/connector/
format/version combination; `dc validate` alone does not prove persisted
round-trip compatibility.

Polars `Int128` is intentionally not inferred into a shared persisted type:
its full range has no equivalent Spark scalar contract. Cast it explicitly to
a supported bounded type before writing, or the output adapter fails clearly.

For Spark-owned sessions, DataCoolie defaults Parquet timestamp output to
`TIMESTAMP_MICROS`, which preserves the instant annotation for Arrow/Polars
readers. If a notebook or host application supplies the Spark session, the
framework does not mutate its SQL configuration. Set
`spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS` in that session before
writing an instant `timestamp`; an `INT96` session fails explicitly instead of
silently producing a timestamp with different semantics in another engine.

For example, a MySQL `tinyint unsigned` hint resolves to a signed 16-bit
logical integer, because an 8-bit signed type cannot hold the full `0–255`
range. The Iceberg adapter then uses `int` while retaining the same values. A
PostgreSQL `int8` resolves to a signed 64-bit logical integer in all three
format contracts.

The opt-in `usecase-sim` qualification uses one matrix dataflow per source
convention (Spark SQL, PostgreSQL, MySQL, SQL Server, Oracle, and SQLite), plus
typed Parquet and weak CSV/JSON/JSONL no-hint flows. Each matrix contains the
mapped scalar families for that convention, and the same metadata/input is
executed by Polars and Spark before persisted Parquet, Delta, and Iceberg
observations are compared with an independent contract. The matrix validates
framework execution and output parity.

The live database qualification additionally seeds run-owned tables/files for
PostgreSQL, MySQL, SQL Server, Oracle, and SQLite and runs every dialect through
both framework readers and all three persisted formats. The vendor-specific
matrix covers 12 cells for MySQL, SQL Server, Oracle, and SQLite (four dialects
× three formats); PostgreSQL has a separate three-format paired gate. Together
the current local receipt covers 15 paired database cells with the
vendor-specific numeric/temporal columns that can be represented by the
fixtures. It includes these explicit boundary rules:

| Source boundary | Required configuration or observed meaning |
|---|---|
| MySQL `YEAR` over Spark JDBC | The JDBC result is a date-like value; the Spark adapter extracts the calendar year before casting. |
| SQL Server `DATETIMEOFFSET` | JDBC returns text in the matrix; the hint includes the offset token (`XXX`) so both engines preserve the instant. |
| Oracle `DATE`/`TIMESTAMP` | `DATE` has second precision; fractional and offset values use `TIMESTAMP`/`TIMESTAMP WITH TIME ZONE`. |
| SQLite weak storage | Spark JDBC uses an explicit `customSchema` for integer/temporal text; Polars uses its native SQLite DB-API route so text is not guessed as a date. |

The matrix selects native DB-API routes when exact source typing is the subject
of the gate. PostgreSQL's default remains ConnectorX. A separate ConnectorX
cell is intentionally qualified as a transport policy: PostgreSQL
`NUMERIC(18,2)` is widened to `Decimal(38,10)` by ConnectorX, and the widened
value is documented as the observed result rather than silently treated as
declared-precision parity. This does not claim that every connector or vendor
type has equivalent extraction semantics.

The qualified default stack is Polars 1.40.1, PyArrow 25.0.0, Delta-rs 1.5.1,
PyIceberg 0.12.0, and Spark 3.5.9. The opt-in Spark profile separately passes
the same persisted-format contract with PySpark 4.1.0, Delta Lake 4.2.0, and
Iceberg Spark runtime 1.11.0. Other Spark/connector versions and cloud
backends remain unverified until their own qualification cell runs.

The explicit native PostgreSQL route requires a `psycopg2-binary` DB-API
driver in the runtime (install it alongside the native source profile). The
default PostgreSQL route remains ConnectorX unless metadata opts into
`database_read_engine: "native"`; do not select the native route without its
DB-API driver installed.

The corresponding native routes use `pymysql` for MySQL, `pymssql` for SQL
Server, and `oracledb` for Oracle. SQLite JDBC text columns are recast to
unbounded Spark `StringType` at the database-reader boundary. Some SQLite JDBC
drivers expose an unbounded text expression as `VARCHAR(0)` in Spark's logical
plan; preserving that hidden constraint makes Delta reject non-empty strings.
This is a reader compatibility correction, not a schema hint or destination
format rule.

For MySQL native reads, DataCoolie removes the sign/decimal-point display
characters that PyMySQL includes in its decimal width before constructing the
logical precision. This keeps a declared `DECIMAL(18,2)` from becoming an
accidental `DECIMAL(20,2)` at the reader boundary.

## Validation and migration

`dc validate` checks the authored type-system name, datatype spelling, and
decimal parameters offline. Connector result-schema checks and persisted
format qualification require a runtime and are reported separately. The
framework runtime uses the Python metadata models and shared resolver; it does
not import the CLI JSON Schema.

Existing metadata that relies on an unknown alias or on the former default
NTZ conversion should be made explicit before upgrading to 0.2.0. Keep
`convert_timestamp_ntz: true` only with a deliberate
`timestamp_timezone`; otherwise leave it false to preserve the wall-clock
value.

See [Transform patterns](transform-patterns.md) for transformer ordering and
[Validation checklist](validation-checklist.md) for the preparation/runtime
boundary.

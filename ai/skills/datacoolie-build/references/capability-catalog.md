# DataCoolie Built-in Capability Catalog

This catalog is a quick implementation index for DataCoolie 0.1.3. The
installed runtime and its discovered plugins are authoritative. Re-check them before choosing a
native path because optional dependencies, engine differences, authentication modes, and third-party
plugins can change the usable combinations.

## Scope

- Read when an installed-version capability inventory is needed.
- Owns the built-in source, destination, transform, and load-strategy snapshot only.
- Does not decide native versus custom implementation, define metadata syntax, or specify runner
  behavior. Route those decisions to `framework-boundary.md`, `schema-quick-reference.md`, and the
  runner contracts.

## Contents

1. Runtime check
2. Sources
3. Destinations
4. Transforms
5. Load strategies
6. Unsupported-by-default boundaries

## 1. Runtime Check

Query the runtime instead of deciding from memory:

```text
python <datacoolie-build>/scripts/inspect_capabilities.py
python <datacoolie-build>/scripts/inspect_capabilities.py --output <evidence.json>
```

The deterministic output includes distribution/module version agreement, declared requirements and
their installed status, entry points, and engine, platform, source, destination, transformer, and
resolver registries. It contains no project connection values or credentials.

Registry presence proves only that an implementation is installed. Apply the evidence order and
full-combination decision from `framework-boundary.md` before selecting a native or custom path.

## 2. Sources

| Source family | Registry/format | Built-in scope | Important checks |
|---|---|---|---|
| Flat files | `parquet`, `csv`, `json`, `jsonl`, `avro`, `excel` | Local and platform-backed path reads; file lineage and incremental patterns are available | Reader options and engine/platform storage support; Excel is read-only |
| Delta Lake | `delta` | Path-based Delta reads; named-table behavior depends on engine/platform | `deltalake` or Spark Delta dependencies and addressing mode |
| Apache Iceberg | `iceberg` | Path- or catalog-based Iceberg reads | `pyiceberg`/Spark catalog configuration and engine limitations |
| SQL database | `sql` | Table or query reads, watermark push-down, URL or structured connection config | Driver, authentication, SQL dialect, and source data types |
| REST API | `api` | HTTP endpoint reads with request configuration, pagination, watermark push-down, and range splitting | Authentication, response extraction, pagination contract, rate limits |
| Python function | `function` | Metadata-addressed function returning an engine-compatible DataFrame | Restrict `allowed_function_prefixes`; use only for a justified custom edge |

Built-in database URL generation recognizes:

- `postgresql`
- `mysql`
- `mssql`
- `oracle`
- `sqlite`

Password authentication is general. `service_principal`, `managed_identity`, and `access_token` are
also modeled, but their actual support depends on database type, engine, driver, and host. A custom
SQLAlchemy URL may cover more databases; treat that as a compatibility-tested path rather than a
built-in database-type guarantee.

## 3. Destinations

| Destination family | Registry/format | Addressing | Built-in load scope |
|---|---|---|---|
| Flat files | `parquet`, `csv`, `json`, `jsonl`, `avro` | Path-based | `append`, `overwrite`, `full_load` |
| Delta Lake | `delta` | Path-based or named table where the engine/platform supports it | All registered load strategies plus maintenance |
| Apache Iceberg | `iceberg` | Path- or catalog-based depending on engine/platform | All registered load strategies plus maintenance subject to engine support |

Do not infer destination support from a source reader. In particular, Excel is source-only, and
database, API, and Python-function destinations have no built-in writer.

## 4. Transforms

DataCoolie runs the metadata-driven transformer pipeline in a fixed order. Author the metadata
field shown below instead of calling transformer classes directly.

| Registry key | Metadata trigger | Supported behavior |
|---|---|---|
| `column_value_transformer` | `transform.value_rules` | `trim`, lower/upper `case`, `regex_replace`, `empty_to_null`, `fill_null`, `map` |
| `schema_converter` | `transform.schema_hints` | Portable target-schema casts and schema-hint configuration |
| `hash_column_adder` | `transform.hash_columns` | Deterministic SHA-256 using `dc_hash_v1` serialization |
| `deduplicator` | `deduplicate_columns`, `latest_data_columns` | Latest-wins deduplication; can fall back to merge keys/watermarks |
| `column_adder` | `transform.additional_columns` | Computed columns from engine-supported SQL expressions |
| `row_filter` | `transform.filter_expression` | Post-computed-column SQL predicate filtering |
| `scd2_column_adder` | destination `load_type: scd2` | SCD2 validity columns |
| `system_column_adder` | always | Framework audit columns |
| `partition_handler` | `destination.partition_columns` | Derived or direct partition columns |
| `data_masker` | `transform.masking_rules` | `redact`, `nullify`, `partial`, `numeric_bucket`, `date_truncate` |
| `column_projector` | `select_columns`, `drop_columns`, `rename_columns` | Projection and atomic rename with key protection |
| `column_name_sanitizer` | always | `lower` or `snake` column-name normalization |

Expression syntax and type behavior must be portable across the selected engines when a project is
expected to run on both Spark and Polars. Use the metadata schema and transform tests to confirm
field-level constraints such as supported casts, portable regex, hash input types, and masking key
restrictions.

## 5. Load Strategies

| Metadata value | Meaning | Main constraint |
|---|---|---|
| `overwrite` | Replace the target | Preferred spelling for full refresh |
| `full_load` | Alias of overwrite | Retained for compatibility |
| `append` | Add rows only | Requires a duplicate-safe source contract |
| `merge_upsert` | Latest-wins upsert by key | Requires merge keys and a merge-capable lakehouse destination |
| `merge_overwrite` | Replace rows/windows matched by key or watermark range | Requires merge keys or the configured range contract |
| `scd2` | Preserve Type 2 history | Requires SCD2 keys/effective-column contract |

Flat-file destinations support only append/full refresh behavior. Treat merge and SCD2 strategies as
lakehouse capabilities and verify the selected Delta or Iceberg engine path.

## 6. Unsupported-by-default Boundaries

The 0.1.3 built-ins do not provide:

- a streaming source or destination implementation, even though `streaming` is reserved in the
  connection model;
- an Excel destination;
- database, API, or Python-function destination writers;
- an arbitrary custom transform declared only by class name in metadata.

Treat this list as inventory evidence, not automatic permission to implement a fallback. The
framework boundary reference owns that decision and its evidence requirements.

## Unresolved Questions

- None. Resolve project-specific compatibility from the installed runtime and the selected engine,
  platform, authentication, and dependency set.

---
title: Configure File Metadata — DataCoolie User Guide
description: Author DataCoolie metadata in JSON, YAML, or Excel files and load connections, dataflows, schema hints, and watermarks from disk.
---

# Configure file metadata

**Prerequisites** · A directory you control for metadata and
`pip install "datacoolie[polars]"` to run the local CSV-to-Parquet example.
Add `datacoolie[metadata-yaml]` to read YAML or `datacoolie[metadata-excel]`
to read Excel.

**End state** · `FileProvider` loads one selected JSON, YAML, or Excel metadata
file describing a local orders dataflow.

## Start with JSON

Start with one JSON file. This guide shows equivalent YAML and Excel forms
below; choose one format for the metadata directory used by a run:

```
metadata/
└── orders.json
```

The example reads `data/input/orders/orders.csv` and writes Parquet under
`data/output/orders/`. For a complete project that creates its local CSV input
and runs a CSV-to-Parquet dataflow, see the
[transform recipe](../../examples/dataflows.md#transform-project-recipe). To learn
metadata field by field, use [Build your first metadata file](../metadata/first-metadata-file.md).

## Minimal JSON

```json
{
  "connections": [
    {
      "name": "orders_input",
      "connection_type": "file",
      "format": "csv",
      "configure": {"base_path": "data/input"}
    },
    {
      "name": "orders_output",
      "connection_type": "file",
      "format": "parquet",
      "configure": {"base_path": "data/output"}
    }
  ],
  "dataflows": [
    {
      "name": "orders_to_parquet",
      "stage": "bronze2silver",
      "source": {"connection_name": "orders_input", "table": "orders"},
      "destination": {"connection_name": "orders_output", "table": "orders", "load_type": "overwrite"}
    }
  ]
}
```

## Minimal YAML

```yaml
connections:
  - name: orders_input
    connection_type: file
    format: csv
    configure:
      base_path: data/input

  - name: orders_output
    connection_type: file
    format: parquet
    configure:
      base_path: data/output

dataflows:
  - name: orders_to_parquet
    stage: bronze2silver
    source:
      connection_name: orders_input
      table: orders
    destination:
      connection_name: orders_output
      table: orders
      load_type: overwrite
```

## Minimal Excel

Use a workbook with `connections` and `dataflows` sheets. `schema_hints` is
optional for the minimal case.

`connections` sheet:

| name | connection_type | format | configure |
|---|---|---|---|
| orders_input | file | csv | `{ "base_path": "data/input" }` |
| orders_output | file | parquet | `{ "base_path": "data/output" }` |

`dataflows` sheet:

| name | stage | source_connection_name | source_schema_name | source_table | destination_connection_name | destination_schema_name | destination_table | destination_load_type |
|---|---|---|---|---|---|---|---|---|
| orders_to_parquet | bronze2silver | orders_input |  | orders | orders_output |  | orders | overwrite |

For a short workbook, keep nested JSON in the `configure` and `transform`
cells. The parser also accepts `configure_*` and flat transform columns when
you need easier spreadsheet editing:

- list cells: `transform_select_columns` and `transform_drop_columns`
- JSON object/array cells: `transform_rename_columns`,
  `transform_value_rules`, `transform_hash_columns`,
  `transform_masking_rules`, `transform_additional_columns`, and
  `transform_configure`; `transform_deduplicate_columns` and
  `transform_latest_data_columns` use JSON arrays
- scalar cells: `transform_filter_expression`

Flat values are merged into the `transform` object. JSON cells must contain
valid JSON; list cells accept a JSON array or a comma-separated string.

## Loading one file

```python
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.platforms.local_platform import LocalPlatform

platform = LocalPlatform()
provider = FileProvider(config_path="metadata/orders.json")
provider.bind_platform(platform)
```

Construction only records the source. The Driver startup boundary activates
the accepted loggers, then calls `provider.initialize()` and validates the
complete metadata scope. This makes provider startup failures diagnosable in
the session logs. Standalone callers can call `initialize()` explicitly after
binding the platform; metadata getters use the same startup boundary
automatically.

`FileProvider` detects JSON, `.yaml`/`.yml`, and `.xlsx` paths. Legacy `.xls`
files are rejected explicitly; convert them to `.xlsx` first. The
`config_path` is primary; optional `connections_path` and
`schema_hints_path` files replace those sections. Watermark storage is a
runtime concern owned by `FileProvider`: pass `watermark_base_path` explicitly
for a standalone provider, or let `DataCoolieDriver` bind it from
`state_base_path` (preferred) or the parent of the effective
`log_base_path`:

```python
provider = FileProvider(
    config_path="metadata/dataflows.yaml",
    connections_path="metadata/connections.json",
    schema_hints_path="metadata/schema_hints.xlsx",
    watermark_base_path="state/watermarks",
    sql_base_path=["sql_shared", "sql_project"],
    platform=platform,
)
```

Without a bound or explicit watermark root, metadata loading remains valid but
watermark `get`/`save` operations raise a configuration error. The framework
does not infer a sibling of `config_path`; choose a provider-owned watermark
root or pass a Driver state/log root according to the runtime configuration
precedence.

`sql_base_path` is also provider-owned configuration. Use one string for a
single root or a list for multiple roots; with multiple roots, the final folder
name selects the root in `source.query`. The Driver's `sql_base_path` remains a
session fallback when this provider value is omitted, and equal normalized
values may be supplied in both places.

### Verify startup and hand off to a Driver

Use an explicit startup boundary when a standalone application wants failures
before the first dataflow run:

```python
provider.initialize()
connections = provider.get_connections(active_only=False)
dataflows = provider.get_dataflows(
    stage="bronze2silver",
    active_only=False,
    attach_schema_hints=False,
)
print(f"connections={len(connections)} dataflows={len(dataflows)}")
```

For the engine/platform setup and the Driver handoff, use the shared
[provider startup example](../../examples/configuration.md#provider-startup).
An injected provider remains caller-owned: close it after the Driver finishes.
The Driver closes only a provider that it created itself.

With the default cache, `clear_cache()` makes the next metadata read rebuild the
provider snapshot. `enable_cache=False` still validates the complete configured
scope, but it does not make the provider reread changed files. To pick up an
edited file, create a new `FileProvider` for the next session. Do not clear or
close a provider while a Driver operation is running.

## Loading a deployed artifact folder

Artifact mode keeps metadata separate from runtime state and SQL files:

```text
artifact/
├── metadata/
│   ├── connections.json
│   ├── source2bronze.json
│   └── bronze2silver.json
├── sql/
└── functions/
```

Each metadata shard uses a section wrapper. The stage comes from each
dataflow's `stage` field, not from the filename:

```json
{"dataflows": [{"name": "orders", "stage": "source2bronze", "source": {"connection_name": "src", "table": "orders"}, "destination": {"connection_name": "bronze", "table": "orders", "load_type": "append"}}]}
```

The wrapper is required for every discovered JSON/YAML shard (including
`metadata`-wrapped documents); empty files, bare lists, and arbitrary mappings
fail startup. To represent an intentionally empty scope, use an explicit
section such as `{"dataflows": []}`. An Excel workbook must contain at least
one supported sheet, and an overlay workbook must include its target sheet.

Use the explicit directory when it differs from the artifact convention:

```python
provider = FileProvider(platform=platform, metadata_base_path="deploy/metadata")
```

Or let the factory create and initialize a `FileProvider` from the artifact
root (defaulting to `artifact/metadata`):

```python
driver = create_driver(
    engine=engine,
    platform=platform,
    artifact_base_path="deploy/artifact",
)
```

The direct Driver constructor follows the same assembly rules and also accepts
an explicit metadata directory without requiring a separate provider object:

```python
driver = DataCoolieDriver(
    engine=engine,
    platform=platform,
    metadata_base_path="deploy/metadata",
    artifact_base_path="deploy/artifact",  # still available for SQL files
)
```

If a `FileProvider` is injected, passing the same normalized
`metadata_base_path` is allowed. Passing a different established root, or
combining the path with a provider configured by `config_path`, fails during
Driver construction before metadata is loaded. Database/API providers reject
the explicit file path rather than ignoring it.

`connections_path` and `schema_hints_path` remain explicit section overrides;
they replace the discovered section rather than being appended twice.

## Choose a file format

The JSON, YAML, and Excel examples above express the same two connections and
one dataflow. Save the form you use as `metadata/orders.json`,
`metadata/orders.yaml`, or `metadata/orders.xlsx`, and pass that exact path as
`config_path`. Use `datacoolie[metadata-yaml]` to read YAML and
`datacoolie[metadata-excel]` to read Excel.

When using `metadata_base_path` to discover a directory, keep only one form of
each connection and dataflow there. Discovery reads all supported files in the
directory; putting JSON, YAML, and Excel copies of the same metadata together
causes duplicate identity errors at startup. The repository's scenario
generator can emit sibling formats for testing, but those siblings are not a
deployment layout for directory discovery.

## Gotchas

| Symptom | Cause | Fix |
|---|---|---|
| All rows load as inactive | Excel `is_active` was entered as `False` | Leave `is_active` blank when it should use the default `True`. |
| YAML or Excel cannot be read | The optional parser is missing | Install `datacoolie[metadata-yaml]` or `datacoolie[metadata-excel]` for the selected format. |
| Duplicate connection or dataflow identity | Directory discovery loaded copies in several formats | Keep one selected form of each record in the discovered directory, or pass one exact file as `config_path`. |
| Excel parse error in nested fields | A JSON cell such as `configure`, `secrets_ref`, `source_configure`, `destination_configure`, or `transform` contains invalid JSON | Fix the cell to valid JSON. `configure_*` and `transform_*` columns are supported, but any JSON cell must still be valid JSON. |

## Related

- [Metadata guide for new users](../metadata/index.md) — if you are new to DataCoolie, start with field-by-field guidance for connections, sources, destinations, and transforms
- [Concepts · Metadata providers · File provider](../../reference/concepts/metadata-providers.md#file-provider)
- [Reference · Metadata document](../../reference/metadata-schema.md#metadata-document)

# Configure file metadata

**Prerequisites** · `datacoolie[excel]` or `openpyxl` if you want `.xlsx` generation · `pyyaml` if you want `.yaml` generation · a directory you control for metadata files.
**End state** · Working `FileProvider` reading JSON (canonical) with optional generated YAML/Excel siblings.

## The canonical format is JSON

Keep your source of truth as JSON. One file per use case:

```
metadata/
└── file/
    ├── orders_csv_to_parquet_full_load.json    ← canonical
    ├── orders_csv_to_parquet_full_load.yaml    ← generated
    └── orders_csv_to_parquet_full_load.xlsx    ← generated
```

See the [usecase-sim file metadata folder](https://github.com/datacoolie/datacoolie/tree/main/datacoolie/usecase-sim/metadata/file)
for production-shape examples.

## Minimal JSON

```json
{
  "connections": [
    {
      "name": "src",
      "connection_type": "file",
      "format": "csv",
      "configure": {"base_path": "data/input"}
    },
    {
      "name": "bronze",
      "connection_type": "lakehouse",
      "format": "delta",
      "configure": {"base_path": "data/output/bronze"}
    }
  ],
  "dataflows": [
    {
      "name": "orders_to_bronze",
      "stage": "ingest2bronze",
      "source":      { "connection_name": "src", "schema_name": "sales", "table": "orders" },
      "destination": { "connection_name": "bronze", "schema_name": "sales", "table": "orders", "load_type": "append" }
    }
  ]
}
```

## Minimal YAML

```yaml
connections:
  - name: src
    connection_type: file
    format: csv
    configure:
      base_path: data/input

  - name: bronze
    connection_type: lakehouse
    format: delta
    configure:
      base_path: data/output/bronze

dataflows:
  - name: orders_to_bronze
    stage: ingest2bronze
    source:
      connection_name: src
      schema_name: sales
      table: orders
    destination:
      connection_name: bronze
      schema_name: sales
      table: orders
      load_type: append
```

## Minimal Excel

Use a workbook with `connections` and `dataflows` sheets. `schema_hints` is
optional for the minimal case.

`connections` sheet:

| name | connection_type | format | configure |
|---|---|---|---|
| src | file | csv | `{ "base_path": "data/input" }` |
| bronze | lakehouse | delta | `{ "base_path": "data/output/bronze" }` |

`dataflows` sheet:

| name | stage | source_connection_name | source_schema_name | source_table | destination_connection_name | destination_schema_name | destination_table | destination_load_type |
|---|---|---|---|---|---|---|---|---|
| orders_to_bronze | ingest2bronze | src | sales | orders | bronze | sales | orders | append |

For a short workbook, keep nested JSON in the `configure` cell. The parser also
accepts `configure_*` and `transform_*` columns when you need flatter editing.

## Loading

```python
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.platforms.local_platform import LocalPlatform

platform = LocalPlatform()
provider = FileProvider(config_path="metadata/file/orders_csv_to_parquet_full_load.json", platform=platform)
```

`FileProvider` detects format by extension (`.json`, `.yaml`, `.xlsx`). Pass
a single metadata file — one `config_path` per `FileProvider` instance.

## Generating YAML + Excel from JSON

Use the usecase-sim setup script and target a single canonical JSON file:

```powershell
python usecase-sim/scripts/setup_metadata.py --json usecase-sim/metadata/file/local_use_cases.json --targets file
```

It reads the JSON file passed via `--json` and emits sibling `.yaml` and
`.xlsx` files with the same stem next to it. Regenerate after each JSON edit.

If `pyyaml` is not installed, YAML output is skipped with a warning. If
`openpyxl` is not installed, XLSX output is skipped with a warning.

## Gotchas

| Symptom | Cause | Fix |
|---|---|---|
| All rows load as inactive | Excel `is_active` treated as `False` when blank | Leave `is_active` blank = unset (falls back to `True`); generator preserves this. |
| `.yaml` or `.xlsx` sibling was not created | Required emitter dependency is missing | Install `pyyaml` for YAML and `openpyxl` or `datacoolie[excel]` for XLSX, then rerun `setup_metadata.py --targets file`. |
| YAML or XLSX no longer matches JSON | Generated siblings are not auto-synced after JSON edits | Treat JSON as canonical and rerun `setup_metadata.py --targets file` after each JSON change. |
| Excel parse error in nested fields | A JSON cell such as `configure`, `secrets_ref`, `source_configure`, `destination_configure`, or `transform` contains invalid JSON | Fix the cell to valid JSON. `configure_*` and `transform_*` columns are supported, but any JSON cell must still be valid JSON. |

## Related

- [Concepts · Metadata providers](../concepts/metadata-providers.md)
- [Reference · Metadata schema](../reference/metadata-schema.md)

---
name: datacoolie-init
description: Scaffold new DataCoolie ETL projects and generate metadata from data sources. Use when user asks to create a new project, scaffold, initialize, setup datacoolie, create pipeline, write metadata, introspect sources, generate metadata from files/DDL, plan engine strategy, or start a new ETL workflow.
---

# datacoolie-init

Scaffold DataCoolie ETL projects with best-practice structure, generate metadata from data sources, and recommend engine strategies.

## Scope

This skill handles: project scaffolding, source introspection (folder scan, DDL parse, manual columns), engine advisory.
Does NOT handle: metadata validation/lint (use `datacoolie-metadata`), deployment (use `datacoolie-deploy`), runtime execution.

## AI Workflow (How Gen AI builds metadata)

When an AI assistant helps a user create DataCoolie metadata, follow this workflow:

### Decision: scripts vs AI-generation

- **User has existing data files on disk** → run `introspect.py --mode folder` (auto-detects schema)
- **User has SQL DDL** → run `introspect.py --mode ddl` (extracts from CREATE TABLE)
- **User wants full project structure** → run `scaffold.py` (generates directories + starter metadata)
- **User describes data verbally / provides requirements** → AI generates metadata directly using Schema Quick Reference below
- **Mix**: scaffold first, then AI fills in dataflows based on user requirements

### Step 1: Understand user's data sources

Ask the user:
- What data sources? (files, databases, APIs)
- What format? (CSV, Parquet, Delta, JSON, SQL database type)
- Where does data land? (local, S3, lakehouse, Fabric)
- What load strategy? (full, incremental, merge, SCD2)

### Step 2: Generate metadata using schema knowledge

The AI **must read the Schema Quick Reference** in `datacoolie-metadata` SKILL.md to know the exact structure. Key rules:
- **Never guess table names or column names** — either run introspection scripts to discover them, or ask the user to provide the exact list
- Top-level: `{ "$schema": "https://raw.githubusercontent.com/datacoolie/datacoolie/main/skills/datacoolie-metadata/schemas/0.1.0/metadata.schema.json", "connections": [...], "dataflows": [...] }`
- Every dataflow `source.connection_name` must reference an existing connection `name`
- Use `secrets_ref` for credentials — never hardcode
- Add `watermark_columns` for incremental loads
- Add `merge_keys` for merge/scd2 load types
- Add `transform.schema_hints` when types can't be auto-detected (CSV, JSON sources)

### Step 3: Validate the output

After generating metadata, always run:
```bash
python skills/datacoolie-metadata/scripts/validate.py <generated_file>
python skills/datacoolie-metadata/scripts/lint.py <generated_file> --engine <engine> --env prod
```

### Step 4: Use scripts when applicable

- **User has existing data files**: run `introspect.py --mode folder` to auto-detect schema
- **User has SQL DDL**: run `introspect.py --mode ddl` to extract from CREATE TABLE
- **User wants full project**: run `scaffold.py` to generate directory structure
- **User describes data verbally**: AI generates metadata directly using Schema Quick Reference

### Runner script usage (generated `scripts/run_local.py`)

The scaffolded runner is based on the correct DataCoolie driver pattern:

```python
from datacoolie.core import DataCoolieRunConfig
from datacoolie.engines import PolarsEngine          # or SparkEngine
from datacoolie.metadata import FileProvider
from datacoolie.orchestration import DataCoolieDriver
from datacoolie.platforms import LocalPlatform

platform = LocalPlatform()
engine   = PolarsEngine(platform=platform)
metadata = FileProvider(config_path="metadata/dataflows.json", platform=platform)
config   = DataCoolieRunConfig(dry_run=False)        # WatermarkManager auto-created

with DataCoolieDriver(engine=engine, platform=platform,
                      metadata_provider=metadata, config=config) as driver:
    result = driver.run(stage="source2bronze", column_name_mode="lower")
```

Key rules when generating or explaining runner code:
- `DataCoolieRunConfig` fields: `dry_run`, `job_num`, `job_index`, `max_workers`, `stop_on_error`, `retry_count`, `retry_delay`, `allowed_function_prefixes` — NOT `engine`, `stage`, `metadata_path`
- `stage` is passed to `driver.run(stage=...)`, not to config
- `WatermarkManager` is auto-created when `watermark_manager=None` — don't import or construct it manually
- Always use `with DataCoolieDriver(...) as driver:` for proper log flushing on exit
- `driver.load_dataflows(stage=...)` to inspect without running; `driver.run(stage=...)` to execute
- `FileProvider(config_path=..., platform=...)` for file-based metadata (JSON/YAML/xlsx)

### Cross-skill dependency

This skill depends on `datacoolie-metadata` for:
- Schema definition (single source of truth at `skills/datacoolie-metadata/schemas/`)
- Validation (`validate.py`) and linting (`lint.py`) of generated output
- Format conversion (`convert.py`) if user wants YAML or Excel

## Security Policy

- Never embed credentials in generated files — use `secrets_ref` pattern
- Never scan directories outside the user's explicit path
- Generated `config.yaml` never contains passwords or API keys

## Workflow

1. **Scaffold** — Generate full project directory structure from user choices
2. **Introspect** — Scan data sources to generate metadata (connections, dataflows, schema_hints)
3. **Advisory** — Recommend engine strategy (polars vs spark) per stage

## Scripts

All scripts in `scripts/` (run from this skill's folder: `datacoolie/skills/datacoolie-init/`).

### scaffold.py — Project generation

```bash
python scripts/scaffold.py --name <project> [options]
python scripts/scaffold.py --name sales_pipeline --source-type file --platform local --engine polars
python scripts/scaffold.py --name data_warehouse --source-type database --platform aws --engine mixed --layers source2bronze,bronze2silver,silver2gold
python scripts/scaffold.py --name api_ingest --source-type api --platform local,fabric --engine spark --dest-format iceberg
```

Options:
- `--name` — Project name (required)
- `--source-type` — Source types: `file`, `database`, `api` (comma-separated, default: file)
- `--platform` — Target platforms: `local`, `aws`, `fabric`, `databricks` (comma-separated, default: local)
- `--engine` — Engine strategy: `polars`, `spark`, `mixed` (default: polars)
- `--layers` — Medallion layers: `source2bronze`, `bronze2silver`, `silver2gold` (comma-separated, default: source2bronze)
- `--output` — Output directory (default: `./{name}`)
- `--dest-format` — Destination format: `delta`, `parquet`, `iceberg` (default: delta)

Generated structure:
```
{project}/
├── metadata/
│   ├── connections.json
│   ├── dataflows.json
│   └── environments/
│       ├── dev.yaml
│       └── prod.yaml
├── functions/
│   ├── __init__.py
│   ├── sources.py
│   └── pyproject.toml
├── data/eval/input/
├── data/eval/output/
├── scripts/
│   └── run_local.py
├── .datacoolie/config.yaml
├── .gitignore
└── requirements.txt
```

`functions/sources.py` is scaffolded with `*args, **kwargs` so custom functions are forward-compatible with new kwargs and don't fail when watermarks are unused:
```python
def my_source(*args, **kwargs):
    engine          = kwargs.get("engine")           # BaseEngine instance
    source          = kwargs.get("source")           # Source model
    watermark_start = kwargs.get("watermark_start") # dict | None (first run = None)
    watermark_end   = kwargs.get("watermark_end")   # dict | None (replay ceiling)
    ...
```
or func(engine=..., source=..., watermark_start=..., watermark_end=..., *args, **kwargs)
Reference in metadata: `"source": { "python_function": "functions.sources.my_source" }`

### introspect.py — Source introspection

```bash
# Folder scan — infer schema from data files
python scripts/introspect.py --mode folder --path ./data/input/csv --connection-name my_csv

# DDL parse — extract from CREATE TABLE statements
python scripts/introspect.py --mode ddl --ddl-file schema.sql --connection-name my_db

# Manual — define columns as key:type pairs
python scripts/introspect.py --mode manual --columns "id:int,name:string,amount:decimal,created_at:timestamp"

# Output to file
python scripts/introspect.py --mode folder --path ./data --connection-name src --output metadata.json
```

Modes:
- **folder** — Scan directory (subdirs = tables). Supports CSV, JSON, Parquet, JSONL, Avro, Excel
- **ddl** — Parse `CREATE TABLE` SQL statements, map SQL types → DataCoolie types
- **manual** — Accept `col:type` pairs, generate schema_hints

Type mapping (SQL → DataCoolie):
| SQL Types | DataCoolie |
|-----------|------------|
| int, integer, bigint, serial | int |
| varchar, text, char, string | string |
| decimal, numeric, float, double | decimal |
| timestamp, datetime | timestamp |
| date | date |
| boolean, bool, bit | boolean |

Folder scan inference:
- Column names containing `_at`, `_date`, `timestamp` → timestamp
- Column names containing `_id`, `_count`, `quantity` → int
- Column names containing `amount`, `price`, `cost` → decimal
- Column names containing `is_`, `has_`, `flag` → boolean
- Fallback: sample first 100 rows for value-based inference

### Engine Advisory (built into scaffold.py)

When `--engine mixed` is specified, scaffold.py prints a recommendation table:

```
┌─────────────────────────────────┬──────────────┬──────────────────────────────────────┐
│ Stage                           │ Recommended  │ Reason                               │
├─────────────────────────────────┼──────────────┼──────────────────────────────────────┤
│ source2bronze (file)            │ polars       │ File I/O ingest < 10GB typical       │
│ bronze2silver (file)            │ spark        │ Transforms with joins benefit ...    │
│ silver2gold (file)              │ spark        │ Aggregation/gold stages ...          │
└─────────────────────────────────┴──────────────┴──────────────────────────────────────┘
```

Rules:
- `source2bronze` + file/api → polars (lightweight ingest)
- `source2bronze` + database → spark (distributed reads)
- `bronze2silver` → spark (joins/transforms)
- `silver2gold` → spark (aggregation/large-scale)
- User can override any recommendation

## Dependencies

- Python 3.10+
- `pyarrow` (optional, for Parquet introspection)
- No required pip dependencies beyond stdlib

## Examples

```bash
# Scaffold a project
python scripts/scaffold.py --name my_etl --engine polars --platform local

# Introspect data files into metadata
python scripts/introspect.py --mode folder --path /data/raw/csv --connection-name raw_csv --output my_etl/metadata/introspected.json

# Mixed engine project targeting AWS
python scripts/scaffold.py --name dw_pipeline \
    --source-type database,file \
    --platform local,aws \
    --engine mixed \
    --layers source2bronze,bronze2silver,silver2gold \
    --dest-format delta

# Generate metadata from DDL
python scripts/introspect.py --mode ddl --ddl-file warehouse_schema.sql \
    --connection-name warehouse --dest-format delta --output metadata/dataflows.json
```

> After generating, validate with `datacoolie-metadata` skill (CWD = `datacoolie/`):
> `python skills/datacoolie-metadata/scripts/validate.py <generated_file>`

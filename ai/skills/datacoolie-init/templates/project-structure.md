# DataCoolie Project Structure Template

Create these directories and files, substituting `{variables}` from user input and/or architecture document.

## Variables

| Variable | Source | Default | Description |
|----------|--------|---------|-------------|
| `{project_name}` | User input (always) | — | Project directory and config name |
| `{engine}` | Architecture Engine Strategy or user input | `polars` | Primary engine: polars, spark, or mixed |
| `{platform}` | Architecture Overview or user input | `local` | Target platform for prod |
| `{environments}` | Architecture Environment Differences | dev only | YAML block for config.yaml |
| `{infra_*}` | Architecture Infrastructure Requirements | empty | Resource names (workspace, lakehouse, etc.) |
| `{prod_*_path}` | Architecture Medallion Layers | empty | Storage paths per layer per environment |

## Directory Layout

```
./                                    # Scaffold target (current dir or {project_name}/)
├── metadata/
│   ├── metadata.json                 # Connections + dataflows + schema_hints (combined)
│   └── environments/
│       ├── dev.yaml                  # Dev overlay (always created)
│       └── prod.yaml                 # Prod overlay (if architecture specifies prod)
├── functions/
│   ├── __init__.py
│   ├── sources.py                    # Custom source function stubs
│   └── pyproject.toml                # Package config
├── scripts/
│   └── run_local.py                  # Local runner (engine from architecture)
├── .datacoolie/config.yaml           # Project config (pre-populated from architecture)
├── .gitignore
└── requirements.txt
```

## File Contents

### .datacoolie/config.yaml

**Minimal (no architecture):**

```yaml
project_name: {project_name}
engine: {engine}                      # polars | spark | mixed

environments:
  dev:
    platform: local
```

**Architecture-aware (pre-populated from approved architecture):**

```yaml
project_name: {project_name}
engine: {engine}                      # polars | spark | mixed

environments:
  dev:
    platform: local
    # Dev always uses local platform for fast iteration
  # --- Architecture-derived environments below ---
  # Example for Fabric:
  # test:
  #   platform: fabric
  #   fabric:
  #     workspace: "{infra_test_workspace}"
  # prod:
  #   platform: fabric
  #   fabric:
  #     workspace: "{infra_prod_workspace}"
  #     lakehouse_bronze: "{infra_bronze_lakehouse}"
  #     lakehouse_silver: "{infra_silver_lakehouse}"
  #     warehouse_gold: "{infra_gold_warehouse}"
  #
  # Example for AWS:
  # prod:
  #   platform: aws
  #   aws:
  #     region: "{infra_aws_region}"
  #     bucket: "{infra_aws_bucket}"
  #     role_arn: "{infra_aws_role_arn}"
```

Infrastructure resource names come from the architecture's Infrastructure Requirements table.

### metadata/metadata.json

```json
{
  "connections": [],
  "dataflows": [],
  "schema_hints": []
}
```

### metadata/environments/dev.yaml

```yaml
# Dev environment overlay — merged with base metadata at deploy time
# Override connection paths, secrets, and platform-specific settings

# connections:
#   - name: bronze_storage
#     configure:
#       base_path: "./data/bronze"    # Local paths for dev
#
#   - name: silver_storage
#     configure:
#       base_path: "./data/silver"
```

### metadata/environments/prod.yaml

```yaml
# Prod environment overlay
# Override connection paths, secrets, and platform-specific settings

# connections:
#   - name: bronze_storage
#     configure:
#       base_path: "{prod_bronze_path}"
#     secrets_ref:
#       credential: BRONZE_STORAGE_KEY
#
#   - name: silver_storage
#     configure:
#       base_path: "{prod_silver_path}"
```

### functions/__init__.py

```python
```

### functions/sources.py

```python
def my_source(*args, **kwargs):
    engine          = kwargs.get("engine")           # BaseEngine instance
    source          = kwargs.get("source")           # Source model
    watermark_start = kwargs.get("watermark_start")  # dict | None (first run = None)
    watermark_end   = kwargs.get("watermark_end")    # dict | None (replay ceiling)
    # Custom source logic here
    ...

# or
# def my_source(engine, source, watermark_start, watermark_end, *args, **kwargs):
```

Reference in metadata: `"source": { "python_function": "functions.sources.my_source" }`

### functions/pyproject.toml

```toml
[project]
name = "{project_name}-functions"
version = "0.1.0"
requires-python = ">=3.10"
```

### scripts/run_local.py

**For `engine: polars` (default):**

```python
from datacoolie.core import DataCoolieRunConfig
from datacoolie.engines import PolarsEngine
from datacoolie.metadata import FileProvider
from datacoolie.orchestration import DataCoolieDriver
from datacoolie.platforms import LocalPlatform

platform = LocalPlatform()
engine   = PolarsEngine(platform=platform)
metadata = FileProvider(config_path="metadata/metadata.json", platform=platform)
config   = DataCoolieRunConfig(dry_run=False)

with DataCoolieDriver(engine=engine, platform=platform,
                      metadata_provider=metadata, config=config) as driver:
    result = driver.run(stage="source2bronze", column_name_mode="lower")
    print(f"Rows read: {result.total_rows_read}, written: {result.total_rows_written}")
```

**For `engine: spark`:**

```python
from datacoolie.core import DataCoolieRunConfig
from datacoolie.engines import SparkEngine
from datacoolie.metadata import FileProvider
from datacoolie.orchestration import DataCoolieDriver
from datacoolie.platforms import LocalPlatform

platform = LocalPlatform()
engine   = SparkEngine(platform=platform)
metadata = FileProvider(config_path="metadata/metadata.json", platform=platform)
config   = DataCoolieRunConfig(dry_run=False)

with DataCoolieDriver(engine=engine, platform=platform,
                      metadata_provider=metadata, config=config) as driver:
    result = driver.run(stage="source2bronze", column_name_mode="lower")
    print(f"Rows read: {result.total_rows_read}, written: {result.total_rows_written}")
```

**For `engine: mixed` (from architecture with mixed engine strategy):**

```python
from datacoolie.core import DataCoolieRunConfig
from datacoolie.engines import PolarsEngine, SparkEngine
from datacoolie.metadata import FileProvider
from datacoolie.orchestration import DataCoolieDriver
from datacoolie.platforms import LocalPlatform

platform = LocalPlatform()
metadata = FileProvider(config_path="metadata/metadata.json", platform=platform)
config   = DataCoolieRunConfig(dry_run=False)

# --- Engine selection per stage ---
# Architecture specifies:
#   source→bronze: polars (lightweight ingestion)
#   bronze→silver: polars (simple transforms)
#   silver→gold:   spark  (complex joins/aggregations)

# Bronze + Silver stages (polars)
engine = PolarsEngine(platform=platform)
with DataCoolieDriver(engine=engine, platform=platform,
                      metadata_provider=metadata, config=config) as driver:
    driver.run(stage="source2bronze", column_name_mode="lower")
    driver.run(stage="bronze2silver", column_name_mode="lower")

# Gold stages (spark)
engine = SparkEngine(platform=platform)
with DataCoolieDriver(engine=engine, platform=platform,
                      metadata_provider=metadata, config=config) as driver:
    result = driver.run(stage="silver2gold", column_name_mode="lower")
    print(f"Rows read: {result.total_rows_read}, written: {result.total_rows_written}")
```

Use the variant matching the `{engine}` value from architecture or user input.

### .gitignore

```
__pycache__/
*.pyc
.datacoolie/watermarks/
.env
*.egg-info/
```

### requirements.txt

```
datacoolie
```

### metadata/metadata.json

```json
{
  "$schema": "https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/skills/datacoolie-metadata/schemas/0.1.0/metadata.schema.json",
  "connections": [],
  "dataflows": []
}
```

### metadata/environments/dev.yaml

```yaml
# Dev environment overlay
# Override connection settings for local development
```

### metadata/environments/prod.yaml

```yaml
# Prod environment overlay
# Override connection settings for production
```

## Runner Code Rules

- `DataCoolieRunConfig` fields: `dry_run`, `job_num`, `job_index`, `max_workers`, `stop_on_error`, `retry_count`, `retry_delay`, `allowed_function_prefixes` — NOT `engine`, `stage`, `metadata_path`
- `stage` is passed to `driver.run(stage=...)`, not to config
- `WatermarkManager` is auto-created when `watermark_manager=None` — don't import or construct it manually
- Always use `with DataCoolieDriver(...) as driver:` for proper log flushing on exit
- `driver.load_dataflows(stage=...)` to inspect without running; `driver.run(stage=...)` to execute
- `FileProvider(config_path=..., platform=...)` for file-based metadata (JSON/YAML/xlsx)

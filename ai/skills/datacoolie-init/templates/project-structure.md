# DataCoolie Project Structure Template

Create these directories and files, substituting `{variables}` from user input.

## Directory Layout

```
{project_name}/
├── metadata/
│   ├── metadata.json              # Connections + dataflows (single file)
│   └── environments/
│       ├── dev.yaml              # Dev overlay
│       └── prod.yaml             # Prod overlay
├── functions/
│   ├── __init__.py or pyproject.toml # Package init|config
│   └── sources.py                # Custom source functions
├── scripts/
│   └── run_local.py              # Local runner
├── .datacoolie/config.yaml       # Project config
├── .gitignore                    # Git ignore
└── requirements.txt              # Python deps
```

## File Contents

### .datacoolie/config.yaml

```yaml
project_name: {project_name}
engine: {engine}
environments:
  dev:
    platform: local
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

```python
from datacoolie.core import DataCoolieRunConfig
from datacoolie.engines import PolarsEngine          # or SparkEngine
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
```

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

# DataCoolie Project Structure Template

Create these directories and files, substituting `{variables}` from user input and/or architecture document.

## Variables

| Variable | Source | Default | Description |
|----------|--------|---------|-------------|
| `{project_name}` | User input (always) | - | Project display/config name |
| `{workspace_name}` | Derived from project name | `{normalized_project_name}_dcws` | DataCoolie workspace folder name |
| `{runner_engine}` | Architecture Engine Strategy or user input | `polars` | Default concrete generated runner engine: polars or spark |
| `{engine_strategy}` | Architecture Engine Strategy matrix | dev stages default to polars | Optional env+stage matrix where each value is `polars` or `spark` |
| `{platform}` | Architecture Overview or user input | `local` | Target platform for prod |
| `{environments}` | Architecture Environment Differences | dev only | YAML block for config.yaml |
| `{infra_*}` | Architecture Infrastructure Requirements | empty | Resource names (workspace, lakehouse, etc.) |
| `{prod_*_path}` | Architecture Medallion Layers | empty | Storage paths per layer per environment |

## Directory Layout

```
./                                    # Scaffold parent directory
├── {workspace_name}/                 # Usually {workspace_name} after normalization
│   ├── AGENTS.md                     # Workspace-level DataCoolie operating contract
│   ├── config.yaml                   # Workspace control file (project, environments, artifact paths)
│   ├── discover/
│   ├── architecture/
│   │   └── amendments/
│   ├── metadata/
│   │   ├── connections.json          # Shared connection definitions
│   │   ├── schema_hints.json         # Shared schema hints
│   │   ├── dataflows/
│   │   │   ├── source2bronze.json
│   │   │   ├── bronze2silver.json
│   │   │   └── silver2gold.json
│   │   └── environments/
│   │       ├── dev.yaml              # Dev overlay (always created)
│   │       └── prod.yaml             # Prod overlay (if architecture specifies prod)
│   ├── project_management/
│   │   ├── status.md
│   │   ├── risks.md
│   │   ├── changelog.md
│   │   ├── decisions/
│   │   └── phases/
│   │       ├── architecture/
│   │       │   ├── scope.md
│   │       │   ├── notes.md
│   │       │   ├── evidence.md
│   │       │   └── gate-reviews/
│   │       ├── source2bronze/
│   │       │   ├── scope.md
│   │       │   ├── notes.md
│   │       │   ├── evidence.md
│   │       │   └── gate-reviews/
│   │       ├── bronze2silver/
│   │       │   ├── scope.md
│   │       │   ├── notes.md
│   │       │   ├── evidence.md
│   │       │   └── gate-reviews/
│   │       ├── silver2gold/
│   │       │   ├── scope.md
│   │       │   ├── notes.md
│   │       │   ├── evidence.md
│   │       │   └── gate-reviews/
│   │       └── production/
│   │           ├── scope.md
│   │           ├── notes.md
│   │           ├── evidence.md
│   │           └── gate-reviews/
│   ├── functions/
│   │   ├── __init__.py
│   │   ├── sources.py                # Custom source function stubs
│   │   └── pyproject.toml            # Package config
│   └── generated/                    # Derived artifacts; safe to regenerate
│       ├── dev/
│       │   └── metadata.json         # Merged runtime metadata for dev
│       ├── dist/                     # Built function artifacts
│       └── run_local.py              # Local runner generated from architecture
├── .gitignore
└── requirements.txt
```

## File Contents

### {workspace_name}/AGENTS.md

Copy from [ai/AGENTS.md](https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/AGENTS.md) if the file does not already exist. Do not overwrite an existing workspace guide.

### {workspace_name}/config.yaml

`config.yaml` is the workspace control file. It identifies the project, workspace folder,
valid environments, target platform per environment, and generated artifact locations.
It affects init, metadata merge defaults, runner generation, deploy target selection, and
promotion checks. It is not the source of truth for dataflow logic, runtime engine strategy,
gate status, or secrets.

**Minimal (no architecture):**

```yaml
schema_version: 1

project:
  name: {project_name}
  workspace_name: {workspace_name}

defaults:
  environment: dev
  metadata_layout: modular

artifacts:
  generated_dir: generated
  metadata_dir: metadata
  project_management_dir: project_management

environments:
  dev:
    platform: local
    paths:
      data_root: ./data
      logs: ./logs
    generated_metadata: generated/dev/metadata.json
```

**Architecture-aware (pre-populated from approved architecture):**

```yaml
schema_version: 1

project:
  name: {project_name}
  workspace_name: {workspace_name}

defaults:
  environment: dev
  metadata_layout: modular

artifacts:
  generated_dir: generated
  metadata_dir: metadata
  project_management_dir: project_management

environments:
  dev:
    platform: local
    paths:
      data_root: ./data
      logs: ./logs
    generated_metadata: generated/dev/metadata.json
  # --- Architecture-derived environments below ---
  # Example for Fabric:
  # test:
  #   platform: fabric
  #   generated_metadata: generated/test/metadata.json
  #   fabric:
  #     workspace: "{infra_test_workspace}"
  # prod:
  #   platform: fabric
  #   generated_metadata: generated/prod/metadata.json
  #   fabric:
  #     workspace: "{infra_prod_workspace}"
  #     lakehouse_bronze: "{infra_bronze_lakehouse}"
  #     lakehouse_silver: "{infra_silver_lakehouse}"
  #     warehouse_gold: "{infra_gold_warehouse}"
  #
  # Example for AWS:
  # prod:
  #   platform: aws
  #   generated_metadata: generated/prod/metadata.json
  #   aws:
  #     region: "{infra_aws_region}"
  #     bucket: "{infra_aws_bucket}"
  #     role_arn: "{infra_aws_role_arn}"
```

Infrastructure resource names come from the architecture's Infrastructure Requirements table.
Keep platform-specific values under the matching environment (`fabric`, `aws`, `databricks`,
or `local`). Store secret references in metadata overlays, environment variables, or the target
platform's secret manager; do not store secret values in `config.yaml`.

Engine strategy is intentionally not stored in `config.yaml`. Runtime engine choice can vary by
stage and environment, so it belongs in the architecture and generated runner/deploy artifacts.
Each selected engine must be `polars` or `spark`; there is no third engine value.

### {workspace_name}/metadata/connections.json

```json
[]
```

### {workspace_name}/metadata/schema_hints.json

```json
[]
```

### {workspace_name}/metadata/dataflows/source2bronze.json

```json
[]
```

### {workspace_name}/metadata/dataflows/bronze2silver.json

```json
[]
```

### {workspace_name}/metadata/dataflows/silver2gold.json

```json
[]
```

For tiny projects, a single `{workspace_name}/metadata/metadata.json` with `connections`,
`dataflows`, and `schema_hints` remains supported. The scaffold default uses modular stage files
so stage ownership and gate review stay clear.

### {workspace_name}/metadata/environments/dev.yaml

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

### {workspace_name}/metadata/environments/prod.yaml

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

### {workspace_name}/project_management/status.md

```markdown
# Project Status

- Current stage: architecture
- Last approved gate: none
- Current blocker: none
```

### {workspace_name}/project_management/risks.md

```markdown
# Risks

| Risk | Impact | Owner | Status |
|---|---|---|---|
```

### {workspace_name}/project_management/changelog.md

```markdown
# Changelog

## Initial scaffold

- Created DataCoolie workspace.
```

### {workspace_name}/project_management/phases/{phase}/scope.md

```markdown
# Scope

- Phase:
- Owner:
- In scope:
- Out of scope:
```

### {workspace_name}/project_management/phases/{phase}/notes.md

```markdown
# Notes
```

### {workspace_name}/project_management/phases/{phase}/evidence.md

```markdown
# Evidence

| Check | Result | Link |
|---|---|---|
```

### {workspace_name}/project_management/phases/{phase}/gate-reviews/YYMMDD_{phase}-review.md

```markdown
---
gate: source2bronze
status: pending
reviewer:
reviewed_at:
next_allowed: bronze2silver
---

# Source2Bronze Gate Review

## Evidence

- Schema check:
- Row count:
- Freshness:
- Reconciliation:
- Quality checks:
- Dev/test deploy:

## Decision

Pending.

## Notes

- 
```

### {workspace_name}/functions/__init__.py

```python
```

### {workspace_name}/functions/sources.py

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

### {workspace_name}/functions/pyproject.toml

```toml
[project]
name = "{project_name}-functions"
version = "0.1.0"
requires-python = ">=3.10"
```

### {workspace_name}/generated/

`generated/` contains derived outputs and can be recreated from `config.yaml`, metadata, and
architecture. Do not treat it as hand-authored source of truth.

Expected contents:

```text
{workspace_name}/generated/
  dev/
    metadata.json
  test/
    metadata.json
  prod/
    metadata.json
  dist/
    functions*.whl
  run_local.py
```

### {workspace_name}/generated/run_local.py

**For `runner_engine: polars` (default):**

```python
from datacoolie.core import DataCoolieRunConfig
from datacoolie.engines import PolarsEngine
from datacoolie.metadata import FileProvider
from datacoolie.orchestration import DataCoolieDriver
from datacoolie.platforms import LocalPlatform

platform = LocalPlatform()
engine   = PolarsEngine(platform=platform)
metadata = FileProvider(config_path="{workspace_name}/generated/dev/metadata.json", platform=platform)
config   = DataCoolieRunConfig(dry_run=False)

with DataCoolieDriver(engine=engine, metadata_provider=metadata, config=config) as driver:
    result = driver.run(stage="source2bronze", column_name_mode="lower")
    print(f"Rows read: {result.total_rows_read}, written: {result.total_rows_written}")
```

**For `runner_engine: spark`:**

```python
from datacoolie.core import DataCoolieRunConfig
from datacoolie.engines import SparkEngine
from datacoolie.metadata import FileProvider
from datacoolie.orchestration import DataCoolieDriver
from datacoolie.platforms import LocalPlatform

# If existing `spark` session is available in the architecture, use it; otherwise create a new local session
# from pyspark.sql import SparkSession
# spark    = SparkSession.builder.appName("{project_name}").getOrCreate()

platform = LocalPlatform()
engine   = SparkEngine(spark_session=spark, platform=platform)
metadata = FileProvider(config_path="{workspace_name}/generated/dev/metadata.json", platform=platform)
config   = DataCoolieRunConfig(dry_run=False)

with DataCoolieDriver(engine=engine, metadata_provider=metadata, config=config) as driver:
    result = driver.run(stage="source2bronze", column_name_mode="lower")
    print(f"Rows read: {result.total_rows_read}, written: {result.total_rows_written}")
```

The architecture may define an engine strategy matrix by environment and stage, but generated
runner scripts should be concrete. Resolve the target environment/stage first, then generate
either the `polars` variant or the `spark` variant above. Do not generate a runner that carries
the strategy matrix and chooses engines dynamically at runtime.

Run metadata merge before executing the generated runner:

```bash
python datacoolie/ai/skills/datacoolie-metadata/scripts/merge.py --base {workspace_name}/metadata --env dev
```

### .gitignore

```
__pycache__/
*.pyc
{workspace_name}/watermarks/
.env
*.egg-info/
```

### requirements.txt

```
datacoolie
```

### Alternative: {workspace_name}/metadata/metadata.json

```json
{
  "$schema": "https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/skills/datacoolie-metadata/schemas/0.1.0/metadata.schema.json",
  "connections": [],
  "dataflows": [],
  "schema_hints": []
}
```

Use this unified file only when the user chooses the small-project layout instead of the default modular scaffold.
`$schema` is recommended for unified metadata because it enables editor and CI validation.
Do not add `$schema` to modular array files such as `connections.json`; validate the merged
runtime metadata instead.

### {workspace_name}/metadata/environments/dev.yaml

```yaml
# Dev environment overlay
# Override connection settings for local development
```

### {workspace_name}/metadata/environments/prod.yaml

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

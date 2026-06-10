# DataCoolie Project Structure Template

Use these templates to create directories and files only when the current request needs them.
Do not create every listed directory during initial workspace bootstrap.

## Variables

| Variable | Source | Default | Description |
|----------|--------|---------|-------------|
| `{project_name}` | User input (always) | - | Project display/config name |
| `{workspace_name}` | Derived from project name | `{normalized_project_name}_dcws` | DataCoolie workspace folder name |
| `{runner_engine}` | Architecture Engine Strategy or user input | `polars` | Default concrete generated runner engine: polars or spark |
| `{engine_strategy}` | Architecture Engine Strategy matrix | dev stages default to polars | Optional env+stage matrix where each value is `polars` or `spark` |
| `{platform}` | Architecture Overview or user input | `local` | Target platform for prod |
| `{environments}` | Architecture Environment Differences | dev only | Environment names and platform values for config.yaml |
| `{infra_*}` | Architecture Infrastructure Requirements | empty | Resource names (workspace, lakehouse, etc.) |
| `{prod_*_path}` | Architecture Medallion Layers | empty | Storage paths per layer per environment |

## Directory Layout

Minimum bootstrap:

```text
./
└── {workspace_name}/
    ├── AGENTS.md
    ├── config.yaml
    └── project_management/
        └── status.md
```

Potential layout after phases run:

```
./                                    # Scaffold parent directory
├── {workspace_name}/                 # Created at bootstrap
│   ├── AGENTS.md                     # Workspace-level DataCoolie operating contract
│   ├── config.yaml                   # Workspace control file (project, environments, platforms)
│   ├── discover/                     # Created by datacoolie-discover
│   ├── architecture/                 # Created by datacoolie-architect
│   │   └── amendments/
│   ├── metadata/                     # Created by datacoolie-metadata
│   │   ├── connections.json          # Shared connection definitions
│   │   ├── schema_hints.json         # Shared schema hints
│   │   ├── dataflows/
│   │   │   └── {stage}.json         # Create only in-scope stage files
│   │   └── environments/
│   │       └── {env}.yaml            # Create only when env overrides are needed
│   ├── project_management/           # Created as phases need management artifacts
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
│   ├── functions/                    # Created only when custom Python functions are needed
│   │   ├── __init__.py
│   │   ├── sources.py                # Custom source function stubs
│   │   └── pyproject.toml            # Package config
│   ├── runners/                      # Created only for durable custom runners/notebooks
│   │   └── run_{platform}.{ext}
│   └── generated/                    # Created by metadata merge, runner generation, or deploy
│       ├── dev/
│       │   └── metadata.json         # Merged runtime metadata for dev
│       ├── dist/                     # Built function artifacts
│       └── run_{platform}.{ext}      # Derived runner output; safe to regenerate
├── .gitignore
└── requirements.txt
```

Create phase directories just in time. For example, source2bronze work creates only
`project_management/phases/source2bronze/`; it does not create bronze2silver, silver2gold, or
production phase folders until those phases are active.

## File Contents

### {workspace_name}/AGENTS.md

Copy from [ai/AGENTS.md](https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/AGENTS.md) if the file does not already exist. Do not overwrite an existing workspace guide.

### {workspace_name}/config.yaml

`config.yaml` is the workspace control file. It identifies only the project, workspace folder,
valid environments, and target platform per environment. It helps init/deploy select the active
workspace and platform. It is not the source of truth for workflow defaults, artifact directories,
dataflow logic, runtime engine strategy, environment-specific runtime paths, connection overrides,
generated metadata paths, gate status, or secrets.

**Minimal (no architecture):**

```yaml
schema_version: 1

project:
  name: {project_name}
  workspace_name: {workspace_name}

environments:
  dev:
    platform: local
```

**Architecture-aware (pre-populated from approved architecture):**

```yaml
schema_version: 1

project:
  name: {project_name}
  workspace_name: {workspace_name}

environments:
  dev:
    platform: local
  # --- Architecture-derived environments below ---
  # Example for Fabric:
  # test:
  #   platform: fabric
  # prod:
  #   platform: fabric
  #
  # Example for AWS:
  # prod:
  #   platform: aws
```

Infrastructure resource names come from the architecture's Infrastructure Requirements table and
provision outputs, not `config.yaml`. Put environment-specific connection settings, runtime paths,
secret references, workspace/lakehouse/catalog/bucket values, platform-specific overrides, and
generated metadata deployment locations in `metadata/environments/{env}.yaml`, runner/deploy
parameters, environment variables, or the target platform's secret manager. Do not store secret
values in `config.yaml`.

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

### {workspace_name}/metadata/dataflows/{stage}.json

Create one file per in-scope stage only. Examples: `source2bronze.json`,
`bronze2silver.json`, `silver2gold.json`, or nested files such as
`source2bronze/sap.json`.

```json
[]
```

For tiny projects, a single `{workspace_name}/metadata/metadata.json` with `connections`,
`dataflows`, and `schema_hints` remains supported. The scaffold default uses modular stage files
so stage ownership and gate review stay clear.

### {workspace_name}/metadata/environments/{env}.yaml

Create an environment overlay only when the environment needs connection, path, secret reference,
or platform-specific overrides.

```yaml
# Environment overlay — merged with base metadata at deploy time
# Override connection paths, secrets, and platform-specific settings

# connections:
#   - name: bronze_storage
#     configure:
#       base_path: "./data/bronze"
#
#   - name: silver_storage
#     configure:
#       base_path: "./data/silver"
```

### {workspace_name}/project_management/status.md

```markdown
---
artifact_type: project_status
project_name: "{project_name}"
workspace_name: "{workspace_name}"
status: active
current_phase: architecture
last_approved_gate: none
updated_at:
---

# Project Status

- Current stage: architecture
- Last approved gate: none
- Current blocker: none
```

### {workspace_name}/project_management/risks.md

```markdown
---
artifact_type: risk_register
project_name: "{project_name}"
workspace_name: "{workspace_name}"
status: active
updated_at:
---

# Risks

| Risk | Impact | Owner | Status |
|---|---|---|---|
```

### {workspace_name}/project_management/changelog.md

```markdown
---
artifact_type: changelog
project_name: "{project_name}"
workspace_name: "{workspace_name}"
status: active
updated_at:
---

# Changelog

## Initial scaffold

- Created DataCoolie workspace.
```

### {workspace_name}/project_management/decisions/YYMMDD_{decision}.md

```markdown
---
artifact_type: decision_record
project_name: "{project_name}"
workspace_name: "{workspace_name}"
decision_id:
status: proposed
scope:
decided_at:
owner:
---

# Decision

## Context

## Decision

## Alternatives Considered

## Consequences

## Verification

## Unresolved Questions
```

### {workspace_name}/project_management/phases/{phase}/scope.md

```markdown
---
artifact_type: phase_scope
project_name: "{project_name}"
workspace_name: "{workspace_name}"
phase: "{phase}"
status: draft
owner:
updated_at:
---

# Scope

- Phase:
- Owner:
- In scope:
- Out of scope:
```

### {workspace_name}/project_management/phases/{phase}/notes.md

```markdown
---
artifact_type: phase_notes
project_name: "{project_name}"
workspace_name: "{workspace_name}"
phase: "{phase}"
status: active
updated_at:
---

# Notes
```

### {workspace_name}/project_management/phases/{phase}/evidence.md

```markdown
---
artifact_type: phase_evidence
project_name: "{project_name}"
workspace_name: "{workspace_name}"
phase: "{phase}"
status: collecting
updated_at:
---

# Evidence

| Check | Result | Link |
|---|---|---|
```

### {workspace_name}/project_management/phases/{phase}/gate-reviews/YYMMDD_{phase}-review.md

Each gate review file records one decision. AI workflow must evaluate gate state
from the latest review in this folder: select by YAML frontmatter `reviewed_at`;
if `reviewed_at` is missing, use the lexicographically greatest filename. The
gate is approved only when that latest review has `status: approved`.
`project_management/status.md` is a summary, not the approval source of truth.

```markdown
---
artifact_type: gate_review
project_name: "{project_name}"
workspace_name: "{workspace_name}"
gate: source2bronze
phase: source2bronze
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
architecture. Do not treat it as hand-authored source of truth. Users may inspect, run, or
temporarily debug generated runners, but durable edits must move back to metadata, architecture,
templates, or `{workspace_name}/runners/`.

Possible contents:

```text
{workspace_name}/generated/
  {env}/
    metadata.json
  dist/
    functions*.whl
  run_{platform}.{ext}
```

### {workspace_name}/runners/

`runners/` contains optional hand-authored pipeline entrypoints that should survive regeneration.
Create it only when the project needs a custom runner or notebook. If a matching runner exists
under `runners/`, deploy/generation workflows should use it as the source and may copy or render it
into `generated/` for packaging. If no runner source exists, generate a default runner directly
from skill reference examples.

Possible contents:

```text
{workspace_name}/runners/
  run_local.py
  run_fabric.ipynb
  run_databricks.ipynb
  run_aws_glue.py
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

### {workspace_name}/metadata/environments/{env}.yaml

```yaml
# Environment overlay
# Create only when connection settings differ for this environment
```

## Runner Code Rules

- `DataCoolieRunConfig` fields: `dry_run`, `job_num`, `job_index`, `max_workers`, `stop_on_error`, `retry_count`, `retry_delay`, `allowed_function_prefixes` — NOT `engine`, `stage`, `metadata_path`
- `stage` is passed to `driver.run(stage=...)`, not to config
- `WatermarkManager` is auto-created when `watermark_manager=None` — don't import or construct it manually
- Always use `with DataCoolieDriver(...) as driver:` for proper log flushing on exit
- `driver.load_dataflows(stage=...)` to inspect without running; `driver.run(stage=...)` to execute
- `FileProvider(config_path=..., platform=...)` for file-based metadata (JSON/YAML/xlsx)

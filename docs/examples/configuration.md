---
title: Configuration examples
description: DataCoolie Driver construction, provider ownership, component roots, run attributes and logging configuration examples.
---

# Configuration examples

Configuration belongs at the boundary that owns it. A runner constructs the
engine, platform, provider and Driver session; metadata preparation resolves
queries and secrets before a reader is created.

The standalone provider fixture below is a local contract check. It requires
Python 3.11+ and the pinned provider profiles
`datacoolie[polars,metadata-db,source-api]==0.2.0`: Polars executes the optional
dataflow, `metadata-db` supplies SQLAlchemy for SQLite metadata, and
`source-api` supplies the HTTP client used by the loopback API provider. See
[provider installation](../guide/providers/index.md) and the
[versioned installation handoff](../guide/getting-started/installation.md)
before running it. If the handoff uses a preview/source wheel, apply
`[polars,metadata-db,source-api]` to that matching wheel; the installation
guide's generic `cli,polars-delta` profile supplies Polars but does not include
the SQLAlchemy and HTTPX dependencies required by this fixture. The other files
on this page are snippets or authoring references; they do not seed a production
metadata service.

## The three common path modes {#artifact-and-explicit-roots}

| Mode | Driver input | When to use |
|---|---|---|
| Artifact | `artifact_base_path=".../current/dev"` | A CLI-built environment contains `metadata/`; SQL may be artifact-relative. |
| Explicit file metadata | `metadata_base_path=".../metadata"` or a `FileProvider` | A standalone metadata folder is owned by the caller. |
| Non-file metadata | `DatabaseProvider` or `APIProvider` plus optional SQL roots | Metadata is remote/provider-owned; file-only watermark roots are not inferred for it. |

The artifact and explicit roots implementation is **configuration/provider_construction.py**
([source](source/configuration/provider_construction.py.md) ·
[raw](files/configuration/provider_construction.py)). The standalone
FileProvider variant is **configuration/standalone_file_provider.py**
([source](source/configuration/standalone_file_provider.py.md) ·
[raw](files/configuration/standalone_file_provider.py)). The [runtime
configuration guide](../guide/operations/runtime-configuration.md) is the normative
explanation of fallback and conflict rules.

## A minimal explicit provider {#provider-startup}

```python
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform

platform = LocalPlatform()
engine = PolarsEngine(platform=platform)
metadata = FileProvider(
    metadata_base_path="./metadata",
    platform=platform,
    sql_base_path=["./sql"],
)

metadata.initialize()
connections = metadata.get_connections(active_only=False)
dataflows = metadata.get_dataflows(active_only=False, attach_schema_hints=False)
try:
    with DataCoolieDriver(
        engine=engine,
        metadata_provider=metadata,
        state_base_path="./.runtime",
    ) as driver:
        result = driver.run(stage="bronze2silver")
finally:
    # The provider was injected, so the caller owns its lifecycle.
    metadata.close()
```

Constructing `FileProvider` without a platform is also supported when no I/O
occurs during construction; the Driver startup must bind a platform before
metadata is initialized. An injected provider remains caller-owned.

## Local Database and API contract fixture

**configuration/provider_fixtures.py**
([source](source/configuration/provider_fixtures.py.md) ·
[raw](files/configuration/provider_fixtures.py)) supplies a small orders
dataflow from an in-memory SQLite database or a temporary loopback HTTP
service. It is a local contract check for provider startup and Driver handoff,
not an application seeder or service template.

Run it from the DataCoolie package checkout directory that contains `docs/`.
The default command only hydrates metadata. The opt-in execution creates a new
work directory, writes a three-row Parquet input, and runs one
`bronze2silver` stage through both providers:

```powershell
python -m pip install "datacoolie[polars,metadata-db,source-api]==0.2.0"
python docs/examples/files/configuration/provider_fixtures.py --provider both
python docs/examples/files/configuration/provider_fixtures.py --provider both --run-dataflow --work-dir .scratch/provider-orders-both
```

Choose a work directory that does not exist yet; this command will not reuse
an existing path. The SQLite and API examples use the same local input and
output format, so the metadata backend is the meaningful change. If the
checkout is outside the framework repository, copy or download this standalone
fixture and install the same package profiles before running it; it is not an
extracted project archive.

The command reports `connections=2 dataflows=1` for each provider and exits
non-zero if the local stage does not succeed. It validates provider metadata
and the Driver handoff. With `--run-dataflow`, it reports
`executed=1 connections=2 dataflows=1` for both providers and writes each
provider's input, output and state below the new work directory. It does not
prove permissions or connectivity to a production source or destination. To
repeat the check from a clean state, choose another new work directory rather
than removing the first one.

## Run attributes and logging {#external-run-attributes}

`DataCoolieRunConfig.run_attributes` is a strict JSON object for external
correlation values such as a Data Factory pipeline run ID or a Glue job ID. It
is copied at construction and is recorded with the job runtime; it is not a
replacement for `job_id`, sharding fields or framework status.

```python
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.logging import LogConfig

config = DataCoolieRunConfig(
    run_attributes={"factory_pipeline_run_id": "pipeline-2026-09-15-001"},
)
log_config = LogConfig(
    output_path="./.runtime/logs",
    persistence_mode="snapshot",
)
```

### Logging modes {#logging-modes}

The complete logging example is **configuration/logging_modes.py**
([source](source/configuration/logging_modes.py.md) ·
[raw](files/configuration/logging_modes.py)). Snapshot mode replaces the
stable projection; batch mode emits JSON Lines records in `.json` files.

### Multiple SQL roots {#multiple-sql-roots}

The SQL-roots sample is **configuration/sql_roots.py**
([source](source/configuration/sql_roots.py.md) ·
[raw](files/configuration/sql_roots.py)). It shows how a relative query reference
is resolved through more than one explicit root. The framework does not reserve
a fixed `sql/` folder; provider roots own metadata-associated SQL files, while
Driver roots remain the session fallback.

Persisted records keep the original metadata query reference while runtime
records may include the actual SQL sent to the reader in
`source_action["query"]`. Console formatting is a presentation choice; it does
not change the structured log payload.

## Configuration source files

The catalog remains the discovery inventory. When the configuration sample is
known, use **configuration/provider_construction.py**
([source](source/configuration/provider_construction.py.md) ·
[raw](files/configuration/provider_construction.py)). Project-owned
configuration belongs to the project's `project-files` section in the catalog;
its complete checkout is the project's single `download` action (a `.zip`
archive).

Use [CLI project workflow](../guide/cli/project.md) for authoring and building
`datacoolie.yml`; do not copy Driver session settings into that project contract.

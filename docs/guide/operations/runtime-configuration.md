---
title: Runtime configuration and path ownership
description: Configure DataCoolie Driver sessions, providers, artifact and state roots, SQL files, logs, watermarks, replay and caller run attributes.
---

# Runtime configuration and path ownership

DataCoolie separates project preparation from execution. A project may be
built with the CLI, but a runner still supplies the runtime objects needed by a
Driver session. Keep each value at the boundary that owns its behavior.

## Configure the Driver

The public constructor accepts an engine, optional execution platform,
metadata provider, watermark manager, `DataCoolieRunConfig`, secret provider,
loggers/configuration, and component roots such as `artifact_base_path`,
`state_base_path`, `metadata_base_path`, `sql_base_path` and `log_base_path`.
`sql_base_path` may be declared on the metadata provider or offered by the
Driver as a session fallback.

Use the [Driver API](../../reference/api/orchestration.md#datacoolie.orchestration.driver.DataCoolieDriver)
for the constructor and the [runtime field reference](../../reference/runtime-configuration.md)
for exact session, replay and logging defaults. Pass a `DataCoolieRunConfig`
object with `config=`; the `create_driver` factory accepts the run fields
directly instead.

Use a provider when metadata comes from a database or API. If no provider is
given, `metadata_base_path` creates a FileProvider; if only
`artifact_base_path` is supplied, metadata defaults to `<artifact>/metadata`.
An explicit provider and a conflicting metadata root are configuration errors.
An explicit non-file provider may still be used with `artifact_base_path` for
SQL files; the artifact root does not silently replace that provider. The
Driver does not instantiate a DB/API provider from a path.

```python
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform

platform = LocalPlatform()
engine = PolarsEngine(platform=platform)

with DataCoolieDriver(
    engine=engine,
    metadata_provider=FileProvider(
        metadata_base_path="./metadata",
        platform=platform,
        sql_base_path=["./sql"],
    ),
    state_base_path="./.runtime",
) as driver:
    result = driver.run(stage="bronze2silver")
```

The FileProvider may be constructed without a platform, but platform-backed
I/O must be available before it is initialized. An injected provider remains
caller-owned; a provider inferred by the Driver follows the Driver lifecycle.

## Root ownership and fallback

| Concern | Owner | Fallback / rule |
|---|---|---|
| Metadata files | FileProvider | Explicit metadata root, or `<artifact>/metadata` when artifact mode creates the provider. |
| SQL files | Metadata provider plus Driver preparation | Provider `sql_base_path` is preferred; Driver `sql_base_path` is the session fallback. One root or several are accepted; `artifact:/...` is artifact-relative. |
| Logs | Logger configuration / Driver session | `log_base_path`, logger output path, or `<state_base_path>/logs`. |
| File watermarks | FileProvider | Explicit watermark root, then `<state>/watermarks`, then the parent of the effective log root plus `watermarks`. |
| DB/API watermarks | Provider/backend | Do not force file paths onto a non-file provider. |
| Runtime state | Driver caller | Prefer a project `.runtime` directory for local logs and watermarks. |

`log_base_path` does not need to end in `/logs`; watermark inference uses its
parent. Metadata-only reads can work without a state root, but watermark access
must have a valid provider-owned root and fails at preparation/use time when it
is required.

## Query preparation

`source.query` remains the original metadata value for metadata logging. Before
a reader is created, preparation resolves an inline SQL string or reads a SQL
file. A relative reference such as `sql/orders/incremental.sql` is resolved by
the provider's SQL roots when configured, or by the Driver session roots when
the provider has none; `artifact:/sql/orders/incremental.sql` is explicitly
artifact-relative. There is no fixed `sql/` folder in the framework.

When both provider and Driver roots are supplied, they must be equivalent after
normalization. The Driver does not write its fallback into the provider, and
the provider does not need the Driver platform just to retain the roots.

The execution log may include the actual SQL sent to the reader in
`source_action["query"]`, including generated filters. Preparation errors occur
before retries or business reads. `dry_run` can load metadata and resolve query
files, but does not read business data, write destinations or save watermarks.

## Run attributes and replay

`DataCoolieRunConfig.run_attributes` is a JSON-compatible mapping supplied by
the caller for external identifiers such as a data-factory pipeline run ID or a
Glue job ID. It is persisted once in the session JobRuntime summary; do not put
secrets there.

`ReplayConfig` is call-specific. It uses the same Driver paths and log config,
executes `[start, end)` chunks, and only changes watermarks when
`save_watermark=True`. Treat concurrent replay of a production incremental flow
as an operational conflict unless the destination and watermark policy make it
safe.

See [logging · Run attributes](../../reference/concepts/logging.md#run-attributes), [watermarks · Replay watermark behaviour](../../reference/concepts/watermarks.md#replay-watermark-behaviour)
and [replay & backfill](replay-and-backfill.md) for the corresponding
contracts and safety checks.

Secret lookup and automatic console color also read process environment values.
See the [environment-variable reference](../../reference/environment-variables.md)
for `env:` prefix expansion and the precedence of `NO_COLOR`, `TERM`, and
explicit color settings.

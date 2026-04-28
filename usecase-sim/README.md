# usecase-sim — DataCoolie Testbed & Scenarios

End-to-end integration testbed and executable demo for the `datacoolie` ETL framework.
Exercises every combination of `{polars, spark}` × `{file, database, api}` metadata
sources × `{local, aws}` storage platforms, plus lakehouse maintenance
(compact / cleanup). A companion `docker-compose` stack provides realistic
backends (Postgres/MySQL/MSSQL/Oracle, MinIO, Iceberg REST, Trino, a mock REST
API, and a metadata REST API).

> Examples use PowerShell syntax. Bash users: drop the leading `.\` on venv
> paths and swap `\` for `/` in file arguments. All commands assume the working
> directory is `datacoolie/` (repo: `datacoolie/datacoolie/`).

---

## 1. What is usecase-sim?

- **Library under test:** the `datacoolie` package in `datacoolie/src/datacoolie/`.
- **What it exercises:** 22 named scenarios that combine 2 engines, 3 metadata
  sources, 2 storage platforms, and lakehouse maintenance.
- **Why it exists:** one-command regression coverage for ETL behaviour across
  the supported matrix, plus a teaching surface for new contributors.

---

## 2. Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | ≥ 3.11 | Tested on 3.11.9 (Windows) |
| Virtualenv | — | Recommended at the active checkout root as `.venv` |
| `datacoolie` extras | `[polars,spark]` | Installed editable from repo root |
| Docker Desktop | optional | Only required for P1 / P2 scenarios |
| Java 17 | for Spark | Only required for `spark_*` scenarios |

Install into a fresh checkout-root venv:

```powershell
# From the checkout root
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .\datacoolie[polars,spark]
```

---

## 3. Directory map

```
usecase-sim/
├── scripts/       # Bootstrap + reset entrypoints (7 scripts + _common.py)
├── runner/        # ETL runners (run.py, maintenance.py) + dispatcher (run_scenario.py)
├── scenarios/     # scenarios.json (22 entries) + SCENARIOS.md (field reference)
├── metadata/
│   ├── file/      # JSON use-case files (local, aws, perf) + generated yaml/xlsx
│   ├── database/  # DDL per dialect + verify_metadata.py
│   └── api/       # Standalone dev metadata server (reads JSON)
├── docker/        # docker-compose.yml, mock_api_server.py, pg_api_metadata_server.py
├── functions/     # Custom Python source functions (sources.py)
├── data/          # Generated inputs/outputs (gitignored)
├── logs/          # Framework logs (system_logs/, etl_logs/) + scenarios/ console logs (gitignored)
└── platforms/
    ├── databricks/  # DEFERRED
    └── fabric/      # Prepared metadata + notebook samples (runner integration deferred)
```

---

## 4. Full start

The simplest reliable path: bring up the entire stack, seed every metadata
target, generate every input dataset, and run every scenario. Takes a few
minutes cold but has no "is service X up?" guesswork.

```powershell
# From the inner package directory
cd datacoolie

# 1. Start all 10 containers and wait for ports (≈ 60 s cold, 10 s warm)
python usecase-sim/scripts/setup_platform.py

# 2. Seed metadata into file + every DB dialect + api-db
python usecase-sim/scripts/setup_metadata.py `
    --targets file,db:sqlite,db:postgresql,db:mysql,db:mssql,db:oracle `
    --truncate

# 3. Generate 30-row sample inputs across every target
python usecase-sim/scripts/generate_data.py

# 4. Run every scenario
python usecase-sim/runner/run_scenario.py --all
```

**Expected result** (≈ a few minutes, depending on cold/warm JVM):

```
Total: 22 | PASS: 22 | FAIL: 0
```

Or, for a first sanity check after step 3:

```powershell
python usecase-sim/runner/run_scenario.py --scenario local_polars_file
# ETL complete — Success: 121, Failed: 0, Skipped: 0
# Total: 1 | PASS: 1 | FAIL: 0
```

Outputs land in `usecase-sim/data/output/` (Delta tables, Parquet, JSON, …)
and watermarks in `usecase-sim/metadata/file/watermarks/`.

Tear everything down:

```powershell
python usecase-sim/scripts/setup_platform.py --down --volumes
```

### Running only what you need

Once the full path works, you can trim. Minimum required services per scenario
tier:

| Tier | Services |
|---|---|
| P0 `*_file` (polars / spark) | `minio` + `iceberg-rest` (+ `mock-api` if not skipping API sources) |
| P1 database — SQLite | none |
| P1 database — `postgres` / `mysql` / `mssql` / `oracle` | the matching container |
| P1 api | `postgres` + `metadata-api` |
| P1 / P2 iceberg maintenance | `minio` + `iceberg-rest` |
| P2 `aws_*` | `minio` + `iceberg-rest` |

> The runner eagerly builds an Iceberg REST catalog for every Polars `*_file`
> scenario, so `iceberg-rest` and `minio` are hard requirements even when no
> Iceberg stage runs. If you want a smaller stack, target a different scenario
> family (database / api / maintenance).

> `setup_metadata.py` with no args defaults to `file,db:sqlite,db:postgresql`
> and therefore needs the `postgres` container. Narrow `--targets` when
> running it standalone against a smaller stack.

---

## 5. Running scenarios

```mermaid
flowchart LR
    A[scenarios.json] -->|--scenario / --priority| B[run_scenario.py]
    B -->|metadata_type == maintenance| C[maintenance.py]
    B -->|otherwise| D[run.py]
    D --> E[DataCoolieDriver.run]
    C --> F[DataCoolieDriver.run_maintenance]
```

### Invocation modes

```powershell
# Single scenario
python usecase-sim/runner/run_scenario.py --scenario local_polars_file

# All scenarios at a given priority
python usecase-sim/runner/run_scenario.py --priority P0

# Everything (long; P1/P2 need Docker)
python usecase-sim/runner/run_scenario.py --all
```

### Dispatcher behaviour

| Concern | Handling |
|---|---|
| Spark JVM cooldown | 6 s pause + stale `spark-warehouse/` + `metastore_db/` wipe between Spark scenarios |
| Timeouts | 300 s (spark), 180 s (polars), 600 s (maintenance); override per scenario via `"timeout_seconds"` |
| Exit code | `0` iff every scenario passed |

### Direct runner invocation (advanced)

When debugging one metadata row, bypass the dispatcher and call `run.py`
directly — all flags documented in section 7.

```powershell
python usecase-sim/runner/run.py `
    --engine polars --metadata-source file --platform local `
    --metadata-path usecase-sim/metadata/file/local_use_cases.json `
    --stage local_csv2parquet
```

---

## 6. Scenarios reference

Full field-level reference: [scenarios/SCENARIOS.md](scenarios/SCENARIOS.md).

### P0 — local filesystem I/O (still needs `minio` + `iceberg-rest`)

| Key | Engine | Source |
|---|---|---|
| `local_polars_file` | polars | file (JSON) |
| `local_polars_file_yaml` | polars | file (YAML) |
| `local_polars_file_excel` | polars | file (XLSX) |
| `local_spark_file` | spark | file (JSON) |

### P1 — needs Docker services

| Key | Engine | Source | Requires |
|---|---|---|---|
| `local_polars_database` | polars | database (SQLite) | — |
| `local_polars_database_postgres` | polars | database | postgres |
| `local_polars_database_mysql` | polars | database | mysql |
| `local_polars_database_mssql` | polars | database | mssql |
| `local_polars_database_oracle` | polars | database | oracle |
| `local_spark_database` | spark | database (SQLite) | — |
| `local_polars_api` | polars | api | metadata-api |
| `local_spark_api` | spark | api | metadata-api |
| `local_polars_delta_maintenance` | polars | maintenance | — |
| `local_polars_iceberg_maintenance` | polars | maintenance | minio + iceberg-rest |
| `local_spark_delta_maintenance` | spark | maintenance | — |
| `local_spark_iceberg_maintenance` | spark | maintenance | minio + iceberg-rest |

### P2 — AWS platform (MinIO + Iceberg REST)

| Key | Engine | Source | Requires |
|---|---|---|---|
| `aws_polars_file` | polars | file | minio + iceberg-rest |
| `aws_spark_file` | spark | file | minio + iceberg-rest |
| `aws_polars_delta_maintenance` | polars | maintenance | minio |
| `aws_polars_iceberg_maintenance` | polars | maintenance | minio + iceberg-rest |
| `aws_spark_delta_maintenance` | spark | maintenance | minio |
| `aws_spark_iceberg_maintenance` | spark | maintenance | minio + iceberg-rest |

> All four P0 scenarios require `minio` + `iceberg-rest` because the Polars
> runner eagerly builds an Iceberg REST catalog. `local_polars_file` and
> `local_polars_file_yaml` additionally need `mock-api` on port 8082 for their
> API-source dataflows; the `_excel` and `_spark_file` variants set
> `skip_api_sources: true` and therefore avoid that dependency.

---

## 7. The runners

Two runner scripts replace what used to be eight. They are thin shells over
`DataCoolieDriver` from the library.

### `runner/run.py`

Unified ETL runner. Dispatches any `(engine × metadata-source)` combination.

| Flag | Req | Default | Purpose |
|---|---|---|---|
| `--engine` | ✓ | — | `polars` \| `spark` |
| `--metadata-source` | ✓ | — | `file` \| `database` \| `api` |
| `--platform` | | `local` | `local` \| `aws` (chooses `LocalPlatform` or `AWSPlatform`) |
| `--metadata-path` | file | — | `.json` \| `.yaml` \| `.xlsx` |
| `--metadata-db-connection-string` | db | — | SQLAlchemy URL |
| `--metadata-api-url` | api | — | Base URL of metadata API |
| `--metadata-api-key` | | `""` | Optional API key |
| `--metadata-workspace-id` | db/api | — | Workspace ID |
| `--stage` | ✓ | — | Stage name(s); `""` runs all stages |
| `--column-name-mode` | | `lower` | `lower` \| `snake` |
| `--dry-run` | | off | Skip writes |
| `--storage-options KEY=VALUE` | | `[]` | Repeatable; passed to Polars / object store |
| `--iceberg-catalog-uri` | | `None` | Override Iceberg REST URI |
| `--catalog-preset` | | `local` | `local` \| `unity_catalog` |
| `--uc-token` / `--uc-credential` | | `""` | Unity Catalog auth |
| `--log-path` | | `None` | Directory for framework logs; driver writes `system_logs/` and `etl_logs/` under it |
| `--max-workers` | | `None` | Parallel dataflow workers |
| `--skip-api-sources` | | off | Skip dataflows with `connection_type=api` |
| `--app-name` | spark | `DataCoolie-UseCase` | Spark app name |
| `--spark-config KEY=VALUE` | spark | `[]` | Extra Spark configs |

### `runner/maintenance.py`

Compact + cleanup for Delta and Iceberg tables.

| Flag | Req | Default | Purpose |
|---|---|---|---|
| `--engine` | ✓ | — | `polars` \| `spark` |
| `--platform` | | `local` | `local` \| `aws` |
| `--metadata-path` | ✓ | — | Path to metadata file |
| `--connection` | | `None` | Filter to a single connection name |
| `--do-compact` / `--no-compact` | | on | Enable/disable compaction |
| `--do-cleanup` / `--no-cleanup` | | on | Enable/disable cleanup |
| `--retention-hours` | | `168` | File retention window |
| `--dry-run` | | off | Dry-run mode |
| `--storage-options KEY=VALUE` | | `[]` | Repeatable |
| `--catalog-preset` | | `local` | `local` \| `unity_catalog` |
| `--iceberg-catalog-uri` | | `None` | Override Iceberg REST URI |
| `--uc-token` / `--uc-credential` | | `""` | Unity Catalog auth |
| `--log-path` | | `None` | Directory for framework logs (same layout as `run.py`) |
| `--skip-api-sources` | | off | Skip api-source dataflows |
| `--app-name` | spark | `DataCoolie-Maintenance` | Spark app name |
| `--spark-config KEY=VALUE` | spark | `[]` | Extra Spark configs |

### `runner/run_scenario.py`

| Flag | Default | Purpose |
|---|---|---|
| `--scenario NAME` | — | Single scenario key |
| `--all` | off | Run every scenario |
| `--priority P0\|P1\|P2` | — | Run every scenario at a priority tier |
| `--scenarios-path PATH` | `scenarios/scenarios.json` | Override scenarios file |

The dispatcher writes three kinds of logs under `usecase-sim/logs/`:

- `logs/system_logs/` and `logs/etl_logs/` — driver output (forwarded via `--log-path`).
- `logs/scenarios/run_scenario.log` — dispatcher's own log (which scenarios ran, commands, pass/fail summary).
- `logs/scenarios/<name>.console.log` — full stdout+stderr tee of each scenario's child process (also streamed live to the terminal).

### `runner/_runner_utils.py`

Shared factory module. Key exports:

- `build_spark_session(...)` — picks Scala 2.12 / 2.13, injects Delta + Iceberg + S3A + JDBC drivers.
- `build_iceberg_rest_catalog(...)` — builds a `pyiceberg` REST catalog for Polars.
- `setup_platform(is_aws, storage_opts, logger)` — returns `LocalPlatform` or `AWSPlatform`.
- `run_and_report(driver, stage, ...)` — runs the driver, logs the result, optionally stops Spark.

Three catalog presets are supported:

| Preset | `--iceberg-catalog-uri` default | Auth |
|---|---|---|
| `local` | `http://localhost:8181` (tabulario/iceberg-rest) | none |
| `unity_catalog` (OSS) | `http://<host>:8080/api/2.1/unity-catalog/iceberg` | `--uc-credential client_id:secret` |
| `unity_catalog` (Databricks) | `https://<ws>.azuredatabricks.net/api/2.1/unity-catalog/iceberg` | `--uc-token <pat>` |

---

## 8. Bootstrap scripts

All scripts live in [`scripts/`](scripts/) and import shared helpers from
`scripts/_common.py`. Run from the `datacoolie/` directory.

### `setup_platform.py` — start / stop Docker stack

```powershell
python usecase-sim/scripts/setup_platform.py                         # up all, wait for ports
python usecase-sim/scripts/setup_platform.py --services postgres minio
python usecase-sim/scripts/setup_platform.py --down --volumes        # tear down + wipe data
```

| Flag | Default | Purpose |
|---|---|---|
| `--down` | off | Stop the stack (`docker compose down`) |
| `--volumes` | off | With `--down`, also remove named volumes |
| `--services s1 s2 …` | all | Bring up/down a subset |
| `--timeout SECONDS` | `180` | Port-readiness poll timeout |
| `--no-wait` | off | Skip readiness polling |

### `setup_metadata.py` — fan-out use-cases to targets

```powershell
# Default: file + db:sqlite + db:postgresql (local workspace)
python usecase-sim/scripts/setup_metadata.py

# Seed every dialect, truncating first
python usecase-sim/scripts/setup_metadata.py `
    --targets file,db:postgresql,db:mysql,db:mssql,db:oracle --truncate

# AWS workspace into postgres
python usecase-sim/scripts/setup_metadata.py `
    --json usecase-sim/metadata/file/aws_use_cases.json `
    --workspace-id aws-workspace --targets db:postgresql --truncate
```

| Flag | Default | Purpose |
|---|---|---|
| `--json PATH` | `local_use_cases.json` | Source JSON file |
| `--workspace-id STR` | `local-workspace` | Workspace ID written into DB rows |
| `--targets LIST` | `file,db:sqlite,db:postgresql` | Comma-separated targets |
| `--truncate` | off | Truncate tables before seeding |
| `--db-url DIALECT=URL` | `[]` | Override the default SQLAlchemy URL per dialect |

Targets: `file`, `db:sqlite`, `db:postgresql`, `db:mysql`, `db:mssql`,
`db:oracle`, `api-db` (alias for `db:postgresql`).

### `generate_data.py` — write sample inputs

30-row sample dataset into every requested target. Replaces the old
`generate_sample_data.py` + `bootstrap_{postgres,mysql,mssql,oracle,minio}.py`.

| Flag | Default | Purpose |
|---|---|---|
| `--targets LIST` | all reachable | `local`, `minio`, `pg`, `mysql`, `mssql`, `oracle` |

```powershell
python usecase-sim/scripts/generate_data.py --targets local,minio
python usecase-sim/scripts/generate_data.py --targets pg,mysql
```

### `reset_watermarks.py` — delete watermarks only

| Flag | Default | Purpose |
|---|---|---|
| `--dry-run` | off | Report without deleting |
| `--skip-local` / `--skip-minio` / `--skip-db` | off | Skip individual stores |
| `--dialects D1 D2 …` | all 5 | DB dialects to target |
| `--db-url DIALECT=URL` | `[]` | Override SQLAlchemy URL |

### `reset_data.py` — full reset

Wipes `data/output/`, drops MinIO output + iceberg-warehouse prefixes, drops
the Iceberg `default` namespace, and calls `reset_watermarks.py`.

| Flag | Default | Purpose |
|---|---|---|
| `--dry-run` | off | Report without deleting |
| `--local-only` | off | Skip MinIO + Iceberg + DB cleanup |
| `--dialects D1 D2 …` | all 5 | DB dialects for watermark cleanup |

### `generate_perf_data.py` — perf benchmark inputs

| Flag | Default | Purpose |
|---|---|---|
| `--sizes LIST` | all 8 | `10k,50k,100k,500k,1m,5m,10m,50m` |
| `--formats LIST` | all | `jsonl,parquet,delta,iceberg` |
| `--targets LIST` | all | `local,minio,iceberg` |
| `--iceberg-uri URL` | `http://localhost:8181` | Iceberg REST URI |

Notes:

- JSONL inputs are only generated through `1m` rows. The script skips JSONL for `5m`, `10m`, and `50m`.
- `minio` is skipped automatically when `localhost:9000` is unavailable.
- `iceberg` requires the local REST catalog and is skipped when the catalog is unavailable.

### `reset_perf_data.py` — reset perf artifacts

| Flag | Default | Purpose |
|---|---|---|
| `--all` | off | Also reset inputs (`data/perf/input/` + `perf_src` namespace) |
| `--dry-run` | off | Report without deleting |

---

## 9. Docker stack (P1 / P2)

`usecase-sim/docker/docker-compose.yml` brings up a 10-service stack. All
credentials are hardcoded — intended for local dev only.

| Service | Port(s) | Credentials | Purpose |
|---|---|---|---|
| `postgres` | 5432 | `datacoolie/datacoolie` | Metadata DB + Iceberg JDBC catalog |
| `mysql` | 3306 | `datacoolie/datacoolie` | Metadata DB variant |
| `mssql` | 1433 | `sa / Datacoolie@1` | Metadata DB variant |
| `oracle` | 1521 | `datacoolie/datacoolie`, service `FREEPDB1` | Metadata DB variant |
| `minio` | 9000 / 9001 | `minioadmin/minioadmin` | S3-compatible object store |
| `iceberg-rest` | 8181 | — | Tabular Iceberg REST catalog |
| `trino` | 8080 | — | SQL engine over Iceberg |
| `mock-api` | 8082 | env-configured | Simulates REST API sources |
| `metadata-api` | 8000 | via `DATABASE_URL` | Flask API over postgres metadata |
| `sqlpad` | 3000 | `admin@datacoolie.local / admin` | Web SQL editor |

### UI endpoints

- MinIO console: <http://localhost:9001>
- Trino UI: <http://localhost:8080>
- SQLPad: <http://localhost:3000>
- Mock REST API: <http://localhost:8082>
- Metadata REST API: <http://localhost:8000>

### Bringing up only what you need

```powershell
# P1 database scenario on postgres only
python usecase-sim/scripts/setup_platform.py --services postgres

# P2 AWS iceberg scenario
python usecase-sim/scripts/setup_platform.py --services minio iceberg-rest

# P1 metadata-api scenario (postgres + api server)
python usecase-sim/scripts/setup_platform.py --services postgres metadata-api
```

### MSSQL URL encoding

The SQL Server password `Datacoolie@1` must be URL-encoded in SQLAlchemy URLs
as `Datacoolie%401`. The default URL in `scenarios.json` already handles this.

---

## 10. Metadata sources

Three interchangeable ways of providing the same connection + dataflow + schema
hint definitions.

### File

Primary sources: [local_use_cases.json](metadata/file/local_use_cases.json),
[aws_use_cases.json](metadata/file/aws_use_cases.json),
[perf_test.json](metadata/file/perf_test.json). Running
`setup_metadata.py --targets file` emits YAML and XLSX siblings used by the
`*_yaml` / `*_excel` scenarios.

### Database

Schema files per dialect: `metadata/database/schema.sql` (SQLite),
`schema_postgres.sql`, `schema_mysql.sql`, `schema_mssql.sql`,
`schema_oracle.sql`. All create the same five `dc_framework_*` tables.
Seeded by `setup_metadata.py --targets db:<dialect>`.

Verify DB matches JSON source:

```powershell
python usecase-sim/metadata/database/verify_metadata.py `
    --json-path usecase-sim/metadata/file/local_use_cases.json `
    --connection-string "postgresql+psycopg2://datacoolie:datacoolie@localhost:5432/datacoolie" `
    --workspace-id local-workspace
```

### REST API

- **Containerized (recommended):** `docker/pg_api_metadata_server.py`, running
  in the `metadata-api` service on port 8000. Reads from postgres.
- **Standalone dev:** `metadata/api/api_metadata_server.py` — reads a JSON file
  directly; useful when you don't want Docker.

Both serve the same REST contract consumed by `APIClient`.

---

## 11. Custom Python sources

[`functions/sources.py`](functions/sources.py) hosts Python functions that are
callable from metadata via `source.python_function` dotted paths. Current
function:

- `sql_query_orders(engine, source, watermark)` — queries Delta tables via SQL;
  registers tables for Polars, queries the metastore for Spark. Supports Local,
  AWS, Fabric, and Databricks platforms via `base_path` lookups.

To add your own, define a function in `functions/sources.py` and reference it
from the metadata JSON:

```json
{
  "source": {
    "type": "python_function",
    "python_function": "sources.my_custom_reader"
  }
}
```

---

## 12. Maintenance runs

Maintenance scenarios call `DataCoolieDriver.run_maintenance(...)` to compact
(optimize file layout) and cleanup (vacuum expired files) Delta and Iceberg
tables listed in the metadata.

```powershell
python usecase-sim/runner/run_scenario.py --scenario local_polars_delta_maintenance
python usecase-sim/runner/run_scenario.py --scenario local_spark_iceberg_maintenance
```

Or directly:

```powershell
python usecase-sim/runner/maintenance.py `
    --engine polars --platform local `
    --metadata-path usecase-sim/metadata/file/local_use_cases.json `
    --connection local_delta_dest `
    --retention-hours 168
```

**Known sharp edge.** Two maintenance dataflows writing to the same physical
Delta path (e.g. both pointing at `orders_schema_evolve`) can race and surface
as *"concurrent transaction deleted the same data your transaction deletes"*.
This is a concurrency property of the lakehouse format, not a usecase-sim
bug — serialise such jobs or point them at distinct paths.

---

## 13. Perf benchmarks

```powershell
# Generate full benchmark inputs.
# JSONL is only generated through 1m; parquet/delta/iceberg go through 50m.
python usecase-sim/scripts/generate_perf_data.py

# Run each engine from a clean output state for a fair comparison.
python usecase-sim/runner/run_perf_benchmark.py --engine polars --max-size 50m --reset
python usecase-sim/runner/run_perf_benchmark.py --engine spark  --max-size 50m --reset

# Regenerate the final merged report from the two JSON result files.
python usecase-sim/runner/run_perf_benchmark.py --report-only
```

Notes:

- Run all benchmark commands from the inner `datacoolie/` directory.
- `--reset` calls `usecase-sim/scripts/reset_perf_data.py` and clears perf outputs only. It does not delete generated inputs or `benchmark_results/`.
- Each engine run writes one JSON file to `./benchmark_results/` and also regenerates `perf_report.md` from whatever result files already exist.
- `--report-only` is the clean way to rebuild the final comparison after both engine runs finish.
- If you want to regenerate inputs as well, use `python usecase-sim/scripts/reset_perf_data.py --all` before `generate_perf_data.py`.
- If Docker-backed services are unavailable, use `--no-iceberg` and keep `--max-size 1m` because JSONL inputs stop at `1m`.

---

## 14. Troubleshooting

| Symptom | Fix |
|---|---|
| `ERROR DerbyLockFile` on second Spark run | `run_scenario.py` auto-wipes `spark-warehouse/` + `metastore_db/`; if running `run.py` directly, delete them between runs |
| `BucketAlreadyOwnedByYou` on MinIO | Benign; `_common.ensure_bucket` is idempotent |
| `psycopg2` / `pymssql` / `oracledb` import error | Install the corresponding extra: `pip install psycopg2-binary pymssql oracledb` |
| MSSQL auth fails with `Datacoolie@1` | Use URL-encoded form `Datacoolie%401` in SQLAlchemy URLs |
| MSSQL `Login failed for user 'sa'` with state 38 | The `datacoolie` user database is missing — the MSSQL image has no auto-create env var. `setup_platform.py` creates it after the container is up; to do it manually: `docker exec datacoolie-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Datacoolie@1' -No -Q "IF DB_ID('datacoolie') IS NULL CREATE DATABASE datacoolie;"` |
| API-source dataflows fail in `local_polars_file` | `mock-api` container is down; either `setup_platform.py --services mock-api` or use `--skip-api-sources` |
| Iceberg scenarios fail with connection refused on `:8181` | `iceberg-rest` container is down: `setup_platform.py --services iceberg-rest` |
| `json.decoder.JSONDecodeError` loading scenarios.json | File likely corrupted; reseed from git and reapply edits |

---

## 15. Deferred platforms

Databricks runner integration is planned but not implemented in this testbed.
Fabric runner integration is also deferred, but prepared Fabric assets are now
available in this repository.

Fabric assets:

- [`platforms/fabric/README.md`](platforms/fabric/README.md)
- [`platforms/fabric/fabric_use_cases.json`](platforms/fabric/fabric_use_cases.json)
- [`platforms/fabric/sample_fabric_spark.ipynb`](platforms/fabric/sample_fabric_spark.ipynb)
- [`platforms/fabric/sample_fabric_polars.ipynb`](platforms/fabric/sample_fabric_polars.ipynb)

Deferred status details:

- [`platforms/databricks/DEFERRED.md`](platforms/databricks/DEFERRED.md)
- [`platforms/fabric/DEFERRED.md`](platforms/fabric/DEFERRED.md)

---

## License

AGPL-3.0-or-later — same as the parent `datacoolie` package. See [../LICENSE](../LICENSE).

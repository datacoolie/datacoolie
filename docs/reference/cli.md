# CLI

DataCoolie ships as a library — there is no `datacoolie` console script. The
canonical runner scripts live in the `usecase-sim` testbed and are the
template you should copy into your own repo.

## Runners (from `usecase-sim/runner/`)

| Script | Purpose |
|---|---|
| `run.py` | Unified ETL runner for `engine × metadata-source × platform` combinations. Wraps `DataCoolieDriver.run`. |
| `maintenance.py` | Run `OPTIMIZE` / `VACUUM` via `DataCoolieDriver.run_maintenance`. |
| `run_scenario.py` | Dispatches to `run.py` or `maintenance.py` based on `scenarios.json`. |
| `run_perf_benchmark.py` | Generates a perf report comparing engines. |


## `run.py`

Required flags:

| Flag | Values | Notes |
|---|---|---|
| `--engine` | `polars` · `spark` | required |
| `--metadata-source` | `file` · `database` · `api` | required |
| `--stage` | string or comma-list | required; pass `""` to run all loaded dataflows |

Source-specific flags:

| Metadata source | Required flags |
|---|---|
| `file` | `--metadata-path` |
| `database` | `--metadata-db-connection-string` + `--metadata-workspace-id` |
| `api` | `--metadata-api-url` + `--metadata-workspace-id` |

Common optional flags include `--platform`, `--column-name-mode`, `--dry-run`,
`--storage-options KEY=VALUE`, `--log-path`, `--max-workers`,
`--skip-api-sources`, `--catalog-preset`, `--iceberg-catalog-uri`,
`--uc-token`, and `--uc-credential`.

Spark-only optional flags: `--app-name` and repeatable `--spark-config KEY=VALUE`.

Typical invocation:

```powershell
# From datacoolie/
python usecase-sim/runner/run.py `
    --engine polars `
    --metadata-source file `
    --metadata-path usecase-sim/metadata/file/orders_csv_to_parquet_full_load.json `
    --stage ingest2bronze
```

## `maintenance.py`

Required flags:

| Flag | Values | Notes |
|---|---|---|
| `--engine` | `polars` · `spark` | required |
| `--metadata-path` | file path | required |

Common optional flags include `--platform`, `--connection`, `--retention-hours`,
`--dry-run`, `--storage-options KEY=VALUE`, `--log-path`,
`--skip-api-sources`, `--catalog-preset`, `--iceberg-catalog-uri`,
`--uc-token`, and `--uc-credential`.

Maintenance behavior toggles:

- `--no-compact` disables compaction.
- `--no-cleanup` disables cleanup.

Spark-only optional flags: `--app-name` and repeatable `--spark-config KEY=VALUE`.

Typical invocation:

```powershell
python usecase-sim/runner/maintenance.py `
    --engine polars `
    --metadata-path usecase-sim/metadata/file/local_use_cases.json `
    --connection local_bronze `
    --retention-hours 168
```

## `run_scenario.py`

Dispatches named scenarios from `usecase-sim/scenarios/scenarios.json`.

Selection flags are mutually exclusive:

- `--scenario <name>`
- `--all`
- `--priority P0|P1|P2`

Optional flag: `--scenarios-path` to point at a different scenario catalog.

## `run_perf_benchmark.py`

Performance benchmark runner for the large perf metadata set.

Key flags:

- `--engine polars|spark` is required unless you use `--report-only`.
- `--metadata-path` defaults to the perf metadata file.
- `--stages` accepts a comma-separated stage list.
- `--max-size` caps the largest dataset size.
- `--output-dir` chooses where JSON results and the markdown report are written.
- `--reset`, `--report-only`, and `--no-iceberg` control benchmark behavior.

## Scripts (one-shot helpers)

Under `usecase-sim/scripts/`:

| Script | Purpose |
|---|---|
| `setup_platform.py` | Bring up / down the Docker stack. |
| `setup_metadata.py` | Seed metadata into file, DB dialects, and the API. |
| `generate_data.py` | Produce sample inputs. |
| `generate_perf_data.py` | Larger dataset for benchmarks. |
| `reset_data.py`, `reset_perf_data.py`, `reset_watermarks.py` | Clean slate between runs. |

See the [usecase-sim README](https://github.com/datacoolie/datacoolie/blob/main/datacoolie/usecase-sim/README.md)
for the full set and invocation examples.

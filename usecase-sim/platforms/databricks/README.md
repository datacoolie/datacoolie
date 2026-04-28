# Databricks Assets for usecase-sim

Prepared Databricks assets for file + delta scenarios.

This folder mirrors the Fabric notebook workflow, but targets Databricks
(Unity Catalog Volumes as primary path style) and uses file metadata.

Database metadata is intentionally out of scope for this asset set.

## What is in this folder

- `databricks_use_cases.json`:
  - Derived from `metadata/file/local_use_cases.json`.
  - Constrained to file + delta scenarios only.
  - Includes stages: `read_file`, `write_file`, `read_lakehouse`, `load_delta`.
  - Paths rewritten to Databricks UC Volume style.
- `sample_databricks_spark.ipynb`:
  - Primary sample for Databricks using `SparkEngine`.
  - Uses `DataCoolieRunConfig(max_workers=8)`.
- `sample_databricks_polars.ipynb`:
  - Secondary sample using `PolarsEngine`.
  - Uses `DataCoolieRunConfig(max_workers=4)`.
- `sample_databricks_secrets.ipynb`:
  - Separate secret-scope validation notebook.
  - Tests `DatabricksPlatform` + `secrets_ref` resolution without printing raw secret values.

## Path convention used

Default root in notebooks:

- `/Volumes/workspace/default/datacoolie_sim`

Subfolders expected by metadata:

- Input files:
  - `/Volumes/workspace/default/datacoolie_sim/input/csv`
  - `/Volumes/workspace/default/datacoolie_sim/input/json`
  - `/Volumes/workspace/default/datacoolie_sim/input/jsonl`
  - `/Volumes/workspace/default/datacoolie_sim/input/parquet`
  - `/Volumes/workspace/default/datacoolie_sim/input/avro`
  - `/Volumes/workspace/default/datacoolie_sim/input/excel`
  - `/Volumes/workspace/default/datacoolie_sim/input/parquet_dated`
  - `/Volumes/workspace/default/datacoolie_sim/input/parquet_hive`
- File outputs:
  - `/Volumes/workspace/default/datacoolie_sim/output/...`
- Delta tables (Unity Catalog):
  - ``workspace.default.<table>`` (no schema)
- Metadata file:
  - `/Volumes/workspace/default/datacoolie_sim/metadata/databricks_use_cases.json`
- Logs:
  - `/Volumes/workspace/default/datacoolie_sim/logs/datacoolie`

## Prerequisites

1. Databricks workspace with a running cluster.
2. Databricks Runtime compatible with your DataCoolie version.
3. `datacoolie` installed (the notebooks include `%pip install`).
4. UC Volume write/read permissions for your notebook principal.
5. Input files uploaded from local repo folder:
   - `usecase-sim/data/input/*`
6. `databricks_use_cases.json` uploaded to the metadata path above.

## Quick start (Spark notebook, recommended)

1. Upload input folders from `usecase-sim/data/input/` into the UC Volume paths.
2. Upload `databricks_use_cases.json` to `/Volumes/workspace/default/datacoolie_sim/metadata/`.
3. Open `sample_databricks_spark.ipynb`.
4. Update `VOLUME_ROOT` if your volume path is different.
5. Run with `STAGE = "read_file,load_delta"` first.
6. Then set `STAGE = ""` to run all file + delta scenarios in this metadata file.

Expected outcome:

- File inputs are read from `/Volumes/.../input/...`.
- File outputs are written under `/Volumes/.../output/...`.
- Delta outputs are written as Unity Catalog tables under ``workspace.default``.

## Polars sample

Run `sample_databricks_polars.ipynb` after Spark succeeds.

This notebook uses the same metadata and path structure.

> **Known limitations for PolarsEngine on Databricks:**
>
> - The Polars notebook currently works with **file-format connections only**
>   (both source and destination). File stages (`read_file`, `write_file`,
>   `read_file`, `write_file`) operate correctly against UC Volume paths.
> - **Delta format is not supported** via `/Volumes` paths or Unity Catalog
>   table references with PolarsEngine. Polars reads/writes delta via a
>   direct cloud storage URI (e.g. `abfss://container@account.dfs.core.windows.net/...`
>   or `s3://bucket/...`). Mounting a UC Volume path through Polars delta
>   is not equivalent and will fail.
> - To test delta scenarios with Polars, replace `base_path` in the delta
>   connections inside `databricks_use_cases.json` with the underlying
>   cloud storage URI of the volume location.

## Secret scope validation

Use `sample_databricks_secrets.ipynb` to validate Databricks scope-based
secret retrieval independently of file/delta ETL.

Suggested setup:

1. Create scope: `datacoolie-scope`
2. Add key: `sample-db-password`
3. Grant notebook identity read permission to that scope
4. Run the notebook and confirm success output

## Scope boundary

- Implemented here:
  - Databricks file metadata + notebook execution assets
  - Spark and Polars notebook samples
  - Separate Databricks secret-scope validation notebook
- Still deferred:
  - usecase-sim runner wiring (`run.py`, `run_scenario.py`) for Databricks
  - scenario catalog integration for Databricks profile


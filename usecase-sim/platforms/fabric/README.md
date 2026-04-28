# Fabric Assets for usecase-sim

Prepared Fabric assets for file + delta scenarios.

This folder provides metadata and notebook code samples you can use in a
Microsoft Fabric environment. Full usecase-sim runner integration is still
deferred, so these assets are intended for direct notebook execution.

## What is in this folder

- `fabric_use_cases.json`:
  - Near-full clone of `metadata/file/local_use_cases.json`.
  - Constrained to file + delta flows only.
  - Rewritten to OneLake ABFSS paths.
- `sample_fabric_spark.ipynb`:
  - Primary sample for Fabric notebooks using `SparkEngine`.
- `sample_fabric_polars.ipynb`:
  - Secondary sample using `PolarsEngine` with `FabricPlatform`.

## OneLake path convention used

- Files root:
  - `abfss://DataCoolie_DEV_WS@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files`
- Delta tables root:
  - `abfss://DataCoolie_DEV_WS@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Tables`

The metadata sets delta `schema_name` to `demo`, so table paths resolve as:

- `.../Tables/demo/<table>`

## Prerequisites

1. Fabric workspace + lakehouse available.
2. Notebook kernel has DataCoolie installed (`datacoolie[fabric]` preferred).
3. Input files uploaded under the lakehouse Files input folders used in the
   metadata (for example `Files/input/csv`, `Files/input/parquet`, etc.).
4. Metadata file available to the notebook path you configure.

## Quick start (Spark sample, recommended)

1. Open a Fabric notebook.
2. Upload or copy `fabric_use_cases.json` to a notebook-accessible path.
3. Open `sample_fabric_spark.ipynb`.
4. Attach a Lakehouse to the notebook.
5. Set `METADATA_PATH` in the script.
6. Run with `STAGE = "read_file,load_delta"` first.

Expected outcome:

- File inputs are read from `Files/input/...`.
- Delta outputs are written under `Tables/demo/...`.

## Polars sample note

The Polars sample is provided for parity. Spark is the primary path in Fabric
notebooks. Depending on workspace packaging and ABFSS credential behavior,
Polars delta operations may require extra `storage_options`.

## Scope boundary

- Prepared here:
  - Fabric metadata and notebook samples.
- Still deferred in usecase-sim runner stack:
  - `run.py` / `run_scenario.py` `--platform fabric` wiring.

# Fabric platform smoke

This folder checks representative execution on the real fabric platform.
Full feature/format/merge/schema/replay matrices belong to local simulation.
Passing local tests does not qualify cloud storage, catalog, credentials or runtime.

[fabric_use_cases.json](fabric_use_cases.json) is the only metadata source
for this platform smoke. It is ordinary framework JSON: upload it directly;
no composition, shared documents or generation step is needed.

The smoke runs this ordered chain:

1. `read__csv`: read sample CSV and write Parquet.
2. `load_delta__overwrite`: overwrite a dedicated Delta smoke table.
3. `read__delta`: read that table back to Parquet.

All target tables start with `platform_smoke_`. Configure their destinations
before running; repeated runs overwrite only those configured smoke targets.
Keep `STAGE = ""` to run the complete chain including readback. Individual stage
selection requires the upstream target to have been created first.

## Run

Use the [public platform setup guide](../../../docs/examples/runners.md#platform-smoke)
for compatible runtime/bootstrap instructions. Attach an accessible Lakehouse, configure ROOT_PATH and run the Spark or Polars notebook. The database SQL notebook is an optional provider example, separate from the file-metadata smoke.

1. Update every connection input/output path in `fabric_use_cases.json`.
2. Upload the generated sample CSV (`.runtime/data/input/csv/sample`) to the CSV
   connection's configured `base_path`, retaining the `sample` table path.
3. Upload `fabric_use_cases.json` to the metadata path configured by the runner.
4. Execute the complete smoke chain with the selected engine.
5. Confirm no failed flows, inspect Parquet outputs and reconcile row IDs/values
   against the CSV. A successful exit alone is smoke evidence, not feature coverage.

Entrypoints:

- [sample_fabric_spark.ipynb](sample_fabric_spark.ipynb)
- [sample_fabric_polars.ipynb](sample_fabric_polars.ipynb)
- [sample_fabric_database_sql.ipynb](sample_fabric_database_sql.ipynb)

Live platform qualification remains deferred until the declared engine/runtime and persisted outputs are verified. See [DEFERRED.md](DEFERRED.md).

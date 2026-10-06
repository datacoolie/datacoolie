# Databricks platform smoke

This folder checks representative execution on the real databricks platform.
Full feature/format/merge/schema/replay matrices belong to local simulation.
Passing local tests does not qualify cloud storage, catalog, credentials or runtime.

[databricks_use_cases.json](databricks_use_cases.json) is the only metadata source
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
for compatible runtime/bootstrap instructions. Set VOLUME_ROOT to an accessible Unity Catalog Volume, configure the workspace/catalog permissions and choose the Spark or Polars notebook. The secrets notebook is an optional credentials example, separate from the storage smoke.

1. Update every connection input/output path in `databricks_use_cases.json`.
2. Upload the generated sample CSV (`.runtime/data/input/csv/sample`) to the CSV
   connection's configured `base_path`, retaining the `sample` table path.
3. Upload `databricks_use_cases.json` to the metadata path configured by the runner.
4. Execute the complete smoke chain with the selected engine.
5. Confirm no failed flows, inspect Parquet outputs and reconcile row IDs/values
   against the CSV. A successful exit alone is smoke evidence, not feature coverage.

Entrypoints:

- [sample_databricks_spark.ipynb](sample_databricks_spark.ipynb)
- [sample_databricks_polars.ipynb](sample_databricks_polars.ipynb)
- [sample_databricks_secrets.ipynb](sample_databricks_secrets.ipynb)

Live platform qualification remains deferred until the declared engine/runtime and persisted outputs are verified. See [DEFERRED.md](DEFERRED.md).

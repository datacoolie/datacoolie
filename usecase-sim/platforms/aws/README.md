# Aws platform smoke

This folder checks representative execution on the real aws platform.
Full feature/format/merge/schema/replay matrices belong to local simulation.
Passing local tests does not qualify cloud storage, catalog, credentials or runtime.

[aws_glue_use_cases.json](aws_glue_use_cases.json) is the only metadata source
for this platform smoke. It is ordinary framework JSON: upload it directly;
no composition, shared documents or generation step is needed.

The smoke runs this ordered chain:

1. `read__csv`: read sample CSV and write Parquet.
2. `load_delta__overwrite`: overwrite a dedicated Delta smoke table.
3. `read__delta`: read that table back to Parquet.
4. `load_iceberg__overwrite`: overwrite an Iceberg smoke table.
5. `read__iceberg`: read that table back to Parquet.

All target tables start with `platform_smoke_`. Configure their destinations
before running; repeated runs overwrite only those configured smoke targets.
Keep `STAGE = ""` to run the complete chain including readback. Individual stage
selection requires the upstream target to have been created first.

## Run

Use the [public platform setup guide](../../../docs/examples/runners.md#platform-smoke)
for compatible runtime/bootstrap instructions. Use the Glue Spark sample for AWS Glue, the Docker sample for Glue compute against real AWS storage, or the local Polars sample against real AWS. Configure BUCKET, REGION, PROFILE where supported, METADATA_PATH and cloud credentials. Iceberg needs the Glue catalog and engine dependencies; the Polars sample needs Python 3.11+ and pyiceberg. Keep connection bucket/catalog/database settings consistent with runner arguments.

1. Update every connection input/output path in `aws_glue_use_cases.json`.
2. Upload the generated sample CSV (`.runtime/data/input/csv/sample`) to the CSV
   connection's configured `base_path`, retaining the `sample` table path.
3. Upload `aws_glue_use_cases.json` to the metadata path configured by the runner.
4. Execute the complete smoke chain with the selected engine.
5. Confirm no failed flows, inspect Parquet outputs and reconcile row IDs/values
   against the CSV. A successful exit alone is smoke evidence, not feature coverage.

Entrypoints:

- [sample_aws_glue_spark.py](sample_aws_glue_spark.py)
- [sample_aws_glue_docker.py](sample_aws_glue_docker.py)
- [sample_aws_local_polars.py](sample_aws_local_polars.py)
- [run_glue_docker.py](run_glue_docker.py)

Live platform qualification remains deferred until the declared engine/runtime and persisted outputs are verified. See [DEFERRED.md](DEFERRED.md).

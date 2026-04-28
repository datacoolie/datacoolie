# AWS Glue — Deferred Scope

AWS Glue platform assets for file + Delta + Iceberg scenarios are now available
in this folder:

- `aws_glue_use_cases.json` (file + Delta + Iceberg, shared by both engines)
- `sample_aws_glue_spark.py` (Glue Spark / PySpark)
- `sample_aws_glue_polars.py` (Glue Python Shell / Polars)
- `README.md`

## What is still deferred

### Runner integration

- `run.py` / `run_scenario.py` currently instantiate `FileProvider` with
  `LocalPlatform`. An `AWSPlatform` runner mode that reads the metadata file
  directly from S3 is not yet wired. The scripts in this folder bypass the
  runner by constructing `FileProvider(s3_path, platform=AWSPlatform(...))` directly.

### Polars + Iceberg (pyiceberg Glue catalog)

- `PolarsEngine` delegates Iceberg reads/writes to `pyiceberg`.
- Full Glue catalog integration requires `pyiceberg[glue]` and appropriate
  environment configuration (`PYICEBERG_CATALOG__GLUE__TYPE=glue`).
- The Polars Iceberg path in `aws_glue_use_cases.json` (`base_path`) is present
  as a path hint, but the catalog-level create/register is driven by pyiceberg
  at runtime. Validate pyiceberg Glue catalog connectivity separately before
  running `load_iceberg` stages with PolarsEngine.

### Polars Delta — IAM credential propagation edge cases

- `storage_options` is derived from `boto3.Session` frozen credentials in
  `sample_aws_glue_polars.py`. This covers Glue IAM role credentials (STS tokens).
- If running outside Glue (e.g. local dev with assumed role), ensure
  `AWS_PROFILE` or `AWS_*` environment variables are set before the session
  is created, or extend `storage_options` accordingly.

### Schema evolution for Delta on Polars

- Delta schema evolution (`mergeSchema`) is supported by `deltalake` but the
  exact flag names differ from Spark. Validate merge/append with new columns
  against the installed `deltalake` version before using `schema_evolve_delta`
  stages with PolarsEngine.

### Scenario catalog / profile integration

- `scenarios.json` and any profile-based runner integration for AWS Glue are
  not yet designed.
- End-to-end CI coverage for Spark + Polars on real AWS Glue is not set up.

### Excel format

- Excel is not included in `aws_glue_use_cases.json`. AWS Glue Python Shell
  supports `openpyxl` via `--additional-python-modules`, but bandwidth cost
  and Glue Python Shell memory limits make large Excel files impractical.
  Add an Excel connection manually if needed.

### Database (SQL) connections

- The `aws_secrets_example_source` connection in the metadata is a structural
  demonstration of Secrets Manager `secrets_ref` only. It points to a
  fictional RDS instance.
- Actual RDS / Redshift connectivity requires network-level Glue VPC configuration
  (VPC, subnet, security group settings) and is out of scope for this asset set.

## Status

Partially implemented: Glue script-first AWS assets are in place.
Runner-level integration and pyiceberg Glue catalog validation remain deferred.

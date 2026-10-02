# AWS Glue — Deferred Scope

AWS Glue platform assets for file + Delta + Iceberg scenarios are now available
in this folder:

- `aws_glue_use_cases.json` (file + Delta + Iceberg, shared by both engines)
- `sample_aws_glue_spark.py` (Glue Spark / PySpark)
- `sample_aws_local_polars.py` (controlled Python 3.11+ / Polars, not Glue Python Shell)
- `README.md`

## What is still deferred

### Runner integration

- The unified runner now passes the selected platform to `DataCoolieDriver` and
  leaves provider construction to the Driver for artifact/directory modes.
  Exact-file AWS examples still construct an explicit `FileProvider` for their
  own script-first flow; they should use the current provider lifecycle (bind a
  platform before initialization) rather than the removed constructor shortcut.

### Polars + Iceberg (pyiceberg Glue catalog)

- `PolarsEngine` delegates Iceberg reads/writes to `pyiceberg`.
- The sample now initialises a Glue-backed pyiceberg catalog in code using the
  active AWS role.
- Full end-to-end validation still requires `pyiceberg` plus working Glue
  catalog permissions in the target account.
- The Polars Iceberg path in `aws_glue_use_cases.json` (`base_path`) is present
  as a path hint, but the catalog-level create/register is driven by pyiceberg
  at runtime. Validate pyiceberg Glue catalog connectivity separately before
  running `load_iceberg` stages with PolarsEngine.

### Polars Delta — IAM credential propagation edge cases

- `storage_options` is derived from `boto3.Session` frozen credentials in
  `sample_aws_local_polars.py`. This carries the active AWS credential chain, including assumed-role STS tokens.
- For local/container execution (e.g. with an assumed role), ensure
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

- Excel is not included in `aws_glue_use_cases.json`. A controlled Python 3.11+ runtime can install `openpyxl`; qualify memory and
  transfer costs before processing large Excel files. Glue Python Shell Python
  3.9 is incompatible with the current DataCoolie package.
  Add an Excel connection manually if needed.

### Database (SQL) connections

- The `aws_secrets_example_source` connection in the metadata is a structural
  demonstration of Secrets Manager `secrets_ref` only. It points to a
  fictional RDS instance.
- Actual RDS / Redshift connectivity requires network-level Glue VPC configuration
  (VPC, subnet, security group settings) and is out of scope for this asset set.

## Status

Partially implemented: Glue script-first AWS assets are in place.
Runner-level integration and real AWS validation for the pyiceberg Glue catalog
remain deferred.

# Dependency Routing

## Scope

Use this reference only to prepare the selected packaged probe. It does not define source evidence,
fallback inspection logic, architecture, or runtime dependencies for the generated DataCoolie
project.

## Select One Capability

| Probe | Install first | Additional requirement |
|---|---|---|
| OpenAPI, GraphQL, OData | `scripts/requirements-api.txt` | None |
| Relational catalog or custom SQL | `scripts/requirements-databases.txt` | Exactly one SQLAlchemy driver for the selected source |
| Parquet, CSV, JSON, ORC | `scripts/requirements-files.txt` | Remote filesystem package only when the URI requires it |
| Delta, Avro, Excel | `scripts/requirements-files.txt` | Respectively `deltalake`, `fastavro`, or `openpyxl` |
| Iceberg REST | `scripts/requirements-lakehouse.txt` | None |
| Hive | `scripts/requirements-lakehouse.txt` plus `scripts/requirements-hive.txt` | Reachable HiveServer2 endpoint |
| Unity Catalog | No Python group | Configured current `databricks` CLI |
| AWS Glue | No Python group | Configured AWS CLI v2 |

For remote files, install only the selected filesystem implementation, such as `s3fs`, `adlfs`, or
`gcsfs`. Do not install every database driver, filesystem, catalog client, or file-format extra.

## Preflight

Before connecting:

1. Run the selected script with `--help`.
2. Confirm the selected Python import or external executable is available.
3. For an external CLI, inspect its current command help and configured identity. Do not copy
   historical command syntax from a skill reference.
4. Keep dependency/version evidence in the discovery report when it explains a failed or partial
   probe; do not copy these packages into project runtime requirements unless build independently
   needs them.

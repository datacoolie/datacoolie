# Databricks smoke qualification

Status: deferred for live-cloud execution.

Scope: `databricks_use_cases.json`, the representative file/Delta
chain described in [README.md](README.md). Full feature matrices are tested locally.

Qualification requires configured storage/catalog/credentials, compatible engine
dependencies, complete smoke execution, and persisted row/value reconciliation.
Record platform, engine/runtime versions, metadata revision and results. Local
tests, Docker compute and metadata validation do not establish real-cloud parity.

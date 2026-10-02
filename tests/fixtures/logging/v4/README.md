# Logging v4 fixtures

These deterministic JSON Lines samples describe the current framework logging
envelope. Every non-empty line is one compact JSON object and the `.json`
suffix is intentional for platform preview compatibility.

`manifest.json` records stream kinds, row counts, and persistence modes. The
job snapshot is a single replace-one record; dataflow and system parts are
immutable batch parts. Every stream record starts with `log_schema_version`
followed by its `_type` discriminator (`job_run_log`, `dataflow_run_log`, or
`system_log`).

Runtime and phase diagnostics use the single `message` field. Job persistence
truncation is reported by `message_truncated`.

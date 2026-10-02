# Logging v3 fixtures

These deterministic JSON Lines samples describe the framework logging envelope after the
`log_session_id` session correlation. Every non-empty line is one compact JSON object and the `.json` suffix is
intentional for platform preview compatibility.

`manifest.json` records stream kinds, row counts, and persistence modes. The job snapshot is a
single replace-one record; dataflow and system parts are immutable batch parts. Every stream record
starts with `log_schema_version` followed by its `_type` discriminator (`job_run_log`,
`dataflow_run_log`, or `system_log`).

These samples predate the additive `datacoolie_version` field. Keep them as
historical v3 records without producer metadata; current writer/projection
tests verify the field against the installed package version. Readers must
tolerate its absence and ignore unknown fields. A package release does not
rewrite these historical samples or increment the log schema counter.

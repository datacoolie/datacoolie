# Simulator runners

- `run_scenario.py`: named recipe/group dispatch, setup, final validation and receipts.
- `run.py`: normal ETL execution for selected engine/provider/platform.
- `maintenance.py`: compact/cleanup execution.
- `run_perf_benchmark.py`: measurement suite execution.
- `qualified_sql_setup.py`: engine table registration for qualified SQL recipes.
- `_runner_utils.py`: shared runner assembly and replay helpers.
- `runtime_paths.py`: canonical data, logs, watermarks and dependency roots.
- `runtime_logs.py`: read-only framework record lookup by job/current session.

Runtime helpers have no standalone command. They are imported through `runner`;
execute only the entrypoints above. Framework logs remain under `.runtime/logs`.

## Metadata composition

`metadata_composition.py` resolves simulator group/shared/workflow references
without framework or service imports. The runners compose local inputs before
engine creation; ordinary JSON and remote URI inputs keep their existing behavior.
Runtime copies are content-addressed. Use its `--source` CLI to export ordinary
metadata for platform upload; see [metadata layout](../metadata/file/README.md).
`metadata_documents.py` owns read-only recursive loading; `metadata_document_rules.py`
owns reference and shared-document validation. The CLI module owns runtime writes.

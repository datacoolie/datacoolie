# Metadata by behavior

This directory owns metadata for local, Docker and AWS-MinIO simulation.
Real-cloud qualification metadata lives with its platform under
one JSON in `platforms/<platform>/`; local feature results do not qualify cloud execution.

Select an exact fixture in `scenarios/scenarios.json`. Each topic owns its
authored metadata; engine variants and negative cases remain separate so
loading a positive case cannot accidentally include a failure fixture.

| Topic | Coverage |
|---|---|
| `workflows/` | Ordered manifests for local/AWS-MinIO suites and intentional composite flows |
| `write_basic/`, `merge/`, `scd2/`, `maintenance/` | Group-owned write and stateful cases |
| `incremental/` | Pagination failure, recovery and continuation per engine |
| `schema_types/` | Canonical type declarations and format qualification |
| `replay/` | Bounded file, SQL, API, AWS and Iceberg replay |
| `read/` | Qualified SQL over lakehouse sources |
| `execution/` | Startup failure fixture |
| `transform/` | Positive transformations and isolated negative stages |
| `performance/` | Benchmark dataflows |

Each primary flow definition belongs to its group folder, including flows from
former local/AWS-MinIO baseline bundles. Setup/readback can stay with the owning
focused case as support. The catalog audit rejects primary group/path mismatch.

Profile documents such as `merge/local.json` contain group-owned flows and a
`shared_metadata` reference to `metadata/file/shared/<profile>.json`, the single owner
of profile connections and schema hints. Standalone fixtures remain plain JSON.
`shared/` belongs to the file-metadata source tree; exporting the composed
document to YAML, Excel, database or API does not change that source ownership.

`workflows/` manifests reference group files through `metadata_documents` and
preserve original input order with `flow_order`. Intentional combined behaviors
have their own flow in `workflows/<profile>.json`. Never copy canonical group flows
into a manually maintained aggregate.

New feature cases belong in their focused topic with an explicit recipe.
Do not load this entire directory as one metadata project: topics reuse
connection names and include deliberate failures.

The runners resolve local composition references before engine startup.
`bootstrap/setup_metadata.py` composes the selected source and exports ordinary
JSON/YAML/Excel to `.runtime/data/metadata/`, or seeds database providers.
For AWS-MinIO export, compile the manifest first:

```powershell
python usecase-sim/runner/metadata_composition.py --source usecase-sim/metadata/file/workflows/aws_use_cases.json
```

Use the resulting ordinary JSON, not the manifest. Group selection does not
prepare upstream targets; use the declared recipe/preconditions. Artifact-directory
resolution fixtures retain their required layouts under `artifacts/` and have
`metadata_artifacts` ownership.

SQL/artifact/state-root resolution fixtures live in `metadata_artifacts/`.
Use [case ownership](../../cases/README.md) for assertions and gaps.

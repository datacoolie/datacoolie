# Feature metadata

Select an exact fixture in `scenarios/scenarios.json`. Each topic owns its
authored metadata; engine variants and negative cases remain separate so
loading a positive case cannot accidentally include a failure fixture.

| Topic | Coverage |
|---|---|
| `baseline/` | Shared local/AWS source and destination matrix, YAML/Excel provider parity |
| `api_incremental/` | Pagination failure, recovery and continuation per engine |
| `datatypes/` | Canonical type declarations and format qualification |
| `replay/` | Bounded file, SQL, API, AWS and Iceberg replay |
| `sql/` | Qualified SQL, query roots, multiple roots and shared SQL files |
| `runtime/` | Startup errors and independent log/watermark paths |
| `transformers/` | Positive transformations and isolated negative stages |
| `performance/` | Benchmark dataflows |

`baseline` is a broad provider/source integration suite. New feature cases
belong in their focused topic, with a scenario that selects only that feature.
Do not load this entire directory as one metadata project: topics reuse
connection names and include deliberate failures.

`bootstrap/setup_metadata.py` regenerates YAML/Excel siblings from the selected
JSON and seeds database providers. Runtime-generated qualification metadata
belongs under `.runtime/data/<topic>/metadata/`.

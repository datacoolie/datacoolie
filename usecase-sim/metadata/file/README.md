# File metadata

Each behavior group has one flat JSON file, such as `merge.json`, `scd2.json`,
`read.json` or `incremental.json`. Group files contain `dataflows`; every authored
flow name is unique across the simulator. `connections.json` owns shared
connections and `schema_hints.json` owns global hints. Real-cloud smoke metadata
remains one self-contained JSON under each `platforms/<platform>/`.

FileProvider reads exact group files and the shared section files directly,
validates their complete model snapshot, and then selects the scenario flows.
No merged JSON or `.runtime/metadata` directory is required. For example:

```powershell
python usecase-sim/runner/run.py --engine polars --platform local --metadata-source file `
  --metadata-path usecase-sim/metadata/file/merge.json `
  --connections-path usecase-sim/metadata/file/connections.json `
  --schema-hints-path usecase-sim/metadata/file/schema_hints.json `
  --stage load_delta --dataflow load_delta__merge_upsert
```

Use declared scenarios when inputs or existing targets need preparation.
[Scenario recipes](../../scenarios/scenarios.json) select exact flow names;
[metadata recipes](../../scenarios/metadata_recipes.json) describe focused subsets
and step bindings. Bindings change invocation configuration in memory while
preserving canonical flow name, stage and ID. Recovery steps share that identity
and their engine-specific watermark scope.

Ordered broad selections live in [workflow manifests](../../scenarios/workflows/).
Bootstrap, API and database tooling read these selections in memory. Explicit
JSON/YAML/Excel exports belong to `.runtime/data/output/metadata_exports/` or the
requested `--output-dir`; SQLite metadata belongs to
`metadata/database/datacoolie_metadata.db`.

Deliberately invalid startup documents live under `scenarios/errors/` and load
independently. Standalone error probes are steps of `error_messages`; each step
has its own setup, engine, validator and qualification receipt. Stateful
fail/recovery/continuation sequences keep their declared order and checkpoint
scope. See [case ownership](../../cases/README.md) for assertions and gaps.

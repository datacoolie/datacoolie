# DataCoolie Operational Runner Extensions

## Scope

- Read together with `references/runner-contract.md` only when replay or maintenance is required.
- Owns replay ranges/chunking/watermark mutation and maintenance inspection/mutation safety gates.
- Inherits common identity, parameters, construction, notebook decoding, durable/generated, and
  receipt behavior from the runner contract; it does not redefine them.

## Entrypoint identity

```text
replay_{platform}_{engine}[_{provider}].py|ipynb
maintenance_{platform}_{engine}[_{provider}].py|ipynb
```

Keep replay and maintenance separate from normal `run` and from each other. The caller selects the
exact entrypoint; do not add a runtime operation selector.

## Replay extension

In addition to common parameters, accept `start`, `end`, optional `chunk_interval`, optional
`chunk_column`, and `save_watermark`.

- Construct `ReplayConfig` and call `driver.load_dataflows(stage=group)` followed by
  `driver.run_replay(dataflows=flows, replay=replay)`.
- Preserve `[start, end)` and boundary types. Parse integer boundaries as integers; otherwise pass
  strings through for the framework to interpret.
- Accept one positive chunk interval key from `years`, `months`, `weeks`, `days`, `hours`,
  `minutes`, or `step`; omit it for one replay window.
- A stage string, comma string, or list remains one framework selection. Repeated groups run
  sequentially in occurrence order and stop after a failed group. No stage loads all dataflows once.
- Default `save_watermark` to false. Enabling it requires separate explicit confirmation because it
  advances persistent state and can interfere with a concurrent incremental run.
- Use `CHUNK_INTERVAL_JSON` for notebook/job chunk configuration. Databricks widget booleans and
  integers arrive as strings and must be decoded explicitly.

Within one replay call, DataCoolie runs dataflows in parallel and chunks for each dataflow
sequentially. Do not recreate either scheduler in the entrypoint.

## Maintenance extension

In addition to common parameters, accept optional connection filters, compact/cleanup selection,
retention hours, worker count, and inspection or mutation intent.

- Inspection calls `driver.load_maintenance_dataflows(connection=...)`, reports deduplicated
  targets, and does not invoke maintenance.
- Mutation calls `driver.run_maintenance(connection=..., do_compact=..., do_cleanup=...)` and
  requires an explicit confirmation.
- Keep at least one of compact or cleanup enabled and require positive explicit retention.
- Do not expose or describe `dry_run` as maintenance protection unless the installed runtime proves
  `run_maintenance` enforces it; target inspection is the safe preview path.
- Use maintenance only for installed supported destinations. Retention must not be shorter than
  active-reader or recovery requirements.

Use `CONNECTIONS_JSON` for notebook/job connection filters. Inspection prints a stable JSON array
sorted by framework `destination_key`. Each entry contains only the dataflow name or ID,
destination connection name and format, destination key, full table name, path, requested
operations, and retention hours. Do not emit secrets or arbitrary connection configuration.
Mutation also reports the requested operations and retention before invoking maintenance.

DataCoolie owns physical-target deduplication and parallel dispatch. Do not reproduce that logic in
the entrypoint.

## Operation-specific notebook gates

Confirmation values default to false and must be checked in executable code. A comment or markdown
warning is not a gate. Decode widget strings for stage groups, chunk intervals, connections,
booleans, and numeric values without changing their types or ordering.

## Operation-specific verification

- Replay covers range boundaries, chunking, stage groups, failure barriers, and both watermark
  modes.
- Maintenance covers inspection, filters, operation selection, retention, confirmation,
  physical-target deduplication, and failure reporting.
- The build receipt records the selected operation and its objective results; common artifact and
  receipt checks remain owned by the runner contract and build validator.

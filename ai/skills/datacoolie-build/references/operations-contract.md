# DataCoolie Operational Runner Extensions

## Scope

- Read together with `references/runner-contract.md` only when replay or maintenance is required.
- Owns replay ranges/chunking/watermark mutation and maintenance mutation confirmation.
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

- Construct `ReplayConfig` and call `driver.load_dataflows(stage=stage)` followed by
  `driver.run_replay(dataflows=flows, replay=replay)`.
- Preserve `[start, end)` and boundary types. Decode integer strings from text-only transports;
  otherwise pass boundary values through without runner-owned range validation.
- Decode an optional JSON chunk interval into its native value, then let `ReplayConfig` and replay
  execution validate its keys, shape, values, and range. Omit it for one replay window.
- A stage string, comma string, or list remains one framework selection. Repeated groups run
  sequentially in occurrence order and stop after a failed group. No stage loads all dataflows once.
- Default `save_watermark` to false. Enabling it requires separate explicit confirmation because it
  advances persistent state and can interfere with a concurrent incremental run.
- Use the same JSON chunk representation for Python and notebook/job entrypoints. Databricks widget
  booleans and run-config integers arrive as strings and require transport decoding.

Within one replay call, DataCoolie runs dataflows in parallel and chunks for each dataflow
sequentially. Do not recreate either scheduler in the entrypoint.

## Maintenance extension

In addition to common parameters, accept optional connection filters, compact/cleanup selection,
retention hours, worker count, and explicit mutation confirmation.

- Call `driver.run_maintenance(connection=..., do_compact=..., do_cleanup=...)` once after explicit
  confirmation.
- Keep at least one of compact or cleanup enabled. Decode retention as an integer and let
  `DataCoolieRunConfig` validate it.
- Do not add runner-owned preview, target-description, scheduling, or `dry_run` behavior. If runtime
  usage needs new operational evidence or validation, add it to the framework contract first.
- Use maintenance only for installed supported destinations. Retention must not be shorter than
  active-reader or recovery requirements.

Pass one optional connection selection unchanged; DataCoolie accepts a scalar, comma-separated
string, JSON-array string, or native list. Map an omitted notebook value to `None`.

DataCoolie owns supported-target selection, physical-target deduplication, parallel dispatch,
logging, and result aggregation. Do not reproduce that logic in the entrypoint.

## Operation-specific notebook gates

Confirmation values default to false and must be checked in executable code. A comment or markdown
warning is not a gate. Pass stage and connection selections unchanged and decode widget strings
only where the framework requires native JSON, boolean, or numeric values.

## Operation-specific verification

- Replay covers range boundaries, chunking, one stage value, failure handling, and both watermark
  modes.
- Maintenance covers direct filters, operation selection, retention, confirmation, one framework
  call, and failure reporting.
- The build receipt records the selected operation and its objective results; common artifact and
  receipt checks remain owned by the runner contract and build validator.

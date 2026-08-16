# Fallback Probes

## Scope

Use this reference only when a packaged probe cannot connect or cannot answer one material evidence
gap. It does not replace normal introspection, define dependencies, choose ingestion behavior, or
author runtime metadata.

## Fallback Order

1. Confirm the failure is a real probe or source limitation rather than a missing optional
   dependency, identity, permission, filter, or network route.
2. Use the source's current official catalog, schema, or API documentation to select the smallest
   read-only operation that answers the gap.
3. Bound scope with explicit objects, namespaces, paths, rows, bytes, pages, and timeouts.
4. Write raw output to `.scratch/discover/`. Retain a sanitized copy under `discover/raw/` only when
   it materially improves reproducibility.
5. Normalize only facts represented by the current observation contract. Put source-level methods,
   limitations, and unanswered questions in probe status or `report.md`; do not invent CSV values.

## Database Query Gap

Put one `SELECT` or `WITH` statement in a file and run `scripts/probe_db.py` with environment-backed
connection information, a scratch JSON `--output`, `--max-rows`, and `--timeout-seconds`. The SQL
file must be self-contained because the probe does not bind parameters. The script rejects multiple
or mutating statements, rolls back the transaction, and refuses drivers that cannot enforce the
requested timeout unless the operator explicitly accepts that limitation.

## Unsupported Source

Use `templates/observations.tpl.csv` for a source that no packaged script can represent. Record the
source protocol, official documentation consulted, probe bounds, and unknown facts in `report.md`.
Use `notes` only for a material limitation tied to one exact observation row. Leave an observation
field empty when the source cannot establish that fact, then validate the file through
`scripts/merge_observations.py`.

Never turn missing source evidence into an assumed key, relationship, watermark, schema, or loading
strategy.

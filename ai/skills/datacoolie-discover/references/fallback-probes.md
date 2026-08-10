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
5. Normalize only supported facts into the observation contract. Mark inference and unresolved
   coverage explicitly.

## Database Query Gap

Put one `SELECT` or `WITH` statement in a file and run `scripts/probe_db.py` with environment-backed
connection information, `--max-rows`, and `--timeout-seconds`. The script rejects multiple or
mutating statements, rolls back the transaction, and refuses drivers that cannot enforce the
requested timeout unless the operator explicitly accepts that limitation.

## Unsupported Source

Use `templates/observations.tpl.csv` for a source that no packaged script can represent. Record the
source protocol and official documentation consulted in `method` or `notes`, preserve unknowns as
`unresolved`, then validate the file through `scripts/merge_observations.py`.

Never turn missing source evidence into an assumed key, relationship, watermark, schema, or loading
strategy.

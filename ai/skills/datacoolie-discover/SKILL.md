---
name: datacoolie-discover
description: Inspect data sources and produce verified source evidence for DataCoolie design or build work. Use for every new DataCoolie project before design, for every declared source type, and when an existing source is new, changed, missing evidence, or contradictory. Discovery is read-only evidence and never creates runtime metadata, workspace code, infrastructure, or releases.
---

# DataCoolie Discover

## Outcome And Boundary

Produce compact, reproducible source facts with provenance. Own inspection of source objects,
columns, types, constraints, relationships, size estimates, change signals, layouts, capabilities,
access limitations, and probe failures.

Do not turn candidates into architecture or ingestion decisions. A possible watermark remains
evidence. Do not create configuration, runtime metadata, runners, functions, infrastructure, or
releases.

## Probe Contract

Use the smallest packaged read-only probe before manual investigation. Scripts generate repeatable
rows and status; the agent enriches only verified gaps.

- Resolve credentials, headers, and storage options from environment variables or ambient identity.
- Reject secret-bearing command arguments and do not retain source values by default.
- Bound remote traversal, catalog inspection, custom queries, samples, and network calls.
- Treat exit `0` as complete, exit `3` as usable but partial, and any other non-zero exit as failed.
- Write every probe to its own scratch artifact. Duplicate stable keys are errors, not overwrite
  instructions.

## Resource Routing

Resolve resources relative to this skill and read only the matching reference.

| Need | Resource |
|---|---|
| Relational catalog or bounded SQL gap | `scripts/introspect_db.py`, `scripts/probe_db.py` |
| File schema or bounded directory layout | `scripts/introspect_files.py` |
| OpenAPI, GraphQL, or OData | `scripts/introspect_api.py` |
| Iceberg, Hive, Unity Catalog, or Glue | `scripts/introspect_lakehouse.py` |
| Merge or enrich observations | `scripts/merge_observations.py`, `scripts/enrich_observations.py` |
| Observation fields or manual rows | `references/observation-contract.md` |
| Python packages or external CLI prerequisites | `references/dependency-routing.md` |
| Unsupported source or unresolved probe gap | `references/fallback-probes.md` |
| Final summary or interview | `templates/discovery-report.tpl.md`, `templates/interview-questions.md` |

## Workflow

1. Inventory every declared source boundary. For a new project, probe each source even when the
   supplied connection details and descriptions appear complete; declared facts select and bound
   the probe but do not replace observed evidence. For existing work, scope probes to the new,
   changed, missing, or contradictory facts.
2. Read `references/dependency-routing.md`; install only the selected probe's dependencies and
   preflight any required external CLI.
3. Run each probe into a distinct `.scratch/discover/{probe}.csv` and status JSON. Keep bounded
   layout summaries under `.scratch/discover/`; they are supporting evidence, not another inventory.
4. Resolve or report partial and failed probes. Never describe partial coverage as complete.
5. Merge all usable probe CSVs, including a single input, with `merge_observations.py` into
   `discover/observations.csv`. This validates the canonical contract and exact stable keys.
6. Classify evidence as `declared`, `observed`, `inferred`, or `unresolved`. Preserve exact observed
   column types so build can author shared schema hints without re-querying or guessing. Watermark candidates
   use an evidence class or remain empty; they are not booleans.
7. Investigate only unresolved gaps. Use `references/fallback-probes.md` rather than copying static
   vendor commands. Custom database SQL must be one read-only `SELECT` or `WITH` statement from a
   file with explicit row and timeout limits.
8. Merge small verified annotations with `enrich_observations.py`; do not broadly rewrite generated
   rows.
9. Write `discover/report.md` with scope, exclusions, methods, partial or failed probes, and
   unresolved questions.

## Output And Handoff

```text
{workspace}/discover/
  observations.csv
  report.md
  raw/                  # optional safe evidence required for reproducibility
```

`observations.csv` is the only durable machine-readable inventory. `report.md` summarizes
high-signal evidence without copying the inventory, samples, or credentials.

Hand exact evidence paths to design when intent remains open, or to build when the design is
sufficient. Runtime code must not import discovery artifacts. Discovery approves neither outcome.
End with unresolved questions; state `None` when there are none.

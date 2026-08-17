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
| Merge/refresh observations or assess watermark signals | `scripts/merge_observations.py`, `scripts/assess_watermarks.py` |
| Finalize complete object decisions | `scripts/finalize_watermark_assessment.py`, `templates/watermark-assessment.example.json` |
| Apply verified observation annotations | `scripts/enrich_observations.py` |
| Observation fields or manual rows | `references/observation-contract.md` |
| Python packages or external CLI prerequisites | `references/dependency-routing.md` |
| Unsupported source or unresolved probe gap | `references/fallback-probes.md` |
| Bounded database evidence query | `references/evidence-queries.md`, `scripts/probe_db.py` |
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
5. For a new project, merge all usable probe CSVs, including a single input, with
   `merge_observations.py`. For an existing source, use refresh mode with the current inventory as
   `--base`, explicit `--replace-source` and status inputs, a scratch candidate output, and a scratch
   diff. Do not replace prior complete evidence with a partial probe unless that exact source is
   explicitly accepted. Promote the validated candidate to `discover/observations.csv`.
6. Preserve exact observed column types so build can author shared schema hints without re-querying
   or guessing. Keep probe time, method, scope, and failures in status JSON and the report rather
   than duplicating them on every observation row.
7. Run `assess_watermarks.py` into `.scratch/discover/watermark-assessment.csv` with
   `--summary-output .scratch/discover/object-summary.json`. Read the JSON summary first and open
   detailed observations only for ambiguous objects. Normalized identifier and structural signals
   are scratch evidence, not final candidates.
8. Assess every object for mutation coverage, ordering, duplicates, resets, late changes,
   filtering, and delete visibility. Investigate only decision-changing gaps. Use
   `references/evidence-queries.md` for one bounded custom database query and
   `references/fallback-probes.md` for unsupported sources; never execute reference examples as a
   batch. Custom SQL is self-contained and `probe_db.py` writes its bounded envelope to scratch
   JSON. For file sources, also verify delivery semantics: whether modification times are stable,
   whether files are replaced in place, and whether the path has real year/month/day/hour levels.
   Keep these object/layout facts in the report; framework virtual columns are not observed source
   columns.
9. Complete the scratch object decisions and run `finalize_watermark_assessment.py`. It must cover
   every observed object and generates both watermark annotations and the report table. Apply its
   annotations with `enrich_observations.py`; use direct annotations only for other verified gaps.
10. Assemble `discover/report.md` with scope, exclusions, methods, partial or failed probes, and the
    generated assessment table. Discovery recommends evidence and fallbacks but leaves the final
    load strategy to design or an explicit human decision. Describe a transaction/business date as
    a backward fallback, not as complete change coverage, when no reliable change signal exists.

## Output And Handoff

```text
{workspace}/discover/
  observations.csv
  report.md
  raw/                  # optional safe evidence required for reproducibility
```

`observations.csv` is the only durable machine-readable inventory. `report.md` summarizes
high-signal evidence without copying the inventory, samples, or credentials.

Shortlists, compact summaries, object decisions, generated annotations, report fragments, refresh
candidates, and diffs are scratch-only. They must never be imported by runtime code or mistaken for
additional durable inventories.

Hand exact evidence paths to design when intent remains open, or to build when the design is
sufficient. Runtime code must not import discovery artifacts. Discovery approves neither outcome.
End with unresolved questions; state `None` when there are none.

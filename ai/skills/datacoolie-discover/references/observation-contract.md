# Observation And Watermark Assessment Contract

## Scope

Use this reference to interpret, create, merge, annotate, or assess discovery observations. It does
not select probes, ingestion strategies, architecture, or runtime metadata.

## Durable Observation Artifact

`discover/observations.csv` is the only durable machine-readable source inventory. Generate it with
`scripts/merge_observations.py`, including for one probe, so the exact header, values, stable keys,
and deterministic ordering are validated.

The exact 19-column header is:

```text
source,object_type,catalog,schema,object,source_operation,column,native_type,data_type,format,precision,scale,nullable,ordinal,key,reference,row_estimate,watermark_candidate,notes
```

The stable key is:

```text
source + object_type + catalog + schema + object + source_operation + column
```

`source_operation` is a source-native discriminator such as an HTTP method. Leave it empty when the
source has no operation dimension. Duplicate stable keys are errors.

## Field Boundaries

| Field | Meaning |
|---|---|
| `key` | Declared fact only: empty, `primary`, or `unique:<constraint-name>`; use `unique:unnamed` when uniqueness is declared without a name. |
| `reference` | Canonical source-native target locator without display decoration. Relational probes use `[catalog.]schema.object.column` or `object.column`; GraphQL/OData use the referenced type/entity; API probes retain their canonical schema locator. |
| `watermark_candidate` | Confirmed candidate roles only. It does not record evidence strength or select a load strategy. |
| `notes` | Material row-specific limitation or provenance. Probe timing, method, scope, and failures belong in status JSON and `report.md`. |

GraphQL `ID` is a serialization scalar, not proof of a primary key. Hard deletes are not observable
by polling ordinary base-table columns after the row disappears; delete coverage requires a durable
tombstone, CDC, change tracking, or equivalent signal.

## Watermark Roles

Use unique tokens separated by `|` in this canonical order:

```text
change | insert | update | delete | append | auxiliary | backward
```

| Role | Use only when |
|---|---|
| `change` | One ordered/filterable value advances for every relevant row change. |
| `insert` | The value captures row creation. |
| `update` | The value captures row modification. |
| `delete` | A persistent soft-delete, tombstone, or delete event remains queryable. |
| `append` | A monotonic identity/sequence exists; it is standalone only for confirmed append-only data. |
| `auxiliary` | The value is a tie-breaker or secondary candidate combined with another role/column. |
| `backward` | A transaction/business date can bound a lookback; it is not full change coverage. |

DataCoolie combines multiple watermark columns with OR predicates. Separate insert, update, and
delete-event columns can therefore form one candidate set. Record the set, alternatives, and
limitations in `report.md`; do not add another grouping convention to the CSV.

### Backward and file-source evidence

Use `backward` primarily when an object has no reliable column or source-native feed that captures
every relevant change, but a filterable transaction/business date can bound a correction window.
Do not present that date as equivalent to a true change watermark. Confirm the expected late-change
horizon and record that corrections older than the chosen lookback can be missed.

For file sources, inspect delivery behavior separately from row schema:

- Determine whether the storage platform exposes stable file modification times and whether copy,
  rewrite, or restore operations preserve their intended meaning. This evidence lets build prefer
  DataCoolie's file-modification-time mechanism without inventing an observed column.
- Record a date-folder pattern only when the physical path really contains ordered
  year/month/day/hour levels. A folder date bounds path discovery; it does not prove row-change
  coverage.
- Assess a column inside the file as a row watermark only when its semantics independently satisfy
  the normal mutation-coverage checks.

`__file_modification_time` and the internal date-folder watermark are framework values, not source
schema columns. Keep their feasibility and limitations in `report.md`, not as fabricated rows in
`observations.csv`.

## Mandatory Assessment

After merging observations:

1. Run `scripts/assess_watermarks.py` into `.scratch/discover/watermark-assessment.csv` with
   `--summary-output .scratch/discover/object-summary.json`. Identifier matching normalizes common
   casing and delimiters but preserves the original observed name.
2. Treat normalized name/type/key signals as a shortlist, never a confirmed result. A missed custom
   name still requires agent assessment; do not expand a hardcoded dictionary to cover a domain.
3. Assess every object using bounded read-only evidence or owner clarification: mutation coverage,
   nulls, duplicates, ordering, precision/time zone, reset/reuse, late changes, filterability, and
   delete behavior.
4. Fill the scratch object-decision contract from
   `templates/watermark-assessment.example.json`. Use exactly one outcome per object:
   `confirmed_candidate`, `source_native_change`, `backward_fallback`, `full_refresh`, or
   `human_decision`.
5. Run `scripts/finalize_watermark_assessment.py`. It rejects missing or unknown objects and emits
   exact-key annotations plus the complete report table from the same decisions.
6. Apply the generated annotations through `scripts/enrich_observations.py` and place the generated
   table in `report.md`.

An identity is `append` alone only after append-only behavior is confirmed. On mutable data it may
be `append|auxiliary` alongside update/change evidence. A `backward` candidate must state the
lookback/correction horizon, target replacement behavior, and risk of missing older corrections.

If no reliable candidate exists, recommend full load/overwrite only when project-specific volume,
duration, source pressure, cost, and SLA make it acceptable. For large or unknown cases, record a
human decision. Do not invent a universal row-count threshold.

## Manual Rows And Annotations

Use `templates/observations.tpl.csv` only when no packaged probe represents the source, then pass it
through `merge_observations.py`.

For verified gaps, copy `templates/observation-annotations.example.json`. Match all stable-key
fields. `set` may change only `key`, `reference`, and `watermark_candidate`; optional
`append_notes` adds one material limitation. Annotations do not carry timestamps or methods because
those are probe/report-level provenance.

Watermark annotations should normally be generated by the assessment finalizer. Author an
annotation directly only for a verified key/reference gap or when no object-level decision is being
changed.

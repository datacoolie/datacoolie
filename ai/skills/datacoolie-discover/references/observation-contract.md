# Observation Contract

## Scope

Use this reference only to interpret, manually create, or annotate discovery observation rows. It
does not select probes, dependencies, ingestion strategies, architecture, or runtime metadata.

## Canonical Artifact

`discover/observations.csv` is the only durable machine-readable source inventory. Generate it with
`scripts/merge_observations.py`, even when there is only one probe input, so header, values,
timestamps, uniqueness, and deterministic ordering are validated.

The stable key is:

```text
source + object_type + catalog + schema + object + operation + column
```

Keep `operation` empty when the source has no operation dimension. A duplicate stable key is an
error because silent replacement destroys provenance.

## Evidence Semantics

| Field | Meaning |
|---|---|
| `evidence_class` | `declared`, `observed`, `inferred`, or `unresolved` |
| `watermark_candidate` | `declared`, `observed`, `inferred`, or empty |
| `method` | The catalog, protocol, bounded probe, or interview that produced the fact |
| `observed_at` | ISO-8601 timestamp with timezone for the original observation |
| `notes` | Short provenance or limitation; never retained source values or credentials |

A candidate is not a selected loading strategy. Generic identifiers and event timestamps are not
watermarks without stronger evidence about ordering, updates, deletes, and replay behavior.

## Manual And Annotated Rows

Use `templates/observations.tpl.csv` only when no packaged probe can represent the source. Validate
manual rows by passing them through `merge_observations.py`.

For verified gaps in generated rows, copy `templates/observation-annotations.example.json`, match
all stable-key fields, and apply it with `scripts/enrich_observations.py`. An annotation may change
only evidence-owned fields and must append its own method, timestamp, and note without replacing the
original provenance.

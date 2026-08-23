---
title: ADR-0005 — Qualified SQL relations in PolarsEngine | DataCoolie
description: How Delta paths and Iceberg catalog identifiers map to portable 1-4 part SQL table names without eagerly registering every table.
---

# ADR-0005 — Qualified SQL relations in PolarsEngine

**Status** · Accepted

## Context

Polars `SQLContext` maps frames to flat relation names, while portable
DataCoolie SQL may refer to `catalog.database.schema.table` or any unique
suffix of that name. Delta discovers tables from paths; Iceberg discovers
them from a catalog and namespace. Flattening these identities with a user
separator loses hierarchy, creates collisions, and eagerly creates scans for
tables a query may never use.

## Decision

`PolarsEngine` keeps one canonical logical identifier containing one to four
components. Delta paths and Iceberg catalog identifiers use separate discovery
adapters but produce common lazy relation descriptors.

Registration with `preload=False` indexes descriptors only. On SQL execution,
the engine parses external table sources with the optional SQLGlot dependency,
resolves each unique logical suffix, creates the referenced `LazyFrame` once,
and registers it under a deterministic private SQLContext alias. Later queries
reuse that registration for the engine lifetime.

Short names are never selected implicitly when ambiguous. `include` and
`exclude` use component-aware globs: `*` stays inside one component, `**`
crosses zero or more components, and exclude wins.

There is no public alias separator or `max_sql_name_levels`. Users control the
canonical name with the physical discovery root and `logical_prefix`; the
suffix index supplies the 4/3/2/1-part query forms automatically.

SQL rewriting replaces only parser-verified source-name spans in the original
text. It does not serialize or transpile the complete SQL AST.

## Compatibility

- `register_table` and raw one-part SQL remain supported.
- Registration methods continue returning `list[str]`.
- `logical_prefix` is the only root-mapping API; there is no flat-prefix or
  physical-separator compatibility layer.
- Discovery errors raise by default; `on_error="skip"` provides observable
  best-effort discovery.
- Indexed relations are visible through `PolarsEngine.execute_sql`. Consumers
  that execute directly on `engine.sql_context` use `preload=True`.
- SQLGlot belongs to the `polars-sql` extra, not the basic `polars` extra.

## Consequences

- Delta and Iceberg share SQL behavior without pretending their discovery APIs
  are identical.
- Registering a broad root remains cheap relative to eager scan creation, and
  repeated queries reuse already-bound frames.
- Ambiguity becomes an explicit, diagnosable error.
- Qualified SQL users install one additional optional dependency.
- Registry and SQLContext mutations require synchronization so concurrent
  first use of one table registers it only once.

## Related

- [Concepts · Engines](../concepts/engines.md)
- [API reference · Engines](../reference/api/engines.md)

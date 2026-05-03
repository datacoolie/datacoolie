---
title: ADR-0003 — Transformer Ordering Slots | DataCoolie
description: Why DataCoolie assigns numeric ordering slots to built-in transformers so custom transformers can compose predictably.
---

# ADR-0003 — Number-slot transformer ordering

**Status** · Accepted

## Context

Transformers must run in a **specific order** to be correct:

- Schema conversion must precede deduplication (so keys are the right type).
- User-configured `ColumnAdder` must precede `SCD2ColumnAdder` so that any
  calculated columns are already present when SCD2 validity columns are
  derived from the source effective-date column.
- SCD2 validity columns must be populated before `SystemColumnAdder` so that
  `__valid_from` mirrors the business effective date and isn't confused with
  the later framework audit timestamps.
- Column name sanitization must run last (or partition columns get
  renamed after the partition spec is fixed).

Alphabetical ordering and dependency graphs were both tried and both broke
when plugins entered the picture — plugins can't know to alphabetically
sort between `ColumnAdder` and `SystemColumnAdder`.

## Decision

Each transformer declares an **integer `order`**. Slots:

| Slots | Owner |
|---|---|
| 0–9 | Reserved (future framework pre-cast work) |
| 10 | `SchemaConverter` |
| 20 | `Deduplicator` |
| 30 | `ColumnAdder` |
| **40–50** | **User plugins** |
| 60 | `SCD2ColumnAdder` |
| 70 | `SystemColumnAdder` |
| 80 | `PartitionHandler` |
| 90 | `ColumnNameSanitizer` |
| 100+ | Reserved (future framework post-sanitize work) |

`build_pipeline` sorts by `order`; ties broken by class name for
determinism.

## Consequences

- Plugins have a clearly reserved range (40–50) with room for two
  plugins to coexist without colliding.
- Framework updates that insert new built-in transformers must choose an
  unused slot and not shift existing ones.
- Renumbering slots is a **breaking change** — ADR supersession required.

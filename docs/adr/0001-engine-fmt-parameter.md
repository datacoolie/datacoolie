# ADR-0001 — Engine `fmt=` parameter across format-aware methods

**Status** · Accepted

## Context

Early iterations of the engine API had separate methods per format
(`read_parquet`, `read_delta`, `read_iceberg`, `read_csv`, …) and
corresponding writer variants (`write_parquet`, `write_delta`, …).
Destinations and sources dispatched on format by branching at call sites.

This made two things hard:

1. **Adding a new format** required touching every destination and source.
2. **Table-aware methods** (`read_table`, `table_exists_by_name`,
   `merge_to_table`) had no natural place for Iceberg — it was a
   copy-paste of the Delta variant with small changes.

## Decision

Format-aware methods accept a **`fmt` string** parameter. The engine
dispatches internally:

```python
def read_path(self, path: str, fmt: str, options=None):
def write_to_path(self, df, path, mode, fmt, partition_columns=None, options=None):
def read_table(self, table_name: str, fmt: str = "delta", options=None):
def merge_to_table(self, df, table_name, merge_keys, fmt: str = "delta", options=None):
def table_exists_by_name(self, table_name: str, *, fmt: str = "delta") -> bool:
```

`table_exists_by_name` uses **keyword-only** `fmt` — defensive because
positional args can confuse `table_name` with format.

Supported `fmt` values are listed in `datacoolie.core.constants.Format`.

## Consequences

- Adding a new format is a **single engine change** plus destination/source
  plugins.
- Removes ~500 lines of dispatch boilerplate from destinations/sources.
- `fmt` is now the canonical term throughout docstrings, logs, and metadata
  — never "format" or "type".

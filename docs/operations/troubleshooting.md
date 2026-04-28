# Troubleshooting

## "`pl.sql_expr` unknown function …"

**Polars** uses a SQL subset. Common offenders:

| Doesn't work in Polars | Use |
|---|---|
| `current_timestamp()` | Literal cast, or framework-added `__updated_at`. |
| `date_format(col, 'yyyy-MM-dd')` | `CAST(col AS DATE)`. |
| `year(col)` | `EXTRACT(YEAR FROM col)`. |

See [Partitioning](../how-to/partitioning-and-sanitization.md#sql-expression-portability).

## Row count mismatch for multi-line JSON

JSONL treats each line as a record; JSON arrays treat each element as a
record. If the source file has *embedded newlines inside string fields*:

- `format: "jsonl"` → one row per file line → **wrong**.
- `format: "json"` → one row per array element → correct.

## Excel `is_active` column loads everything as inactive

The Excel generator leaves `is_active` blank when unset, which is correctly
parsed as `None` → defaults to `True`. If someone hand-typed `FALSE` in the
cell it is parsed as `False`. Fix: blank the cell.

## "dead lock detected" on Delta optimize

Two dataflows writing the same destination are running `OPTIMIZE` in parallel.
DataCoolie deduplicates maintenance by destination automatically — if you're
seeing this, you probably called `run_maintenance()` twice in quick
succession from an external scheduler. Serialise the scheduler side.

## Docker preflight fails on scenarios

Some scenarios (database metadata, API metadata) require Docker. Run:

```powershell
docker compose -f datacoolie/usecase-sim/docker/docker-compose.yml up -d
```

Then re-run the scenario.

## `fmt='parquet'` Iceberg writes not appearing in Athena

Iceberg needs Glue catalog registration. Set `register_symlink_table = true`
or use the engine's `register_iceberg_table` helper explicitly.

## `WatermarkManager` throws on first run

First run has no persisted watermark — `get_watermark()` returns `None`. If
your source reader crashes on `None`, it's a bug in the reader: it must
treat `None` (or empty dict after decoding) as "no filter, read everything".

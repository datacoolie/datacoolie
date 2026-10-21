---
title: Incremental reads and look-back | DataCoolie
description: Configure incremental reads and look-back with metadata, examples and backend-specific guidance.
---

# Incremental reads and look-back

[All source recipes](source-patterns.md).

## Incremental reads with `watermark_columns`

For any source type, add `watermark_columns` to the `source` block to enable
incremental loading:

```json
"source": {
  "connection_name":   "postgres_src",
  "schema_name":       "public",
  "table":             "orders",
  "watermark_columns": ["updated_at"]
}
```

The first run has no stored lower bound, unless a configured initial range or
replay window supplies one. Later reads use the stored watermark and any
configured look-back. A database reader adds a SQL watermark condition; other
readers use different mechanisms. Checkpoint advancement also differs: most
readers use the maximum observed watermark, while canonical API bindings use
their configured `watermark_value` (`observed_max` or `request_end`).

Behavior differs by source family:

| Source family | Watermark behavior |
|---------------|--------------------|
| Parquet / Delta / Iceberg | Engine DataFrame filter after read |
| Database | `WHERE` clause pushed to SQL; query mode filters its returned columns |
| API | Fields with an active `range_param_mapping` binding are pushed into request params/body; unmapped fields are filtered in the engine after fetching |
| File formats | `__file_modification_time` can select files during listing; row-column watermarks filter in the engine after read |
| Python function | Incremental watermarks are passed into the function first; bounded `read_range` is a separate source-owned contract, then the framework filters again after the function returns |

API request mapping can reduce remote fetches; without it, local filtering
does not prevent a full API fetch. See [API watermark push-down](source-api.md#push-down-and-split-watermark-ranges)
and [Concepts · Watermarks · Storage ownership and path binding](../../reference/concepts/watermarks.md#storage-ownership-and-path-binding)
for watermark storage and provider interaction.

## Incremental windows and look-back

An incremental source normally starts at the saved watermark. A look-back
reopens part of that range so late-arriving or corrected records can be read
again. The runtime-computed property is called `date_backward`; do not author
`date_backward` in metadata. Author one of the supported `backward_*` fields or
the nested `backward` object in `source.configure`.

### Choose a fixed look-back

Put a reusable default in [`connections[].configure`](../../reference/metadata-schema.md#connectionsconfigure):

```json
{
  "connections": [
    {
      "name": "orders_source",
      "connection_type": "database",
      "format": "sql",
      "configure": {
        "database_type": "postgresql",
        "url": "ORDERS_DATABASE_URL",
        "backward_days": 3
      },
      "secrets_ref": {"env:": ["url"]}
    }
  ]
}
```

The supported top-level shorthand fields are:

| Authored field | Effective meaning |
|---|---|
| `backward_hours` | Subtract hours from the saved datetime watermark |
| `backward_days` | Subtract days from the saved watermark |
| `backward_months` | Subtract calendar months, with calendar-safe clamping |
| `backward_years` | Subtract calendar years, with calendar-safe clamping |
| `backward_closing_day` | Use the closing-day strategy with that day of the month |

You can express the same values in one nested object:

```json
{
  "source": {
    "connection_name": "orders_source",
    "table": "orders",
    "configure": {
      "backward": {
        "days": 3,
        "hours": 6
      }
    }
  }
}
```

The nested keys are `years`, `months`, `days`, `hours`, and `closing_day`.
Use one clear strategy per connection unless you intentionally need combined
fixed offsets such as three days plus six hours. If the nested object repeats
a unit also supplied by a shorthand field, the nested value wins during
parsing.

On the first run, there is no saved watermark to adjust, so the look-back has
no effect until a saved watermark exists. Non-datetime watermark values pass through
unchanged. When a file reader has a date-folder watermark, the offset applies
**only** to that folder key; `__file_modification_time` remains anchored to its
saved value. Other readers adjust datetime values in the stored watermark. See
[Late-arriving and updated files](late-arriving-files.md) for the two-stage
folder and mtime selection.

### Override the connection for one source

A look-back in [`source.configure`](../../reference/metadata-schema.md#dataflowssourceconfigure)
overrides the entire connection-level look-back when the source has a
non-empty backward configuration:

```json
{
  "source": {
    "connection_name": "orders_source",
    "table": "orders",
    "watermark_columns": ["updated_at"],
    "configure": {
      "backward_hours": 12
    }
  }
}
```

This source uses twelve hours, not “the connection's three days plus twelve
hours”. If a source should inherit the connection default, omit its backward
fields. If it should have a combined offset, author the complete combination
at the source level:

```json
{
  "source": {
    "connection_name": "orders_source",
    "table": "orders",
    "watermark_columns": ["updated_at"],
    "configure": {
      "backward": {"days": 2, "hours": 6}
    }
  }
}
```

### Use a closing-day strategy for monthly corrections

`closing_day` computes an absolute start boundary from the current date rather
than subtracting a fixed number of days. It is useful when upstream closes a
period on a known day of each month:

```json
{
  "source": {
    "connection_name": "orders_source",
    "table": "orders",
    "watermark_columns": ["updated_at"],
    "configure": {
      "backward": {"closing_day": 10}
    }
  }
}
```

When `closing_day` is present, it takes priority over fixed offset keys. The
optional `months` and `years` values can move the closing-day boundary farther
back. Use a day valid for the upstream business calendar and test a boundary
around month/year changes before production use.

### Combine look-back with watermark-window replacement

Window replacement combines a source watermark and authored look-back with
the [Destination](../../reference/metadata-schema.md#destination) setting
[`destination.configure.replace_by_watermark`](../../reference/metadata-schema.md#dataflowsdestinationconfigure)
under `load_type: merge_overwrite`. The effective source window must cover the
destination delete scope. The source must return every row to retain in that
scope and preserve or deterministically map the watermark output column. With
a usable replacement window, `merge_keys` are not required; the key-based
fallback needs them.

The [Cross-boundary combinations](../../reference/metadata-schema.md#cross-boundary-combinations)
section collects these source and destination requirements.

A source read that returns `None` or `rows_read == 0` never enters replacement,
including during a bounded replay, so it cannot delete the selected scope. If
the non-empty source result becomes empty during transformation, the writer
still receives that result and replacement may delete the selected scope.
Follow the [complete window recipe](watermark-window-replacement.md) for its
assumptions, first run, output mapping, and delete/append failure boundary.

### Relate look-back to API ranges and replay

Look-back changes the lower bound derived from the saved watermark. API range
splitting separately divides a `[from, to)` interval into requests; configure
the selected field's `range_param_mapping` and the canonical range interval
controls on the API source. See [Push down and split watermark ranges](source-api.md#push-down-and-split-watermark-ranges)
for timezone and inclusive-upper-bound handling.

Replay supplies an explicit bounded range and can cap an API range's upper
bound. It is an operational choice, not a replacement for ordinary look-back.
Keep replay configuration in the operations guide and use this section only
to understand how the source's effective lower bound is formed.

### Validate an incremental configuration

- Set `watermark_columns` and confirm every selected column is returned by the
  table/query/API response.
- Choose either connection inheritance or a complete source override.
- Do not author computed `date_backward`.
- For `closing_day`, test month-end, leap-year, and upstream period-boundary
  behavior.
- For window replacement, pair `merge_overwrite` with
  `replace_by_watermark: true` and a usable look-back.
- Confirm the source returns complete window coverage and preserves watermark
  columns.
- Test first run, normal empty increment, and explicit replay separately.

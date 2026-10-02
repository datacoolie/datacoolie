---
title: Replace a complete watermark window — DataCoolie User Guide
description: Combine a complete source window, look-back, and merge_overwrite to reflect corrections and deletions.
---

# Replace a complete watermark window

Use this pattern when an upstream source can return the **complete current
state** for every record in a bounded watermark interval, including unchanged
records that must remain. A change-only feed or an API response stopped by
`max_pages` cannot safely replace that interval. Learn the individual
[watermark and look-back](source-patterns.md#incremental-windows-and-look-back),
[source](source-patterns.md) and
[destination strategy](destination-and-load-patterns.md#replace-by-watermark)
first. Exact shapes are in the [Source](../../reference/metadata-schema.md#source)
and [Destination](../../reference/metadata-schema.md#destination) reference.

## Configure the combined window

This is one dataflow fragment; `orders_source` must return all rows currently
belonging to the requested interval. `orders_delta` is an existing Delta
destination after its first successful load.

```json
{
  "name": "orders_window",
  "stage": "daily",
  "source": {
    "connection_name": "orders_source",
    "table": "orders",
    "watermark_columns": ["updated_at"],
    "configure": {"backward_days": 3}
  },
  "destination": {
    "connection_name": "orders_delta",
    "table": "orders",
    "load_type": "merge_overwrite",
    "configure": {"replace_by_watermark": true}
  }
}
```

The source or its connection needs an authored look-back so the read covers
the deletion scope. `date_backward` is computed at runtime; it is not a JSON
field. With a usable replacement window, the engine deletes all target rows
within the mapped watermark bounds and appends the returned rows. `merge_keys`
are unnecessary for that window path. If an existing destination has no
usable window, the key-based `merge_overwrite` fallback requires `merge_keys`.

For example, suppose the target has A, B and C in the interval. The source's
next complete interval contains corrected A and unchanged B but no C. Window
replacement leaves A and B, removing C. A key-based merge receiving A and B
cannot discover C's deletion.

## Preserve the watermark through transforms

The final output must retain the active watermark column or have a
deterministic mapping to its renamed output column. A projection that drops it,
an unknown mapping, or a collision between mapped watermark columns fails
before destination mutation. Review
[projection and renaming](transform-patterns.md#columnprojector) and
[sanitization](transform-patterns.md#columnnamesanitizer) together with this
window. A deterministic rename is permitted when the runtime can map it.

## First run, empty reads, and recovery

An absent destination is initialized with an overwrite-style write. A normal
empty incremental read skips replacement. An explicitly bounded empty replay
can delete the specified target window; an empty window on an absent target
does not create an unrelated table. Replay boundaries and commands belong to
[Operations](../operations/replay-and-backfill.md).

The default engine operation deletes the bounded rows and then appends the
batch; it is not guaranteed atomic. If append fails after deletion, recover
using the source's complete interval and a deliberately bounded replay. Verify
the provider can produce that complete interval before using this strategy.

---
title: Incremental API with pagination — DataCoolie User Guide
description: Combine API watermark ranges, pagination, look-back and idempotent destination loading.
---

# Incremental API with pagination

Use this pattern when an API accepts both lower and upper time parameters and
returns a stable, paginated result for each interval. Configure connection
[authentication](connections.md#api-authentication), then use
[API source configuration](source-patterns.md#api-source-configuration) for each request and
pagination setting. The [Source reference](../../reference/metadata-schema.md#source)
defines their types and options.

## Combine ranges, pages, and look-back

This dataflow fragment assumes `orders_api` is a configured API connection and
`orders_delta` is a Delta lakehouse connection. The API accepts `updated_since`
as an inclusive lower bound and `updated_before` as an exclusive upper bound;
it returns a stable array at `data.items` and a numeric `meta.total` per
interval. Verify those provider assumptions before using the example.

```json
{
  "name": "api_orders_incremental",
  "stage": "daily",
  "source": {
    "connection_name": "orders_api",
    "table": "orders",
    "watermark_columns": ["updated_at"],
    "configure": {
      "endpoint": "/orders",
      "watermark_param_mapping": {"updated_at": "updated_since"},
      "watermark_to_param": "updated_before",
      "watermark_range_start": "2026-01-01T00:00:00Z",
      "watermark_range_interval_unit": "day",
      "watermark_range_interval_amount": 1,
      "watermark_range_max_workers": 2,
      "backward_days": 2,
      "pagination_type": "offset",
      "data_path": "data.items",
      "total_path": "meta.total",
      "page_size": 200,
      "max_pages": 1000,
      "offset_max_workers": 2
    }
  },
  "destination": {
    "connection_name": "orders_delta",
    "table": "orders",
    "load_type": "merge_upsert",
    "merge_keys": ["order_id"]
  }
}
```

On the first run, `watermark_range_start` supplies the lower bound. After a
watermark is saved, the two-day source look-back reopens earlier updates
before the reader splits `[from, to)` into daily intervals. Each interval has
its own pages. This intentionally rereads rows; `merge_upsert` updates by
`order_id` rather than blindly appending duplicates. It does not reflect a
source-side deletion that the API never sends. Check that the provider's
offset parameter means **record offset**, not page number, and that it returns
the complete interval at the chosen page limit. See
[offset pagination](source-patterns.md#offset-pagination-with-provider-specific-parameter-names)
and [incremental windows](source-patterns.md#incremental-windows-and-look-back).

## Boundary and completeness checks

This example uses the **legacy incremental split** fields. Set
`watermark_range_to_exclusive_offset` only when that endpoint's upper bound is
inclusive and its precision is known. `watermark_range_start` is the first-run
lower bound for this legacy mode. For bounded replay, declare canonical
`range_param_mapping` instead; do not use legacy split fields or their offset
to emulate an exclusive boundary. Choose `watermark_value: "observed_max"`
when the response contains the source column, or `"request_end"` when the
endpoint confirms the exclusive covered end. A request-end continuation
resumes with `>=`; an observed maximum resumes with `>`.
The mapped selection field may be different from the persisted
`source.watermark_columns`; configure both lower and upper bindings for that
field, and remember that saving a replay still persists only authored
watermark columns.
If pagination reaches `max_pages` while a cursor or next link remains, or an
offset page is still full at the cap, the read fails instead of returning a
partial result. With `total_path`, configure the provider's exact record count;
the reader rejects a count above the page budget or a fetched count that does
not match the declared total.

Canonical bounded API reads preserve the source value type and declared wire
precision. A finite integer interval is one exact bounded read; use a replay
`chunk_interval` with `{"step": ...}` only when you intentionally want
multiple numeric chunks. Date and datetime values must match the binding's
precision, and an unrepresentable fractional value fails before HTTP.

The API reader accepts `offset`, `cursor`, and `next_link` pagination. Omit the
field (or set it to null) for one response; an unsupported value fails before
authentication or the first API request. For `next_link`, relative URLs are
resolved against the current page and every continuation must remain on the
configured HTTP(S) origin. Foreign hosts or ports, scheme downgrades, URL
userinfo, and non-HTTP(S) links fail without dispatching a credential-bearing
request. The full continuation URL is opaque and followed verbatim by
default. Use `next_link_bound_mode: "repeat_query_bounds"` only when the
endpoint explicitly requires the original range parameters to be repeated;
matching values are checked and conflicting or duplicate bindings fail.

Range calls may run concurrently, and offset pagination with `total_path`
can run page calls concurrently within each range. Here the two worker limits
can create multiple simultaneous requests. `rate_limit_delay` only delays
sequential page fetching; it is not a global limiter. Reduce worker limits
to meet the provider's rate contract. For source watermark state and replay
boundaries see [Operations](../operations/replay-and-backfill.md).

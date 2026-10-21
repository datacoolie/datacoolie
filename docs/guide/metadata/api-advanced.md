---
title: Incremental API with pagination — DataCoolie User Guide
description: Combine canonical API ranges, pagination, look-back and idempotent destination loading.
---

# Incremental API with pagination

Use this pattern when an API accepts lower and upper range parameters and
returns a stable, paginated result for each interval. Configure connection
[authentication](connections.md#api-authentication), then use
[API source configuration](source-api.md#api-source-configuration) for the
request and pagination settings. The [Source reference](../../reference/metadata-schema.md#source)
defines their types and options.

The examples use the canonical-only API range contract targeted for unreleased
`0.2.0`. Verify the installed runtime and schema before an operational cutover;
this page does not claim a public release or publication.

## Combine ranges, pages, and look-back

This dataflow fragment assumes `orders_api` is a configured API connection and
`orders_delta` is a Delta lakehouse connection. The endpoint accepts
`updated_since` as an inclusive lower bound and `updated_before` as an
exclusive upper bound. It returns a stable array at `data.items` and a numeric
`meta.total` per interval. Verify those provider assumptions before using the
example.

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
      "range_param_mapping": {
        "updated_at": {
          "lower": {"location": "params", "name": "updated_since", "operator": ">="},
          "upper": {"location": "params", "name": "updated_before", "operator": "<"},
          "format": "iso",
          "response_column": "updated_at",
          "watermark_value": "observed_max"
        }
      },
      "watermark_to_param_timezone": "Asia/Ho_Chi_Minh",
      "watermark_range_start": "2026-01-01T00:00:00+07:00",
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
watermark is saved, the two-day source look-back reopens earlier updates before
the reader splits `[from, to)` into daily intervals. Each interval has its own
pages. This intentionally rereads rows; `merge_upsert` updates by `order_id`
rather than blindly appending duplicates. It does not reflect a source-side
deletion that the API never sends. Check that the provider's offset parameter
means **record offset**, not page number, and that it returns the complete
interval at the chosen page limit. See [offset pagination](source-api.md#offset-pagination-with-provider-specific-parameter-names)
and [incremental windows](source-incremental.md#incremental-windows-and-look-back).

## Boundary and completeness checks

`range_param_mapping` is per selected response field. Each entry can use a
parameter-name string for `lower` or `upper`, or an object with `name`,
`location` (`params` or `body`), and an explicit endpoint `operator`. The
binding's `format` keeps the schema-supported temporal forms and integer
aliases: `integer`, `int`, `native_integer`, and `number`. Do not use a
runtime-only alias; declare `format` in the binding.

`watermark_value: "observed_max"` saves the maximum returned response value;
the binding needs a non-null `response_column`. `watermark_value: "request_end"`
saves the exclusive upper bound only when the endpoint has
confirmed complete coverage and pagination. An observed maximum continues with
`>`, while a saved covered request end continues with `>=`. Active fields with
mixed state meanings are rejected before HTTP.

`request_end` requires a concrete request whose end was actually covered. An
unbounded first read does not synthesize that checkpoint: bootstrap with
explicit bounds/replay save, or with the existing split configuration plus
`watermark_range_start`. A first-run `observed_max` is derived normally from
the returned rows. Regenerate any derived YAML/Excel metadata from the updated
source document before validation or execution; this cutover does not rewrite
already-generated metadata. See the
[file metadata workflow](../providers/file.md).

For bounded replay, the effective range is `[start, end)`. If an endpoint only
offers an inclusive upper operator (`<=`), declare that real operator and use
`response_column` for the residual `< end` check. Inclusive endpoint operators
plus residual filtering preserve the boundary; do not subtract an epsilon.
If the endpoint cannot return the required boundary, or the response cannot be
filtered exactly, the bounded request must be rejected before HTTP. A page cap,
request error, or incomplete pagination is not evidence that a `request_end`
interval was covered.

The selected range field may differ from the persisted
`source.watermark_columns`. Configure a `range_param_mapping` entry for the
selection field; only authored watermark columns are persisted as state. Do
not treat a maximum from a slice as proof that another persisted watermark's
history was completely read.

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
request. The full continuation URL is opaque and followed verbatim by default.
Use `next_link_bound_mode: "repeat_query_bounds"` only when the endpoint
explicitly requires the original range parameters to be repeated; matching
values are checked and conflicting or duplicate bindings fail.

Range calls may run concurrently, and offset pagination with `total_path` can
run page calls concurrently within each range. Here the two worker limits can
create multiple simultaneous requests. `rate_limit_delay` only delays
sequential page fetching; it is not a global limiter. Reduce worker limits to
meet the provider's rate contract. For source watermark state and replay
boundaries see [Operations](../operations/replay-and-backfill.md).

!!! warning "Manual cutover and state meaning"
    Treat the canonical-only change as a manual operator cutover. A lower-only
    historical range describes an `observed_max`; a lower-plus-upper range may
    describe a covered `request_end` only after endpoint completeness and
    checkpoint meaning are verified. Do not reset, coerce, or infer state from
    a scalar's shape. If precision or completeness is uncertain, pause
    continuation and run a controlled, operator-approved replay for a verified
    interval. Rolling back the binary or metadata is not a rollback of already
    committed destination rows or saved state; reconcile or replay from
    operator-held snapshots instead.

### Removed configuration during cutover

The unreleased `0.2.0` source contract rejects the presence of
`watermark_param_mapping`, `watermark_to_param`, `watermark_param_location`,
`watermark_param_format`, or `watermark_range_to_exclusive_offset` in
`source.configure`, including null or empty values. These names are not
supported configuration and are not automatically converted. Replace them
manually with `range_param_mapping`, verify the endpoint operators and state
meaning, then validate before sending an HTTP request.

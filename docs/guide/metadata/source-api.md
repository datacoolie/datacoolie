---
title: REST API sources | DataCoolie
description: Configure rest api sources with metadata, examples and backend-specific guidance.
---

# REST API sources

[All source recipes](source-patterns.md).

## REST API source

Use when reading JSON records from a REST API, with or without pagination.
For request-shape fields, see [Connection settings by endpoint type](../../reference/metadata-schema.md#connection-settings-by-endpoint-type)
and [`source.configure`](../../reference/metadata-schema.md#dataflowssourceconfigure)
in the Metadata reference.

```json
{
  "name":            "orders_api",
  "connection_type": "api",
  "format":          "api",
  "configure": {
    "base_url":        "https://api.example.com/v1",
    "auth_type":       "bearer",
    "auth_token":      "DC_ORDERS_API_TOKEN",
    "timeout":         30,
    "default_headers": { "Accept": "application/json" }
  },
  "secrets_ref": {
    "env:": ["auth_token"]
  }
}
```

Put request shape and pagination on the **source**, not on the connection:

```json
"source": {
  "connection_name":   "orders_api",
  "table":             "orders",
  "watermark_columns": ["updated_at"],
  "configure": {
    "endpoint":        "/orders",
    "method":          "GET",
    "params":          { "status": "open" },
    "pagination_type": "offset",
    "page_size":       200,
    "max_pages":       1000,
    "total_path":      "meta.total",
    "data_path":       "data.items"
  }
}
```

This example expects a response shaped like
`{"meta":{"total":2},"data":{"items":[{"id":1,"updated_at":"2026-01-01T00:00:00Z"},{"id":2,"updated_at":"2026-01-02T00:00:00Z"}]}}`.
Check `data_path` against a real response: a missing path produces zero records,
not a path-validation error. Omit `pagination_type` and `total_path` for a
single-response endpoint. `watermark_columns` alone filters rows **after**
fetching; it does not make the API return only changed records. Use
[watermark push-down](source-api.md#push-down-and-split-watermark-ranges) when the endpoint accepts
incremental parameters. The API source also applies `source.filter_expression`
locally to the returned DataFrame before transforms; it does not add the
predicate to request parameters or body.

If `pagination_type` is present, it must be `offset`, `cursor`, or `next_link`.
An omitted or null value means one response. A typo or unsupported value fails
before credentials are resolved or an API request is sent, so an incremental
run cannot advance its watermark without a known completion contract.

With `total_path`, offset pages are fetched concurrently (four workers by
default), and `rate_limit_delay` is **not** applied. `max_pages` is a safety
budget: if the response still requires another page at that limit, the read
fails instead of returning a partial result. Match `page_size`, `max_pages`, and
`offset_max_workers` to the provider's contract and rate limit. For sequential
offset requests, omit `total_path`; then `rate_limit_delay` applies between
pages. See [pagination contracts](source-api.md#choose-the-pagination-contract).

| Connection-level key | Purpose |
|----------------------|---------|
| `base_url` | Required root URL |
| `auth_type` | `bearer`, `basic`, `api_key`, `oauth2_client_credentials`, `aws_sigv4` |
| `auth_token` / `username` / `password` / `api_key_*` | Auth credentials |
| `token_url`, `client_id`, `client_secret` | OAuth2 client-credentials flow |
| `default_headers`, `timeout` | Shared request defaults |

| Source-level key | Purpose |
|------------------|---------|
| `endpoint` | Path appended to `base_url` |
| `method`, `params`, `body` | Request shape |
| `pagination_type` | `offset`, `cursor`, or `next_link` |
| `page_size`, `max_pages`, `data_path` | Response traversal and size |
| `total_path`, `offset_max_workers` | Parallel offset pagination |
| `rate_limit_delay`, `max_retries` | Delay between sequential pages; retry HTTP 429 responses |
| `range_param_mapping` | Canonical per-field lower/upper API bindings, operators, format, residual response field, and state meaning |
| `watermark_to_param_timezone` | Source override for generated API upper bounds and watermark advancement |
| `watermark_range_interval_unit`, `watermark_range_interval_amount`, `watermark_range_start`, `watermark_range_max_workers` | Canonical incremental range splitting and its first-run/worker controls |

`source.filter_expression` is deliberately absent from the request table. Put
server-side filters in the endpoint-specific `params` or `body` when the API
supports them, and use the source predicate for a framework-side safety filter
over the returned columns.

The table above is the basic API shape. Use
[Connections](connections.md#api-authentication) for authentication and the
[API source configuration](source-api.md#api-source-configuration) section below for all
request, pagination, timezone, and split-range variants. For a combined example see
[Incremental API with pagination](api-advanced.md).

For API incremental loading, choose between local filtering, request
watermark push-down, and split ranges. The complete field combinations,
timezone handling, and inclusive-boundary rules are documented in
[API source configuration](source-api.md#api-source-configuration), especially
[Push down and split watermark ranges](source-api.md#push-down-and-split-watermark-ranges).

## API source configuration

This section owns the request, response, pagination, and API watermark fields
that belong to `source.configure`. [Connections](connections.md#api-authentication)
owns the base URL, authentication, and secrets. The exact source field types
and allowed values are in the [Source reference](../../reference/metadata-schema.md#source)
and [`source.configure`](../../reference/metadata-schema.md#dataflowssourceconfigure).

An authenticated source can use any supported pagination and watermark mode.
The examples below are source fragments, not complete metadata documents.

| Decision | Choose | Where to configure |
|---|---|---|
| Response | Root JSON list, nested list, or one nested object | `source.configure.data_path` when not a root list |
| Pagination | One response, record offset, cursor, or next-link | `source.configure.pagination_type` and its matching path/parameter keys |
| Incremental read | Fetch then filter, API push-down, or bounded split ranges | `source.watermark_columns` plus the applicable `source.configure` keys |

### Match the request and response shape

`source.configure.method` is an HTTP method string (`GET` by default), not a
DataCoolie enum. `params` becomes URL query parameters and `body` becomes a
JSON request body. For example, a POST endpoint that returns one JSON object
under `result` can use:

```json
{
  "source": {
    "connection_name": "orders_api_oauth",
    "table": "order_summary",
    "configure": {
      "endpoint": "/reports/orders",
      "method": "POST",
      "params": {"region": "west"},
      "body": {"status": "open"},
      "data_path": "result"
    }
  }
}
```

The response `{"result":{"count":12}}` becomes one record. With no
`data_path`, the response must be a root-level JSON list. A wrong path yields
zero records. These keys belong to the source because different endpoints on
one connection may use different request and response shapes.

### Choose the pagination contract

Pagination settings belong to `source.configure` because different endpoints
on the same API may return different response shapes. For every paginated
mode, choose `max_pages` large enough for the intended result. Reaching the cap
while another page is required raises a source error instead of returning
partial data. Use a provider-defined stable sort or snapshot when available,
especially for concurrent offset requests against changing data.

#### One response (no pagination)

Omit `pagination_type` for an endpoint that returns all records in one
response. For a root-level list, omit `data_path` too; for a nested list, set
`data_path` to its dot-separated path. The reader makes one request and does
not follow a cursor, link, or offset unless a pagination mode is configured.

#### Offset pagination with provider-specific parameter names

```json
{
  "source": {
    "connection_name": "orders_api_key",
    "table": "orders",
    "configure": {
      "endpoint": "/orders",
      "pagination_type": "offset",
      "page_size": 200,
      "offset_param": "skip",
      "limit_param": "per_page",
      "total_path": "meta.total",
      "data_path": "data.items",
      "offset_max_workers": 4
    }
  }
}
```

With `total_path`, the first response provides the total and remaining pages
can be fetched concurrently. Without it, offset pages are fetched
sequentially until a short page or empty response is returned. The reader
sends **record offsets** (`0`, `200`, `400`, ... in this example), not page
numbers. A provider whose `page` parameter expects `1`, `2`, `3`, ... does not
fit this built-in offset mode; do not merely rename `offset_param` to `page`.

The first response above must contain a numeric `meta.total` and an array at
`data.items`. A wrong `data_path` is treated as an empty result, so test it
against a real response. If `total_path` is missing or non-numeric, the
concurrent path fails. Setting `total_path` also enables up to four concurrent
offset workers by default; `rate_limit_delay` only delays **sequential** page
requests. `max_pages` defaults to 1,000 and is a safety budget: the reader
fails when the declared total needs more pages than that budget or when the
aggregate fetched count does not equal the declared total. `max_retries`
retries HTTP 429 responses, not arbitrary HTTP failures.

#### Cursor pagination

```json
{
  "source": {
    "connection_name": "orders_api_oauth",
    "table": "orders",
    "configure": {
      "endpoint": "/orders",
      "pagination_type": "cursor",
      "page_size": 100,
      "cursor_path": "paging.next_cursor",
      "cursor_param": "after",
      "data_path": "results"
    }
  }
}
```

The reader sends the returned cursor under `cursor_param`. The default response
path is `next_cursor` and the default request parameter is `cursor`. The
authored request options are grouped under
[`source.configure`](../../reference/metadata-schema.md#dataflowssourceconfigure).

#### Next-link pagination

```json
{
  "source": {
    "connection_name": "orders_api_basic",
    "table": "orders",
    "configure": {
      "endpoint": "/orders",
      "pagination_type": "next_link",
      "next_link_path": "paging.next",
      "data_path": "data"
    }
  }
}
```

When the response contains an absolute next-link URL, the reader follows it
verbatim and uses the link's parameters. The continuation token is opaque: the
reader does not decode it or append the original query bounds by default. Set
`next_link_bound_mode: "repeat_query_bounds"` only for an endpoint whose
contract explicitly requires the original range parameters on every
continuation; matching values are checked and conflicting/duplicate values
fail before the request. It stops when the configured path is empty.
Relative links are resolved against the current page URL. A continuation must
stay on the configured HTTP(S) origin (scheme, host, and effective port); a
foreign host or port, scheme downgrade, userinfo, or non-HTTP(S) link fails
before the next request. Connection credentials therefore remain scoped to the
configured origin. Redirect following is disabled for the API client, so a
redirect cannot bypass the continuation check. Providers that require
cross-origin continuation need an explicit credential-scoping policy outside
this built-in mode.

### Push down and split watermark ranges

For a bounded replay or another source-owned range, use the explicit
`range_param_mapping` contract. Each field declares its lower and upper API
parameter, the wire format, the response field used for residual filtering,
and how a successful read advances state:

```json
{
  "source": {
    "connection_name": "orders_api",
    "table": "orders",
    "watermark_columns": ["updated_at"],
    "configure": {
      "endpoint": "/orders",
      "range_param_mapping": {
        "updated_at": {
          "lower": {"name": "created_from", "operator": ">="},
          "upper": {"name": "created_before", "operator": "<"},
          "format": "iso",
          "response_column": "updated_at",
          "watermark_value": "observed_max"
        }
      },
      "pagination_type": "next_link",
      "next_link_path": "paging.next"
    }
  }
}
```

`watermark_value: "observed_max"` saves the maximum response value after a
successful read. `watermark_value: "request_end"` saves the exclusive upper
bound that the endpoint confirmed. Omitted incremental lower operators resolve
to `>` for `observed_max` and `>=` for `request_end`; active fields with mixed
semantics are rejected before HTTP. A bounded replay uses `[start, end)` and
requires endpoint operators and a `response_column` that can enforce any
boundary the endpoint cannot enforce itself. If the endpoint uses an inclusive
upper operator (`<=`), keep that operator in the binding and apply the exact
residual `< end` check to `response_column`; do not subtract an epsilon.

A tracked `observed_max` binding must have a non-null `response_column`; the
reader rejects a missing column mapping before HTTP, even when watermark saving
is disabled. A `request_end` binding can omit it when the endpoint enforces the
requested operators exactly and guarantees complete interval pagination.

New bindings encode explicit bounds without rounding. `iso` retains the
datetime offset and microseconds; nonzero ISO fractional digits beyond six
cannot be represented and are rejected. ISO timezone offsets that Python
cannot parse without precision loss are also rejected. `date` requires midnight, `datetime`
requires whole seconds, and `datetime_ms` / `timestamp_ms` require millisecond
alignment. `timestamp` preserves fractional Unix seconds using exact decimal
encoding. For example, `12:30:00.123456` cannot use `datetime_ms`; use `iso` or
supply a bound that is already aligned. Validation happens before HTTP for
both incremental and bounded reads. String lower/upper shorthand and the
schema-supported integer aliases (`integer`, `int`, `native_integer`, and
`number`) remain valid. Declare `format` in the binding; do not use a
runtime-only alias.

When the reader generates an incremental upper bound from the current time,
it chooses a ceiling compatible with the binding's precision and logical
date/datetime type before creating requests. Split requests and `request_end`
state use that same covered ceiling. Explicit bounds and stored lower values
are never rounded to make them fit.

The mapped field does not have to be one of `source.watermark_columns` when it
is used only for bounded selection. For example, set `chunk_column="created_at"`
and define `range_param_mapping.created_at` while keeping
`watermark_columns: ["updated_at"]`. Replay sends the `created_at` bounds and
the API reader continues to calculate/persist state only for the configured
watermark columns. A maximum observed from that slice must not be treated as
proof that the complete `updated_at` history was read.

Set a binding's `location` to `params` for query parameters or `body` for a
top-level JSON body field. Nested body paths are outside this generic mapping
contract and require a source-specific adapter.

Canonical incremental splitting uses the same `range_param_mapping` as bounded
reads and replay. Keep the split controls together with a complete lower and
upper binding for the active field:

```json
{
  "source": {
    "connection_name": "orders_api_oauth",
    "watermark_columns": ["updated_at"],
    "configure": {
      "endpoint": "/orders",
      "range_param_mapping": {
        "updated_at": {
          "lower": "updated_since",
          "upper": {"name": "updated_before", "operator": "<"},
          "format": "iso",
          "response_column": "updated_at",
          "watermark_value": "observed_max"
        }
      },
      "watermark_to_param_timezone": "Asia/Ho_Chi_Minh",
      "watermark_range_interval_unit": "day",
      "watermark_range_interval_amount": 1,
      "watermark_range_start": "2026-01-01T00:00:00+07:00",
      "watermark_range_max_workers": 4
    }
  }
}
```

`watermark_range_interval_unit` supports `hour`, `day`, `month`, and `year`.
The first split run needs either a stored lower watermark or
`watermark_range_start`. `watermark_range_interval_amount` controls the size
of each interval and `watermark_range_max_workers` controls concurrent range
requests. These controls apply to canonical incremental splitting; explicit
bounded reads and replay receive their `[start, end)` range from the source
contract and do not need them.

`watermark_to_param_timezone` accepts an IANA name such as
`Asia/Ho_Chi_Minh` or a UTC offset such as `+07:00`. A source-level value wins
over the same connection-level default. The range endpoint and the stored
watermark must use a precision and timezone that the API understands.

`range_param_mapping.<field>.format` controls how a bound is encoded:

| Format | Example output | Behavior |
|---|---|---|
| `iso` | `2026-01-02T03:04:05+00:00` | ISO-8601 output; preserves the datetime's timezone state |
| `date` | `2026-01-02` | Calendar date only |
| `timestamp` | `1767323045.0` | Unix seconds; naive datetimes are treated as UTC |
| `timestamp_ms` | `1767323045000` | Unix milliseconds; naive datetimes are treated as UTC |
| `datetime` | `2026-01-02T03:04:05` | Naive ISO datetime at whole-second precision |
| `datetime_ms` | `2026-01-02T03:04:05.123` | Naive ISO datetime retaining millisecond precision |
| `integer`, `int`, `native_integer`, `number` | `42` | Integer aliases for native numeric API bounds |

String lower/upper shorthand is equivalent to an object with `name` and the
default location/operator. Use an object when the endpoint needs `body` or an
explicit operator. Bounds that cannot be represented at the declared precision
fail before HTTP; no lossy truncation is used.

### Avoid invalid combinations

- Do not put pagination keys on the connection; they belong to the source.
- For the unreleased `0.2.0` cutover, the removed source names
  `watermark_param_mapping`, `watermark_to_param`, `watermark_param_location`,
  `watermark_param_format`, and `watermark_range_to_exclusive_offset` are
  rejected even when their values are null or empty. Replace them manually
  with `range_param_mapping`; no automatic migration is performed.
- Do not use `cursor_param` or `next_link_path` with the wrong
  `pagination_type`.
- Do not use a custom or misspelled `pagination_type`; only `offset`, `cursor`,
  and `next_link` are supported, and an omitted value means one response.
- Put auth settings and secrets on the [connection](connections.md#api-authentication).
- Do not let the API response omit a pushed-down watermark column without
  understanding the reader's `now` advancement behavior.

!!! note "`table` is optional for APIs"
    The API reader uses `connection.configure.base_url` plus
    `source.configure.endpoint`. `source.table` is best treated as a logical
    label for your metadata or logging, not as the actual HTTP path. Keep a
    stable `table` when [shared schema hints](data-types.md#global-hints-for-a-source-table)
    must match this source by connection/table identity.

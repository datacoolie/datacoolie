# Discovery Report — {{ source_name }}

**Date:** {{ date }}
**Source:** {{ source_name }}
**Status:** In Progress

---

## Source Identity

| Property | Value |
|---|---|
| Source name | {{ source_name }} |
| Technology | TODO |
| Version | TODO |
| Hosting | TODO (on-prem / cloud — region) |
| Approximate size | TODO (table count, total GB) |
| Owner / contact | TODO |

## Schema Summary

Full schema inventory: [{{ date }}_{{ source_name }}_schema.md](./{{ date }}_{{ source_name }}_schema.md)

<!-- For database / file / lakehouse sources -->

| Property | Value |
|---|---|
| Total tables | TODO |
| Key tables | TODO (list the most important ones) |
| Tables without PK | TODO |
| Composite keys | TODO |

<!-- For API sources -->
<!--
| Property | Value |
|---|---|
| Total endpoints | TODO |
| Key endpoints | TODO (list the most important ones) |
| Pagination types | TODO (offset / cursor / next_link) |
-->

## Data Characteristics

| Property | Value |
|---|---|
| History tracking | TODO (audit tables, temporal tables, SCD, none) |
| Soft delete pattern | TODO (deleted_at, is_active, none) |
| Late-arriving data | TODO (yes/no, SLA if known) |
| Data formats | TODO (date formats, currencies, encodings, timezones) |
| Known quality issues | TODO (nulls, duplicates, stale data, encoding problems) |

## Change Capture

| Property | Value |
|---|---|
| CDC available | TODO (Debezium, GoldenGate, SQL Server CT, none) |
| Incremental markers | TODO (columns: updated_at, id, sequence) |
| Watermark reliability | TODO (monotonic, gaps, backdating) |
| Full-load-only tables | TODO (tables with no good incremental column) |

## Load Patterns

| Property | Value |
|---|---|
| Expected frequency | TODO (real-time, hourly, daily, weekly, on-demand) |
| Backfill required | TODO (yes/no, how far back) |
| Growth rate | TODO (rows/day, GB/month) |
| Peak window to avoid | TODO |

## Access & Connectivity

| Property | Value |
|---|---|
| Connection method | TODO (JDBC, ODBC, REST, SDK, file mount, S3) |
| Authentication | TODO (user/pass, OAuth, service principal, IAM role) |
| Network restrictions | TODO (VPN, private endpoint, IP whitelist, firewall) |
| Rate limits | TODO (requests/min, concurrent connections) |
| Environments | TODO (dev/test/prod — what differs) |

<!-- Additional rows for API sources -->
<!--
| Base URL | TODO |
| Auth type | TODO (bearer / api_key / basic / oauth2_client_credentials / aws_sigv4) |
| Token URL | TODO (if oauth2) |
| API spec | TODO (OpenAPI 3.0 at /path, GraphQL, OData, none) |
| Default headers | TODO (if any) |
-->

## Performance

| Property | Value |
|---|---|
| Query performance | TODO (avg response for full scan) |
| API latency | TODO (p50, p99 — if API source) |
| Timeout limits | TODO |
| Constraints | TODO (no parallel reads, single-threaded export) |

## Recommendations

### Load Strategy per Table / Endpoint

| Table / Endpoint | Strategy | Watermark Column | Rationale |
|---|---|---|---|
| TODO | TODO | TODO | TODO |

### Watermark Candidates

| Column | Table | Type | Reliable? | Notes |
|---|---|---|---|---|
| TODO | TODO | TODO | TODO | TODO |

### Risks & Concerns

| Risk | Severity | Mitigation |
|---|---|---|
| TODO | TODO | TODO |

---

## Next Steps

1. Complete all TODO items above
2. If more sources to discover, run discovery for each
3. Design architecture based on all discovery reports

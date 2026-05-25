# Discovery Report — {{ source_name }}

**Date:** {{ date }}
**Source:** {{ source_name }}
**Status:** In Progress

---

## Auto-Discovery Results

{{ auto_results | default("_Not yet run. Execute `introspect.py` to populate this section._") }}

## Operational Assessment

### Source Identity

- **What is the source name / system name?**
  {{ source_name }}

- **What is the technology stack?**
  TODO

- **Approximate sizing? (table count, total GB, largest table rows)**
  TODO

- **On-prem or cloud? Which cloud/region?**
  TODO

### Schema Understanding

- **How many tables are relevant for this project? Which ones?**
  TODO

- **Which tables have primary keys? What are they?**
  TODO

- **Are there foreign key relationships between tables?**
  TODO

- **Any composite keys?**
  TODO

### Data Characteristics

- **Does the source maintain history? (audit tables, temporal tables, SCD)**
  TODO

- **Is there soft delete? (deleted_at, is_active flag)**
  TODO

- **Is there late-arriving data? What's the SLA?**
  TODO

- **Special data formats? (date formats, currencies, encodings, timezones)**
  TODO

### Change Capture

- **Is CDC available? (Debezium, GoldenGate, SQL Server CT, etc.)**
  TODO

- **Which columns can serve as incremental markers? (updated_at, id, sequence)**
  TODO

- **Are watermark candidates reliable? (monotonic, no gaps, no backdating)**
  TODO

- **Tables with no good incremental column? (require full load)**
  TODO

### Load Patterns

- **Expected load frequency? (real-time, hourly, daily, weekly, on-demand)**
  TODO

- **Is backfill required? How far back?**
  TODO

- **Growth rate? (rows/day, GB/month)**
  TODO

- **Daily/monthly volume?**
  TODO

- **Peak load time window to avoid?**
  TODO

### Access & Connectivity

- **How do we connect? (JDBC, ODBC, REST, SDK, file mount, S3)**
  TODO

- **Authentication mechanism? (user/pass, OAuth, service principal, IAM role)**
  TODO

- **Network restrictions? (VPN, private endpoint, IP whitelist, firewall)**
  TODO

- **Rate limits or throttling? (requests/min, concurrent connections)**
  TODO

- **How many environments? (dev, test, prod) What differs between them?**
  TODO

### Performance

- **Typical query performance? (avg response for full scan, index availability)**
  TODO

- **API latency? (p50, p99 if API source)**
  TODO

- **Query timeout limits?**
  TODO

- **Known performance constraints? (no parallel reads, single-threaded export)**
  TODO

## Recommendations

### Load Strategy per Table

| Table | Strategy | Watermark | Rationale |
|-------|----------|-----------|-----------|
| TODO | TODO | TODO | TODO |

### Watermark Candidates

TODO — identify monotonically increasing columns suitable for incremental loads

### Risks & Concerns

- TODO

---

## Next Steps

1. Complete all TODO items above
2. Run `datacoolie-architect` to design architecture based on this report

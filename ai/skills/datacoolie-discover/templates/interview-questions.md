# Discovery Interview — Question Bank

Use these questions to gather operational intelligence from the user. Skip questions already answered by auto-introspection. Ask conversationally — group related questions, ask a few at a time.

## Source Identity

- What is the source name / system name?
- What is the technology stack and version? (PostgreSQL 14, Oracle 19c, REST API, etc.)
- What is the approximate sizing? (table count, total GB, largest table)
- On-prem or cloud? Which cloud/region?
- Who owns this source? Who to contact for access or issues?

## Schema (only when auto-introspection is not possible)

- How many tables are relevant for this project? Which ones?
- Which tables have primary keys? What are they?
- Are there foreign key relationships between tables?
- Any composite keys?

## Data Characteristics

- Does the source maintain history data? (audit tables, temporal tables, SCD)
- Is there soft delete? (deleted_at column, is_active flag)
- Is there late-arriving data? What's the SLA?
- What data formats? (dates, currencies, encodings, timezones)
- Any known data quality issues? (nulls, duplicates, stale data, encoding problems)

## Change Capture

- Is CDC available? (Debezium, Oracle GoldenGate, SQL Server CT, etc.)
- Which columns can serve as incremental markers? (updated_at, id, sequence)
- Are there reliable watermark candidates? (monotonically increasing, no gaps)
- Are there tables with no good incremental column? (require full load)

## Load Patterns

- What is the expected frequency? (real-time, hourly, daily, weekly, on-demand)
- Is backfill required? How far back?
- What is the growth rate? (rows/day, GB/month)
- Is there a peak load time window to avoid?

## Access & Connectivity

- How do we connect? (JDBC, ODBC, REST, SDK, file mount)
- What authentication mechanism? (username/password, OAuth, service principal, IAM role)
- Are there network restrictions? (VPN, private endpoint, IP whitelist, firewall rules)
- Are there rate limits or throttling? (requests/min, concurrent connections)
- How many environments? (dev, test, prod) What differs between them?

## API-Specific (only for REST / GraphQL / OData sources)

- What is the base URL?
- What auth type? (bearer token, API key, basic auth, OAuth2 client credentials, AWS SigV4)
- If OAuth2: what is the token URL? Client credentials or authorization code?
- Is there an API spec? (OpenAPI/Swagger URL, GraphQL introspection, OData $metadata)
- What pagination pattern? (offset-based, cursor-based, next-link)
- Where is the data in the response? (e.g. `data.items`, `results`, root array)
- Are there any required default headers? (API version header, tenant header)
- What are the key endpoints we need to ingest?
- Do endpoints support filtering by date/timestamp for incremental loads?

## Performance

- What is the typical query performance? (avg response time for full table scan)
- What is the API latency? (p50, p99)
- Are there query timeout limits?
- Any known performance constraints? (no parallel reads, single-threaded export)


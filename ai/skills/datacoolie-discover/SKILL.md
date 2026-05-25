---
name: datacoolie-discover
description: >
  Discover and profile data sources for DataCoolie ETL projects.
  Two modes: auto (script connects and extracts DDL-level schema) and interview (AI-driven Q&A for operational intelligence).
  Use when user says "discover sources", "explore data", "profile source", "what tables exist",
  "understand my data", "source assessment", "new data source", "connect to source",
  or before designing architecture for a new data platform.
---

# datacoolie-discover

Discover and profile data sources through automated introspection and guided operational assessment.

## Scope

This skill handles: source schema extraction (DDL-level), operational intelligence gathering (Q&A), discovery report generation.
Does NOT handle: metadata generation (use `datacoolie-metadata`), project scaffolding (use `datacoolie-init`), architecture design (use `datacoolie-architect`).

## Modes

| Mode | Purpose | Mechanism |
|---|---|---|
| Auto | Connect to source, extract DDL | Script (`introspect.py`) |
| Interview | Guided Q&A for ops intelligence | AI asks, user answers (no script) |
| Both (default) | Run auto first, then interview | Combined |

Default behavior: run both modes sequentially. User can run either independently.

## AI Workflow

### Mode Selection

- **User provides a connection string / file path / API spec** → run auto first, then follow up with interview
- **User cannot provide access (firewall, no credentials yet)** → run interview only
- **User has already run auto** → skip to interview to fill operational sections

### Auto Mode (introspect.py)

1. Determine source type from user context:
   - Database (PostgreSQL, MySQL, MSSQL, Oracle, SQLite)
   - File storage (local folder, S3 path, lakehouse path)
   - API (OpenAPI spec URL or manual endpoint description)
   - Lakehouse catalog (Glue, Unity Catalog, Fabric)

2. Run introspection:
   ```bash
   python scripts/introspect.py --source-type db --connection "$CONNECTION_STRING"
   python scripts/introspect.py --source-type file --path ./data/
   python scripts/introspect.py --source-type api --spec-url https://api.example.com/openapi.json
   python scripts/introspect.py --source-type lakehouse --catalog glue --database mydb
   ```

3. Review output: tables, columns, PKs, types, row estimates

4. Append results to `Auto-Discovery Results` section of the report

### Interview Mode (Q&A)

AI asks questions organized by category. Skip questions already answered by auto mode.

#### Source Identity
- What is the source name / system name?
- What is the technology stack? (PostgreSQL 14, Oracle 19c, REST API, etc.)
- What is the approximate sizing? (table count, total GB, largest table)
- On-prem or cloud? Which cloud/region?

#### Schema Understanding
- How many tables are relevant for this project?
- Which tables have primary keys? What are they?
- Are there foreign key relationships between tables?
- Any composite keys?

#### Data Characteristics
- Does the source maintain history data? (audit tables, temporal tables)
- Is there soft delete? (deleted_at column, is_active flag)
- Is there late-arriving data? What's the SLA?
- What data formats? (dates, currencies, encodings)

#### Change Capture
- Is CDC available? (Debezium, Oracle GoldenGate, SQL Server CT, etc.)
- Which columns can serve as incremental markers? (updated_at, id, sequence)
- Are there reliable watermark candidates? (monotonically increasing, no gaps)
- Are there tables with no good incremental column? (require full load)

#### Load Patterns
- What is the expected frequency? (real-time, hourly, daily, weekly, on-demand)
- Is backfill required? How far back?
- What is the growth rate? (rows/day, GB/month)
- What is the daily/monthly volume?
- Is there a peak load time window?

#### Access & Connectivity
- How do we connect? (JDBC, ODBC, REST, SDK, file mount)
- What authentication mechanism? (username/password, OAuth, service principal, IAM role)
- Are there network restrictions? (VPN, private endpoint, IP whitelist, firewall rules)
- Are there rate limits or throttling? (requests/min, concurrent connections)
- How many environments? (dev, test, prod) What differs between them?

#### Performance
- What is the typical query performance? (avg response time for full table scan)
- What is the API latency? (p50, p99)
- Are there query timeout limits?
- Any known performance constraints? (no parallel reads, single-threaded export)

### After Both Modes

1. Generate recommendations:
   - Suggested load strategy per table (full, incremental, merge, scd2)
   - Watermark candidates with rationale
   - Risks and concerns

2. Write the discovery report:
   ```
   .datacoolie/discover/yymmdd_discovery-report.md
   ```
   Where `yymmdd` is the current date (e.g., `260523`).

3. Inform user: "Discovery complete. Next step: run `datacoolie-architect` to design the architecture based on this report."

## Scripts

All scripts at `ai/skills/datacoolie-discover/scripts/`.

### introspect.py — Source schema extraction

```bash
python scripts/introspect.py --source-type db --connection <conn_string>
python scripts/introspect.py --source-type file --path <directory>
python scripts/introspect.py --source-type api --spec-url <url>
python scripts/introspect.py --source-type lakehouse --catalog <type> --database <name>
```

**Supported source types:**

| Source Type | Introspection Method |
|---|---|
| `db` | INFORMATION_SCHEMA queries (PostgreSQL, MySQL, MSSQL, Oracle, SQLite) |
| `file` | Directory scan + header read (CSV, Parquet, JSON, JSONL, Avro, Excel) — local + cloud (S3, ADLS, GCS, OneLake) |
| `api` | OpenAPI/Swagger spec parse, GraphQL introspection, OData $metadata |
| `lakehouse` | Catalog API (AWS Glue, Databricks Unity, Microsoft Fabric, Apache Iceberg, Delta Lake, Hive) |

**Cloud file storage options:**
```bash
# AWS S3 (ambient credentials)
python scripts/introspect.py --source-type file --path "s3://bucket/prefix/"
# ADLS Gen2 / OneLake (DefaultAzureCredential)
python scripts/introspect.py --source-type file --path "abfss://container@account.dfs.core.windows.net/path/" --storage-options '{"credential":"default"}'
# Custom storage options (JSON)
python scripts/introspect.py --source-type file --path "s3://bucket/path/" --storage-options '{"key":"$AWS_KEY","secret":"$AWS_SECRET"}'
```

**Database auth modes:**
```bash
# Standard connection string
python scripts/introspect.py --source-type db --connection "postgresql://user:pass@host:5432/db"
# AWS RDS IAM token auth
python scripts/introspect.py --source-type db --auth rds_iam --dialect postgresql --host mydb.rds.amazonaws.com --user iam_user --region us-east-1
# Azure AD token auth
python scripts/introspect.py --source-type db --auth azure_ad --dialect mssql --host myserver.database.windows.net --database mydb
# OAuth2 client_credentials
python scripts/introspect.py --source-type db --auth oauth2 --dialect postgresql --host host --token-url https://auth.example.com/token --client-id $CLIENT_ID --client-secret $CLIENT_SECRET
```

**API introspection options:**
```bash
# OpenAPI (default)
python scripts/introspect.py --source-type api --spec-url https://api.example.com/openapi.json
# GraphQL introspection
python scripts/introspect.py --source-type api --spec-url https://api.example.com/graphql --api-type graphql
# OData metadata
python scripts/introspect.py --source-type api --spec-url https://api.example.com/$metadata --api-type odata
# With auth
python scripts/introspect.py --source-type api --spec-url https://api.example.com/openapi.json --api-auth bearer --api-token $TOKEN
# Sample call inference
python scripts/introspect.py --source-type api --spec-url https://api.example.com/openapi.json --sample-call
```

**Lakehouse catalog options:**
```bash
# AWS Glue
python scripts/introspect.py --source-type lakehouse --catalog glue --database mydb --region us-east-1
# Databricks Unity Catalog
python scripts/introspect.py --source-type lakehouse --catalog unity --database my_catalog --schema my_schema
# Apache Iceberg (REST catalog)
python scripts/introspect.py --source-type lakehouse --catalog iceberg --database my_namespace --catalog-uri http://localhost:8181
# Delta Lake (local path)
python scripts/introspect.py --source-type lakehouse --catalog delta --database /path/to/delta/tables
```

**Output**: Structured text (tables, columns, PKs, types) written to stdout or `--output` file.

**Exit codes**: 0 = success, 1 = connection/introspection error, 2 = input error.

### Report Generation (AI-native)

The interview and report generation are handled by the AI directly using the template at
`templates/discovery-report.tpl.md`. No script needed — the AI asks questions from the
question bank in the Interview Mode section above and fills the template inline.

## Output Format

File: `.datacoolie/discover/yymmdd_discovery-report.md`

The report follows the template at `templates/discovery-report.tpl.md`.

## Security Policy

- Never store connection strings or credentials in the output report
- Never log credentials during introspection
- Connection strings read from environment variables or user prompt — never hardcoded
- Read-only operations only (SELECT on catalog views, no writes to source)
- Refuse requests to scan systems without user's explicit authorization

## Cross-Skill Dependencies

| Dependency | Direction |
|---|---|
| `datacoolie-architect` | Downstream — reads discovery report to design architecture |
| `datacoolie-metadata` | None — discover does not produce metadata |
| `datacoolie-init` | None — discover does not scaffold projects |

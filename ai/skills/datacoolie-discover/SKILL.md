---
name: datacoolie-discover
description: >
  Discover and profile data sources for DataCoolie ETL projects.
  Produces per-source discovery reports (operational context + schema inventory).
  Two retrieval methods: auto-introspection (connect to source) or interview (ask user).
  Supports multiple sources — each source gets its own report.
  Use when user says "discover sources", "explore data", "profile source", "what tables exist",
  "understand my data", "source assessment", "new data source", "connect to source",
  or before designing architecture for a new data platform.
---

# datacoolie-discover

Produce discovery reports that capture everything needed to design an ETL architecture for one or more data sources.

## Scope

This skill handles: source schema extraction, operational intelligence gathering, discovery report generation.
Does NOT handle: metadata generation, project scaffolding, architecture design.

## Multi-Source Model

Each source is discovered independently. One discovery run = one source = two output files.

```
.datacoolie/discover/
├── yymmdd_erp.md                # Discovery report (operational context + recommendations)
├── yymmdd_erp_schema.csv        # Schema inventory (CSV — 14 columns)
├── yymmdd_crm-api.md
├── yymmdd_crm-api_schema.csv
├── yymmdd_file-exports.md
└── yymmdd_file-exports_schema.csv
```

**Naming**: `yymmdd_{source-name}.md` and `yymmdd_{source-name}_schema.csv` where `source-name` is a kebab-case identifier from the user.

When user has multiple sources, run discovery for each source separately. Sources can be discovered at different times — re-running discovery for one source does not affect others.

## Desired Output

Per source, two files:

### Discovery Report (`yymmdd_{source-name}.md`)

| Section | Content | Populated By |
|---|---|---|
| Source identity | Name, tech stack, sizing, hosting, owner | Auto or Interview |
| Schema summary | Table count, key tables, link to schema file | Auto or Interview |
| Data characteristics | History tracking, soft deletes, late-arriving data, formats, quality | Interview |
| Change capture | CDC availability, watermark candidates, incremental markers | Auto + Interview |
| Load patterns | Frequency, backfill needs, growth rate, peak windows | Interview |
| Access & connectivity | Protocol, auth, network, rate limits, environments | Interview |
| Performance | Query latency, timeouts, constraints | Interview |
| Recommendations | Load strategy per table, watermark candidates, risks | AI-generated |

Template: `templates/discovery-report.tpl.md`

### Schema Inventory (`yymmdd_{source-name}_schema.csv`)

A flat CSV file with 14 columns. Same contract for all source types (database, file, API).

| Column | Description |
|---|---|
| `source` | Source name (kebab-case) |
| `schema` | Database schema or empty for files/APIs |
| `table` | Table name, file path, or API endpoint path |
| `column` | Column/field name (dot notation for nested: `customer.id`) |
| `type` | Canonical type: `string`, `integer`, `long`, `double`, `float`, `decimal(p,s)`, `boolean`, `date`, `timestamp`, `timestamp_tz`, `time`, `binary`, `array`, `struct` |
| `format` | Source-specific format hint (e.g. `parquet`, `uuid`, `email`) or empty |
| `precision` | Numeric precision, string max length, or empty |
| `scale` | Numeric scale or empty |
| `nullable` | `true` or `false` |
| `pk` | `true` if primary key, else empty |
| `fk` | FK reference (e.g. `→ public.orders.customer_id`) or empty |
| `ordinal_position` | Column ordinal (1-based) |
| `row_estimate` | Approximate row count or empty |
| `notes` | Free text (e.g. `inferred from sample rows`, `GET; pagination=cursor`) |

Template: `templates/schema-inventory.tpl.csv`

## Retrieval Methods

### Method 1: Auto-Introspection

Connect to the source and extract schema automatically using the introspection scripts.

**When to use**: User provides a connection string, file path, or API spec.

#### Introspection Scripts

Located in `scripts/`. Install dependencies first: `pip install -r scripts/requirements.txt`

| Script | Source Type | Key Args |
|---|---|---|
| `introspect_db.py` | Databases (PostgreSQL, MySQL, MSSQL, Oracle, SQLite, Snowflake, BigQuery, Redshift) | `--url`, `--source`, `--schemas`, `--tables`, `--output` |
| `introspect_files.py structure` | File/folder structure (local, S3, ADLS, GCS) | `--path`, `--output`, `--storage-options` |
| `introspect_files.py schema` | File schema (Parquet, CSV, JSON, Delta, Avro, ORC, Excel) | `--path`, `--format`, `--source`, `--table`, `--output` |
| `introspect_api.py` | APIs (OpenAPI JSON/YAML, GraphQL, OData) | `--spec` / `--graphql` / `--odata`, `--source`, `--output` |
| `introspect_lakehouse.py` | Lakehouse catalogs (Iceberg REST, Hive Metastore, Unity Catalog, AWS Glue) | `--iceberg` / `--hive` / `--unity` / `--glue`, `--source`, `--output` |

All scripts output the same 14-column CSV to stdout or `--output` file.

#### Workflow

1. Determine source type (database, file, API, lakehouse catalog)
2. Run the appropriate introspection script
3. Write output to `yymmdd_{source-name}_schema.csv`
4. Proceed to interview for remaining sections (data characteristics, load patterns, etc.)

#### Fallback

If scripts fail (missing driver, network issue, unsupported source):
- Consult `references/source-introspection-guide.md` for manual SQL/CLI approaches
- Use whatever SQL client or CLI is available in the terminal
- Output must still conform to the 14-column CSV contract

Key principles:
- Filter out system schemas (`information_schema`, `pg_catalog`, `sys`, `mysql`, `performance_schema`)
- For unlisted databases: (1) try INFORMATION_SCHEMA, (2) try `SHOW TABLES` / `DESCRIBE TABLE`, (3) consult docs
- Normalize discovered types to canonical forms

### Method 2: Interview

Ask the user questions to gather information that can't be auto-extracted.

**When to use**: No source access (firewall, no credentials), or to supplement auto-introspection results.

Questions are at `templates/interview-questions.md`. Key rules:
- Skip questions already answered by auto-introspection
- Ask conversationally — don't dump the full list
- Group related questions, ask a few at a time
- Fill answers directly into the report

### Workflow

1. Ask user for source name (used in filenames)
2. **User provides source access** → auto-introspect first, write schema file, then interview for remaining gaps
3. **User cannot provide access** → interview only (schema section filled manually or left for later)
4. **After both** → AI generates recommendations (load strategy, watermarks, risks)
5. Write report to `.datacoolie/discover/yymmdd_{source-name}.md`
6. If more sources to discover → repeat for next source
7. Inform user: "Discovery complete. Next step: design the architecture based on these reports."

## Security Policy

- Never store connection strings or credentials in the output report
- Never log credentials during introspection
- Connection strings read from environment variables or user prompt — never hardcoded
- Read-only operations only (SELECT on catalog views, no writes to source)
- Refuse requests to scan systems without user's explicit authorization

## Output Contracts

| Artifact | Path | Format |
|---|---|---|
| Discovery report | `.datacoolie/discover/yymmdd_{source-name}.md` | Markdown |
| Schema inventory | `.datacoolie/discover/yymmdd_{source-name}_schema.csv` | CSV (14 columns) |

## Dependencies

Script dependencies are listed in `scripts/requirements.txt`. Core:
- `sqlalchemy>=2.0` — database introspection
- `fsspec` — cross-platform file system abstraction
- `pyarrow` — file schema extraction (Parquet, CSV, JSON, ORC)
- `deltalake` — Delta table schema extraction
- `pyyaml` — OpenAPI YAML parsing
- `requests` — API spec fetching

Optional (install per source type):
- Database drivers: `psycopg2-binary`, `pymysql`, `pyodbc`, `oracledb`, `snowflake-sqlalchemy`, `sqlalchemy-bigquery`, `sqlalchemy-redshift`
- Cloud storage: `s3fs`, `adlfs`, `gcsfs`
- File formats: `fastavro`, `openpyxl`
- Lakehouse catalogs: `pyhive[hive]` (Hive Metastore), `trino` (Iceberg via Trino)

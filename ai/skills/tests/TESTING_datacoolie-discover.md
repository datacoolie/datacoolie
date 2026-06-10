# datacoolie-discover — Testing Guide

This skill uses **introspection scripts** (`scripts/introspect_db.py`, `introspect_files.py`, `introspect_api.py`) plus knowledge-based AI rules in SKILL.md. All scripts output a 14-column CSV.

---

## Quick Start

```sh
cd datacoolie/ai/skills/tests

# Unit tests (mocked, no Docker needed)
python -m pytest unit/test_introspect_db.py unit/test_introspect_files.py unit/test_introspect_api.py -v -o "addopts="

# Integration tests (scripts against local fixtures)
python run_discover.py

# Integration tests with Docker databases
docker compose up -d --wait
python run_discover.py --docker
```

---

## What to Test

### 1. Unit Tests (112 tests)

| File | Covers |
|------|--------|
| `unit/test_introspect_db.py` | Type mapping, FK map, URL/ODBC masking, connection env resolution, CSV contract, mocked introspection |
| `unit/test_introspect_files.py` | Arrow/Delta type mapping, format detection, Parquet/CSV/JSON/Delta schema |
| `unit/test_introspect_api.py` | OpenAPI parsing, type mapping, $ref resolution, pagination detection |

### 2. Script Smoke Tests (run_discover.py)

Runs scripts against local fixtures and validates output:
- Parquet → 9 rows, 14 cols
- CSV → 7 rows, 14 cols
- Delta → 5 rows, 14 cols
- Structure report → Markdown with Tree + Summary
- OpenAPI → 63+ rows, 14 cols

### 3. Docker Database Tests (run_discover.py --docker)

Start the shared test environment:
```sh
cd datacoolie/ai/skills/tests
docker compose up -d --wait
python run_all.py --no-docker
```

Ask the AI to discover each source. Verify it runs appropriate SQL and produces correct output:

| Source | Connection | Expected |
|--------|-----------|----------|
| PostgreSQL | `postgresql://datacoolie:datacoolie@localhost:5442/pagila` | ~11 tables + 3 views, 3 schemas, 60+ columns, FKs |
| MySQL | `mysql+pymysql://datacoolie:datacoolie@localhost:3316/sakila` | 10 tables + 3 views, 50+ columns |
| MSSQL | `mssql+pyodbc://sa:Testing%40123@localhost:1444/AdventureWorksLT` | SalesLT schema, 12 tables + 2 views |
| Oracle | `oracle+oracledb://hr:hr@localhost:1522/?service_name=FREEPDB1` | 7 tables + 1 view, 40+ columns |

Verification checklist per source:
- [ ] AI runs `information_schema` or equivalent queries
- [ ] For database sources, AI uses `scripts/introspect_db.py` rather than custom schema-discovery scripts
- [ ] Connection secrets are provided through `--url-env` or `--odbc-connstr-env` when possible
- [ ] Tables, columns, data types, PKs, and FKs are extracted
- [ ] Row count estimates are included
- [ ] Discovery report follows the template format
- [ ] Discovery report and schema inventory start with YAML frontmatter (`artifact_type`, `date`, `source_name`, `status` or `source_type`)

### 4. Manual Workflow Testing — Files

Point the AI at `fixtures/files/` and verify it:
- [ ] Detects file formats (csv, jsonl, parquet, json, avro, xlsx, delta)
- [ ] Reads headers/schemas using appropriate commands (`head`, `parquet-tools`, Python one-liners)
- [ ] Infers data types beyond just "string" for typed formats (Parquet, Avro)

### 5. Manual Workflow Testing — APIs

Ask the AI to discover from an OpenAPI spec:
- [ ] Static file: `fixtures/api/openapi-petstore.json` — extracts endpoints, methods, parameters, response fields
- [ ] Live URL (if Docker running): `http://localhost:8092/openapi.json`
- [ ] AI resolves `$ref` schemas correctly

### 6. Manual Workflow Testing — Lakehouses

| Catalog | Connection | Expected |
|---------|-----------|----------|
| Iceberg | `http://localhost:8182`, database `sales` | 3 tables, 12 columns |
| Delta | `fixtures/files`, delta sub-directory | delta_products, 5 columns |
| Hive | `thrift://localhost:10000`, database `datacoolie_test` | 3 tables, 14 columns |

### 7. Interview Mode

Trigger interview-only mode (no connection provided) and verify:
- [ ] AI asks structured questions about data volumes, freshness, SLAs, ownership
- [ ] Responses populate the operational intelligence sections of the report
- [ ] Report is generated even without auto-mode introspection

---

## What to Inspect in Results

After running, open CSV files in Excel:

| File | Key columns to check |
|------|---------------------|
| `catalog.csv` | table, schema, column, type, nullable, is_pk |
| `endpoints.csv` | method, path, summary, parameters, response_fields, pagination |

- Verify schema names match the sample DB structure
- Verify FK columns are not marked as PKs unless they are
- Verify views appear alongside base tables
- Verify inferred types look reasonable (no `object` for numeric columns)
- Verify `response_fields` populated for `--sample-call` runs (inferred from live response)
- Verify `pagination` column: `cursor` / `offset` / `link` as appropriate

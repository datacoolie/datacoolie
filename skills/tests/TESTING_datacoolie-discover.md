# datacoolie-discover — Testing Guide

All commands run from `datacoolie/skills/tests/` with the venv activated.

```
# Activate venv first
# Windows:  ..\..\..\.venv\Scripts\activate
# macOS/Linux: source ../../../.venv/bin/activate
```

Short alias for the script:
```python
# Use in terminal — Python is the interpreter after venv activation
python ../datacoolie-discover/scripts/introspect.py [args]
```

---

## Prerequisites

1. Start the shared test environment:
   ```
   cd datacoolie/skills/tests
   docker compose up -d --wait
   python run_all.py --no-docker    # seed MSSQL + Iceberg, skip docker start
   # OR:
   python run_all.py                # start docker + seed everything
   ```

2. Services available after startup:

   | Service | URL / Connection |
   |---------|----------------|
   | PostgreSQL (Pagila) | `postgresql://datacoolie:datacoolie@localhost:5442/pagila` |
   | MySQL (Sakila) | `mysql+pymysql://datacoolie:datacoolie@localhost:3316/sakila` |
   | MSSQL (AdventureWorks LT) | `mssql+pyodbc://sa:Testing%40123@localhost:1444/AdventureWorksLT` |
   | Oracle (HR) | `oracle+oracledb://hr:hr@localhost:1522/?service_name=FREEPDB1` |
   | Trino (Iceberg) | `http://localhost:8090` |
   | Hive (Thrift) | `thrift://localhost:10000` |
   | Mock API spec | `http://localhost:8092/openapi.json` |
   | Sample files | `fixtures/files/` (csv, jsonl, parquet, json, avro, xlsx, delta) |

---

## Automated Run

```
python run_discover.py                    # all sources
python run_discover.py postgres mysql     # subset
```

Output at `test-results/discover/{source}/catalog.csv` (or `endpoints.csv` for API).

---

## 1. Database — PostgreSQL (Pagila)

**Schema**: `inventory` (film, actor, category, film_actor, film_category, store_inventory) + `sales` (address, customer, rental, payment) + `staff` (store)
**Expected**: ~11 tables + 3 views across 3 schemas, 60+ columns, FKs detected

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type db \
  --connection "postgresql://datacoolie:datacoolie@localhost:5442/pagila" \
  --format text
```

CSV output to directory:
```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type db \
  --connection "postgresql://datacoolie:datacoolie@localhost:5442/pagila" \
  --format csv \
  --output-dir test-results/discover/postgres
```

What to inspect in `catalog.csv`:
- `schema` column shows `inventory`, `sales`, `staff`
- `is_pk` = `True` for primary key columns
- Multiple rows per table (one per column)
- Views present (daily_revenue, customer_list, etc.)

---

## 2. Database — MySQL (Sakila)

**Schema**: 10 tables + 3 views — film, actor, category, address, customer, store, inventory, rental, payment, film_actor, film_category
**Expected**: 10 base tables + 3 views, 50+ columns

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type db \
  --connection "mysql+pymysql://datacoolie:datacoolie@localhost:3316/sakila" \
  --format csv \
  --output-dir test-results/discover/mysql
```

---

## 3. Database — MSSQL (AdventureWorks LT)

**Schema**: `SalesLT` — 12 tables + 2 views
**Expected**: SalesLT schema, Product/Customer/SalesOrder hierarchy, computed columns

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type db \
  --connection "mssql+pyodbc://sa:Testing%40123@localhost:1444/AdventureWorksLT?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes" \
  --format csv \
  --output-dir test-results/discover/mssql
```

What to inspect:
- `schema` column = `SalesLT`
- FK chain: SalesOrderDetail → SalesOrderHeader → Customer
- Views: vProductAndDescription, vGetAllCategories

---

## 4. Database — Oracle (HR)

**Schema**: HR user (7 tables + 1 view)
Tables: REGIONS, COUNTRIES, LOCATIONS, DEPARTMENTS, JOBS, EMPLOYEES, JOB_HISTORY
**Expected**: 7 tables + 1 view (EMP_DETAILS_VIEW), 40+ columns, cascading FKs

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type db \
  --connection "oracle+oracledb://hr:hr@localhost:1522/?service_name=FREEPDB1" \
  --format csv \
  --output-dir test-results/discover/oracle
```

---

## 5. Files — Local directory

**Source**: `fixtures/files/` — 7 file groups: csv, jsonl, parquet, json, avro, xlsx, delta  
**Expected**: 45 rows in catalog.csv, one group per logical table, types inferred per format

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type file \
  --path fixtures/files \
  --format csv \
  --output-dir test-results/discover/files
```

What to inspect:
- `format` column: csv, jsonl, parquet, json, avro, xlsx, parquet (delta sub-dir)
- `representative_file` shows the sampled filename
- `file_count` shows how many files in the group
- JSON types: integer/boolean/decimal/string (not all "string")
- Avro types: int/double/string from Avro writer schema
- Excel types: string (header-only inference)

Generate binary fixtures if not present:
```sh
python generate_extra_fixtures.py
```

---

## 6. API — OpenAPI Petstore (no auth)

**Source**: Petstore spec served at `http://localhost:8092/openapi.json`  
13 endpoints (GET/POST/PUT/DELETE for pet/store/user), $ref resolution

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type api \
  --spec-url "http://localhost:8092/openapi.json" \
  --format csv \
  --output-dir test-results/discover/api
```

What to inspect in `endpoints.csv`:
- `method` column: GET, POST, PUT, DELETE
- `path` column: /pet, /pet/{petId}, /store/inventory, /user/{username}, etc.
- `parameters` column: lists query/path params with required marker (*)
- `response_fields` column: resolved property names from $ref schemas

Alternative — static file (no Docker needed):
```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type api \
  --spec-url "fixtures/api/openapi-petstore.json" \
  --format text
```

---

## 7. API — Auth modes

### API Key (X-API-Key header)
```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type api \
  --spec-url "http://localhost:8092/openapi-apikey.json" \
  --api-auth api_key \
  --api-key testkey123 \
  --format csv --output-dir test-results/discover/api-apikey
```

### Bearer token
```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type api \
  --spec-url "http://localhost:8092/openapi-bearer.json" \
  --api-auth bearer \
  --api-token testtoken456 \
  --format csv --output-dir test-results/discover/api-bearer
```

### HTTP Basic auth
`--api-token` accepts `user:password` — base64-encoded automatically.
```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type api \
  --spec-url "http://localhost:8092/openapi-basic.json" \
  --api-auth basic \
  --api-token "testuser:testpass" \
  --format csv --output-dir test-results/discover/api-basic
```

What to verify: all three auth modes return the same 18 endpoints as the unauthenticated spec. A 401 error from the server means credentials were not sent correctly.

---

## 8. API — GraphQL, OData

### GraphQL introspection
```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type api \
  --spec-url "http://localhost:8092/graphql" \
  --api-type graphql \
  --format csv --output-dir test-results/discover/api-graphql
```
Expected: 2 rows (Pet, Order types) with `parameters` listing each field name.

### OData $metadata
```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type api \
  --spec-url "http://localhost:8092/\$metadata" \
  --api-type odata \
  --format csv --output-dir test-results/discover/api-odata
```
Expected: 2 rows (Pet, Order entity types).

---

## 9. API — Pagination detection

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type api \
  --spec-url "http://localhost:8092/openapi-paginated.json" \
  --format csv --output-dir test-results/discover/api-pagination
```

Expected `pagination` column values:

| path | pagination |
|------|------------|
| /items/cursor | cursor |
| /items/offset | offset |
| /items/nextlink | link |
| /items/{id} | _(empty)_ |

---

## 10. API — Sample-call response inference

`--sample-call` makes live GET requests to endpoints without path params and infers `response_fields` from the actual JSON response body.

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type api \
  --spec-url "http://localhost:8092/openapi-sample.json" \
  --sample-call \
  --format csv --output-dir test-results/discover/api-sample-call
```

Expected:
- `/pets` → `response_fields`: `id, name, status, tag` (inferred from live `/pets` response)
- `/pets/{petId}` → `response_fields`: _(empty — path param endpoint skipped)_

---

## 11. API — YAML spec (local file)

Spec can be a local file path instead of a URL. YAML format is detected automatically.

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type api \
  --spec-url "fixtures/api/openapi-inventory.yaml" \
  --format csv --output-dir test-results/discover/api-yaml
```

Expected: 5 endpoints from the Inventory API spec (GET/POST /items, GET/PUT/DELETE /items/{itemId}).

---

## 12. Database — Schema filter

Use `--schema` to restrict introspection to a single schema.

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type db \
  --connection "postgresql://datacoolie:datacoolie@localhost:5442/pagila" \
  --schema sales \
  --format csv --output-dir test-results/discover/postgres-schema
```

Expected: only `sales` schema tables (address, customer, rental, payment — ~28 columns). No `inventory` or `staff` tables.

---

## 13. Lakehouse — Iceberg

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type lakehouse \
  --catalog iceberg \
  --database sales \
  --catalog-uri "http://localhost:8182" \
  --format csv --output-dir test-results/discover/iceberg
```

Expected: 3 tables (customers, orders, products) in `sales` namespace, 12 columns total.

---

## 14. Lakehouse — Delta

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type lakehouse \
  --catalog delta \
  --database fixtures/files \
  --format csv --output-dir test-results/discover/delta
```

Expected: `delta_products` table, 5 columns (product_id, name, price, category, in_stock).

---

## 15. Lakehouse — Hive (Docker)

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type lakehouse \
  --catalog hive \
  --database datacoolie_test \
  --catalog-uri "thrift://localhost:10000" \
  --format csv --output-dir test-results/discover/hive
```

Expected: 3 tables (customers, orders, products) in `datacoolie_test`, 14 columns total.

---

## 16. Lakehouse — Fabric (env-gated)

Requires `FABRIC_TOKEN` environment variable (Azure AD token).
`--workspace-name` / `--lakehouse-name` accept display names and are resolved to GUIDs automatically.

```
python ../datacoolie-discover/scripts/introspect.py \
  --source-type lakehouse \
  --catalog fabric \
  --database lh_bronze \
  --workspace-name "DataCoolie_DEV_WS" \
  --format csv --output-dir test-results/discover/fabric
```

---

## 17. Unit Tests (no Docker required)

Assert-based tests for pure logic in the discover scripts:

```sh
# From workspace root (d:\GitHub\datacoolie-arch-5)
python -m pytest datacoolie/skills/tests/unit/ -v
```

89 tests, 4 modules, no services required:

| Test file | Coverage |
|-----------|----------|
| `test_api_introspect.py` | `_detect_pagination` (cursor/offset/link precedence, 13 cases), `_build_auth_headers` (all modes + missing-value safety, 10 cases) |
| `test_auth.py` | `resolve_env_var`, `build_mssql_token_struct` format, `build_connection_string` per dialect + mocked token flows (17 cases) |
| `test_schema_parsers.py` | Golden schemas for CSV/TSV/JSON/JSONL/Parquet/Avro/Excel, `infer_json_type` edge cases (30 cases) |
| `test_file_introspect.py` | `max_files` boundary values (0/1/exact/exceeded+WARN), `_resolve_storage_options` env-var expansion, `introspect_files` smoke (19 cases) |

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

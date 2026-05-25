# datacoolie Skills — Integration Test Environment

Shared Docker-based integration environment for all datacoolie skill tests.  
All skill runners are pure Python — no PowerShell or shell scripts required.

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Docker Desktop | Engine 24+ recommended |
| Python 3.10+ | Venv at `../../.venv` from `datacoolie/` |
| ODBC Driver 18 | For MSSQL (SQL Server) tests only |
| Oracle Instant Client | NOT required — uses `python-oracledb` thin mode |

---

## Quick Start

```sh
# 1. From workspace root, activate venv
#    Windows:  .venv\Scripts\activate
#    macOS/Linux: source .venv/bin/activate

# 2. Navigate to tests folder
cd datacoolie/ai/skills/tests

# 3. Start all services + seed databases
python run_all.py

# 4. Run a specific skill
python run_discover.py

# 5. Tear down
docker compose down -v
```

`run_all.py` accepts an optional skill filter:
```sh
python run_all.py discover         # only run discover tests
python run_all.py --no-docker      # skip docker start (services already running)
```

---

## Port Reference

These ports are intentionally offset from `usecase-sim` so both can run simultaneously.

| Service | Test Port | usecase-sim Port |
|---------|-----------|-----------------|
| PostgreSQL (Pagila) | **5442** | 5432 |
| MySQL (Sakila) | **3316** | 3306 |
| MSSQL (AdventureWorks LT) | **1444** | 1433 |
| Oracle (HR) | **1522** | 1521 |
| MinIO API | **9010** | 9000 |
| MinIO Console | **9011** | 9001 |
| Iceberg REST | **8182** | 8181 |
| Trino | **8090** | 8080 |
| Mock API | **8092** | 8082 || Hive | **10000** | — |
---

## Service Credentials

| Service | Username | Password | Database / Schema |
|---------|----------|----------|-------------------|
| PostgreSQL | `datacoolie` | `datacoolie` | `pagila` |
| MySQL | `datacoolie` | `datacoolie` | `sakila` |
| MSSQL | `sa` | `Testing@123` | `AdventureWorksLT` |
| Oracle | `hr` | `hr` | `FREEPDB1` |
| MinIO | `minioadmin` | `minioadmin` | bucket: `skills-test` |
| Trino | `admin` | _(none)_ | catalog: `iceberg` |

---

## Connection Strings

```python
CONNECTIONS = {
    "postgres": "postgresql://datacoolie:datacoolie@localhost:5442/pagila",
    "mysql":    "mysql+pymysql://datacoolie:datacoolie@localhost:3316/sakila",
    "mssql":    (
        "mssql+pyodbc://sa:Testing%40123@localhost:1444/AdventureWorksLT"
        "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    ),
    "oracle":   "oracle+oracledb://hr:hr@localhost:1522/?service_name=FREEPDB1",
    "trino":    "trino://admin@localhost:8090/iceberg",
    "api":      "http://localhost:8092/openapi.json",
}
```

---

## Sample Databases

| Service | Database | Tables | Source / Theme |
|---------|----------|--------|----------------|
| PostgreSQL | Pagila | 11 tables, 3 views | DVD rental (3 schemas: inventory/sales/staff) |
| MySQL | Sakila | 10 tables, 3 views | DVD rental (official MySQL sample) |
| MSSQL | AdventureWorks LT | 12 tables, 2 views | E-commerce / SalesLT schema |
| Oracle | HR | 7 tables, 1 view | Human Resources (classic Oracle sample) |
| Iceberg | iceberg catalog | 4 tables | sales + analytics schemas |
| Hive | datacoolie_test | 3 tables | customers, orders, products |

---

## Sample Files

Located at `fixtures/files/`:

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `products.csv` | CSV | 12 | Product catalog (7 columns) |
| `events.jsonl` | JSONL | 10 | Clickstream events (9 columns) |
| `sales.parquet` | Parquet | 10 | Sales orders (9 columns) |
| `users.json` | JSON | 3 | User records (5 columns) |
| `inventory.avro` | Avro | 3 | Inventory items (5 columns) |
| `departments.xlsx` | Excel | 4 | Department records (5 columns) |
| `delta_products/` | Delta | 5 | Product delta table (5 columns) |

Generate binary fixtures (avro + xlsx) if missing:
```sh
python generate_extra_fixtures.py
```

---

## Skill Runners

| Runner | Skill | What it tests |
|--------|-------|---------------|
| `run_all.py` | all | Start env, seed DBs, run unit tests + all skill runners |
| `run_discover.py` | datacoolie-discover | 18 source types: DB/File/API/Lakehouse |
| `run_deploy.py` | datacoolie-deploy | Deploy preflight + package checks |
| `run_init.py` | datacoolie-init | Scaffold project structure |
| `run_metadata.py` | datacoolie-metadata | Metadata validation |

## Unit Tests

Pure-Python tests that require no running services. Run standalone:

```sh
# From workspace root (d:\GitHub\datacoolie-arch-5)
python -m pytest datacoolie/ai/skills/tests/unit/ -v
```

`run_all.py` runs these automatically before the integration suite.

| Test file | What it covers |
|-----------|----------------|
| `unit/test_api_introspect.py` | `_detect_pagination` (13 cases), `_build_auth_headers` (9 cases) |
| `unit/test_auth.py` | `resolve_env_var`, `build_mssql_token_struct`, `build_connection_string` (all dialects + mock token flows) |
| `unit/test_schema_parsers.py` | Golden schema assertions for CSV/TSV/JSON/JSONL/Parquet/Avro/Excel |
| `unit/test_file_introspect.py` | `max_files` boundary values, `_resolve_storage_options`, `introspect_files` smoke |

---

## Test Results

Output written to `test-results/` (not committed to git):

```
test-results/
├── discover/
│   ├── postgres/catalog.csv
│   ├── mysql/catalog.csv
│   ├── mssql/catalog.csv
│   ├── oracle/catalog.csv
│   ├── files/catalog.csv          (csv/jsonl/parquet/json/avro/xlsx/delta)
│   ├── files-s3/catalog.csv
│   ├── api/endpoints.csv
│   ├── api-apikey/endpoints.csv
│   ├── api-bearer/endpoints.csv
│   ├── api-basic/endpoints.csv
│   ├── api-graphql/endpoints.csv
│   ├── api-odata/endpoints.csv
│   ├── api-pagination/endpoints.csv
│   ├── api-sample-call/endpoints.csv
│   ├── api-yaml/endpoints.csv
│   ├── postgres-schema/catalog.csv
│   ├── iceberg/catalog.csv
│   └── delta/catalog.csv
├── deploy/
├── init/
└── metadata/
```

---

## Testing Guides (per skill)

| Skill | Guide |
|-------|-------|
| datacoolie-discover | [TESTING_datacoolie-discover.md](TESTING_datacoolie-discover.md) |
| datacoolie-deploy | [TESTING_datacoolie-deploy.md](TESTING_datacoolie-deploy.md) |
| datacoolie-init | [TESTING_datacoolie-init.md](TESTING_datacoolie-init.md) |
| datacoolie-metadata | [TESTING_datacoolie-metadata.md](TESTING_datacoolie-metadata.md) |

---

## Tear Down

```sh
# Stop + remove containers and volumes (drops all seeded data)
docker compose down -v

# Keep volumes (faster restart)
docker compose down
```

---

## Notes

- **Oracle seed** uses `gvenzl/oracle-free:23-slim`. First pull takes ~1-2 min. First start takes ~60s for DB initialization.
- **MSSQL seed** is applied by `run_all.py` via `docker exec sqlcmd` after the container health check passes.
- **Iceberg seed** (`fixtures/iceberg/seed_iceberg.py`) is run by `run_all.py` after Trino is healthy. It creates MinIO bucket `warehouse` automatically.
- **Hive** uses `apache/hive:4.0.0` with embedded Derby metastore. The `hive-init` one-shot container seeds `datacoolie_test` DB (customers, orders, products) via Beeline after Hive is healthy.
- **Both environments can run simultaneously** — ports are intentionally non-overlapping with `usecase-sim`.

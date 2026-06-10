# Source Introspection Guide

Use the packaged introspection scripts first. This guide is a fallback for unsupported sources,
driver failures, or manual verification. Do not duplicate database schema discovery logic in a
temporary script when `scripts/introspect_db.py` can connect.

## Database Introspection

Preferred command:

```bash
python scripts/introspect_db.py --url-env DATACOOLIE_DISCOVERY_URL --source <source> --output <schema.csv>
```

For ODBC-based SQL Server, Azure SQL, and Fabric SQL Database:

```bash
python scripts/introspect_db.py --odbc-connstr-env DATACOOLIE_ODBC_CONNSTR --source <source> --output <schema.csv>
```

If the script cannot run, connect using `psql`, `mysql`, `sqlcmd`, `sqlplus`, `sqlite3`, or any SQL client available in the terminal. Filter out system schemas (`information_schema`, `pg_catalog`, `sys`, `mysql`, `performance_schema`).

### PostgreSQL

```sql
-- Tables
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
ORDER BY table_schema, table_name;

-- Columns
SELECT column_name, data_type, is_nullable, ordinal_position
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = '<TABLE>'
ORDER BY ordinal_position;

-- Primary keys
SELECT a.attname FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE i.indisprimary AND n.nspname = '<SCHEMA>' AND c.relname = '<TABLE>';

-- Row estimate (stats-based, no full scan)
SELECT reltuples::bigint FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = '<SCHEMA>' AND c.relname = '<TABLE>';

-- Foreign keys
SELECT con.conname, attr.attname AS column_name, cls_r.relname AS ref_table, attr_r.attname AS ref_column
FROM pg_constraint con
JOIN pg_class cls ON cls.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
JOIN pg_class cls_r ON cls_r.oid = con.confrelid,
unnest(con.conkey, con.confkey) AS pairs(fk_col, ref_col)
JOIN pg_attribute attr ON attr.attrelid = con.conrelid AND attr.attnum = pairs.fk_col
JOIN pg_attribute attr_r ON attr_r.attrelid = con.confrelid AND attr_r.attnum = pairs.ref_col
WHERE con.contype = 'f' AND nsp.nspname = '<SCHEMA>' AND cls.relname = '<TABLE>';
```

### MySQL

```sql
-- Tables
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
ORDER BY table_schema, table_name;

-- Columns (same INFORMATION_SCHEMA pattern)
SELECT column_name, data_type, is_nullable, ordinal_position
FROM information_schema.columns
WHERE table_schema = '<SCHEMA>' AND table_name = '<TABLE>'
ORDER BY ordinal_position;

-- Primary keys (constraint name is always 'PRIMARY')
SELECT column_name FROM information_schema.key_column_usage
WHERE table_schema = '<SCHEMA>' AND table_name = '<TABLE>' AND constraint_name = 'PRIMARY'
ORDER BY ordinal_position;

-- Row estimate
SELECT table_rows FROM information_schema.tables
WHERE table_schema = '<SCHEMA>' AND table_name = '<TABLE>';

-- Foreign keys
SELECT constraint_name, column_name, referenced_table_name, referenced_column_name
FROM information_schema.key_column_usage
WHERE table_schema = '<SCHEMA>' AND table_name = '<TABLE>' AND referenced_table_name IS NOT NULL
ORDER BY constraint_name, ordinal_position;
```

### MSSQL

```sql
-- Tables
SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- Columns
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '<SCHEMA>' AND TABLE_NAME = '<TABLE>'
ORDER BY ORDINAL_POSITION;

-- Primary keys (must join on constraint_type, PK name varies)
SELECT kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_SCHEMA = '<SCHEMA>' AND tc.TABLE_NAME = '<TABLE>'
ORDER BY kcu.ORDINAL_POSITION;

-- Row estimate
SELECT SUM(p.rows) FROM sys.partitions p
JOIN sys.tables t ON p.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = '<SCHEMA>' AND t.name = '<TABLE>' AND p.index_id IN (0,1);

-- Foreign keys
SELECT fk.name AS constraint_name, col.name AS column_name,
       OBJECT_NAME(fkc.referenced_object_id) AS ref_table,
       rcol.name AS ref_column
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
JOIN sys.columns col ON col.object_id = fkc.parent_object_id AND col.column_id = fkc.parent_column_id
JOIN sys.columns rcol ON rcol.object_id = fkc.referenced_object_id AND rcol.column_id = fkc.referenced_column_id
JOIN sys.tables t ON t.object_id = fkc.parent_object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = '<SCHEMA>' AND t.name = '<TABLE>';
```

### Oracle

```sql
-- Tables (current user)
SELECT owner, table_name FROM all_tables
WHERE owner = SYS_CONTEXT('USERENV', 'SESSION_USER') ORDER BY table_name;

-- Columns
SELECT column_name, data_type, nullable, column_id
FROM all_tab_columns WHERE owner = '<SCHEMA>' AND table_name = '<TABLE>' ORDER BY column_id;

-- Primary keys
SELECT cols.column_name FROM all_constraints cons
JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
WHERE cons.constraint_type = 'P' AND cons.owner = '<SCHEMA>' AND cons.table_name = '<TABLE>'
ORDER BY cols.position;

-- Row estimate (requires DBMS_STATS)
SELECT num_rows FROM all_tables WHERE owner = '<SCHEMA>' AND table_name = '<TABLE>';
```

### SQLite

```sql
-- Tables
SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';

-- Columns + PKs (pk > 0 = primary key)
PRAGMA table_info('<TABLE>');

-- Foreign keys
PRAGMA foreign_key_list('<TABLE>');

-- Row count (no stats, must scan)
SELECT COUNT(*) FROM <TABLE>;
```

### Snowflake

```sql
-- Tables
SHOW TABLES IN SCHEMA <database>.<schema>;

-- Columns
SELECT column_name, data_type, is_nullable, ordinal_position
FROM <database>.information_schema.columns
WHERE table_schema = '<SCHEMA>' AND table_name = '<TABLE>'
ORDER BY ordinal_position;

-- Primary keys
SHOW PRIMARY KEYS IN TABLE <database>.<schema>.<table>;

-- Row estimate (requires ACCESS_HISTORY or ACCOUNT_USAGE)
SELECT row_count FROM <database>.information_schema.tables
WHERE table_schema = '<SCHEMA>' AND table_name = '<TABLE>';

-- Foreign keys
SHOW IMPORTED KEYS IN TABLE <database>.<schema>.<table>;
```

### BigQuery

```sql
-- Tables
SELECT table_name, table_type FROM <project>.<dataset>.INFORMATION_SCHEMA.TABLES;

-- Columns
SELECT column_name, data_type, is_nullable, ordinal_position
FROM <project>.<dataset>.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = '<TABLE>'
ORDER BY ordinal_position;

-- Primary keys (BigQuery uses CONSTRAINT_COLUMN_USAGE)
SELECT ccu.column_name FROM <project>.<dataset>.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN <project>.<dataset>.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
  ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = '<TABLE>';

-- Row estimate
SELECT row_count FROM <project>.<dataset>.__TABLES__ WHERE table_id = '<TABLE>';

-- BigQuery has no foreign keys — document relationships manually
```

### Redshift

```sql
-- Tables
SELECT schemaname, tablename FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, tablename;

-- Columns
SELECT column_name, data_type, is_nullable, ordinal_position
FROM information_schema.columns
WHERE table_schema = '<SCHEMA>' AND table_name = '<TABLE>'
ORDER BY ordinal_position;

-- Primary keys (same as PostgreSQL pattern)
SELECT a.attname FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE i.indisprimary AND n.nspname = '<SCHEMA>' AND c.relname = '<TABLE>';

-- Row estimate
SELECT "rows" FROM svv_table_info
WHERE "schema" = '<SCHEMA>' AND "table" = '<TABLE>';
```

### Any Other Database (DuckDB, CockroachDB, Trino, Databricks SQL, etc.)

Adapt the patterns above. Most modern databases support `INFORMATION_SCHEMA`:
- **DuckDB / CockroachDB**: Same INFORMATION_SCHEMA pattern as PostgreSQL
- **Trino / Presto**: `SHOW TABLES FROM <schema>`, `DESCRIBE <schema>.<table>`, `SHOW COLUMNS FROM <schema>.<table>`
- **Databricks SQL**: `SHOW TABLES IN <catalog>.<schema>`, `DESCRIBE TABLE <catalog>.<schema>.<table>`

For any unlisted database, the AI should: (1) check if INFORMATION_SCHEMA is available, (2) try `SHOW TABLES` / `DESCRIBE TABLE`, (3) check the database's documentation for catalog views.

## File Introspection

| Format | How to Inspect | Command |
|---|---|---|
| CSV | Read first 100 lines, infer headers + types | `head -100 data.csv` (Linux) or `Get-Content data.csv -First 100` (PowerShell) |
| Parquet | Read schema via pyarrow | `python -c "import pyarrow.parquet as pq; print(pq.read_schema('file.parquet'))"` |
| JSON/JSONL | Read first 100 records, extract keys | `head -100 data.jsonl \| python -c "import json,sys; print(list(json.load(sys.stdin).keys()))"` |
| Avro | Read schema via fastavro | `python -c "import fastavro; r=fastavro.reader(open('f.avro','rb')); print(r.writer_schema)"` |
| Excel | Read headers via openpyxl | `python -c "import openpyxl; wb=openpyxl.load_workbook('f.xlsx'); print(list(wb.active.iter_rows(max_row=1, values_only=True)))"` |
| Delta | Read schema from delta log | `python -c "from deltalake import DeltaTable; print(DeltaTable('path').schema())"` |
| Iceberg | Read schema from catalog or manifest | Use REST catalog API or `pyiceberg` |

**Cloud file discovery**:
- **AWS S3**: `aws s3 ls s3://bucket/prefix/ --recursive`
- **Azure ADLS/OneLake**: `az storage blob list --container-name <c> --account-name <a> --prefix <p>`
- **GCS**: `gsutil ls gs://bucket/prefix/`

## API Introspection

| API Type | Discovery Method |
|---|---|
| OpenAPI/Swagger | Fetch spec URL, parse JSON/YAML, extract paths + schemas |
| GraphQL | Send introspection query: `{ __schema { types { name fields { name type { name } } } } }` |
| OData | Fetch `$metadata` endpoint, parse XML EDMX |
| No spec | Ask user to describe endpoints, manually document request/response structure |

For authenticated APIs, ask user for auth method and credentials (via env vars). Use `curl` or `python requests` to make sample calls.

## Lakehouse Catalog Introspection

| Catalog | Command |
|---|---|
| AWS Glue | `aws glue get-tables --database-name <db> --region <region>` |
| Databricks Unity | `databricks unity-catalog list-tables --catalog <cat> --schema <schema>` |
| Apache Iceberg (REST) | `curl http://catalog:8181/v1/namespaces/<ns>/tables` |
| Delta Lake (path) | Scan directory for `_delta_log/` subdirectories |
| Hive Metastore | `beeline -u jdbc:hive2://host:10000 -e "SHOW TABLES IN db"` |
| Microsoft Fabric | Use Fabric REST API or `fab` CLI (microsoft/fabric-cli) |

## Type Normalization Reference

When recording discovered types, normalize source-specific types to these canonical forms:

| Source Type | Canonical Type |
|---|---|
| `int`, `int4`, `integer`, `INT`, `NUMBER(n,0)` | `integer` |
| `bigint`, `int8`, `BIGINT`, `NUMBER(19,0)` | `long` |
| `smallint`, `int2`, `SMALLINT` | `short` |
| `tinyint`, `TINYINT` | `byte` |
| `float`, `float4`, `real`, `FLOAT` | `float` |
| `double`, `float8`, `double precision`, `DOUBLE` | `double` |
| `numeric`, `decimal`, `NUMBER(p,s)`, `DECIMAL(p,s)` | `decimal(p,s)` |
| `varchar`, `nvarchar`, `text`, `char`, `VARCHAR2`, `CLOB`, `STRING` | `string` |
| `boolean`, `bool`, `bit`, `BOOLEAN` | `boolean` |
| `date`, `DATE` | `date` |
| `timestamp`, `datetime`, `datetime2`, `TIMESTAMP` | `timestamp` |
| `timestamp with time zone`, `timestamptz`, `TIMESTAMP_TZ` | `timestamp_tz` |
| `binary`, `varbinary`, `bytea`, `BLOB`, `RAW` | `binary` |
| `json`, `jsonb`, `VARIANT` | `string` (note: JSON content) |
| `uuid`, `uniqueidentifier` | `string` (note: UUID) |
| `array`, `ARRAY` | `array` |
| `map`, `struct`, `OBJECT` | `struct` |

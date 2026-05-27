# datacoolie-init — Testing Guide

This skill is **knowledge-based** — the AI reads SKILL.md rules and `templates/project-structure.md`, then creates project files directly using file tools. There are no scripts to execute.

---

## What to Test

### 1. SKILL.md Content Validation

Open `datacoolie-init/SKILL.md` and verify:

- [ ] AI workflow steps are clear: understand sources → scaffold → generate metadata → introspect
- [ ] All supported source types listed: database, file, API
- [ ] All supported engines listed: polars, spark, mixed
- [ ] All supported platforms listed: local, aws, databricks, fabric
- [ ] All supported layers listed: source2bronze, bronze2silver, silver2gold
- [ ] Metadata generation rules reference `datacoolie-metadata/references/schema-quick-reference.md`
- [ ] File introspection commands are documented (head, parquet-tools, Python one-liners)
- [ ] Engine advisory logic for "mixed" mode is described

### 2. Template Completeness

Check `datacoolie-init/templates/project-structure.md`:

- [ ] Directory tree covers all expected files: `.datacoolie/config.yaml`, `metadata/`, `functions/`, `scripts/`, `.gitignore`, `requirements.txt`
- [ ] Variable placeholders (`{project_name}`, `{engine}`, etc.) are documented
- [ ] Config.yaml template includes: `project_name`, `engine`, `platforms`, `environments`
- [ ] Metadata layout options documented (combined `metadata.json` vs split `connections.json`/`dataflows.json`)
- [ ] Functions package structure includes `pyproject.toml`, `__init__.py`, `sources.py`

### 3. Manual Workflow Testing — Basic Scaffolding

Ask the AI to scaffold a new project and verify output:

| Test Case | Prompt | Verify |
|-----------|--------|--------|
| Minimal defaults | "Create a datacoolie project called my_project" | All files from template exist, config.yaml has correct project_name |
| Engine selection | "Init project acme_etl with polars engine" | config.yaml shows `default_engine: polars` |
| Spark engine | "Scaffold spark_proj with spark" | run_local.py references SparkSession/SparkEngine |
| Multiple platforms | "Create cloud_proj for local, aws, fabric" | config.yaml lists all three platforms |
| Multiple layers | "Init medal_proj with source2bronze, bronze2silver, silver2gold" | Three dataflows in metadata, names prefixed with project name |
| Database source | "Create db_proj for database sources" | Connection format = `sql`, connection_type = `database` |
| API source | "Create api_proj for API sources" | Connection format = `api` |
| Parquet destination | "Create pq_proj with parquet destination" | Destination connection format = `parquet` |
| Iceberg destination | "Create ice_proj with iceberg destination" | Destination connection format = `iceberg` |

### 4. Manual Workflow Testing — Metadata Generation

- [ ] Generated `metadata.json` follows the datacoolie schema (v0.1.0)
- [ ] `connections` array has valid entries with `secrets_ref` for credentials (never hardcoded)
- [ ] `dataflows` reference existing connection names
- [ ] Incremental loads include `watermark_columns`
- [ ] Merge/SCD2 loads include `merge_keys`

### 5. Manual Workflow Testing — Source Introspection

- [ ] Given CSV files on disk, AI reads headers and generates column metadata
- [ ] Given SQL DDL text, AI parses CREATE TABLE and generates metadata
- [ ] Given verbal description, AI generates reasonable metadata
- [ ] For live databases, AI delegates to `datacoolie-discover` skill

### 6. Edge Cases

- [ ] Invalid source type (e.g., "kafka") → AI rejects or asks for correction
- [ ] Invalid platform (e.g., "gcp") → AI rejects or asks for correction
- [ ] Invalid layer name → AI rejects or asks for correction
- [ ] Output directory already exists → AI handles gracefully (warns, continues)
#   (stderr shown by PowerShell as a NativeCommandError object — that is expected, not a real failure)
# Exit 0
python -c "import shutil; shutil.rmtree(\"/tmp/sc_exists\")"
```

### 1.19 --metadata-layout combined — generates metadata.json only, runner uses it

```sh
python skills/datacoolie-init/scripts/scaffold.py --name comb_test --metadata-layout combined --output /tmp/sc_comb
python -c "import os; print(os.path.exists(\"/tmp/sc_comb/metadata/metadata.json\"  ))"  # True
python -c "import os; print(os.path.exists(\"/tmp/sc_comb/metadata/connections.json\"  ))"  # False
python -c "import os; print(os.path.exists(\"/tmp/sc_comb/metadata/dataflows.json\"  ))"  # False
python -c "import re; [print(l, end='') for l in open(\"/tmp/sc_comb/scripts/run_local.py\"  ) if re.search(r\"metadata.json\"  , l)]"
# Should show: _METADATA_PATH = str(... / "metadata" / "metadata.json")
python -c "import re; [print(l, end='') for l in open(\"/tmp/sc_comb/scripts/run_local.py\"  ) if re.search(r\"connections_path\"  , l)]"
# (no output — combined mode has no connections_path)
python -c "import shutil; shutil.rmtree(\"/tmp/sc_comb\")"
```

### 1.20 --metadata-layout split — generates connections.json + dataflows.json, runner uses connections_path

```sh
python skills/datacoolie-init/scripts/scaffold.py --name split_test --metadata-layout split --output /tmp/sc_split
python -c "import os; print(os.path.exists(\"/tmp/sc_split/metadata/metadata.json\"  ))"  # False
python -c "import os; print(os.path.exists(\"/tmp/sc_split/metadata/connections.json\"  ))"  # True
python -c "import os; print(os.path.exists(\"/tmp/sc_split/metadata/dataflows.json\"  ))"  # True
python -c "import re; [print(l, end='') for l in open(\"/tmp/sc_split/scripts/run_local.py\"  ) if re.search(r\"connections_path\"  , l)]"
# Should show: connections_path=_CONNECTIONS_PATH
python -c "import re; [print(l, end='') for l in open(\"/tmp/sc_split/scripts/run_local.py\"  ) if re.search(r\"dataflows.json\"  , l)]"
# Should show: _DATAFLOWS_PATH = str(... / "metadata" / "dataflows.json")
python -c "import shutil; shutil.rmtree(\"/tmp/sc_split\")"
```

---

## 2. introspect.py — folder mode

### Setup: create sample data directories

```sh
python -c "import os; os.makedirs(\"/tmp/intro_data/orders\"  , exist_ok=True)"
python -c "import os; os.makedirs(\"/tmp/intro_data/customers\"  , exist_ok=True)"

@"
order_id,customer_id,amount,created_at,status
1,101,29.99,2024-01-01,active
2,102,149.50,2024-01-02,completed
"@ | Set-Content /tmp/intro_data/orders/orders.csv

@"
customer_id,name,email,is_active,balance
101,Alice,alice@example.com,true,500.00
102,Bob,bob@example.com,false,0.00
"@ | Set-Content /tmp/intro_data/customers/customers.csv
```

### 2.1 Scan folder with CSV subdirectories

```sh
python skills/datacoolie-init/scripts/introspect.py --mode folder --path /tmp/intro_data --connection-name raw_data
# Outputs valid JSON to stdout
python skills/datacoolie-init/scripts/introspect.py --mode folder --path /tmp/intro_data --connection-name raw_data | ConvertFrom-Json
# .connections[0].name     → raw_data
# .connections[0].format   → csv
# .dataflows.Count         → 2 (orders, customers)
```

### 2.2 Schema hints inferred — amount is decimal, customer_id is int

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode folder --path /tmp/intro_data --connection-name src | ConvertFrom-Json
$orders_df = $meta.dataflows | Where-Object { $_.name -eq "ingest_orders" }
$hints = $orders_df.transform.schema_hints
($hints | Where-Object { $_.column_name -eq "amount" }).data_type     # decimal
($hints | Where-Object { $_.column_name -eq "customer_id" }).data_type  # int
($hints | Where-Object { $_.column_name -eq "created_at" }).data_type   # timestamp
```

### 2.3 Boolean column inferred from is_active

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode folder --path /tmp/intro_data --connection-name src | ConvertFrom-Json
$cust_df = $meta.dataflows | Where-Object { $_.name -eq "ingest_customers" }
$hints = $cust_df.transform.schema_hints
($hints | Where-Object { $_.column_name -eq "is_active" }).data_type   # boolean
```

### 2.4 Write to output file

```sh
python skills/datacoolie-init/scripts/introspect.py --mode folder --path /tmp/intro_data --connection-name src --output /tmp/intro_out.json
python -c "import os; print(os.path.exists(\"/tmp/intro_out.json\"  ))"  # True
(Get-Content /tmp/intro_out.json | ConvertFrom-Json).connections.Count  # 2
Remove-Item /tmp/intro_out.json
```

### 2.5 Custom destination connection name and format

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode folder --path /tmp/intro_data --connection-name src --dest-connection bronze_layer --dest-format parquet | ConvertFrom-Json
($meta.connections | Where-Object { $_.name -eq "bronze_layer" }).format  # parquet
($meta.dataflows[0].destination.connection_name)  # bronze_layer
```

### 2.6 Flat directory (no subdirs) — single table

```sh
python -c "import os; os.makedirs(\"/tmp/flat_data\"  , exist_ok=True)"
@"
id,value
1,hello
2,world
"@ | Set-Content /tmp/flat_data/records.csv

$meta = python skills/datacoolie-init/scripts/introspect.py --mode folder --path /tmp/flat_data --connection-name flat_src | ConvertFrom-Json
$meta.dataflows.Count  # 1
$meta.dataflows[0].name  # ingest_flat_data
python -c "import shutil; shutil.rmtree(\"/tmp/flat_data\")"
```

### 2.7 JSON files — schema inferred from first record

```sh
python -c "import os; os.makedirs(\"/tmp/json_data/events\"  , exist_ok=True)"
'[{"event_id":1,"amount":19.99,"created_at":"2024-01-01","is_paid":true}]' | Set-Content /tmp/json_data/events/events.json

$meta = python skills/datacoolie-init/scripts/introspect.py --mode folder --path /tmp/json_data --connection-name json_src | ConvertFrom-Json
$hints = $meta.dataflows[0].transform.schema_hints
($hints | Where-Object { $_.column_name -eq "amount" }).data_type     # decimal
($hints | Where-Object { $_.column_name -eq "event_id" }).data_type   # int
($hints | Where-Object { $_.column_name -eq "is_paid" }).data_type    # boolean
python -c "import shutil; shutil.rmtree(\"/tmp/json_data\")"
```

### 2.8 Path is not a directory — expect exit 2

```sh
python skills/datacoolie-init/scripts/introspect.py --mode folder --path /tmp/nonexistent_dir --connection-name src
# ERROR: Not a directory: /tmp/nonexistent_dir
# Exit 2
echo $?  # print exit code  # Exit: 2
```

### 2.9 Empty directory — expect exit 1

```sh
python -c "import os; os.makedirs(\"/tmp/empty_dir\"  , exist_ok=True)"
python skills/datacoolie-init/scripts/introspect.py --mode folder --path /tmp/empty_dir --connection-name src
# Exit 1 (no recognizable data files)
python -c "import shutil; shutil.rmtree(\"/tmp/empty_dir\")"
```

### 2.10 --path missing for folder mode — expect exit 2

```sh
python skills/datacoolie-init/scripts/introspect.py --mode folder --connection-name src
# ERROR: --path is required for folder mode.
# Exit 2
```

### Cleanup

```sh
python -c "import shutil; shutil.rmtree(\"/tmp/intro_data\")"
```

---

## 3. introspect.py — ddl mode

### Setup: create DDL files

```sh
@"
CREATE TABLE IF NOT EXISTS orders (
    order_id     BIGINT       NOT NULL,
    customer_id  INT          NOT NULL,
    amount       DECIMAL(10,2),
    status       VARCHAR(50),
    created_at   TIMESTAMP,
    is_active    BOOLEAN
);

CREATE TABLE customers (
    customer_id  INT          PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    email        VARCHAR(255),
    balance      FLOAT
);
"@ | Set-Content /tmp/schema.sql
```

### 3.1 Parse DDL — two tables, correct column count

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode ddl --ddl-file /tmp/schema.sql --connection-name db_src | ConvertFrom-Json
$meta.dataflows.Count  # 2 (orders, customers)
$meta.dataflows[0].name  # ingest_orders
$meta.dataflows[1].name  # ingest_customers
```

### 3.2 SQL types mapped correctly

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode ddl --ddl-file /tmp/schema.sql --connection-name db_src | ConvertFrom-Json
$orders_hints = ($meta.dataflows | Where-Object { $_.name -eq "ingest_orders" }).transform.schema_hints
($orders_hints | Where-Object { $_.column_name -eq "order_id" }).data_type    # int
($orders_hints | Where-Object { $_.column_name -eq "amount" }).data_type      # decimal
($orders_hints | Where-Object { $_.column_name -eq "status" }).data_type      # string
($orders_hints | Where-Object { $_.column_name -eq "created_at" }).data_type  # timestamp
($orders_hints | Where-Object { $_.column_name -eq "is_active" }).data_type   # boolean
```

### 3.3 Database connection generated with sql format

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode ddl --ddl-file /tmp/schema.sql --connection-name db_src | ConvertFrom-Json
$meta.connections[0].format   # sql
$meta.connections[0].connection_type  # database
```

### 3.4 PRIMARY KEY keyword skipped — not added as column

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode ddl --ddl-file /tmp/schema.sql --connection-name db_src | ConvertFrom-Json
$cust_hints = ($meta.dataflows | Where-Object { $_.name -eq "ingest_customers" }).transform.schema_hints
$cust_hints.column_name | Where-Object { $_ -eq "PRIMARY" }  # (empty)
```

### 3.5 Custom dest-connection and dest-format

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode ddl --ddl-file /tmp/schema.sql --connection-name db_src --dest-connection silver --dest-format iceberg | ConvertFrom-Json
($meta.connections | Where-Object { $_.name -eq "silver" }).format  # iceberg
($meta.dataflows[0].destination.connection_name)  # silver
```

### 3.6 Write to output file

```sh
python skills/datacoolie-init/scripts/introspect.py --mode ddl --ddl-file /tmp/schema.sql --connection-name db_src --output /tmp/ddl_out.json
python -c "import os; print(os.path.exists(\"/tmp/ddl_out.json\"  ))"  # True
Remove-Item /tmp/ddl_out.json
```

### 3.7 DDL file not found — expect exit 2

```sh
python skills/datacoolie-init/scripts/introspect.py --mode ddl --ddl-file /tmp/missing.sql --connection-name src
# ERROR: File not found: /tmp/missing.sql
# Exit 2
```

### 3.8 --ddl-file missing for ddl mode — expect exit 2

```sh
python skills/datacoolie-init/scripts/introspect.py --mode ddl --connection-name src
# ERROR: --ddl-file is required for ddl mode.
# Exit 2
```

### 3.9 DDL with IF NOT EXISTS — table name still parsed

```sh
'CREATE TABLE IF NOT EXISTS my_table (id INT, name VARCHAR(100));' | Set-Content /tmp/ifnotexists.sql
$meta = python skills/datacoolie-init/scripts/introspect.py --mode ddl --ddl-file /tmp/ifnotexists.sql --connection-name src | ConvertFrom-Json
$meta.dataflows[0].name  # ingest_my_table
Remove-Item /tmp/ifnotexists.sql
```

### 3.10 Empty DDL file — zero dataflows

```sh
'' | Set-Content /tmp/empty.sql
$meta = python skills/datacoolie-init/scripts/introspect.py --mode ddl --ddl-file /tmp/empty.sql --connection-name src | ConvertFrom-Json
$meta.dataflows.Count  # 0
Remove-Item /tmp/empty.sql
```

### Cleanup

```sh
Remove-Item /tmp/schema.sql
```

---

## 4. introspect.py — manual mode

### 4.1 Basic column definitions

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode manual --columns "id:int,name:string,amount:decimal,created_at:timestamp" | ConvertFrom-Json
$hints = $meta.dataflows[0].transform.schema_hints
$hints.Count  # 4
($hints | Where-Object { $_.column_name -eq "id" }).data_type            # int
($hints | Where-Object { $_.column_name -eq "amount" }).data_type        # decimal
($hints | Where-Object { $_.column_name -eq "created_at" }).data_type    # timestamp
```

### 4.2 ordinal_position is sequential

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode manual --columns "a:int,b:string,c:decimal" | ConvertFrom-Json
$hints = $meta.dataflows[0].transform.schema_hints
$hints[0].ordinal_position  # 1
$hints[1].ordinal_position  # 2
$hints[2].ordinal_position  # 3
```

### 4.3 SQL type aliases mapped — bigint → int, varchar → string

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode manual --columns "user_id:bigint,label:varchar,score:float,event_date:datetime" | ConvertFrom-Json
$hints = $meta.dataflows[0].transform.schema_hints
($hints | Where-Object { $_.column_name -eq "user_id" }).data_type    # int
($hints | Where-Object { $_.column_name -eq "label" }).data_type      # string
($hints | Where-Object { $_.column_name -eq "score" }).data_type      # decimal
($hints | Where-Object { $_.column_name -eq "event_date" }).data_type # timestamp
```

### 4.4 Column without type — defaults to string

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode manual --columns "id:int,notes" | ConvertFrom-Json
$hints = $meta.dataflows[0].transform.schema_hints
($hints | Where-Object { $_.column_name -eq "notes" }).data_type  # string
```

### 4.5 Custom connection-name

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode manual --columns "id:int" --connection-name my_src | ConvertFrom-Json
$meta.connections[0].name  # my_src
```

### 4.6 Custom dest-connection

```sh
$meta = python skills/datacoolie-init/scripts/introspect.py --mode manual --columns "id:int" --dest-connection landing | ConvertFrom-Json
$meta.dataflows[0].destination.connection_name  # landing
```

### 4.7 --columns missing for manual mode — expect exit 2

```sh
python skills/datacoolie-init/scripts/introspect.py --mode manual --connection-name src
# ERROR: --columns is required for manual mode.
# Exit 2
```

### 4.8 Output is valid DataCoolie metadata (passes validate)

```sh
python skills/datacoolie-init/scripts/introspect.py --mode manual --columns "id:int,name:string" --output /tmp/manual_out.json
python skills/datacoolie-metadata/scripts/validate.py /tmp/manual_out.json
# ✓ manual_out.json is valid (schema v0.1.0)
Remove-Item /tmp/manual_out.json
```

---

## 5. End-to-end flows

### 5.1 Scaffold combined → validate

```sh
python skills/datacoolie-init/scripts/scaffold.py --name e2e_proj --metadata-layout combined --output /tmp/e2e_test
python skills/datacoolie-metadata/scripts/validate.py /tmp/e2e_test/metadata/metadata.json
# ✓ metadata.json is valid (schema v0.1.0)
python skills/datacoolie-metadata/scripts/lint.py /tmp/e2e_test/metadata/metadata.json --env dev
# ✓ metadata.json: no lint warnings  (or INFO only)
python -c "import shutil; shutil.rmtree(\"/tmp/e2e_test\")"
```

### 5.1b Scaffold split → validate both files

```sh
python skills/datacoolie-init/scripts/scaffold.py --name e2e_split --metadata-layout split --output /tmp/e2e_split
# Validate connections standalone (connections-only file is valid via anyOf)
python skills/datacoolie-metadata/scripts/validate.py /tmp/e2e_split/metadata/connections.json
# Validate dataflows standalone
python skills/datacoolie-metadata/scripts/validate.py /tmp/e2e_split/metadata/dataflows.json
python -c "import shutil; shutil.rmtree(\"/tmp/e2e_split\")"
```

### 5.2 Scaffold → deploy preflight

```sh
python skills/datacoolie-init/scripts/scaffold.py --name deploy_ready --output /tmp/deploy_ready
python skills/datacoolie-deploy/scripts/preflight.py --platform local --project-dir /tmp/deploy_ready
# ✓ Metadata found (1 file(s) in metadata/)
# ✓ Functions packageable (functions/pyproject.toml found (wheel build))
# Exit 0
python -c "import shutil; shutil.rmtree(\"/tmp/deploy_ready\")"
```

### 5.3 Folder introspect → validate

```sh
python -c "import os; os.makedirs(\"/tmp/e2e_intro/sales\"  , exist_ok=True)"
"id,revenue,sale_date`n1,100.0,2024-01-01" | Set-Content /tmp/e2e_intro/sales/sales.csv
python skills/datacoolie-init/scripts/introspect.py --mode folder --path /tmp/e2e_intro --connection-name raw --output /tmp/e2e_intro_out.json
python skills/datacoolie-metadata/scripts/validate.py /tmp/e2e_intro_out.json
# ✓ e2e_intro_out.json is valid (schema v0.1.0)
python -c "import shutil; shutil.rmtree(\"/tmp/e2e_intro\")"
Remove-Item /tmp/e2e_intro_out.json
```

### 5.4 DDL introspect → validate

```sh
"CREATE TABLE products (product_id INT, name VARCHAR(200), price DECIMAL(10,2));" | Set-Content /tmp/e2e_ddl.sql
python skills/datacoolie-init/scripts/introspect.py --mode ddl --ddl-file /tmp/e2e_ddl.sql --connection-name db --output /tmp/e2e_ddl_out.json
python skills/datacoolie-metadata/scripts/validate.py /tmp/e2e_ddl_out.json
# ✓ e2e_ddl_out.json is valid (schema v0.1.0)
Remove-Item /tmp/e2e_ddl.sql, /tmp/e2e_ddl_out.json
```

### 5.5 Manual introspect → validate

```sh
python skills/datacoolie-init/scripts/introspect.py --mode manual --columns "id:int,name:string,price:decimal" --output /tmp/e2e_manual_out.json
python skills/datacoolie-metadata/scripts/validate.py /tmp/e2e_manual_out.json
# ✓ e2e_manual_out.json is valid (schema v0.1.0)
Remove-Item /tmp/e2e_manual_out.json
```

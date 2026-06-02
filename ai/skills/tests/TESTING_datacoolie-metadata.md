# datacoolie-metadata — Testing Guide

## Test Environment

Start the shared integration environment before running these tests:
```sh
cd datacoolie/ai/skills/tests
docker compose up -d --wait
python run_all.py --no-docker   # seed MSSQL + Iceberg
```

See [README.md](README.md) for full connection details.

---

All commands run from `datacoolie/ai/skills/tests/` with venv activated.

Sample metadata files used throughout:
```
usecase-sim/metadata/file/local_use_cases.json   # 32 connections, 131 dataflows
usecase-sim/metadata/file/local_use_cases.xlsx   # same data in native xlsx format
usecase-sim/metadata/file/aws_use_cases.json
usecase-sim/metadata/file/perf_test.json
```

---

## 1. validate.py

### 1.1 Valid file — expect exit 0

```sh
python skills/datacoolie-metadata/scripts/validate.py usecase-sim/metadata/file/local_use_cases.json
# ✓ local_use_cases.json is valid (schema v0.1.0)
```

### 1.2 Valid file, explicit schema version

```sh
python skills/datacoolie-metadata/scripts/validate.py usecase-sim/metadata/file/local_use_cases.json --schema-version 0.1.0
# ✓ local_use_cases.json is valid (schema v0.1.0)
```

### 1.3 YAML input

```sh
python skills/datacoolie-metadata/scripts/convert.py usecase-sim/metadata/file/local_use_cases.json --to yaml --output test.yaml
python skills/datacoolie-metadata/scripts/validate.py test.yaml
# ✓ test.yaml is valid (schema v0.1.0)
Remove-Item test.yaml
```

### 1.4 Invalid file — expect exit 1 with error path

```sh
# Invalid format enum ('xlsx' is not allowed; valid value is 'excel')
python -c "import json; d={'connections':[{'name':'c1','connection_type':'file','format':'xlsx'}],'dataflows':[]}; json.dump(d,open('bad.json','w'))"
python skills/datacoolie-metadata/scripts/validate.py bad.json
# ✗ bad.json has N validation error(s):
#   connections[0].format: 'xlsx' is not one of ['delta', 'iceberg', ...]
Remove-Item bad.json
```

```sh
# Dataflow missing required 'name' field
python -c "import json; d={'connections':[{'name':'c1','format':'csv','connection_type':'file','configure':{'base_path':'/tmp'}}],'dataflows':[{'source':{'connection_name':'c1'},'destination':{'connection_name':'c1','table':'t','load_type':'full_load'}}]}; json.dump(d,open('bad2.json','w'))"
python skills/datacoolie-metadata/scripts/validate.py bad2.json
# ✗ bad2.json has N validation error(s):
#   dataflows[0]: 'name' is a required property
Remove-Item bad2.json
```

### 1.5 File not found — expect exit 2

```sh
python skills/datacoolie-metadata/scripts/validate.py nonexistent.json
# ERROR: File not found: nonexistent.json
# Exit 2
```

### 1.6 Quiet mode for CI — expect no output, only exit code

```sh
python skills/datacoolie-metadata/scripts/validate.py usecase-sim/metadata/file/local_use_cases.json --quiet
echo $?  # print exit code   # Exit: 0  (no output)

python -c "import json; json.dump({'connections':[{'name':'c1','connection_type':'file','format':'xlsx'}],'dataflows':[]},open('bad.json','w'))"
python skills/datacoolie-metadata/scripts/validate.py bad.json --quiet
echo $?  # print exit code   # Exit: 1  (no output)
Remove-Item bad.json
```

### 1.7 Fetch latest schema from GitHub

```sh
python skills/datacoolie-metadata/scripts/validate.py --fetch-latest
# ✓ Schema vX.Y.Z fetched from GitHub and cached in ~/.datacoolie/schemas/
# Exit 0 (schema-refresh-only; no file given)
```

### 1.8 Fetch and validate in one command

```sh
python skills/datacoolie-metadata/scripts/validate.py --fetch-latest usecase-sim/metadata/file/local_use_cases.json
# ✓ Schema refreshed ... 
# ✓ local_use_cases.json is valid (schema v0.1.0)
```

### 1.9 Custom schemas directory

```sh
python skills/datacoolie-metadata/scripts/validate.py usecase-sim/metadata/file/local_use_cases.json --schemas-dir skills/datacoolie-metadata/schemas
# Uses skill-bundled schemas explicitly
```

### 1.10 All metadata files batch

```sh
Get-ChildItem usecase-sim/metadata/file/*.json | ForEach-Object {
    python skills/datacoolie-metadata/scripts/validate.py $_.FullName
}
# Each file should print ✓
```

### 1.11 Excel (.xlsx) input

```sh
python skills/datacoolie-metadata/scripts/validate.py usecase-sim/metadata/file/local_use_cases.xlsx
# ✓ local_use_cases.xlsx is valid (schema v0.1.0)
```

---

## 2. lint.py

### 2.1 Clean file — expect exit 0

```sh
python skills/datacoolie-metadata/scripts/lint.py usecase-sim/metadata/file/local_use_cases.json
# ✓ local_use_cases.json: no lint warnings (polars/dev)
```

### 2.2 Excel (.xlsx) input

```sh
python skills/datacoolie-metadata/scripts/lint.py usecase-sim/metadata/file/local_use_cases.xlsx --engine polars --env dev
# ⚠ ... N warning(s): watermark-for-incremental on incremental dataflows without watermark_columns
```

### 2.3 Non-dev environment (inferSchema warning)

```sh
python skills/datacoolie-metadata/scripts/lint.py usecase-sim/metadata/file/local_use_cases.json --env prod
# ⚠ ... N warning(s):
#   [WARN] dataflows[N].source.configure.read_options: inferSchema is enabled in non-dev environment.
```

### 2.4 Spark engine (dot-notation filter suppressed)

```sh
python skills/datacoolie-metadata/scripts/lint.py usecase-sim/metadata/file/local_use_cases.json --engine spark --env dev
# portable-filter-expression warnings should NOT appear for Spark engine
```

### 2.5 Quiet mode for CI

```sh
python skills/datacoolie-metadata/scripts/lint.py usecase-sim/metadata/file/local_use_cases.json --quiet
echo $?  # print exit code   # Exit: 0 or 1, no output
```

### 2.6 Rule: undefined-connection

```sh
python -c "
import json
data = {
    'connections': [{'name': 'real_conn', 'format': 'csv', 'connection_type': 'file', 'configure': {'base_path': '/tmp'}}],
    'dataflows': [{'name': 'df1',
        'source': {'connection_name': 'typo_conn', 'table': 'x'},
        'destination': {'connection_name': 'real_conn', 'table': 'y', 'load_type': 'full_load'}}]
}
json.dump(data, open('lint_test.json','w'), indent=2)
"
python skills/datacoolie-metadata/scripts/lint.py lint_test.json
# [WARN] dataflows[0].source.connection_name: References connection 'typo_conn' which is not defined in connections[].
Remove-Item lint_test.json
```

### 2.7 Rule: merge-keys-required (merge_upsert / merge_overwrite / scd2)

```sh
python -c "
import json
data = {
    'connections': [{'name': 'c1', 'format': 'delta', 'connection_type': 'lakehouse', 'configure': {'base_path': '/tmp'}}],
    'dataflows': [
        {'name': 'df1',
            'source': {'connection_name': 'c1', 'table': 'x'},
            'destination': {'connection_name': 'c1', 'table': 'y', 'load_type': 'merge_upsert', 'merge_keys': []}},
        {'name': 'df2',
            'source': {'connection_name': 'c1', 'table': 'a'},
            'destination': {'connection_name': 'c1', 'table': 'b', 'load_type': 'scd2', 'configure': {'scd2_effective_column': 'modified_at'}}}
    ]
}
json.dump(data, open('lint_test.json','w'), indent=2)
"
python skills/datacoolie-metadata/scripts/lint.py lint_test.json
# [WARN] dataflows[0].destination: load_type is 'merge_upsert' but merge_keys is empty.
# [WARN] dataflows[1].destination: load_type is 'scd2' but merge_keys is empty.
Remove-Item lint_test.json
```

### 2.8 Rule: watermark-for-incremental

```sh
python -c "
import json
data = {
    'connections': [{'name': 'c1', 'format': 'delta', 'connection_type': 'lakehouse', 'configure': {'base_path': '/tmp'}}],
    'dataflows': [{'name': 'df1',
        'source': {'connection_name': 'c1', 'table': 'x', 'watermark_columns': []},
        'destination': {'connection_name': 'c1', 'table': 'y', 'load_type': 'append',
                        'merge_keys': ['id']}}]
}
json.dump(data, open('lint_test.json','w'), indent=2)
"
python skills/datacoolie-metadata/scripts/lint.py lint_test.json
# [WARN] dataflows[0].source: Incremental load_type 'append' but source has no watermark_columns.
Remove-Item lint_test.json
```

### 2.9 Rule: inactive-connection

```sh
python -c "
import json
data = {
    'connections': [{'name': 'c1', 'format': 'csv', 'connection_type': 'file', 'is_active': False, 'configure': {'base_path': '/tmp'}}],
    'dataflows': [{'name': 'df1',
        'source': {'connection_name': 'c1', 'table': 'x'},
        'destination': {'connection_name': 'c1', 'table': 'y', 'load_type': 'full_load'}}]
}
json.dump(data, open('lint_test.json','w'), indent=2)
"
python skills/datacoolie-metadata/scripts/lint.py lint_test.json
# [WARN] dataflows[0].source.connection_name: References connection 'c1' which is marked is_active: false.
Remove-Item lint_test.json
```

### 2.10 Rule: unique-dataflow-name

```sh
python -c "
import json
data = {
    'connections': [{'name': 'c1', 'format': 'csv', 'connection_type': 'file', 'configure': {'base_path': '/tmp'}}],
    'dataflows': [
        {'name': 'dup_name', 'source': {'connection_name': 'c1', 'table': 'x'}, 'destination': {'connection_name': 'c1', 'table': 'y', 'load_type': 'full_load'}},
        {'name': 'dup_name', 'source': {'connection_name': 'c1', 'table': 'a'}, 'destination': {'connection_name': 'c1', 'table': 'b', 'load_type': 'full_load'}}
    ]
}
json.dump(data, open('lint_test.json','w'), indent=2)
"
python skills/datacoolie-metadata/scripts/lint.py lint_test.json
# [WARN] dataflows[1].name: Duplicate dataflow name 'dup_name' (first at index 0).
Remove-Item lint_test.json
```

### 2.11 Rule: partition-large-tables (REMOVED)

```sh
# This rule was removed — watermark_columns does NOT imply partition_columns needed.
# Partition decisions depend on table size and partition granularity (not determinable from metadata).
# No test case needed.
```

### 2.12 Rule: scd2-effective-column-required

```sh
python -c "
import json
data = {
    'connections': [{'name': 'c1', 'format': 'delta', 'connection_type': 'lakehouse', 'configure': {'base_path': '/tmp'}}],
    'dataflows': [{'name': 'df1',
        'source': {'connection_name': 'c1', 'table': 'x', 'watermark_columns': ['modified_at']},
        'destination': {'connection_name': 'c1', 'table': 'y', 'load_type': 'scd2', 'merge_keys': ['id']}}]
}
json.dump(data, open('lint_test.json','w'), indent=2)
"
python skills/datacoolie-metadata/scripts/lint.py lint_test.json
# [WARN] dataflows[0].destination.configure: load_type is 'scd2' but scd2_effective_column is not set.
#        → Set destination.configure.scd2_effective_column to the column used as __valid_from (e.g. 'modified_at').
Remove-Item lint_test.json
```

---

## 3. convert.py

### 3.1 JSON → YAML

```sh
python skills/datacoolie-metadata/scripts/convert.py usecase-sim/metadata/file/local_use_cases.json --to yaml --output out.yaml
# ✓ Converted local_use_cases.json → out.yaml (yaml)
Remove-Item out.yaml
```

### 3.2 YAML → JSON

```sh
python skills/datacoolie-metadata/scripts/convert.py usecase-sim/metadata/file/local_use_cases.json --to yaml --output tmp.yaml
python skills/datacoolie-metadata/scripts/convert.py tmp.yaml --to json --output out.json
python skills/datacoolie-metadata/scripts/validate.py out.json
# ✓ out.json is valid (schema v0.1.0)
Remove-Item tmp.yaml, out.json
```

### 3.3 JSON → Excel

```sh
python skills/datacoolie-metadata/scripts/convert.py usecase-sim/metadata/file/local_use_cases.json --to excel --output out.xlsx
# ✓ Converted local_use_cases.json → out.xlsx (excel)
# Open out.xlsx: should have sheets 'connections', 'dataflows', 'schema_hints'
# Columns: source_connection_name, destination_table, source_watermark_columns, etc.
Remove-Item out.xlsx
```

### 3.4 Excel → JSON (round-trip)

```sh
python skills/datacoolie-metadata/scripts/convert.py usecase-sim/metadata/file/local_use_cases.json --to excel --output rt.xlsx
python skills/datacoolie-metadata/scripts/convert.py rt.xlsx --to json --output rt.json
python skills/datacoolie-metadata/scripts/validate.py rt.json
# ✓ rt.json is valid (schema v0.1.0)
Remove-Item rt.xlsx, rt.json
```

### 3.5 Excel → YAML

```sh
python skills/datacoolie-metadata/scripts/convert.py usecase-sim/metadata/file/local_use_cases.json --to excel --output rt.xlsx
python skills/datacoolie-metadata/scripts/convert.py rt.xlsx --to yaml --output rt.yaml
# ✓ Converted rt.xlsx → rt.yaml (yaml)
Remove-Item rt.xlsx, rt.yaml
```

### 3.6 Note — $schema not preserved in Excel round-trip

```sh
# Native xlsx format uses three data sheets (connections, dataflows, schema_hints).
# Top-level scalar fields like $schema are NOT stored in Excel.
# Add $schema back manually when converting xlsx → json/yaml, or carry it in the JSON source.
@'
import json
d = json.load(open('usecase-sim/metadata/file/local_use_cases.json'))
d['$schema'] = 'schemas/0.1.0/metadata.schema.json'
json.dump(d, open('with_schema.json', 'w'), indent=2)
'@ | Set-Content _tmp_test.py
python _tmp_test.py
python skills/datacoolie-metadata/scripts/convert.py with_schema.json --to excel --output rt.xlsx
python skills/datacoolie-metadata/scripts/convert.py rt.xlsx --to json --output rt.json
@'
import json
d = json.load(open('rt.json'))
print('Has $schema:', '$schema' in d)
'@ | Set-Content _tmp_test.py
python _tmp_test.py
# Has $schema: False  (expected — $schema is not stored in Excel native format)
Remove-Item with_schema.json, rt.xlsx, rt.json, _tmp_test.py
```

### 3.7 Default output path (no --output)

```sh
Copy-Item usecase-sim/metadata/file/local_use_cases.json ./local_copy.json
python skills/datacoolie-metadata/scripts/convert.py local_copy.json --to yaml
# Output: local_copy.yaml  (same name, new extension)
Remove-Item local_copy.json, local_copy.yaml
```

### 3.8 Error — Excel input with --to excel

```sh
python skills/datacoolie-metadata/scripts/convert.py usecase-sim/metadata/file/local_use_cases.json --to excel --output rt.xlsx
python skills/datacoolie-metadata/scripts/convert.py rt.xlsx --to excel
# ERROR: Input and output are both Excel. Use --to json or --to yaml.
# Exit 2
Remove-Item rt.xlsx
```

### 3.9 Error — same input and output path

```sh
Copy-Item usecase-sim/metadata/file/local_use_cases.json ./same.json
python skills/datacoolie-metadata/scripts/convert.py same.json --to json --output same.json
# ERROR: Output path is same as input. Use --output to specify a different path.
# Exit 2
Remove-Item same.json
```

### 3.10 Error — file not found

```sh
python skills/datacoolie-metadata/scripts/convert.py missing.json --to yaml
# ERROR: File not found: missing.json
# Exit 2
```

---

## 4. merge.py

### Setup: create a base directory with split files

```sh
python -c "import os; os.makedirs(\"test_base\environments\"  , exist_ok=True)"

# connections.json
python -c "
import json
data = [
    {'name': 'src', 'format': 'csv', 'connection_type': 'file', 'configure': {'base_path': '/dev/input'}},
    {'name': 'dst', 'format': 'delta', 'connection_type': 'lakehouse', 'configure': {'base_path': '/dev/output'}}
]
json.dump(data, open('test_base/connections.json','w'), indent=2)
"

# dataflows.json
python -c "
import json
data = [
    {'name': 'flow1', 'source': {'connection_name': 'src', 'table': 'raw'}, 'destination': {'connection_name': 'dst', 'table': 'bronze', 'load_type': 'full_load'}},
    {'name': 'flow2', 'source': {'connection_name': 'src', 'table': 'raw2'}, 'destination': {'connection_name': 'dst', 'table': 'bronze2', 'load_type': 'full_load'}}
]
json.dump(data, open('test_base/dataflows.json','w'), indent=2)
"

# environments/prod.yaml
@"
connections:
  - name: src
    configure:
      base_path: /prod/input
  - name: dst
    configure:
      base_path: /prod/output
dataflows:
  - name: flow1
    destination:
      load_type: merge_upsert
      merge_keys: [id]
"@ | Set-Content test_base\environments\prod.yaml
```

### 4.1 Basic merge

```sh
python skills/datacoolie-metadata/scripts/merge.py --base test_base --env prod
# ✓ Merged 2 connections + 2 dataflows → .datacoolie/generated/metadata.prod.json
# Check: src.configure.base_path should be /prod/input
python -c "import json; d=json.load(open('.datacoolie/generated/metadata.prod.json')); print(next(c for c in d['connections'] if c['name']=='src')['configure']['base_path'])"
# /prod/input
```

### 4.2 Custom output path

```sh
python skills/datacoolie-metadata/scripts/merge.py --base test_base --env prod --output merged_out.json
# ✓ Merged 2 connections + 2 dataflows → merged_out.json
python skills/datacoolie-metadata/scripts/validate.py merged_out.json
# ✓ merged_out.json is valid (schema v0.1.0)
Remove-Item merged_out.json
```

### 4.3 Base items not in overlay are preserved unchanged

```sh
python skills/datacoolie-metadata/scripts/merge.py --base test_base --env prod --output merged_out.json
python -c "import json; d=json.load(open('merged_out.json')); print('flow2 present:', any(df['name']=='flow2' for df in d['dataflows']))"
# flow2 present: True
Remove-Item merged_out.json
```

### 4.4 Deep merge: nested field override

```sh
python skills/datacoolie-metadata/scripts/merge.py --base test_base --env prod --output merged_out.json
python -c "import json; d=json.load(open('merged_out.json')); flow1=next(df for df in d['dataflows'] if df['name']=='flow1'); print('load_type:', flow1['destination']['load_type'])"
# load_type: merge_upsert  (overridden from overlay)
Remove-Item merged_out.json
```

### 4.5 Missing environment overlay (warning, not error)

```sh
python skills/datacoolie-metadata/scripts/merge.py --base test_base --env staging
# WARNING: No environment overlay found for 'staging' in test_base\environments
# ✓ Merged 2 connections + 2 dataflows → .datacoolie/generated/metadata.staging.json  (base only)
```

### 4.6 Error — base directory not found

```sh
python skills/datacoolie-metadata/scripts/merge.py --base nonexistent --env prod
# ERROR: Base directory not found: nonexistent
# Exit 2
```

### 4.7 Error — missing metadata files

```sh
python -c "import os; os.makedirs('test_empty', exist_ok=True)"
python skills/datacoolie-metadata/scripts/merge.py --base test_empty --env prod
# ERROR: No metadata file found in test_empty (expected metadata.json or connections.json + dataflows.json)
# Exit 2
python -c "import shutil; shutil.rmtree('test_empty')"
```

### 4.8 Unified layout — metadata.json merge

```sh
# Setup: unified base file + overlay
python -c "
import json, os
os.makedirs('test_unified/environments', exist_ok=True)
json.dump({
    'connections': [{'name': 'src_db', 'connection_type': 'mssql', 'configure': {'host': 'localhost'}}],
    'dataflows': [{'name': 'load_orders', 'source': {'connection_name': 'src_db', 'object_name': 'orders'}, 'destination': {'object_name': 'bronze_orders'}}],
    'schema_hints': [{'dataflow_name': 'load_orders', 'column_name': 'id', 'data_type': 'integer'}]
}, open('test_unified/metadata.json', 'w'), indent=2)
import yaml
yaml.dump({
    'connections': [{'name': 'src_db', 'configure': {'host': 'prod-db.example.com'}}],
    'dataflows': [{'name': 'load_orders', 'source': {'filter_expression': 'active = 1'}}]
}, open('test_unified/environments/prod.yaml', 'w'))
"
python skills/datacoolie-metadata/scripts/merge.py --base test_unified --env prod
# ✓ Merged 1 connections + 1 dataflows + 1 schema_hints → .datacoolie/generated/metadata.prod.json
# Verify: host overridden, filter_expression added
python -c "import json; d=json.load(open('.datacoolie/generated/metadata.prod.json')); assert d['connections'][0]['configure']['host'] == 'prod-db.example.com'; assert d['dataflows'][0]['source']['filter_expression'] == 'active = 1'; print('OK: overlay applied')"
```

### 4.9 Unified layout — schema_hints preserved

```sh
# Verify schema_hints pass through from test 4.8
python -c "import json; d=json.load(open('.datacoolie/generated/metadata.prod.json')); assert len(d['schema_hints']) == 1; assert d['schema_hints'][0]['column_name'] == 'id'; print('OK: schema_hints preserved')"
```

### 4.10 Unified layout — $schema preserved

```sh
# Setup: unified file with $schema key
python -c "
import json
json.dump({
    '\$schema': 'https://raw.githubusercontent.com/.../metadata.schema.json',
    'connections': [{'name': 'c1', 'connection_type': 'local_fs'}],
    'dataflows': [{'name': 'd1', 'source': {'connection_name': 'c1'}, 'destination': {'object_name': 'out'}}]
}, open('test_unified/metadata.json', 'w'), indent=2)
"
python skills/datacoolie-metadata/scripts/merge.py --base test_unified --env prod
python -c "import json; d=json.load(open('.datacoolie/generated/metadata.prod.json')); assert '\$schema' in d; print('OK: \$schema preserved')"
```

### 4.11 Partial split layout error — only connections.json

```sh
python -c "
import json, os
os.makedirs('test_partial', exist_ok=True)
json.dump([{'name': 'conn1'}], open('test_partial/connections.json', 'w'))
"
python skills/datacoolie-metadata/scripts/merge.py --base test_partial --env prod
# ERROR: No metadata file found in test_partial (expected metadata.json or connections.json + dataflows.json)
# Exit 2
python -c "import shutil; shutil.rmtree('test_partial')"
```

### Cleanup merge test fixtures

```sh
python -c "import shutil; shutil.rmtree('test_unified', True); shutil.rmtree('.datacoolie', True); shutil.rmtree('test_base', True)"
```

---

## 5. Full pipeline (validate → lint → convert → merge)

```sh
# 1. Validate
python skills/datacoolie-metadata/scripts/validate.py usecase-sim/metadata/file/local_use_cases.json
# ✓ valid

# 2. Lint in dev
python skills/datacoolie-metadata/scripts/lint.py usecase-sim/metadata/file/local_use_cases.json --engine polars --env dev

# 3. Lint in prod (more warnings expected)
python skills/datacoolie-metadata/scripts/lint.py usecase-sim/metadata/file/local_use_cases.json --engine spark --env prod

# 4. Convert to YAML for editing
python skills/datacoolie-metadata/scripts/convert.py usecase-sim/metadata/file/local_use_cases.json --to yaml --output edit_me.yaml
# Edit edit_me.yaml if needed...

# 5. Convert YAML back to JSON
python skills/datacoolie-metadata/scripts/convert.py edit_me.yaml --to json --output reviewed.json

# 6. Validate after round-trip
python skills/datacoolie-metadata/scripts/validate.py reviewed.json
# ✓ valid

# 7. Export to Excel for non-technical review (native format: connections/dataflows/schema_hints sheets)
python skills/datacoolie-metadata/scripts/convert.py reviewed.json --to excel --output review.xlsx
# Send review.xlsx to stakeholders...

# 8. Import stakeholder edits back
python skills/datacoolie-metadata/scripts/convert.py review.xlsx --to json --output final.json
python skills/datacoolie-metadata/scripts/validate.py final.json
# ✓ final.json is valid

# Cleanup
Remove-Item edit_me.yaml, reviewed.json, review.xlsx, final.json
```

---

## 6. CI unit tests

```sh
cd d:\GitHub\datacoolie-arch-5\datacoolie
& d:\GitHub\datacoolie-arch-5\.venv\Scripts\python.exe -m pytest tests/unit/test_schema_enums.py -v
# 5 passed — verifies schema enums stay in sync with constants.py
```

---

## 7. Architecture Consumption (SKILL.md AI Workflow)

These are **prompt-level tests** — verify AI behavior follows the Step 0 logic in SKILL.md. Run each scenario in a clean workspace and check AI output.

### 7.1 Architecture present + approved

- **Setup**: Create `.datacoolie/architect/test_architecture.md` with `Status: Approved`, Source Registry (2 sources), Dataflow Summary Table (3 dataflows)
- **Prompt**: "Generate metadata for this project"
- **Verify**: AI pre-populates connections from Source Registry; pre-populates dataflows from Summary Table; does NOT ask about load strategy, connection types, or source names; DOES ask about `secrets_ref`

### 7.2 Discovery schemas present

- **Setup**: Create `.datacoolie/discover/orders_schema.csv` with columns (id, order_date, amount, customer_id) and `is_pk=true` on id
- **Prompt**: "Generate metadata for this project"
- **Verify**: `transform.schema_hints` populated from CSV columns; `id` suggested as `destination.merge_keys` candidate

### 7.3 No architecture — interactive fallback

- **Setup**: Empty or missing `.datacoolie/` directory
- **Prompt**: "Generate metadata for this project"
- **Verify**: AI asks for all details (source types, connection details, load strategies, engine) — current interactive behavior

### 7.4 Draft architecture — warning

- **Setup**: Create `.datacoolie/architect/test_architecture.md` with `Status: Draft`
- **Prompt**: "Generate metadata"
- **Verify**: AI warns that architecture is not approved; asks for confirmation before proceeding; if confirmed, pre-populates as usual

### 7.5 Secrets never from architecture

- **Setup**: Architecture present with approved status
- **Prompt**: "Generate metadata"
- **Verify**: AI still asks user for `secrets_ref` values — never pre-populated from architecture (security boundary)

---

## Exit Code Reference

| Code | Meaning |
|------|---------|
| 0 | Success / valid / no warnings |
| 1 | Validation errors or lint warnings found |
| 2 | Input error (file not found, parse error, bad arguments) |

## Environment Variables

| Variable | Effect |
|----------|--------|
| `DATACOOLIE_SCHEMAS_DIR` | Override schema directory for all scripts |
| `DATACOOLIE_SCHEMA_GITHUB_BASE` | Override GitHub URL for `--fetch-latest` |

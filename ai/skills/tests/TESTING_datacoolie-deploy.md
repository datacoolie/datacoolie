# datacoolie-deploy — Testing Guide

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

---

## 1. preflight.py

### 1.1 Local platform — always passes (no CLI/auth required)

```sh
python skills/datacoolie-deploy/scripts/preflight.py --platform local
# ✓ Platform CLI installed (skip (local))
# ✓ Authenticated (skip (local))
# ✓ datacoolie installed (0.1.x)
# ✓ Metadata found (N file(s) in metadata/)  OR  ✗ if no metadata/ dir
# ✓ Functions packageable (...)
# Exit 0
```

### 1.2 Missing platform CLI — expect ✗ with install hint

```sh
# On a machine with fab/databricks/aws CLIs installed, these will show ✓ for CLI check.
# On a machine WITHOUT the CLI, expect:
python skills/datacoolie-deploy/scripts/preflight.py --platform fabric
# ✗ Platform CLI installed (Not found. Install: pip install ms-fabric-cli)
# Exit 1

python skills/datacoolie-deploy/scripts/preflight.py --platform databricks
# ✗ Platform CLI installed (Not found. Install: pip install databricks-cli)
# Exit 1
```

### 1.3 Skip auth for CI lint-only mode

```sh
python skills/datacoolie-deploy/scripts/preflight.py --platform aws --skip-auth
# ✓ Authenticated (skipped (--skip-auth))
# Exit depends on other checks (datacoolie installed, metadata, etc.)
```

### 1.4 Metadata directory missing

```sh
# Run from a temp directory with no metadata/
# Note: use absolute path for $py when changing directories
$pyabs = (Resolve-Path $py).Path
Push-Location $env:TEMP
& $pyabs (Resolve-Path "$PSScriptRoot/../../skills/datacoolie-deploy/scripts/preflight.py").Path --platform local
# ✗ Metadata found (Directory not found: ...)
# Exit 1
Pop-Location
```

### 1.5 Metadata directory empty

```sh
python -c "import os; os.makedirs(\"/tmp/empty_meta/metadata\"  , exist_ok=True)"
python skills/datacoolie-deploy/scripts/preflight.py --platform local --project-dir /tmp/empty_meta
# ✗ Metadata found (No metadata files (.json/.yaml) in .../metadata)
# Exit 1
python -c "import shutil; shutil.rmtree(\"/tmp/empty_meta\")"
```

### 1.6 Metadata exists with JSON files — expect ✓

```sh
# From datacoolie/ where usecase-sim/metadata/ has JSON files
python skills/datacoolie-deploy/scripts/preflight.py --platform local --project-dir usecase-sim
# ✓ Metadata found (N file(s) in metadata/)
```

### 1.7 Functions directory absent — skips gracefully

```sh
python -c "import os; os.makedirs(\"/tmp/nofuncs/metadata\"  , exist_ok=True)"
'{}' | Set-Content /tmp/nofuncs/metadata/test.json
python skills/datacoolie-deploy/scripts/preflight.py --platform local --project-dir /tmp/nofuncs
# ✓ Functions packageable (No functions/ directory (optional, skipping))
python -c "import shutil; shutil.rmtree(\"/tmp/nofuncs\")"
```

### 1.8 Functions directory has pyproject.toml — wheel build expected

```sh
python -c "import os; os.makedirs(\"/tmp/hasfuncs/metadata\"  , exist_ok=True)"
python -c "import os; os.makedirs(\"/tmp/hasfuncs/functions\"  , exist_ok=True)"
'{}' | Set-Content /tmp/hasfuncs/metadata/test.json
'[project]' + "`n" + 'name = "functions"' + "`n" + 'version = "0.1.0"' | Set-Content /tmp/hasfuncs/functions/pyproject.toml
python skills/datacoolie-deploy/scripts/preflight.py --platform local --project-dir /tmp/hasfuncs
# ✓ Functions packageable (functions/pyproject.toml found (wheel build))
python -c "import shutil; shutil.rmtree(\"/tmp/hasfuncs\")"
```

### 1.9 Functions directory has __init__.py only — zip package expected

```sh
python -c "import os; os.makedirs(\"/tmp/zipfuncs/metadata\"  , exist_ok=True)"
python -c "import os; os.makedirs(\"/tmp/zipfuncs/functions\"  , exist_ok=True)"
'{}' | Set-Content /tmp/zipfuncs/metadata/test.json
'' | Set-Content /tmp/zipfuncs/functions/__init__.py
python skills/datacoolie-deploy/scripts/preflight.py --platform local --project-dir /tmp/zipfuncs
# ✓ Functions packageable (functions/__init__.py found (zip package))
python -c "import shutil; shutil.rmtree(\"/tmp/zipfuncs\")"
```

### 1.10 Functions directory exists but has neither pyproject.toml nor __init__.py

```sh
python -c "import os; os.makedirs(\"/tmp/badfuncs/metadata\"  , exist_ok=True)"
python -c "import os; os.makedirs(\"/tmp/badfuncs/functions\"  , exist_ok=True)"
'{}' | Set-Content /tmp/badfuncs/metadata/test.json
python skills/datacoolie-deploy/scripts/preflight.py --platform local --project-dir /tmp/badfuncs
# ✗ Functions packageable (functions/ exists but has no pyproject.toml or __init__.py)
python -c "import shutil; shutil.rmtree(\"/tmp/badfuncs\")"
```

### 1.11 Secrets resolution — all refs resolved

```sh
python -c "import os, json; os.makedirs('/tmp/sectest/metadata', exist_ok=True); open('/tmp/sectest/metadata/connections.json','w').write(json.dumps({'connections':[{'name':'db','password':{'secrets_ref':'MY_DB_PASS'}}]}))"
$env:MY_DB_PASS = "supersecret"
python skills/datacoolie-deploy/scripts/preflight.py --platform local --project-dir /tmp/sectest --env dev
# ✓ Secrets resolution (All 1 secret(s) resolved)
$env:MY_DB_PASS = $null
python -c "import shutil; shutil.rmtree('/tmp/sectest')"
```

### 1.12 Secrets resolution — missing ref

```sh
python -c "import os, json; os.makedirs('/tmp/sectest2/metadata', exist_ok=True); open('/tmp/sectest2/metadata/connections.json','w').write(json.dumps({'connections':[{'name':'db','password':{'secrets_ref':'MISSING_SECRET'}}]}))"
$env:MISSING_SECRET = $null
python skills/datacoolie-deploy/scripts/preflight.py --platform local --project-dir /tmp/sectest2 --env dev
# ✗ Secrets resolution (1 unresolved secret(s): MISSING_SECRET)
# Exit 1
python -c "import shutil; shutil.rmtree('/tmp/sectest2')"
```

### 1.13 Infrastructure existence — local always passes

```sh
python skills/datacoolie-deploy/scripts/preflight.py --platform local --env prod
# ✓ Infrastructure exists (skip (local))
```

### 1.14 Infrastructure existence — provision log with failures

```sh
python -c "
import os, pathlib
d = pathlib.Path('/tmp/provtest/.datacoolie/provision')
d.mkdir(parents=True, exist_ok=True)
(d / '250101_provision-log.md').write_text('| bucket | created |\n| schema | failed |')
pathlib.Path('/tmp/provtest/metadata').mkdir(parents=True, exist_ok=True)
open('/tmp/provtest/metadata/f.json','w').write('{}')
"
python skills/datacoolie-deploy/scripts/preflight.py --platform aws --skip-auth --project-dir /tmp/provtest --env prod
# ✗ Infrastructure exists (Provision log shows 1 failed resource(s) — run datacoolie-provision)
# Exit 1
python -c "import shutil; shutil.rmtree('/tmp/provtest')"
```

---

## 2. package.py

### Setup: create a minimal functions package

```sh
python -c "import os; os.makedirs(\"/tmp/pkgtest/functions\"  , exist_ok=True)"
'' | Set-Content /tmp/pkgtest/functions/__init__.py
'def hello(): return "hello"' | Set-Content /tmp/pkgtest/functions/utils.py
```

### 2.1 Auto-detect format (zip when no pyproject.toml)

```sh
python skills/datacoolie-deploy/scripts/package.py --functions-dir /tmp/pkgtest/functions --output /tmp/pkgtest/dist
# ✓ Built zip: functions.zip
# ✓ Import validation passed
# Output: /tmp/pkgtest/dist/functions.zip
python -c "import os; print(os.path.exists(\"/tmp/pkgtest/dist/functions.zip\"  ))"  # True
```

### 2.2 Force zip format explicitly

```sh
python skills/datacoolie-deploy/scripts/package.py --format zip --functions-dir /tmp/pkgtest/functions --output /tmp/pkgtest/dist
# ✓ Built zip: functions.zip
# ✓ Import validation passed
```

### 2.3 Force zip — missing __init__.py — expect error

```sh
python -c "import os; os.makedirs(\"/tmp/noinit/functions\"  , exist_ok=True)"
'def hello(): return 1' | Set-Content /tmp/noinit/functions/utils.py
python skills/datacoolie-deploy/scripts/package.py --format zip --functions-dir /tmp/noinit/functions --output /tmp/noinit/dist
# ERROR: --format zip requires functions/__init__.py
# Exit 1
python -c "import shutil; shutil.rmtree(\"/tmp/noinit\")"
```

### 2.4 Zip excludes __pycache__

```sh
python -c "import os; os.makedirs(\"/tmp/cachedtest/functions/__pycache__\"  , exist_ok=True)"
'' | Set-Content /tmp/cachedtest/functions/__init__.py
'junk' | Set-Content /tmp/cachedtest/functions/__pycache__/utils.cpython-311.pyc
python skills/datacoolie-deploy/scripts/package.py --format zip --functions-dir /tmp/cachedtest/functions --output /tmp/cachedtest/dist
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [System.IO.Compression.ZipFile]::OpenRead("/tmp/cachedtest/dist/functions.zip")
$entries = $zip.Entries | Select-Object -ExpandProperty FullName
$zip.Dispose()
$entries | Where-Object { $_ -match '__pycache__' }
# (empty — no __pycache__ entries)
python -c "import shutil; shutil.rmtree(\"/tmp/cachedtest\")"
```

### 2.5 No functions directory — skip silently

```sh
# Use absolute paths when changing directories
$pyabs = (Resolve-Path $py).Path
$skillabs = (Resolve-Path $skill).Path
Push-Location $env:TEMP
& $pyabs $skillabs/scripts/package.py
# No functions/ directory found — skipping packaging.
# Exit 0
Pop-Location
```

### 2.6 Wheel build (requires pyproject.toml + `build` installed)

```sh
# Requires: pip install build
# Use usecase-sim/functions if it exists, or create a minimal one:
python -c "import os; os.makedirs(\"/tmp/wheeltest/functions\"  , exist_ok=True)"
'' | Set-Content /tmp/wheeltest/functions/__init__.py
@"
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"

[project]
name = "functions"
version = "0.1.0"
"@ | Set-Content /tmp/wheeltest/functions/pyproject.toml

python skills/datacoolie-deploy/scripts/package.py --format wheel --functions-dir /tmp/wheeltest/functions --output /tmp/wheeltest/dist
# ✓ Built wheel: functions-0.1.0-py3-none-any.whl
# ✓ Import validation passed
python -c "import os; print(os.path.exists(\"/tmp/wheeltest/dist/*.whl\"  ))"  # True
python -c "import shutil; shutil.rmtree(\"/tmp/wheeltest\")"
```

### 2.7 Wheel build — missing pyproject.toml — expect error

```sh
python -c "import os; os.makedirs(\"/tmp/nowhl/functions\"  , exist_ok=True)"
'' | Set-Content /tmp/nowhl/functions/__init__.py
python skills/datacoolie-deploy/scripts/package.py --format wheel --functions-dir /tmp/nowhl/functions --output /tmp/nowhl/dist
# ERROR: --format wheel requires functions/pyproject.toml
# Exit 1
python -c "import shutil; shutil.rmtree(\"/tmp/nowhl\")"
```

---

## 3. generate.py

### Setup: minimal project with config

```sh
python -c "import os; os.makedirs(\"/tmp/gentest/.datacoolie\"  , exist_ok=True)"
python -c "import os; os.makedirs(\"/tmp/gentest/metadata\"  , exist_ok=True)"
'{}' | Set-Content /tmp/gentest/metadata/use_cases.json

@"
project_name: my-etl
engine: spark
max_workers: 8
environments:
  dev:
    platform: local
  prod:
    platform: aws
    aws:
      region: ap-southeast-1
      bucket: de-prod-0001
      role_arn: arn:aws:iam::123456789012:role/GlueRole
  staging:
    platform: fabric
    fabric:
      workspace: DEV_Workspace
      lakehouse: ETL_Lakehouse
      root_path: "abfss://DEV_Workspace@onelake.dfs.fabric.microsoft.com/ETL_Lakehouse.Lakehouse"
  analytics:
    platform: databricks
    databricks:
      host: "https://adb-123.azuredatabricks.net"
      catalog: workspace
      schema: default
      volume: datacoolie
"@ | Set-Content /tmp/gentest/.datacoolie/config.yaml
```

### 3.1 Generate local runner

```sh
python skills/datacoolie-deploy/scripts/generate.py --platform local --env dev --project-dir /tmp/gentest
# ✓ Generated: /tmp/gentest/.datacoolie/generated/run_local.py
# ✓ Syntax check passed
Get-Content /tmp/gentest/.datacoolie/generated/run_local.py | Select-Object -First 10
# Should contain: LocalPlatform, PolarsEngine or SparkEngine, DataCoolieDriver
```

### 3.2 Generate AWS Glue runner

```sh
python skills/datacoolie-deploy/scripts/generate.py --platform aws --env prod --project-dir /tmp/gentest
# ✓ Generated: /tmp/gentest/.datacoolie/generated/run_aws_glue.py
# ✓ Syntax check passed
python -c "import re; [print(l, end='') for l in open(\"/tmp/gentest/.datacoolie/generated/run_aws_glue.py\"  ) if re.search(r\"bucket|region\"  , l)]"
# Should show: "ap-southeast-1", "de-prod-0001"
```

### 3.3 Generate Fabric notebook

```sh
python skills/datacoolie-deploy/scripts/generate.py --platform fabric --env staging --project-dir /tmp/gentest
# ✓ Generated: /tmp/gentest/.datacoolie/generated/run_fabric.ipynb
# ✓ Notebook JSON valid
$nb = Get-Content /tmp/gentest/.datacoolie/generated/run_fabric.ipynb | ConvertFrom-Json
$nb.cells.Count   # 4 cells
$nb.cells[0].cell_type  # markdown
```

### 3.4 Fabric notebook contains correct ROOT_PATH

```sh
$nb = Get-Content /tmp/gentest/.datacoolie/generated/run_fabric.ipynb -Raw | ConvertFrom-Json
$cell4 = ($nb.cells | Where-Object { $_.cell_type -eq 'code' })[2].source -join ''
$cell4 | Select-String "DEV_Workspace"
# Should match: ROOT_PATH = "abfss://DEV_Workspace@..."
```

### 3.5 Generate Databricks notebook

```sh
python skills/datacoolie-deploy/scripts/generate.py --platform databricks --env analytics --project-dir /tmp/gentest
# ✓ Generated: /tmp/gentest/.datacoolie/generated/run_databricks.ipynb
# ✓ Notebook JSON valid
$nb = Get-Content /tmp/gentest/.datacoolie/generated/run_databricks.ipynb | ConvertFrom-Json
$nb.cells.Count   # 4 cells
```

### 3.6 Databricks notebook contains correct VOLUME_ROOT

```sh
$nb = Get-Content /tmp/gentest/.datacoolie/generated/run_databricks.ipynb -Raw | ConvertFrom-Json
$code_cells = $nb.cells | Where-Object { $_.cell_type -eq 'code' }
$code_cells[2].source -join '' | Select-String "workspace/default/datacoolie"
# Should match: VOLUME_ROOT = "/Volumes/workspace/default/datacoolie"
```

### 3.7 Custom output path

```sh
python skills/datacoolie-deploy/scripts/generate.py --platform aws --env prod --project-dir /tmp/gentest --output /tmp/gentest/my_runner.py
# ✓ Generated: /tmp/gentest/my_runner.py
# ✓ Syntax check passed
python -c "import os; print(os.path.exists(\"/tmp/gentest/my_runner.py\"  ))"  # True
```

### 3.8 No config file — falls back to defaults

```sh
python -c "import os; os.makedirs(\"/tmp/noconfig/metadata\"  , exist_ok=True)"
'{}' | Set-Content /tmp/noconfig/metadata/use_cases.json
python skills/datacoolie-deploy/scripts/generate.py --platform aws --env prod --project-dir /tmp/noconfig
# ✓ Generated: .../run_aws_glue.py (uses default region, bucket, etc.)
python -c "import re; [print(l, end='') for l in open(\"/tmp/noconfig/.datacoolie/generated/run_aws_glue.py\"  ) if re.search(r\"ap-southeast-1|de-dev-0007\"  , l)]"
python -c "import shutil; shutil.rmtree(\"/tmp/noconfig\")"
```

### Cleanup

```sh
python -c "import shutil; shutil.rmtree(\"/tmp/gentest\")"
```

---

## 4. apply.py — dry-run mode (safe, no real cloud calls)

> All apply.py tests use `--dry-run` to avoid real deployments.

### Setup

```sh
python -c "import os; os.makedirs(\"/tmp/applytest/.datacoolie/generated/dist\"  , exist_ok=True)"
python -c "import os; os.makedirs(\"/tmp/applytest/metadata\"  , exist_ok=True)"
'{}' | Set-Content /tmp/applytest/metadata/use_cases.json

@"
project_name: my-etl
engine: spark
environments:
  prod:
    platform: aws
    aws:
      region: ap-southeast-1
      bucket: de-prod-0001
      role_arn: arn:aws:iam::123456789012:role/GlueRole
  staging:
    platform: fabric
    fabric:
      workspace: DEV_Workspace
      lakehouse: ETL_Lakehouse
  dev:
    platform: databricks
    databricks:
      host: "https://adb-123.azuredatabricks.net"
      catalog: workspace
      schema: default
      volume: datacoolie
"@ | Set-Content /tmp/applytest/.datacoolie/config.yaml

# Simulate pre-built wheel
'fake' | Set-Content /tmp/applytest/.datacoolie/generated/dist/functions-0.1.0-py3-none-any.whl
# Simulate pre-generated runner
'pass' | Set-Content /tmp/applytest/.datacoolie/generated/run_aws_glue.py
```

### 4.1 AWS dry-run — prints S3 upload commands then Glue create

```sh
python skills/datacoolie-deploy/scripts/apply.py --platform aws --env prod --dry-run --project-dir /tmp/applytest
# [DRY-RUN] ... aws s3 cp .../run_aws_glue.py s3://de-prod-0001/scripts/run_aws_glue.py
# [DRY-RUN] ... aws s3 cp .../functions-0.1.0-py3-none-any.whl s3://de-prod-0001/libs/...
# [DRY-RUN] ... aws s3 sync .../metadata/ s3://de-prod-0001/metadata/
# [DRY-RUN] ... aws glue create-job OR update-job ...
# Deploy complete.
# Exit 0
```

### 4.2 AWS dry-run with --skip-upload — no S3 commands

```sh
python skills/datacoolie-deploy/scripts/apply.py --platform aws --env prod --dry-run --skip-upload --project-dir /tmp/applytest
# Should NOT print any [DRY-RUN] aws s3 cp lines
# Should print [DRY-RUN] aws glue create-job OR update-job
python skills/datacoolie-deploy/scripts/apply.py --platform aws --env prod --dry-run --skip-upload --project-dir /tmp/applytest | Select-String "s3 cp"
# (empty — no s3 cp in output)
```

### 4.3 AWS dry-run with --run — prints start-job-run at end

```sh
python skills/datacoolie-deploy/scripts/apply.py --platform aws --env prod --dry-run --run --project-dir /tmp/applytest
# Last [DRY-RUN] line should be: aws glue start-job-run --job-name my-etl-prod ...
python skills/datacoolie-deploy/scripts/apply.py --platform aws --env prod --dry-run --run --project-dir /tmp/applytest | Select-String "start-job-run"
# [DRY-RUN] aws glue start-job-run --job-name my-etl-prod ...
```

### 4.4 Databricks dry-run — prints volume uploads then bundle validate + deploy

```sh
# Simulate pre-generated notebook
'{}' | Set-Content /tmp/applytest/.datacoolie/generated/run_databricks.ipynb
python skills/datacoolie-deploy/scripts/apply.py --platform databricks --env dev --dry-run --project-dir /tmp/applytest
# [DRY-RUN] databricks fs cp --overwrite .../functions-0.1.0-py3-none-any.whl dbfs:/Volumes/workspace/default/datacoolie/libraries/functions-0.1.0-py3-none-any.whl
# [DRY-RUN] databricks fs cp --overwrite .../use_cases.json dbfs:/Volumes/workspace/default/datacoolie/metadata/use_cases.json
# [DRY-RUN] databricks bundle validate
# [DRY-RUN] databricks bundle deploy -t dev --auto-approve
# Deploy complete.
```

### 4.4b Databricks dry-run — verify wheel and metadata uploads appear

```sh
python skills/datacoolie-deploy/scripts/apply.py --platform databricks --env dev --dry-run --project-dir /tmp/applytest | Select-String "databricks fs cp"
# [DRY-RUN] databricks fs cp --overwrite .../libraries/functions-0.1.0-py3-none-any.whl
# [DRY-RUN] databricks fs cp --overwrite .../metadata/use_cases.json
# (two lines — one for libraries/, one for metadata/)
```

### 4.5 Databricks dry-run with --run

```sh
python skills/datacoolie-deploy/scripts/apply.py --platform databricks --env dev --dry-run --run --project-dir /tmp/applytest
# Last [DRY-RUN] line: databricks bundle run -t dev my-etl_dev
python skills/datacoolie-deploy/scripts/apply.py --platform databricks --env dev --dry-run --run --project-dir /tmp/applytest | Select-String "bundle run"
# [DRY-RUN] databricks bundle run -t dev my-etl_dev
```

### 4.6 Fabric dry-run — prints fab cp for wheel + .json + .yaml metadata + fab import

```sh
'{}' | Set-Content /tmp/applytest/.datacoolie/generated/run_fabric.ipynb
python skills/datacoolie-deploy/scripts/apply.py --platform fabric --env staging --dry-run --project-dir /tmp/applytest
# [DRY-RUN] fab cp .../functions-0.1.0-py3-none-any.whl DEV_Workspace.Workspace/ETL_Lakehouse.Lakehouse/Files/libraries/...
# [DRY-RUN] fab cp .../use_cases.json DEV_Workspace.Workspace/.../Files/metadata/use_cases.json
# [DRY-RUN] fab import .../run_fabric.ipynb DEV_Workspace.Workspace
# Deploy complete.
```

### 4.6b Fabric dry-run — verify .yaml metadata files are also uploaded

```sh
# Add a yaml metadata file
'connections: []' | Set-Content /tmp/applytest/metadata/connections.yaml
python skills/datacoolie-deploy/scripts/apply.py --platform fabric --env staging --dry-run --project-dir /tmp/applytest | Select-String "fab cp.*metadata"
# [DRY-RUN] fab cp .../use_cases.json .../metadata/use_cases.json
# [DRY-RUN] fab cp .../connections.yaml .../metadata/connections.yaml
# (both .json and .yaml files appear)
Remove-Item /tmp/applytest/metadata/connections.yaml
```

### 4.7 Fabric dry-run with --run

```sh
python skills/datacoolie-deploy/scripts/apply.py --platform fabric --env staging --dry-run --run --project-dir /tmp/applytest | Select-String "fab start"
# [DRY-RUN] fab start DEV_Workspace.Workspace/my-etl_staging.Notebook
```

### 4.8 Local platform — no deployment step

```sh
python skills/datacoolie-deploy/scripts/apply.py --platform local --env dev --dry-run --project-dir /tmp/applytest
# Local platform — no deployment needed. Run the generated script directly.
# Deploy complete.
# Exit 0
```

### 4.9 Exit code is 0 on dry-run success

```sh
python skills/datacoolie-deploy/scripts/apply.py --platform aws --env prod --dry-run --project-dir /tmp/applytest
echo $?  # print exit code  # Exit: 0
```

### 4.10 apply.py forwards --env to preflight (not hardcoded "dev")

Verify the preflight subprocess is called with the correct environment so checks 6 (secrets) and 7 (infra) validate the right env:

```sh
python skills/datacoolie-deploy/scripts/apply.py --platform aws --env prod --dry-run --project-dir /tmp/applytest 2>&1 | Select-String "preflight"
# [DRY-RUN] python .../preflight.py --platform aws --project-dir .../tmp/applytest --env prod
# Must include "--env prod" — not "--env dev" (the default)
```

### Cleanup

```sh
python -c "import shutil; shutil.rmtree(\"/tmp/applytest\")"
```

---

## 5. cicd.py

### Setup

```sh
python -c "import os; os.makedirs(\"/tmp/cicdtest/.datacoolie\"  , exist_ok=True)"
@"
project_name: my-etl
environments:
  prod:
    platform: aws
    aws:
      region: ap-southeast-1
      bucket: de-prod-0001
  staging:
    platform: fabric
    fabric:
      workspace: DEV_Workspace
  dev:
    platform: databricks
    databricks:
      host: "https://adb-123.azuredatabricks.net"
"@ | Set-Content /tmp/cicdtest/.datacoolie/config.yaml
```

### 5.1 Generate AWS workflow

```sh
python skills/datacoolie-deploy/scripts/cicd.py --platform aws --env prod --project-dir /tmp/cicdtest
# ✓ Workflow: /tmp/cicdtest/.github/workflows/deploy-aws.yml
# Required GitHub Secrets: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
python -c "import os; print(os.path.exists(\"/tmp/cicdtest/.github/workflows/deploy-aws.yml\"  ))"  # True
```

### 5.2 AWS workflow contains S3 upload steps before Glue deploy

```sh
$wf = Get-Content /tmp/cicdtest/.github/workflows/deploy-aws.yml -Raw
$wf | Select-String "Upload script to S3"   # present
$wf | Select-String "Upload functions wheel"  # present
$wf | Select-String "Sync metadata"           # present
$wf | Select-String "skip-upload"             # --skip-upload flag is used
```

### 5.3 AWS workflow bucket is templated from config

```sh
python -c "import re; [print(l, end='') for l in open(\"/tmp/cicdtest/.github/workflows/deploy-aws.yml\"  ) if re.search(r\"de-prod-0001\"  , l)]"
# Should show the bucket name in aws s3 cp and s3 sync lines
```

### 5.4 Generate Databricks workflow

```sh
python skills/datacoolie-deploy/scripts/cicd.py --platform databricks --env dev --project-dir /tmp/cicdtest
# ✓ Workflow: /tmp/cicdtest/.github/workflows/deploy-databricks.yml
# Required GitHub Secrets: DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
python -c "import os; print(os.path.exists(\"/tmp/cicdtest/.github/workflows/deploy-databricks.yml\"  ))"  # True
```

### 5.5 Generate Fabric workflow

```sh
python skills/datacoolie-deploy/scripts/cicd.py --platform fabric --env staging --project-dir /tmp/cicdtest
# ✓ Workflow: /tmp/cicdtest/.github/workflows/deploy-fabric.yml
# Required GitHub Secrets: FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET, FABRIC_TENANT_ID
python -c "import os; print(os.path.exists(\"/tmp/cicdtest/.github/workflows/deploy-fabric.yml\"  ))"  # True
```

### 5.6 Fabric workflow contains fab auth login step

```sh
python -c "import re; [print(l, end='') for l in open(\"/tmp/cicdtest/.github/workflows/deploy-fabric.yml\"  ) if re.search(r\"fab auth login\"  , l)]"
# - run: fab auth login -u $FABRIC_CLIENT_ID -p $FABRIC_CLIENT_SECRET --tenant $FABRIC_TENANT_ID
```

### 5.7 Custom output path

```sh
python skills/datacoolie-deploy/scripts/cicd.py --platform aws --env prod --project-dir /tmp/cicdtest --output /tmp/cicdtest/my-deploy.yml
# ✓ Workflow: /tmp/cicdtest/my-deploy.yml
python -c "import os; print(os.path.exists(\"/tmp/cicdtest/my-deploy.yml\"  ))"  # True
```

### 5.8 Generated YAML is parseable

```sh
# Requires: pip install pyyaml
python -c "
import yaml
for platform in ['aws', 'databricks', 'fabric']:
    content = open(f'/tmp/cicdtest/.github/workflows/deploy-{platform}.yml').read()
    yaml.safe_load(content)
    print(f'deploy-{platform}.yml: valid YAML')
"
# deploy-aws.yml: valid YAML
# deploy-databricks.yml: valid YAML
# deploy-fabric.yml: valid YAML
```

### 5.9 All three workflows have 3-job structure (validate, package, deploy)

```sh
python -c "
import yaml
for platform in ['aws', 'databricks', 'fabric']:
    content = open(f'/tmp/cicdtest/.github/workflows/deploy-{platform}.yml').read()
    wf = yaml.safe_load(content)
    jobs = list(wf['jobs'].keys())
    print(f'{platform}: {jobs}')
"
# aws: ['validate', 'package', 'deploy']
# databricks: ['validate', 'package', 'deploy']
# fabric: ['validate', 'package', 'deploy']
```

### Cleanup

```sh
python -c "import shutil; shutil.rmtree(\"/tmp/cicdtest\")"
```

---

## 6. End-to-end dry-run (all platforms)

Full flow: generate → package (zip) → apply dry-run, for each platform.

### Setup

```sh
python -c "import os; os.makedirs(\"/tmp/e2etest/.datacoolie\"  , exist_ok=True)"
python -c "import os; os.makedirs(\"/tmp/e2etest/metadata\"  , exist_ok=True)"
python -c "import os; os.makedirs(\"/tmp/e2etest/functions\"  , exist_ok=True)"
'{}' | Set-Content /tmp/e2etest/metadata/use_cases.json
'' | Set-Content /tmp/e2etest/functions/__init__.py
'def transform(df): return df' | Set-Content /tmp/e2etest/functions/transforms.py

@"
project_name: e2e-test
engine: spark
environments:
  local_env:
    platform: local
  aws_env:
    platform: aws
    aws:
      region: ap-southeast-1
      bucket: e2e-bucket
      role_arn: arn:aws:iam::000000000000:role/TestRole
  fabric_env:
    platform: fabric
    fabric:
      workspace: E2EWorkspace
      lakehouse: E2ELakehouse
      root_path: "abfss://E2EWorkspace@onelake.dfs.fabric.microsoft.com/E2ELakehouse.Lakehouse"
  dbx_env:
    platform: databricks
    databricks:
      catalog: main
      schema: default
      volume: e2e
"@ | Set-Content /tmp/e2etest/.datacoolie/config.yaml
```

### 6.1 Local end-to-end

```sh
python skills/datacoolie-deploy/scripts/generate.py --platform local --env local_env --project-dir /tmp/e2etest
python skills/datacoolie-deploy/scripts/apply.py --platform local --env local_env --dry-run --project-dir /tmp/e2etest
# Local platform — no deployment needed.
# Exit 0
```

### 6.2 AWS end-to-end dry-run

```sh
python skills/datacoolie-deploy/scripts/generate.py --platform aws --env aws_env --project-dir /tmp/e2etest
python skills/datacoolie-deploy/scripts/package.py --format zip --functions-dir /tmp/e2etest/functions --output /tmp/e2etest/.datacoolie/generated/dist
python skills/datacoolie-deploy/scripts/apply.py --platform aws --env aws_env --dry-run --skip-upload --project-dir /tmp/e2etest
# [DRY-RUN] aws glue create-job ...  with s3://e2e-bucket/scripts/run_aws_glue.py
# Exit 0
```

### 6.3 Fabric end-to-end dry-run

```sh
python skills/datacoolie-deploy/scripts/generate.py --platform fabric --env fabric_env --project-dir /tmp/e2etest
python skills/datacoolie-deploy/scripts/apply.py --platform fabric --env fabric_env --dry-run --project-dir /tmp/e2etest
# [DRY-RUN] fab cp .../functions.zip E2EWorkspace.Workspace/E2ELakehouse.Lakehouse/Files/libraries/functions.zip
# [DRY-RUN] fab cp .../use_cases.json E2EWorkspace.Workspace/.../Files/metadata/use_cases.json
# [DRY-RUN] fab import .../run_fabric.ipynb E2EWorkspace.Workspace
# Exit 0
```

### 6.4 Databricks end-to-end dry-run

```sh
python skills/datacoolie-deploy/scripts/generate.py --platform databricks --env dbx_env --project-dir /tmp/e2etest
python skills/datacoolie-deploy/scripts/apply.py --platform databricks --env dbx_env --dry-run --project-dir /tmp/e2etest
# [DRY-RUN] databricks fs cp --overwrite .../functions.zip dbfs:/Volumes/main/default/e2e/libraries/functions.zip
# [DRY-RUN] databricks fs cp --overwrite .../use_cases.json dbfs:/Volumes/main/default/e2e/metadata/use_cases.json
# [DRY-RUN] databricks bundle validate
# [DRY-RUN] databricks bundle deploy -t dbx_env --auto-approve
# Exit 0
```

### Cleanup

```sh
python -c "import shutil; shutil.rmtree(\"/tmp/e2etest\")"
```

---

## 7. promote.py

All commands run from `datacoolie/` with venv activated.

### Setup

```sh
python -c "
import os, json, pathlib
p = pathlib.Path('/tmp/promtest')
(p / 'metadata').mkdir(parents=True, exist_ok=True)
(p / 'metadata' / 'dataflows.json').write_text(json.dumps({'dataflows': [{'name': 'sales_load', 'load_type': 'full_load'}]}))
"
```

### 7.1 Prod promotion without --confirm — exit 2

```sh
python skills/datacoolie-deploy/scripts/promote.py --from dev --to prod --platform local --project-dir /tmp/promtest --skip-auth
# ERROR: Promotion to 'prod' requires --confirm.
# Exit 2
```

### 7.2 Same-env promotion rejected — exit 2

```sh
python skills/datacoolie-deploy/scripts/promote.py --from dev --to dev --platform local
# ERROR: --from and --to must be different environments.
# Exit 2
```

### 7.3 Successful dev→test promotion (local, --skip-auth)

```sh
python skills/datacoolie-deploy/scripts/promote.py --from dev --to test --platform local --project-dir /tmp/promtest --skip-auth --confirm
# [1/4 Preflight] Running: preflight.py ...
#   ✓ Platform CLI installed (skip (local))
#   ...
# [4/4 Deploy] Running: apply.py ...
# Promotion log: /tmp/promtest/.datacoolie/promote/YYMMDD_promote-dev-to-test.md
# ✓ Promotion dev → test succeeded.
# Exit 0
python -c "import os; print(any(True for _ in __import__('pathlib').Path('/tmp/promtest/.datacoolie/promote').glob('*.md')))"
# True
```

### 7.4 Promotion log content

```sh
python -c "
import pathlib
logs = sorted(pathlib.Path('/tmp/promtest/.datacoolie/promote').glob('*.md'))
print(logs[-1].read_text())
"
# # Promotion Log
# ...
# | Preflight | ✓ pass | ...
# | Deploy    | ✓ pass | ...
```

### 7.5 Prod promotion with --confirm

```sh
python skills/datacoolie-deploy/scripts/promote.py --from dev --to prod --platform local --project-dir /tmp/promtest --skip-auth --confirm
# ✓ Promotion dev → prod succeeded.
# Exit 0
```

### Cleanup

```sh
python -c "import shutil; shutil.rmtree('/tmp/promtest')"
```

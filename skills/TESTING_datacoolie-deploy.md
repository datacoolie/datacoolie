# datacoolie-deploy — Testing Guide

All commands run from:
```
datacoolie/
```

Python interpreter (use your venv path):
```powershell
# Windows
$py = "..\.venv\Scripts\python.exe"

# macOS / Linux
# py="../.venv/bin/python3"
```

Skill root:
```powershell
$skill = "skills/datacoolie-deploy"
```

---

## 1. preflight.py

### 1.1 Local platform — always passes (no CLI/auth required)

```powershell
& $py $skill/scripts/preflight.py --platform local
# ✓ Platform CLI installed (skip (local))
# ✓ Authenticated (skip (local))
# ✓ datacoolie installed (0.1.x)
# ✓ Metadata found (N file(s) in metadata/)  OR  ✗ if no metadata/ dir
# ✓ Functions packageable (...)
# Exit 0
```

### 1.2 Missing platform CLI — expect ✗ with install hint

```powershell
# On a machine with fab/databricks/aws CLIs installed, these will show ✓ for CLI check.
# On a machine WITHOUT the CLI, expect:
& $py $skill/scripts/preflight.py --platform fabric
# ✗ Platform CLI installed (Not found. Install: pip install ms-fabric-cli)
# Exit 1

& $py $skill/scripts/preflight.py --platform databricks
# ✗ Platform CLI installed (Not found. Install: pip install databricks-cli)
# Exit 1
```

### 1.3 Skip auth for CI lint-only mode

```powershell
& $py $skill/scripts/preflight.py --platform aws --skip-auth
# ✓ Authenticated (skipped (--skip-auth))
# Exit depends on other checks (datacoolie installed, metadata, etc.)
```

### 1.4 Metadata directory missing

```powershell
# Run from a temp directory with no metadata/
# Note: use absolute path for $py when changing directories
$pyabs = (Resolve-Path $py).Path
Push-Location $env:TEMP
& $pyabs (Resolve-Path "$PSScriptRoot/../../$skill/scripts/preflight.py").Path --platform local
# ✗ Metadata found (Directory not found: ...)
# Exit 1
Pop-Location
```

### 1.5 Metadata directory empty

```powershell
New-Item -ItemType Directory -Force /tmp/empty_meta/metadata | Out-Null
& $py $skill/scripts/preflight.py --platform local --project-dir /tmp/empty_meta
# ✗ Metadata found (No metadata files (.json/.yaml) in .../metadata)
# Exit 1
Remove-Item -Recurse /tmp/empty_meta
```

### 1.6 Metadata exists with JSON files — expect ✓

```powershell
# From datacoolie/ where usecase-sim/metadata/ has JSON files
& $py $skill/scripts/preflight.py --platform local --project-dir usecase-sim
# ✓ Metadata found (N file(s) in metadata/)
```

### 1.7 Functions directory absent — skips gracefully

```powershell
New-Item -ItemType Directory -Force /tmp/nofuncs/metadata | Out-Null
'{}' | Set-Content /tmp/nofuncs/metadata/test.json
& $py $skill/scripts/preflight.py --platform local --project-dir /tmp/nofuncs
# ✓ Functions packageable (No functions/ directory (optional, skipping))
Remove-Item -Recurse /tmp/nofuncs
```

### 1.8 Functions directory has pyproject.toml — wheel build expected

```powershell
New-Item -ItemType Directory -Force /tmp/hasfuncs/metadata | Out-Null
New-Item -ItemType Directory -Force /tmp/hasfuncs/functions | Out-Null
'{}' | Set-Content /tmp/hasfuncs/metadata/test.json
'[project]' + "`n" + 'name = "functions"' + "`n" + 'version = "0.1.0"' | Set-Content /tmp/hasfuncs/functions/pyproject.toml
& $py $skill/scripts/preflight.py --platform local --project-dir /tmp/hasfuncs
# ✓ Functions packageable (functions/pyproject.toml found (wheel build))
Remove-Item -Recurse /tmp/hasfuncs
```

### 1.9 Functions directory has __init__.py only — zip package expected

```powershell
New-Item -ItemType Directory -Force /tmp/zipfuncs/metadata | Out-Null
New-Item -ItemType Directory -Force /tmp/zipfuncs/functions | Out-Null
'{}' | Set-Content /tmp/zipfuncs/metadata/test.json
'' | Set-Content /tmp/zipfuncs/functions/__init__.py
& $py $skill/scripts/preflight.py --platform local --project-dir /tmp/zipfuncs
# ✓ Functions packageable (functions/__init__.py found (zip package))
Remove-Item -Recurse /tmp/zipfuncs
```

### 1.10 Functions directory exists but has neither pyproject.toml nor __init__.py

```powershell
New-Item -ItemType Directory -Force /tmp/badfuncs/metadata | Out-Null
New-Item -ItemType Directory -Force /tmp/badfuncs/functions | Out-Null
'{}' | Set-Content /tmp/badfuncs/metadata/test.json
& $py $skill/scripts/preflight.py --platform local --project-dir /tmp/badfuncs
# ✗ Functions packageable (functions/ exists but has no pyproject.toml or __init__.py)
Remove-Item -Recurse /tmp/badfuncs
```

---

## 2. package.py

### Setup: create a minimal functions package

```powershell
New-Item -ItemType Directory -Force /tmp/pkgtest/functions | Out-Null
'' | Set-Content /tmp/pkgtest/functions/__init__.py
'def hello(): return "hello"' | Set-Content /tmp/pkgtest/functions/utils.py
```

### 2.1 Auto-detect format (zip when no pyproject.toml)

```powershell
& $py $skill/scripts/package.py --functions-dir /tmp/pkgtest/functions --output /tmp/pkgtest/dist
# ✓ Built zip: functions.zip
# ✓ Import validation passed
# Output: /tmp/pkgtest/dist/functions.zip
Test-Path /tmp/pkgtest/dist/functions.zip  # True
```

### 2.2 Force zip format explicitly

```powershell
& $py $skill/scripts/package.py --format zip --functions-dir /tmp/pkgtest/functions --output /tmp/pkgtest/dist
# ✓ Built zip: functions.zip
# ✓ Import validation passed
```

### 2.3 Force zip — missing __init__.py — expect error

```powershell
New-Item -ItemType Directory -Force /tmp/noinit/functions | Out-Null
'def hello(): return 1' | Set-Content /tmp/noinit/functions/utils.py
& $py $skill/scripts/package.py --format zip --functions-dir /tmp/noinit/functions --output /tmp/noinit/dist
# ERROR: --format zip requires functions/__init__.py
# Exit 1
Remove-Item -Recurse /tmp/noinit
```

### 2.4 Zip excludes __pycache__

```powershell
New-Item -ItemType Directory -Force /tmp/cachedtest/functions/__pycache__ | Out-Null
'' | Set-Content /tmp/cachedtest/functions/__init__.py
'junk' | Set-Content /tmp/cachedtest/functions/__pycache__/utils.cpython-311.pyc
& $py $skill/scripts/package.py --format zip --functions-dir /tmp/cachedtest/functions --output /tmp/cachedtest/dist
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [System.IO.Compression.ZipFile]::OpenRead("/tmp/cachedtest/dist/functions.zip")
$entries = $zip.Entries | Select-Object -ExpandProperty FullName
$zip.Dispose()
$entries | Where-Object { $_ -match '__pycache__' }
# (empty — no __pycache__ entries)
Remove-Item -Recurse /tmp/cachedtest
```

### 2.5 No functions directory — skip silently

```powershell
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

```powershell
# Requires: pip install build
# Use usecase-sim/functions if it exists, or create a minimal one:
New-Item -ItemType Directory -Force /tmp/wheeltest/functions | Out-Null
'' | Set-Content /tmp/wheeltest/functions/__init__.py
@"
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"

[project]
name = "functions"
version = "0.1.0"
"@ | Set-Content /tmp/wheeltest/functions/pyproject.toml

& $py $skill/scripts/package.py --format wheel --functions-dir /tmp/wheeltest/functions --output /tmp/wheeltest/dist
# ✓ Built wheel: functions-0.1.0-py3-none-any.whl
# ✓ Import validation passed
Test-Path /tmp/wheeltest/dist/*.whl  # True
Remove-Item -Recurse /tmp/wheeltest
```

### 2.7 Wheel build — missing pyproject.toml — expect error

```powershell
New-Item -ItemType Directory -Force /tmp/nowhl/functions | Out-Null
'' | Set-Content /tmp/nowhl/functions/__init__.py
& $py $skill/scripts/package.py --format wheel --functions-dir /tmp/nowhl/functions --output /tmp/nowhl/dist
# ERROR: --format wheel requires functions/pyproject.toml
# Exit 1
Remove-Item -Recurse /tmp/nowhl
```

---

## 3. generate.py

### Setup: minimal project with config

```powershell
New-Item -ItemType Directory -Force /tmp/gentest/.datacoolie | Out-Null
New-Item -ItemType Directory -Force /tmp/gentest/metadata | Out-Null
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

```powershell
& $py $skill/scripts/generate.py --platform local --env dev --project-dir /tmp/gentest
# ✓ Generated: /tmp/gentest/.datacoolie/generated/run_local.py
# ✓ Syntax check passed
Get-Content /tmp/gentest/.datacoolie/generated/run_local.py | Select-Object -First 10
# Should contain: LocalPlatform, PolarsEngine or SparkEngine, DataCoolieDriver
```

### 3.2 Generate AWS Glue runner

```powershell
& $py $skill/scripts/generate.py --platform aws --env prod --project-dir /tmp/gentest
# ✓ Generated: /tmp/gentest/.datacoolie/generated/run_aws_glue.py
# ✓ Syntax check passed
Get-Content /tmp/gentest/.datacoolie/generated/run_aws_glue.py | Select-String "bucket|region"
# Should show: "ap-southeast-1", "de-prod-0001"
```

### 3.3 Generate Fabric notebook

```powershell
& $py $skill/scripts/generate.py --platform fabric --env staging --project-dir /tmp/gentest
# ✓ Generated: /tmp/gentest/.datacoolie/generated/run_fabric.ipynb
# ✓ Notebook JSON valid
$nb = Get-Content /tmp/gentest/.datacoolie/generated/run_fabric.ipynb | ConvertFrom-Json
$nb.cells.Count   # 4 cells
$nb.cells[0].cell_type  # markdown
```

### 3.4 Fabric notebook contains correct ROOT_PATH

```powershell
$nb = Get-Content /tmp/gentest/.datacoolie/generated/run_fabric.ipynb -Raw | ConvertFrom-Json
$cell4 = ($nb.cells | Where-Object { $_.cell_type -eq 'code' })[2].source -join ''
$cell4 | Select-String "DEV_Workspace"
# Should match: ROOT_PATH = "abfss://DEV_Workspace@..."
```

### 3.5 Generate Databricks notebook

```powershell
& $py $skill/scripts/generate.py --platform databricks --env analytics --project-dir /tmp/gentest
# ✓ Generated: /tmp/gentest/.datacoolie/generated/run_databricks.ipynb
# ✓ Notebook JSON valid
$nb = Get-Content /tmp/gentest/.datacoolie/generated/run_databricks.ipynb | ConvertFrom-Json
$nb.cells.Count   # 4 cells
```

### 3.6 Databricks notebook contains correct VOLUME_ROOT

```powershell
$nb = Get-Content /tmp/gentest/.datacoolie/generated/run_databricks.ipynb -Raw | ConvertFrom-Json
$code_cells = $nb.cells | Where-Object { $_.cell_type -eq 'code' }
$code_cells[2].source -join '' | Select-String "workspace/default/datacoolie"
# Should match: VOLUME_ROOT = "/Volumes/workspace/default/datacoolie"
```

### 3.7 Custom output path

```powershell
& $py $skill/scripts/generate.py --platform aws --env prod --project-dir /tmp/gentest --output /tmp/gentest/my_runner.py
# ✓ Generated: /tmp/gentest/my_runner.py
# ✓ Syntax check passed
Test-Path /tmp/gentest/my_runner.py  # True
```

### 3.8 No config file — falls back to defaults

```powershell
New-Item -ItemType Directory -Force /tmp/noconfig/metadata | Out-Null
'{}' | Set-Content /tmp/noconfig/metadata/use_cases.json
& $py $skill/scripts/generate.py --platform aws --env prod --project-dir /tmp/noconfig
# ✓ Generated: .../run_aws_glue.py (uses default region, bucket, etc.)
Get-Content /tmp/noconfig/.datacoolie/generated/run_aws_glue.py | Select-String "ap-southeast-1|de-dev-0001"
Remove-Item -Recurse /tmp/noconfig
```

### Cleanup

```powershell
Remove-Item -Recurse /tmp/gentest
```

---

## 4. apply.py — dry-run mode (safe, no real cloud calls)

> All apply.py tests use `--dry-run` to avoid real deployments.

### Setup

```powershell
New-Item -ItemType Directory -Force /tmp/applytest/.datacoolie/generated/dist | Out-Null
New-Item -ItemType Directory -Force /tmp/applytest/metadata | Out-Null
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

```powershell
& $py $skill/scripts/apply.py --platform aws --env prod --dry-run --project-dir /tmp/applytest
# [DRY-RUN] ... aws s3 cp .../run_aws_glue.py s3://de-prod-0001/scripts/run_aws_glue.py
# [DRY-RUN] ... aws s3 cp .../functions-0.1.0-py3-none-any.whl s3://de-prod-0001/libs/...
# [DRY-RUN] ... aws s3 sync .../metadata/ s3://de-prod-0001/metadata/
# [DRY-RUN] ... aws glue create-job OR update-job ...
# Deploy complete.
# Exit 0
```

### 4.2 AWS dry-run with --skip-upload — no S3 commands

```powershell
& $py $skill/scripts/apply.py --platform aws --env prod --dry-run --skip-upload --project-dir /tmp/applytest
# Should NOT print any [DRY-RUN] aws s3 cp lines
# Should print [DRY-RUN] aws glue create-job OR update-job
& $py $skill/scripts/apply.py --platform aws --env prod --dry-run --skip-upload --project-dir /tmp/applytest | Select-String "s3 cp"
# (empty — no s3 cp in output)
```

### 4.3 AWS dry-run with --run — prints start-job-run at end

```powershell
& $py $skill/scripts/apply.py --platform aws --env prod --dry-run --run --project-dir /tmp/applytest
# Last [DRY-RUN] line should be: aws glue start-job-run --job-name my-etl-prod ...
& $py $skill/scripts/apply.py --platform aws --env prod --dry-run --run --project-dir /tmp/applytest | Select-String "start-job-run"
# [DRY-RUN] aws glue start-job-run --job-name my-etl-prod ...
```

### 4.4 Databricks dry-run — prints bundle validate + deploy

```powershell
# Simulate pre-generated notebook
'{}' | Set-Content /tmp/applytest/.datacoolie/generated/run_databricks.ipynb
& $py $skill/scripts/apply.py --platform databricks --env dev --dry-run --project-dir /tmp/applytest
# [DRY-RUN] databricks bundle validate
# [DRY-RUN] databricks bundle deploy -t dev --auto-approve
# Deploy complete.
```

### 4.5 Databricks dry-run with --run

```powershell
& $py $skill/scripts/apply.py --platform databricks --env dev --dry-run --run --project-dir /tmp/applytest
# Last [DRY-RUN] line: databricks bundle run -t dev my-etl_dev
& $py $skill/scripts/apply.py --platform databricks --env dev --dry-run --run --project-dir /tmp/applytest | Select-String "bundle run"
# [DRY-RUN] databricks bundle run -t dev my-etl_dev
```

### 4.6 Fabric dry-run — prints fab cp + fab deploy

```powershell
'{}' | Set-Content /tmp/applytest/.datacoolie/generated/run_fabric.ipynb
& $py $skill/scripts/apply.py --platform fabric --env staging --dry-run --project-dir /tmp/applytest
# [DRY-RUN] fab cp .../functions-0.1.0-py3-none-any.whl DEV_Workspace.Workspace/ETL_Lakehouse.Lakehouse/Files/libraries/...
# [DRY-RUN] fab cp .../use_cases.json DEV_Workspace.Workspace/.../Files/metadata/use_cases.json
# [DRY-RUN] fab import .../run_fabric.ipynb DEV_Workspace.Workspace
# Deploy complete.
```

### 4.7 Fabric dry-run with --run

```powershell
& $py $skill/scripts/apply.py --platform fabric --env staging --dry-run --run --project-dir /tmp/applytest | Select-String "fab start"
# [DRY-RUN] fab start DEV_Workspace.Workspace/my-etl_staging.Notebook
```

### 4.8 Local platform — no deployment step

```powershell
& $py $skill/scripts/apply.py --platform local --env dev --dry-run --project-dir /tmp/applytest
# Local platform — no deployment needed. Run the generated script directly.
# Deploy complete.
# Exit 0
```

### 4.9 Exit code is 0 on dry-run success

```powershell
& $py $skill/scripts/apply.py --platform aws --env prod --dry-run --project-dir /tmp/applytest
echo "Exit: $LASTEXITCODE"  # Exit: 0
```

### Cleanup

```powershell
Remove-Item -Recurse /tmp/applytest
```

---

## 5. cicd.py

### Setup

```powershell
New-Item -ItemType Directory -Force /tmp/cicdtest/.datacoolie | Out-Null
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

```powershell
& $py $skill/scripts/cicd.py --platform aws --env prod --project-dir /tmp/cicdtest
# ✓ Workflow: /tmp/cicdtest/.github/workflows/deploy-aws.yml
# Required GitHub Secrets: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
Test-Path /tmp/cicdtest/.github/workflows/deploy-aws.yml  # True
```

### 5.2 AWS workflow contains S3 upload steps before Glue deploy

```powershell
$wf = Get-Content /tmp/cicdtest/.github/workflows/deploy-aws.yml -Raw
$wf | Select-String "Upload script to S3"   # present
$wf | Select-String "Upload functions wheel"  # present
$wf | Select-String "Sync metadata"           # present
$wf | Select-String "skip-upload"             # --skip-upload flag is used
```

### 5.3 AWS workflow bucket is templated from config

```powershell
Get-Content /tmp/cicdtest/.github/workflows/deploy-aws.yml | Select-String "de-prod-0001"
# Should show the bucket name in aws s3 cp and s3 sync lines
```

### 5.4 Generate Databricks workflow

```powershell
& $py $skill/scripts/cicd.py --platform databricks --env dev --project-dir /tmp/cicdtest
# ✓ Workflow: /tmp/cicdtest/.github/workflows/deploy-databricks.yml
# Required GitHub Secrets: DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
Test-Path /tmp/cicdtest/.github/workflows/deploy-databricks.yml  # True
```

### 5.5 Generate Fabric workflow

```powershell
& $py $skill/scripts/cicd.py --platform fabric --env staging --project-dir /tmp/cicdtest
# ✓ Workflow: /tmp/cicdtest/.github/workflows/deploy-fabric.yml
# Required GitHub Secrets: FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET, FABRIC_TENANT_ID
Test-Path /tmp/cicdtest/.github/workflows/deploy-fabric.yml  # True
```

### 5.6 Fabric workflow contains fab auth login step

```powershell
Get-Content /tmp/cicdtest/.github/workflows/deploy-fabric.yml | Select-String "fab auth login"
# - run: fab auth login -u $FABRIC_CLIENT_ID -p $FABRIC_CLIENT_SECRET --tenant $FABRIC_TENANT_ID
```

### 5.7 Custom output path

```powershell
& $py $skill/scripts/cicd.py --platform aws --env prod --project-dir /tmp/cicdtest --output /tmp/cicdtest/my-deploy.yml
# ✓ Workflow: /tmp/cicdtest/my-deploy.yml
Test-Path /tmp/cicdtest/my-deploy.yml  # True
```

### 5.8 Generated YAML is parseable

```powershell
# Requires: pip install pyyaml
& $py -c "
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

```powershell
& $py -c "
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

```powershell
Remove-Item -Recurse /tmp/cicdtest
```

---

## 6. End-to-end dry-run (all platforms)

Full flow: generate → package (zip) → apply dry-run, for each platform.

### Setup

```powershell
New-Item -ItemType Directory -Force /tmp/e2etest/.datacoolie | Out-Null
New-Item -ItemType Directory -Force /tmp/e2etest/metadata | Out-Null
New-Item -ItemType Directory -Force /tmp/e2etest/functions | Out-Null
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

```powershell
& $py $skill/scripts/generate.py --platform local --env local_env --project-dir /tmp/e2etest
& $py $skill/scripts/apply.py --platform local --env local_env --dry-run --project-dir /tmp/e2etest
# Local platform — no deployment needed.
# Exit 0
```

### 6.2 AWS end-to-end dry-run

```powershell
& $py $skill/scripts/generate.py --platform aws --env aws_env --project-dir /tmp/e2etest
& $py $skill/scripts/package.py --format zip --functions-dir /tmp/e2etest/functions --output /tmp/e2etest/.datacoolie/generated/dist
& $py $skill/scripts/apply.py --platform aws --env aws_env --dry-run --skip-upload --project-dir /tmp/e2etest
# [DRY-RUN] aws glue create-job ...  with s3://e2e-bucket/scripts/run_aws_glue.py
# Exit 0
```

### 6.3 Fabric end-to-end dry-run

```powershell
& $py $skill/scripts/generate.py --platform fabric --env fabric_env --project-dir /tmp/e2etest
& $py $skill/scripts/apply.py --platform fabric --env fabric_env --dry-run --project-dir /tmp/e2etest
# [DRY-RUN] fab cp .../functions.zip E2EWorkspace.Workspace/E2ELakehouse.Lakehouse/Files/libraries/functions.zip
# [DRY-RUN] fab cp .../use_cases.json E2EWorkspace.Workspace/.../Files/metadata/use_cases.json
# [DRY-RUN] fab import .../run_fabric.ipynb E2EWorkspace.Workspace
# Exit 0
```

### 6.4 Databricks end-to-end dry-run

```powershell
& $py $skill/scripts/generate.py --platform databricks --env dbx_env --project-dir /tmp/e2etest
& $py $skill/scripts/apply.py --platform databricks --env dbx_env --dry-run --project-dir /tmp/e2etest
# [DRY-RUN] databricks bundle validate
# [DRY-RUN] databricks bundle deploy -t dbx_env --auto-approve
# Exit 0
```

### Cleanup

```powershell
Remove-Item -Recurse /tmp/e2etest
```

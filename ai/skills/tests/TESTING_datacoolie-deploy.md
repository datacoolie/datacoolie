# datacoolie-deploy — Testing Guide

This skill is **knowledge-based** — the AI reads SKILL.md rules and Jinja2 templates, then runs platform CLI commands directly. There are no scripts to execute.

---

## What to Test

### 1. SKILL.md Content Validation

Open `datacoolie-deploy/SKILL.md` and verify:

- [ ] AI workflow steps are clear: preflight → generate runner → package functions → upload → create/update job → promote
- [ ] All platforms documented: Local, AWS Glue, Databricks, Fabric
- [ ] Preflight checklist covers: CLI installed, authenticated, datacoolie installed, metadata exists, functions packageable, secrets resolved, infra exists
- [ ] Runner generation rules reference templates in `templates/`
- [ ] Functions packaging logic: auto-detect zip vs wheel, exclude `__pycache__`, validate imports
- [ ] CI/CD workflow generation documented (GitHub Actions templates)
- [ ] Security: secrets never hardcoded, `secrets_ref` resolved from env vars

### 2. Template Completeness

Check `datacoolie-deploy/templates/`:

| Template | Verify |
|----------|--------|
| `run_local.py.j2` | References LocalPlatform, engine selection (Polars/Spark), DataCoolieDriver |
| `run_aws_glue.py.j2` | Placeholders for region, bucket, role_arn; uses SparkEngine |
| `run_fabric.ipynb.j2` | 4-cell notebook: markdown header, pip install, config, run; ROOT_PATH placeholder |
| `run_databricks.ipynb.j2` | 4-cell notebook; VOLUME_ROOT placeholder |
| `github-actions-aws.yml.j2` | CI/CD workflow for AWS Glue deployment |
| `github-actions-databricks.yml.j2` | CI/CD workflow for Databricks bundle deploy |
| `github-actions-fabric.yml.j2` | CI/CD workflow for Fabric deployment |

- [ ] All templates use valid Jinja2 syntax
- [ ] Notebook templates produce valid `.ipynb` JSON structure
- [ ] Python templates pass `py_compile` syntax check
- [ ] Config values sourced from `.datacoolie/config.yaml` placeholders

### 3. Manual Workflow Testing — Preflight

Ask the AI to run preflight checks and verify:

| Test Case | Setup | Expected |
|-----------|-------|----------|
| Local — passes | Project with metadata/ | All checks pass |
| Missing CLI | No `aws`/`fab`/`databricks` installed | Fails with install hint |
| No metadata | Empty project dir | "Run datacoolie-init first" |
| Missing secrets | `secrets_ref` in metadata, env var unset | Lists missing vars, stops |
| Functions — no dir | No `functions/` directory | Skips gracefully (optional) |
| Functions — pyproject.toml | `functions/pyproject.toml` exists | Reports "wheel build" |
| Functions — __init__.py only | `functions/__init__.py` exists | Reports "zip package" |
| Provision failures | Provision log has "failed" entries | "Run datacoolie-provision first" |

### 4. Manual Workflow Testing — Runner Generation

Ask the AI to generate runners and verify output:

| Platform | Prompt | Verify |
|----------|--------|--------|
| Local | "Generate local runner" | `.datacoolie/generated/run_local.py` — valid Python, correct engine |
| AWS Glue | "Generate AWS Glue runner for prod" | `run_aws_glue.py` — region, bucket, role_arn from config |
| Fabric | "Generate Fabric runner for staging" | `run_fabric.ipynb` — valid notebook JSON, 4 cells, correct ROOT_PATH |
| Databricks | "Generate Databricks runner" | `run_databricks.ipynb` — valid notebook JSON, correct VOLUME_ROOT |
| Custom output | "Generate AWS runner to /tmp/my_runner.py" | File at specified path |
| No config | Project without `.datacoolie/config.yaml` | Falls back to defaults |

### 5. Manual Workflow Testing — Functions Packaging

Ask the AI to package functions:

| Test Case | Verify |
|-----------|--------|
| Zip (auto-detect, no pyproject.toml) | `functions.zip` created, `__pycache__` excluded |
| Wheel (has pyproject.toml) | `.whl` file created via `python -m build` |
| No functions dir | Skips silently |
| Missing __init__.py for zip | Error reported |

### 6. Manual Workflow Testing — CI/CD Generation

- [ ] "Generate GitHub Actions for AWS" → valid YAML workflow file
- [ ] "Generate GitHub Actions for Fabric" → valid YAML workflow file
- [ ] "Generate GitHub Actions for Databricks" → valid YAML workflow file

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

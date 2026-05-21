---
name: datacoolie-deploy
description: >
  Deploy datacoolie ETL jobs to AWS Glue, Databricks, and Microsoft Fabric.
  Full CLI automation: preflight checks, runner generation, functions packaging,
  artifact upload, job creation/update, and CI/CD workflow generation.
  Use when user says "deploy", "ship to prod", "apply", "preflight", "generate runner",
  "package functions", "cicd", "aws glue deploy", "fabric deploy", "databricks bundle",
  or mentions shipping datacoolie to any cloud platform.
---

# datacoolie-deploy

Deploy datacoolie ETL jobs to cloud platforms via CLI automation.

## Supported Platforms

| Platform | CLI Tool | Deploy Method |
|---|---|---|
| AWS Glue | `aws` CLI | `create-job`/`update-job` (S3 artifacts) |
| Databricks | `databricks` CLI | `bundle deploy` (declarative YAML) |
| Microsoft Fabric | `fab` CLI (ms-fabric-cli) | `fab deploy` / `fab cp` |
| Local | None | Generate runner script only |

## Scripts

All scripts live in `scripts/` relative to this skill directory.

### 1. Preflight (`scripts/preflight.py`)

Validate CLI, auth, metadata, and functions before deploy.

```bash
python scripts/preflight.py --platform aws
python scripts/preflight.py --platform fabric --skip-auth
```

Output: Checklist (✓/✗) + exit code 0 (pass) or 1 (fail).

### 2. Generate (`scripts/generate.py`)

Generate platform-specific runner scripts/notebooks from templates.

```bash
python scripts/generate.py --platform aws --env prod
python scripts/generate.py --platform fabric --env dev
python scripts/generate.py --platform databricks --env prod
python scripts/generate.py --platform local --env dev
```

Output: Runner in `.datacoolie/generated/` (`.py` for AWS/Local, `.ipynb` for Fabric/Databricks).

### 3. Package (`scripts/package.py`)

Build functions/ into a deployable wheel or zip.

```bash
python scripts/package.py                  # auto-detect
python scripts/package.py --format wheel   # force wheel
python scripts/package.py --format zip     # force zip
```

Output: Artifact in `.datacoolie/generated/dist/`.

### 4. Apply (`scripts/apply.py`)

Orchestrate full deployment: preflight → package → generate → deploy.

```bash
python scripts/apply.py --platform aws --env prod
python scripts/apply.py --platform aws --env prod --skip-upload   # CI mode
python scripts/apply.py --platform databricks --env dev --run
python scripts/apply.py --platform fabric --env prod --dry-run
```

Flags:
- `--dry-run`: Print commands without executing
- `--run`: Start a job run after deploy
- `--skip-upload`: Skip S3 upload (CI already handled it)

### 5. CI/CD (`scripts/cicd.py`)

Generate GitHub Actions workflow for automated deployment.

```bash
python scripts/cicd.py --platform aws --env prod
python scripts/cicd.py --platform databricks --env staging
python scripts/cicd.py --platform fabric --env prod
```

Output: `.github/workflows/deploy-{platform}.yml`

## Configuration

Project config at `.datacoolie/config.yaml`:

```yaml
project_name: my-etl-project
engine: spark

environments:
  prod:
    platform: aws
    aws:
      region: ap-southeast-1
      bucket: de-prod-0001
      role_arn: arn:aws:iam::123456789012:role/GlueETLRole
  staging:
    platform: fabric
    fabric:
      workspace: "DataEngineering-Staging"
      lakehouse: "ETL_Lakehouse"
  dev:
    platform: databricks
    databricks:
      host: "https://adb-123.azuredatabricks.net"
      catalog: workspace
      schema: default
      volume: datacoolie
```

## Workflow

Typical deployment flow:

```
1. preflight.py --platform X       → validate environment
2. generate.py --platform X --env Y → create runner
3. package.py                       → build functions wheel
4. apply.py --platform X --env Y    → deploy everything
5. cicd.py --platform X --env Y     → (optional) generate CI workflow
```

Or single-command: `apply.py` runs steps 1-4 automatically.

## AWS Deploy Constraint

AWS Glue `ScriptLocation` is a static S3 URI. The runner script and wheel
must be uploaded to S3 **before** `create-job`/`update-job` is called.
In CI, use `--skip-upload` flag since the workflow uploads artifacts directly.

## Templates

Located in `templates/`:
- `run_local.py.j2` — Local Python runner
- `run_aws_glue.py.j2` — AWS Glue PySpark job
- `run_fabric.ipynb.j2` — Fabric notebook (4 cells)
- `run_databricks.ipynb.j2` — Databricks notebook (4 cells)
- `github-actions-aws.yml.j2` — CI/CD for AWS
- `github-actions-databricks.yml.j2` — CI/CD for Databricks
- `github-actions-fabric.yml.j2` — CI/CD for Fabric

## Prerequisites

- `jinja2` (for template rendering)
- Platform CLI installed and authenticated
- `datacoolie` installed
- Project with `metadata/` directory containing dataflow JSON(s)

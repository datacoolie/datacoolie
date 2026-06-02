---
name: datacoolie-deploy
description: >
  Deploy datacoolie ETL jobs to AWS Glue, Databricks, and Microsoft Fabric.
  AI-driven workflows: preflight checks, runner generation, functions packaging,
  artifact upload, job creation/update, promotion, and CI/CD workflow generation.
  Use when user says "deploy", "ship to prod", "apply", "preflight", "generate runner",
  "package functions", "cicd", "aws glue deploy", "fabric deploy", "databricks bundle",
  or mentions shipping datacoolie to any cloud platform.
---

# datacoolie-deploy

Deploy datacoolie ETL jobs to cloud platforms via direct CLI commands.

## Supported Platforms

| Platform | CLI Tool | Deploy Method |
|---|---|---|
| AWS Glue | `aws` CLI | `create-job`/`update-job` (S3 artifacts) |
| Databricks | `databricks` CLI | `bundle deploy` (declarative YAML) |
| Microsoft Fabric | `fab` CLI (ms-fabric-cli) | `fab deploy` / `fab cp` |
| Local | None | Generate runner script only |

## AI Workflow

### 0. Read Upstream Artifacts

Read project context from prior phases before starting deployment.

**Required artifacts:**

| Artifact | Path | Extract |
|----------|------|--------|
| Project config | `.datacoolie/config.yaml` | `project_name`, `engine`, `environments.{env}.platform`, platform-specific config (bucket, workspace, catalog, etc.) |
| Metadata files | `metadata/*.json` or `metadata/*.yaml` | Dataflow definitions to deploy; validate at least one exists |
| Functions package | `functions/` | Check if `pyproject.toml` (wheel) or `__init__.py` (zip) exists |

**Optional artifacts:**

| Artifact | Path | Extract |
|----------|------|--------|
| Architecture document | `.datacoolie/architect/*_architecture.md` | Platform name, environment config — cross-check against `config.yaml` alignment |
| Environment overlays | `metadata/environments/*.yaml` | Available environments for promotion |

**If `config.yaml` is missing** → ask user for platform, engine, and environment config interactively.

**If `metadata/` is empty** → stop and suggest running `datacoolie-metadata` first.

### 1. Preflight Checklist

| # | Check | Pass | Fail |
|---|---|---|---|
| 1 | Platform CLI installed | Continue | Show platform CLI install docs, stop |
| 2 | Platform CLI authenticated | Continue | Show login command, stop |
| 3 | `datacoolie` installed | Continue | `pip install datacoolie` |
| 4 | `metadata/` has `.json`/`.yaml` files | Continue | Ask user to create metadata first |
| 5 | `functions/` has `pyproject.toml` or `__init__.py` | Continue | Warn (functions package may not be needed) |
| 6 | All `secrets_ref` env vars present | Continue | List missing vars, stop |
| 7 | Target infrastructure exists | Continue | Ask user to provision infrastructure first |

### 2. Generate Runner

1. Determine target platform from user/config
2. Read reference example from `references/run_{platform}.{ext}.example` to understand datacoolie API patterns
3. Generate runner file with actual project values from `.datacoolie/config.yaml`
4. Write to `scripts/run_{platform}.{ext}`

### 3. Package Functions

AI runs these commands to build a deployable artifact:

```bash
# Wheel (preferred — when pyproject.toml exists)
cd functions/ && pip wheel --no-deps -w ../dist .

# Zip (fallback if no pyproject.toml)
cd functions/ && python -m zipfile -c ../dist/functions.zip .
```

### 4. Platform Deploy

Use the current platform CLI to upload artifacts and create/update the job. Key behaviors by platform:

- **AWS Glue**: Upload runner script + wheel to S3 first (ScriptLocation must resolve before create/update-job), then create or update the Glue job
- **Databricks**: Use bundle deploy (`databricks.yml` in project root)
- **Microsoft Fabric**: Upload notebook and wheel to the target lakehouse
- **Local**: Execute the runner script directly — no upload needed

### 5. Promote (env → env)

Deploy to a different environment using the same workflow with the target environment's config:
1. Preflight on target environment
2. Validate and merge metadata for target environment
3. Deploy to target platform

**GATE**: Promotion to `prod` requires explicit user confirmation.

### 6. Generate CI/CD

1. Read reference example from `references/github-actions-{platform}.yml.example`
2. Generate CI/CD workflow with actual project values
3. Write to `.github/workflows/deploy-{platform}.yml`

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

## Full Deploy Flow

Typical deployment sequence:

```
1. Preflight        → validate environment
2. Generate runner   → create platform-specific runner
3. Package functions → build wheel/zip
4. Deploy            → upload + create/update job
5. Generate CI/CD    → (optional) create GitHub Actions workflow
```

Promotion flow (env → env):

```
Preflight (target) → Validate metadata → Merge overlay → Deploy
→ .datacoolie/promote/yymmdd_promote-{from}-to-{to}.md
```

## References

Located in `references/`:
- `run_local.py.example` — Local Python runner API pattern
- `run_aws_glue.py.example` — AWS Glue PySpark job API pattern
- `run_fabric.ipynb.example` — Fabric notebook API pattern
- `run_databricks.ipynb.example` — Databricks notebook API pattern
- `github-actions-aws.yml.example` — CI/CD for AWS
- `github-actions-databricks.yml.example` — CI/CD for Databricks
- `github-actions-fabric.yml.example` — CI/CD for Fabric

## Script Status

This skill is **knowledge-based** — the AI reads SKILL.md and reference examples, then runs platform CLI commands directly. The testing guide contains specifications for future Python scripts (`apply.py`, `generate.py`, `package.py`, `cicd.py`, `promote.py`) that are not yet implemented.

## Prerequisites

- Project config at `.datacoolie/config.yaml` (recommended — falls back to interactive)
- Metadata files in `metadata/` directory
- Platform CLI installed and authenticated (per target platform)
- `datacoolie` installed (`pip install datacoolie`)

## Input Contracts

| Direction | Artifact | Path | Required |
|-----------|----------|------|----------|
| Input | Project config | `.datacoolie/config.yaml` | Yes — platform, engine, env config |
| Input | Metadata files | `metadata/*.json` or `metadata/*.yaml` | Yes — at least one dataflow definition |
| Input | Functions package | `functions/` | No — skipped if absent |
| Input | Architecture document | `.datacoolie/architect/*_architecture.md` | No — optional platform cross-check |
| Input | Environment overlays | `metadata/environments/*.yaml` | No — needed for promotion only |

## Output Contracts

| Artifact | Path | Notes |
|----------|------|-------|
| Runner script | `.datacoolie/generated/run_{platform}.{ext}` | Platform-specific runner |
| Functions artifact | `.datacoolie/generated/dist/functions*.whl` or `functions.zip` | Wheel or zip package |
| CI/CD workflow | `.github/workflows/deploy-{platform}.yml` | Step 6 only |
| Promotion log | `.datacoolie/promote/yymmdd_promote-{from}-to-{to}.md` | Step 5 only |

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
| Project config | `{project_name}_dcws/config.yaml` | `project.name`, `project.workspace_name`, `environments.{env}.platform`, generated metadata path, platform-specific config (bucket, workspace, catalog, etc.) |
| Metadata files | `{project_name}_dcws/metadata/` | Authoring metadata to merge for the target environment |
| Functions package | `{project_name}_dcws/functions/` | Check if `pyproject.toml` (wheel) or `__init__.py` (zip) exists |

**Optional artifacts:**

| Artifact | Path | Extract |
|----------|------|--------|
| Architecture document | `{project_name}_dcws/architecture/current.md` | Platform name, environment config — cross-check against `config.yaml` alignment |
| Gate journals | `{project_name}_dcws/project_management/phases/*/gate-reviews/*.md` | Required approvals for layer and production promotion |
| Environment overlays | `{project_name}_dcws/metadata/environments/*.yaml` | Available environments for promotion |

**If `config.yaml` is missing** → ask user for project name, workspace name, platform, and environment config interactively. Runtime engine is selected from architecture or runner generation context, not top-level project config.

**If `{project_name}_dcws/metadata/` is empty** → stop and suggest running `datacoolie-metadata` first.

### 1. Preflight Checklist

| # | Check | Pass | Fail |
|---|---|---|---|
| 1 | Platform CLI installed | Continue | Show platform CLI install docs, stop |
| 2 | Platform CLI authenticated | Continue | Show login command, stop |
| 3 | `datacoolie` installed | Continue | `pip install datacoolie` |
| 4 | `{project_name}_dcws/metadata/` has unified, split, or modular metadata files | Continue | Ask user to create metadata first |
| 5 | `{project_name}_dcws/functions/` has `pyproject.toml` or `__init__.py` | Continue | Warn (functions package may not be needed) |
| 6 | All `secrets_ref` env vars present | Continue | List missing vars, stop |
| 7 | Target infrastructure exists | Continue | Ask user to provision infrastructure first |

### 2. Generate Runner

1. Determine target platform from user/config
2. Read reference example from `references/run_{platform}.{ext}.example` to understand datacoolie API patterns
3. Generate runner file with actual project values from `{project_name}_dcws/config.yaml`
4. Write to `{project_name}_dcws/generated/run_{platform}.{ext}`

### 3. Package Functions

AI runs these commands to build a deployable artifact:

```bash
# Wheel (preferred — when pyproject.toml exists)
cd {project_name}_dcws/functions/ && pip wheel --no-deps -w ../generated/dist .

# Zip (fallback if no pyproject.toml)
cd {project_name}_dcws/functions/ && python -m zipfile -c ../generated/dist/functions.zip .
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
2. Validate and merge metadata for target environment into `{project_name}_dcws/generated/{env}/metadata.json` unless split output is explicitly requested
3. Deploy to target platform

**GATE**: Promotion or deployment to `prod` requires:
- approved `source2bronze`, `bronze2silver`, and `silver2gold` gate journals when those layers are in scope
- approved `production` gate journal under `{project_name}_dcws/project_management/phases/production/gate-reviews/`
- explicit user confirmation in the current session

### 6. Generate CI/CD

1. Read reference example from `references/github-actions-{platform}.yml.example`
2. Generate CI/CD workflow with actual project values
3. Write to `.github/workflows/deploy-{platform}.yml`

## Configuration

Project config at `{project_name}_dcws/config.yaml`:

```yaml
schema_version: 1

project:
  name: my-etl-project
  workspace_name: my_etl_project_dcws

defaults:
  environment: dev
  metadata_layout: modular

artifacts:
  generated_dir: generated
  metadata_dir: metadata
  project_management_dir: project_management

environments:
  dev:
    platform: local
    paths:
      data_root: ./data
      logs: ./logs
    generated_metadata: generated/dev/metadata.json
  prod:
    platform: aws
    generated_metadata: generated/prod/metadata.json
    aws:
      region: ap-southeast-1
      bucket: de-prod-0001
      role_arn: arn:aws:iam::123456789012:role/GlueETLRole
  staging:
    platform: fabric
    generated_metadata: generated/staging/metadata.json
    fabric:
      workspace: "DataEngineering-Staging"
      lakehouse: "ETL_Lakehouse"
  sandbox:
    platform: databricks
    generated_metadata: generated/sandbox/metadata.json
    databricks:
      host: "https://adb-123.azuredatabricks.net"
      catalog: workspace
      schema: default
      volume: datacoolie
```

`config.yaml` is a workspace control file. Follow the structure documented in
`datacoolie-init/templates/project-structure.md`. It must not contain dataflow definitions,
gate status, secret values, or a top-level runtime engine.

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
→ {project_name}_dcws/promote/yymmdd_promote-{from}-to-{to}.md
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

- Project config at `{project_name}_dcws/config.yaml` (recommended — falls back to interactive)
- Metadata files in `{project_name}_dcws/metadata/` directory
- Platform CLI installed and authenticated (per target platform)
- `datacoolie` installed (`pip install datacoolie`)

## Input Contracts

| Direction | Artifact | Path | Required |
|-----------|----------|------|----------|
| Input | Project config | `{project_name}_dcws/config.yaml` | Yes — project, platform, and environment config |
| Input | Metadata files | `{project_name}_dcws/metadata/` | Yes — unified, split, or modular metadata |
| Input | Functions package | `{project_name}_dcws/functions/` | No — skipped if absent |
| Input | Architecture document | `{project_name}_dcws/architecture/current.md` | No — optional platform cross-check |
| Input | Environment overlays | `{project_name}_dcws/metadata/environments/*.yaml` | No — needed for promotion only |
| Input | Gate journals | `{project_name}_dcws/project_management/phases/*/gate-reviews/*.md` | Yes for production |

## Output Contracts

| Artifact | Path | Notes |
|----------|------|-------|
| Runner script | `{project_name}_dcws/generated/run_{platform}.{ext}` | Platform-specific runner |
| Generated metadata | `{project_name}_dcws/generated/{env}/metadata.json` | Default merged runtime metadata |
| Functions artifact | `{project_name}_dcws/generated/dist/functions*.whl` or `functions.zip` | Wheel or zip package |
| CI/CD workflow | `.github/workflows/deploy-{platform}.yml` | Step 6 only |
| Deploy log | `{project_name}_dcws/deploy/yymmdd_deploy-{env}.md` | Step 4 only |
| Promotion log | `{project_name}_dcws/promote/yymmdd_promote-{from}-to-{to}.md` | Step 5 only |

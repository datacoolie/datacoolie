---
name: datacoolie-init
description: >
  Scaffold DataCoolie ETL project directories with best-practice structure.
  Creates folders, skeleton metadata, runner script, config, and gitignore.
  Use when user says "scaffold project", "init", "initialize", "create project",
  "setup datacoolie", "new project", or "start new ETL".
---

# datacoolie-init

Scaffold a DataCoolie ETL project directory with skeleton files ready to be filled in.

## Scope

This skill handles: project directory creation, skeleton file generation (empty metadata, runner script, config, gitignore, functions package).
Does NOT handle: source discovery, architecture design, metadata content generation, infrastructure provisioning, deployment.

## AI Workflow

### Step 1: Ask project details

Ask the user:
- Project name
- Engine preference (`polars` or `spark`, default: `polars`)

### Step 2: Scaffold

Read `templates/project-structure.md` and create all directories and files.
Substitute `{project_name}` and `{engine}` from user answers.

### Step 3: Confirm

List the created files and confirm the project is ready. The user fills in metadata content and connection details as a next step.

## Output Contracts

| Artifact | Path |
|----------|------|
| Project directory | `{project_name}/` |
| Skeleton metadata | `{project_name}/metadata/metadata.json` |
| Runner script | `{project_name}/scripts/run_local.py` |
| Project config | `{project_name}/.datacoolie/config.yaml` |
| Functions package | `{project_name}/functions/` |

## Security

- Never embed credentials in generated files — use `secrets_ref` pattern
- Generated `config.yaml` never contains passwords or API keys

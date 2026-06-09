---
name: datacoolie-init
description: >
  Scaffold DataCoolie ETL project directories with best-practice structure.
  Consumes the approved architecture document to pre-populate config, runner, and
  environment overlays. Falls back to interactive mode when no architecture exists.
  Use when user says "scaffold project", "init", "initialize", "create project",
  "setup datacoolie", "new project", or "start new ETL".
---

# datacoolie-init

Scaffold a DataCoolie ETL project workspace with skeleton files ready to be filled in.
When an approved architecture document exists, pre-populates config, runner, and environment
overlays from its decisions. Otherwise falls back to interactive mode.

## Scope

This skill handles: `{project_name}_dcws/` creation, skeleton file generation (workspace AGENTS.md,
workspace config, runner script, metadata skeleton, project-management journals, functions package, gitignore),
architecture-to-scaffold mapping (extracting platform, runtime engine, and environment decisions from approved architecture).

Does NOT handle: source discovery (use `datacoolie-discover`), architecture design (use `datacoolie-architect`),
metadata content generation (use `datacoolie-metadata`), infrastructure provisioning, deployment.
Source/file introspection belongs to the discover skill, not init.

## Prerequisites

- Workspace name is `{normalized_project_name}_dcws`, for example `Sales Analytics` -> `sales_analytics_dcws`
- If an approved architecture exists at `{project_name}_dcws/architecture/current.md`, init consumes it
- If no architecture exists, init works in interactive mode (no prerequisites)

## AI Workflow

### Step 0: Read Architecture (if available)

Check for an approved architecture document at `{project_name}_dcws/architecture/current.md`.
Also check for an approved architecture gate journal under
`{project_name}_dcws/project_management/phases/architecture/gate-reviews/`.

**If found and approved**, extract these values to pre-populate the scaffold:

| Architecture Section | Extract | Maps To |
|---------------------|---------|---------|
| Overview → Target platform | Platform name | `config.yaml` → `environments.*.platform` |
| Overview → Medallion layers | Layer count and names | Runner script stage list (comments) |
| Engine Strategy matrix | Concrete engine (`polars` or `spark`) per environment + stage | Resolve target env/stage before generating runner/deploy imports |
| Environment Differences table | Dev/Test/Prod config matrix | `config.yaml` → `environments` block, overlay files |
| Infrastructure Requirements | Resource names per platform | `config.yaml` → platform-specific config stubs |
| Source Registry | Source count and names | Runner script comments, metadata skeleton structure |

If architecture exists but its gate is not approved, stop and ask for architecture review before scaffolding beyond isolated workspace files.

**If not found**, fall back to interactive mode (current behavior — ask user for details in Step 1).

### Step 1: Gather Missing Details

**Always ask:**
- Project name (architecture does not define this)

Derive `{workspace_name}` by normalizing project name to lowercase snake/kebab-safe text and appending `_dcws`.

**Ask only if NOT determined by architecture (Step 0):**
- Default runner engine (`polars` or `spark` — default: `polars`)
- Target platform (default: `local`)

**Ask only for advanced setups:**
- Scaffold parent directory: current directory (default) or a chosen parent directory. The workspace folder inside it is always `{project_name}_dcws/`.
- Metadata layout: modular stage files (default), combined `metadata.json`, or split `connections.json` + `dataflows.json`

### Step 2: Scaffold

1. Read `templates/project-structure.md` for the directory layout and file templates
2. Substitute variables from Step 0 (architecture) and Step 1 (user input):
   - `{project_name}` — from user
   - `{workspace_name}` — normalized `{project_name}_dcws`
   - `{runner_engine}` — default concrete engine from architecture Engine Strategy or user input (`polars` or `spark`)
   - `{engine_strategy}` — optional environment/stage engine matrix from architecture
   - `{platform}` — from architecture Overview or user input
   - `{environments}` — from architecture Environment Differences or defaults (dev only)
   - `{infra_*}` — from architecture Infrastructure Requirements or empty stubs
3. Generate `{workspace_name}/config.yaml` as a workspace control file using the structure in `templates/project-structure.md`
   - Include `schema_version: 1`
   - Include `project.name` and `project.workspace_name`
   - Include environment names, target platforms, platform resource references, artifact paths, and generated metadata paths
   - Do not include dataflows, metadata definitions, gate status, secret values, or top-level runtime engine
4. Create all directories and files under `{workspace_name}/` at the scaffold parent (current directory by default)
5. Create `{workspace_name}/AGENTS.md` from [ai/AGENTS.md](https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/AGENTS.md) if missing. Do not overwrite an existing workspace `AGENTS.md`.
6. If architecture specified multiple environments, create environment overlay files:
   - `{workspace_name}/metadata/environments/dev.yaml` (always)
   - `{workspace_name}/metadata/environments/prod.yaml` (if architecture has prod environment)
   - Additional overlays per architecture's Environment Differences table
7. Create project-management starter files:
   - `{workspace_name}/project_management/status.md`
   - `{workspace_name}/project_management/risks.md`
   - `{workspace_name}/project_management/changelog.md`
   - phase directories for `architecture`, `source2bronze`, `bronze2silver`, `silver2gold`, and `production`, each with `scope.md`, `notes.md`, `evidence.md`, and `gate-reviews/`

### Step 3: Confirm

List created files and directories. If architecture was consumed, show the mapping:

> Architecture consumed: `{project_name}_dcws/architecture/current.md`
> - Platform: Fabric → config.yaml environments pre-filled
> - Runtime engine strategy: dev/source2bronze=polars, prod/silver2gold=spark → selected runner target uses a concrete engine
> - Environments: dev (local), prod (fabric) → overlay files created

The user fills in metadata content and connection details as a next step (Phase 4 — Metadata).

---

## Input Contracts

| Direction | Artifact | Path | Required |
|-----------|----------|------|----------|
| Input | Architecture document | `{project_name}_dcws/architecture/current.md` | No — falls back to interactive |
| Input | Architecture gate journal | `{project_name}_dcws/project_management/phases/architecture/gate-reviews/*.md` | Required if architecture exists |
| Input | User input | Interactive | Yes — project_name always required |

## Output Contracts

| Artifact | Default Path | Notes |
|----------|-------------|-------|
| Workspace guide | `{project_name}_dcws/AGENTS.md` | Copied from `datacoolie/ai/AGENTS.md` if missing |
| Project config | `{project_name}_dcws/config.yaml` | Workspace control file using the documented init template structure; no dataflow logic, gate state, secret values, or top-level engine |
| Skeleton metadata | `{project_name}_dcws/metadata/connections.json`, `{project_name}_dcws/metadata/schema_hints.json`, `{project_name}_dcws/metadata/dataflows/{stage}.json` | Modular default; unified `metadata.json` remains supported |
| Environment overlays | `{project_name}_dcws/metadata/environments/*.yaml` | One per environment from architecture |
| Project status | `{project_name}_dcws/project_management/status.md` | Current stage, last approved gate, blockers |
| Phase management | `{project_name}_dcws/project_management/phases/*/` | Scope, notes, evidence, and gate review journals |
| Runner script | `{project_name}_dcws/generated/run_local.py` | Concrete runtime engine imports match the selected target env/stage |
| Functions package | `{project_name}_dcws/functions/__init__.py`, `{project_name}_dcws/functions/sources.py` | Stub with function signature |
| Git ignore | `.gitignore` | Standard Python + datacoolie ignores |
| Dependencies | `requirements.txt` | `datacoolie` |

When scaffolding into a chosen parent directory, create `{project_name}_dcws/` under that parent and keep all DataCoolie artifacts inside it.

## Security

- Never embed credentials in generated files — use `secrets_ref` pattern
- Generated `config.yaml` never contains passwords, API keys, or secret values

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

Scaffold a DataCoolie ETL project directory with skeleton files ready to be filled in.
When an approved architecture document exists, pre-populates config, runner, and environment
overlays from its decisions. Otherwise falls back to interactive mode.

## Scope

This skill handles: project directory creation, skeleton file generation (config, runner script,
metadata skeleton, functions package, gitignore), architecture-to-scaffold mapping (extracting
platform, engine, environment decisions from approved architecture).

Does NOT handle: source discovery (use `datacoolie-discover`), architecture design (use `datacoolie-architect`),
metadata content generation (use `datacoolie-metadata`), infrastructure provisioning, deployment.
Source/file introspection belongs to the discover skill, not init.

## Prerequisites

- If an approved architecture exists at `.datacoolie/architect/*_architecture.md`, init consumes it
- If no architecture exists, init works in interactive mode (no prerequisites)

## AI Workflow

### Step 0: Read Architecture (if available)

Check for an approved architecture document at `.datacoolie/architect/*_architecture.md`.

**If found**, extract these values to pre-populate the scaffold:

| Architecture Section | Extract | Maps To |
|---------------------|---------|---------|
| Overview → Target platform | Platform name | `config.yaml` → `environments.*.platform` |
| Overview → Medallion layers | Layer count and names | Runner script stage list (comments) |
| Engine Strategy table | Engine per layer transition | `config.yaml` → `engine`, runner script imports |
| Environment Differences table | Dev/Test/Prod config matrix | `config.yaml` → `environments` block, overlay files |
| Infrastructure Requirements | Resource names per platform | `config.yaml` → platform-specific config stubs |
| Source Registry | Source count and names | Runner script comments, metadata skeleton structure |

**If not found**, fall back to interactive mode (current behavior — ask user for details in Step 1).

**Mixed engine handling**: If the architecture specifies different engines for different layers
(e.g., polars for source→bronze, spark for silver→gold), set `engine: mixed` in config.yaml
and generate runner script with both engine imports plus a comment explaining which stages use which.

### Step 1: Gather Missing Details

**Always ask:**
- Project name (architecture does not define this)

**Ask only if NOT determined by architecture (Step 0):**
- Engine preference (`polars`, `spark`, or `mixed` — default: `polars`)
- Target platform (default: `local`)

**Ask only for advanced setups:**
- Scaffold target: current directory (default) or subdirectory (`{project_name}/`)
- Metadata layout: combined `metadata.json` (default) or split `connections.json` + `dataflows.json`

### Step 2: Scaffold

1. Read `templates/project-structure.md` for the directory layout and file templates
2. Substitute variables from Step 0 (architecture) and Step 1 (user input):
   - `{project_name}` — from user
   - `{engine}` — from architecture Engine Strategy or user input
   - `{platform}` — from architecture Overview or user input
   - `{environments}` — from architecture Environment Differences or defaults (dev only)
   - `{infra_*}` — from architecture Infrastructure Requirements or empty stubs
3. Create all directories and files at the scaffold target (current directory by default)
4. If architecture specified multiple environments, create environment overlay files:
   - `metadata/environments/dev.yaml` (always)
   - `metadata/environments/prod.yaml` (if architecture has prod environment)
   - Additional overlays per architecture's Environment Differences table

### Step 3: Confirm

List created files and directories. If architecture was consumed, show the mapping:

> Architecture consumed: `.datacoolie/architect/260531_architecture.md`
> - Platform: Fabric → config.yaml environments pre-filled
> - Engine: polars (source→bronze, bronze→silver), spark (silver→gold) → mixed mode
> - Environments: dev (local), prod (fabric) → overlay files created

The user fills in metadata content and connection details as a next step (Phase 4 — Metadata).

---

## Input Contracts

| Direction | Artifact | Path | Required |
|-----------|----------|------|----------|
| Input | Architecture document | `.datacoolie/architect/*_architecture.md` | No — falls back to interactive |
| Input | User input | Interactive | Yes — project_name always required |

## Output Contracts

| Artifact | Default Path | Notes |
|----------|-------------|-------|
| Project config | `.datacoolie/config.yaml` | Pre-populated from architecture if available |
| Skeleton metadata | `metadata/metadata.json` | Empty connections/dataflows/schema_hints arrays |
| Environment overlays | `metadata/environments/*.yaml` | One per environment from architecture |
| Runner script | `scripts/run_local.py` | Engine imports match architecture |
| Functions package | `functions/__init__.py`, `functions/sources.py` | Stub with function signature |
| Git ignore | `.gitignore` | Standard Python + datacoolie ignores |
| Dependencies | `requirements.txt` | `datacoolie` |

When scaffolding into a subdirectory (`{project_name}/`), all paths are relative to that directory.

## Security

- Never embed credentials in generated files — use `secrets_ref` pattern
- Generated `config.yaml` never contains passwords or API keys

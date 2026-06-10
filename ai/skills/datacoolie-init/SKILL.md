---
name: datacoolie-init
description: >
  Bootstrap DataCoolie ETL project workspace files with best-practice structure.
  Consumes the approved architecture document to pre-populate config and runner source,
  environment overlays only when those artifacts are needed. Falls back to interactive mode when no architecture exists.
  Use when user says "scaffold project", "init", "initialize", "create project",
  "setup datacoolie", "new project", or "start new ETL".
---

# datacoolie-init

Bootstrap a DataCoolie ETL project workspace with only the files needed for the current request.
When an approved architecture document exists, pre-populates config, runner source, and environment
overlays from its decisions only for the active stage/environment. Otherwise falls back to interactive mode.

## Scope

This skill handles: `{project_name}_dcws/` creation, minimal workspace bootstrap (workspace AGENTS.md,
workspace config, project status), and requested artifact scaffolding,
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
Also check the latest architecture gate journal under
`{project_name}_dcws/project_management/phases/architecture/gate-reviews/`.
Select the latest journal by frontmatter `reviewed_at`; if missing, use the
lexicographically greatest filename. Treat the architecture gate as approved
only when that latest journal has `status: approved`.

**If found and approved**, extract these values to pre-populate the scaffold:

| Architecture Section | Extract | Maps To |
|---------------------|---------|---------|
| Overview → Target platform | Platform name | `config.yaml` → `environments.*.platform` |
| Overview → Medallion layers | Layer count and names | Runner script stage list (comments) |
| Engine Strategy matrix | Concrete engine (`polars` or `spark`) per environment + stage | Resolve target env/stage before generating runner/deploy imports |
| Environment Differences table | Dev/Test/Prod names and platform targets | `config.yaml` → `environments` names/platforms; runtime differences go to metadata overlays |
| Infrastructure Requirements | Resource names per platform | Architecture/provision context and metadata environment overlays, not `config.yaml` |
| Source Registry | Source count and names | Runner script comments, metadata skeleton structure |

If architecture exists but its gate is not approved, stop and ask for architecture review before scaffolding beyond isolated workspace files.

**If not found**, fall back to interactive mode (current behavior — ask user for details in Step 1).

### Step 1: Gather Missing Details

**Always ask:**
- Project name (architecture does not define this)

Derive `{workspace_name}` by normalizing project name to lowercase snake/kebab-safe text and appending `_dcws`.

**Ask only when needed by the current request:**
- Target platform, only when the user is not using the default `dev/local` environment or asks for platform-specific runner/provision/deploy scaffolding
- Concrete runner engine (`polars` or `spark`), only when runner generation or a durable runner source is requested

**Ask only for advanced setups:**
- Scaffold parent directory: current directory (default) or a chosen parent directory. The workspace folder inside it is always `{project_name}_dcws/`.
- Metadata layout, only when metadata authoring starts: modular stage files (default), combined `metadata.json`, or split `connections.json` + `dataflows.json`

### Step 2: Lazy Scaffold

1. Read `templates/project-structure.md` for the directory layout and file templates
2. Substitute variables from Step 0 (architecture) and Step 1 (user input):
   - `{project_name}` — from user
   - `{workspace_name}` — normalized `{project_name}_dcws`
   - `{runner_engine}` — default concrete engine from architecture Engine Strategy or user input (`polars` or `spark`)
   - `{engine_strategy}` — optional environment/stage engine matrix from architecture
   - `{platform}` — from architecture Overview or user input
   - `{environments}` — from architecture Environment Differences or defaults (dev only)
   - `{infra_*}` — from architecture Infrastructure Requirements or empty stubs
3. Create only the minimal workspace bootstrap if missing:
   - `{workspace_name}/`
   - `{workspace_name}/AGENTS.md` copied from [ai/AGENTS.md](https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/AGENTS.md) if missing
   - `{workspace_name}/config.yaml`
   - `{workspace_name}/project_management/status.md`
4. Generate `{workspace_name}/config.yaml` as a workspace control file using the structure in `templates/project-structure.md`
   - Include `schema_version: 1`
   - Include `project.name` and `project.workspace_name`
   - Include environment names and target platforms only
   - Do not include dataflows, metadata definitions, workflow defaults, artifact directory names, gate status, runtime paths, generated metadata paths, secret values, or top-level runtime engine
5. Create additional artifacts only when they are requested or required by the active phase:
   - Phase management files only for the active phase: `{workspace_name}/project_management/phases/{phase}/`
   - Metadata files only when metadata authoring starts, and only for stages in scope
   - Environment overlays only when an environment-specific override is needed
   - Functions package only when custom Python functions are needed
   - `{workspace_name}/runners/` only when a durable custom runner or notebook is needed
   - Generated runner/deploy artifacts only during runner generation or deploy
6. All project-management Markdown artifacts start with YAML frontmatter, including `notes.md` because it is an audit/handoff artifact.

### Step 3: Confirm

List created files and directories. If architecture was consumed, show the mapping:

> Architecture consumed: `{project_name}_dcws/architecture/current.md`
> - Platform: Fabric → config.yaml environment platform values pre-filled
> - Runtime engine strategy: dev/source2bronze=polars, prod/silver2gold=spark → selected runner target uses a concrete engine
> - Environments: dev (local), prod (fabric) → overlay files created

The user fills in metadata content and connection details as a next step (Phase 4 — Metadata).

---

## Input Contracts

| Direction | Artifact | Path | Required |
|-----------|----------|------|----------|
| Input | Architecture document | `{project_name}_dcws/architecture/current.md` | No — falls back to interactive |
| Input | Architecture gate journal | `{project_name}_dcws/project_management/phases/architecture/gate-reviews/*.md` | Required if architecture exists; latest journal must be `status: approved` |
| Input | User input | Interactive | Yes — project_name always required |

## Output Contracts

| Artifact | Default Path | Notes |
|----------|-------------|-------|
| Workspace guide | `{project_name}_dcws/AGENTS.md` | Copied from `datacoolie/ai/AGENTS.md` if missing |
| Project config | `{project_name}_dcws/config.yaml` | Workspace control file using the documented init template structure; project, workspace, environment names, and environment platforms only |
| Skeleton metadata | `{project_name}_dcws/metadata/connections.json`, `{project_name}_dcws/metadata/schema_hints.json`, `{project_name}_dcws/metadata/dataflows/{stage}.json` | Created only when metadata authoring starts; modular default; unified `metadata.json` remains supported |
| Environment overlays | `{project_name}_dcws/metadata/environments/*.yaml` | Created only when an environment override is needed |
| Project status | `{project_name}_dcws/project_management/status.md` | Current stage, last approved gate, blockers; YAML frontmatter `artifact_type: project_status` |
| Phase management | `{project_name}_dcws/project_management/phases/{phase}/` | Created only for the active phase; scope, notes, evidence, and gate review journals use YAML frontmatter |
| Runner source | `{project_name}_dcws/runners/run_{platform}.{ext}` | Created only when a durable custom runner or notebook is requested; concrete runtime engine imports match the selected target env/stage |
| Generated runner | `{project_name}_dcws/generated/run_{platform}.{ext}` | Derived from default references or `runners/`; safe to regenerate |
| Functions package | `{project_name}_dcws/functions/__init__.py`, `{project_name}_dcws/functions/sources.py` | Created only when custom Python functions are needed |
| Git ignore | `.gitignore` | Created only when code/dependency scaffolding is requested |
| Dependencies | `requirements.txt` | Created only when dependency scaffolding is requested |

When scaffolding into a chosen parent directory, create `{project_name}_dcws/` under that parent and keep all DataCoolie artifacts inside it.

## Security

- Never embed credentials in generated files — use `secrets_ref` pattern
- Generated `config.yaml` never contains passwords, API keys, or secret values

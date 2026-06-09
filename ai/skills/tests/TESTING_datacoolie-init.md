# datacoolie-init — Testing Guide

This skill is **knowledge-based** — the AI reads SKILL.md rules and `templates/project-structure.md`, then creates project files directly using file tools. There are no scripts to execute.

For source introspection (CSV/JSON/Parquet schema inference, DDL parsing), see the
`datacoolie-discover` skill and its introspection scripts.

---

## What to Test

### 1. SKILL.md Content Validation

Open `datacoolie-init/SKILL.md` and verify:

- [ ] Step 0 (Read Architecture) is documented with extraction table
- [ ] Step 1 (Gather Missing Details) only asks for values not determined by architecture
- [ ] Step 2 (Scaffold) describes variable substitution from architecture and user input
- [ ] Step 3 (Confirm) shows architecture-linked summary example
- [ ] Supported concrete engines listed: polars, spark
- [ ] Engine strategy handling described as env+stage matrix, with each value `polars` or `spark`
- [ ] Config.yaml is documented as workspace control file in AGENTS.md and the init project-structure template
- [ ] Input contracts reference architecture document (optional)
- [ ] Output contracts list all generated files with default paths
- [ ] Scaffold target flexibility documented (current directory vs subdirectory)
- [ ] Scope clarifies introspection belongs to discover, not init

### 2. Template Completeness

Check `datacoolie-init/templates/project-structure.md`:

- [ ] Variables table documents all substitution points with source and defaults
- [ ] Directory tree covers: `{project_name}_dcws/AGENTS.md`, `{project_name}_dcws/config.yaml`, `{project_name}_dcws/metadata/connections.json`, `{project_name}_dcws/metadata/schema_hints.json`, `{project_name}_dcws/metadata/dataflows/`, `{project_name}_dcws/metadata/environments/`, `{project_name}_dcws/functions/`, `{project_name}_dcws/generated/`, `{project_name}_dcws/project_management/phases/`, `.gitignore`, `requirements.txt`
- [ ] Config.yaml has minimal (no-arch) and architecture-aware variants
- [ ] Config.yaml examples include `schema_version`, `project.name`, `project.workspace_name`, `defaults`, `artifacts`, and `environments`
- [ ] Config.yaml architecture-aware variant shows Fabric and AWS environment examples
- [ ] Modular metadata skeleton includes `connections.json`, `schema_hints.json`, and stage files under `dataflows/`
- [ ] Environment overlay templates (`dev.yaml`, `prod.yaml`) have commented examples
- [ ] Runner script has concrete polars and spark variants only; env+stage engine strategy is resolved before runner generation
- [ ] Functions package structure includes `pyproject.toml`, `__init__.py`, `sources.py`

### 3. Manual Workflow Testing — Basic Scaffolding (No Architecture)

Ask the AI to scaffold a new project without an architecture document and verify:

| Test Case | Prompt | Verify |
|-----------|--------|--------|
| Minimal defaults | "Create a datacoolie project called my_project" | All files exist, config.yaml has correct `project.name`, `project.workspace_name`, and generated runner defaults to PolarsEngine |
| Polars engine | "Init project acme_etl with polars runner" | Runner imports PolarsEngine; config.yaml has no top-level `engine` |
| Spark engine | "Scaffold spark_proj with spark runner" | Runner imports SparkEngine and creates/provides SparkSession; config.yaml has no top-level `engine` |
| Subdirectory mode | "Create proj in a subdirectory" | Files created under `proj/` directory |
| Root scaffold | "Init project at current directory" | Files created at `.` (no subdirectory) |

### 4. Manual Workflow Testing — Architecture Consumption

Create a mock `{project_name}_dcws/architecture/current.md` plus an approved architecture gate journal with known values, then test init:

| Test Case | Architecture Setup | Prompt | Verify |
|-----------|-------------------|--------|--------|
| Single engine (polars) | Engine Strategy: all polars | "Init project acme_etl" | Runner imports PolarsEngine only, no engine question asked, config.yaml has no top-level `engine` |
| Env/stage engine strategy | Engine Strategy: dev all polars, prod silver/gold spark | "Init project staged_proj" | Generated runner is concrete for the selected target env/stage; config.yaml has no top-level `engine` |
| Multi-environment | Environment Differences: dev/test/prod | "Init project cloud_proj" | Three environment overlay files, config.yaml has dev + test + prod blocks |
| Fabric platform | Platform: Fabric, Infrastructure: workspace names | "Init project fabric_proj" | config.yaml prod environment has `platform: fabric` with workspace stubs |
| AWS platform | Platform: AWS, Infrastructure: bucket/role | "Init project aws_proj" | config.yaml prod environment has `platform: aws` with bucket/role stubs |
| No architecture | No `{project_name}_dcws/architecture/current.md` file | "Init project basic_proj" | Skill asks for engine and platform interactively, uses defaults |
| Unapproved architecture | Architecture gate journal missing or not `status: approved` | "Init project draft_proj" | Skill stops and requests architecture review before gated scaffolding |

### 5. Scaffold Output Validation

After any scaffold, verify:

- [ ] `{project_name}_dcws/AGENTS.md` exists and was not overwritten if pre-existing
- [ ] `{project_name}_dcws/metadata/connections.json`, `schema_hints.json`, and `dataflows/{stage}.json` are valid JSON arrays
- [ ] `{project_name}_dcws/metadata/environments/dev.yaml` exists with commented overlay examples
- [ ] `{project_name}_dcws/config.yaml` has `schema_version: 1`, `project.name`, `project.workspace_name`, and `environments`, and no top-level `engine` field
- [ ] `{project_name}_dcws/config.yaml` contains no dataflow definitions, gate status, passwords, API keys, or secret values
- [ ] `{project_name}_dcws/generated/run_local.py` has correct imports for the selected engine
- [ ] `{project_name}_dcws/functions/__init__.py` exists (can be empty)
- [ ] `{project_name}_dcws/functions/sources.py` has the `my_source` function stub with correct kwargs
- [ ] `{project_name}_dcws/functions/pyproject.toml` has correct `{project_name}-functions` name
- [ ] `{project_name}_dcws/project_management/phases/` has architecture, source2bronze, bronze2silver, silver2gold, and production directories
- [ ] Each phase has `scope.md`, `notes.md`, `evidence.md`, and `gate-reviews/`
- [ ] `.gitignore` includes `__pycache__/`, `{project_name}_dcws/watermarks/`, `.env`
- [ ] `requirements.txt` lists `datacoolie`
- [ ] No credentials, passwords, or API keys appear in any generated file

### 6. Edge Cases

- [ ] Output directory already exists → AI handles gracefully (warns, continues)
- [ ] Architecture exists but is empty/malformed → falls back to interactive mode
- [ ] User asks to use both engines but no architecture → AI asks for env+stage engine strategy; it does not create a third engine value
- [ ] Project name has spaces or special chars → AI rejects or sanitizes

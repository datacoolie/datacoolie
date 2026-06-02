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
- [ ] All supported engines listed: polars, spark, mixed
- [ ] Mixed engine handling described (different engines per layer)
- [ ] Input contracts reference architecture document (optional)
- [ ] Output contracts list all generated files with default paths
- [ ] Scaffold target flexibility documented (current directory vs subdirectory)
- [ ] Scope clarifies introspection belongs to discover, not init

### 2. Template Completeness

Check `datacoolie-init/templates/project-structure.md`:

- [ ] Variables table documents all substitution points with source and defaults
- [ ] Directory tree covers: `.datacoolie/config.yaml`, `metadata/metadata.json`, `metadata/environments/`, `functions/`, `scripts/`, `.gitignore`, `requirements.txt`
- [ ] Config.yaml has minimal (no-arch) and architecture-aware variants
- [ ] Config.yaml architecture-aware variant shows Fabric and AWS environment examples
- [ ] metadata.json skeleton includes `schema_hints` key (not just connections + dataflows)
- [ ] Environment overlay templates (`dev.yaml`, `prod.yaml`) have commented examples
- [ ] Runner script has polars, spark, and mixed engine variants
- [ ] Functions package structure includes `pyproject.toml`, `__init__.py`, `sources.py`

### 3. Manual Workflow Testing — Basic Scaffolding (No Architecture)

Ask the AI to scaffold a new project without an architecture document and verify:

| Test Case | Prompt | Verify |
|-----------|--------|--------|
| Minimal defaults | "Create a datacoolie project called my_project" | All files exist, config.yaml has correct project_name, engine defaults to polars |
| Polars engine | "Init project acme_etl with polars engine" | config.yaml: `engine: polars`, runner imports PolarsEngine |
| Spark engine | "Scaffold spark_proj with spark" | config.yaml: `engine: spark`, runner imports SparkEngine |
| Subdirectory mode | "Create proj in a subdirectory" | Files created under `proj/` directory |
| Root scaffold | "Init project at current directory" | Files created at `.` (no subdirectory) |

### 4. Manual Workflow Testing — Architecture Consumption

Create a mock `.datacoolie/architect/260531_architecture.md` with known values, then test init:

| Test Case | Architecture Setup | Prompt | Verify |
|-----------|-------------------|--------|--------|
| Single engine (polars) | Engine Strategy: all polars | "Init project acme_etl" | config.yaml: `engine: polars`, runner imports PolarsEngine only, no engine question asked |
| Mixed engines | Engine Strategy: polars for bronze, spark for gold | "Init project mixed_proj" | config.yaml: `engine: mixed`, runner has both engine imports with stage comments |
| Multi-environment | Environment Differences: dev/test/prod | "Init project cloud_proj" | Three environment overlay files, config.yaml has dev + test + prod blocks |
| Fabric platform | Platform: Fabric, Infrastructure: workspace names | "Init project fabric_proj" | config.yaml prod environment has `platform: fabric` with workspace stubs |
| AWS platform | Platform: AWS, Infrastructure: bucket/role | "Init project aws_proj" | config.yaml prod environment has `platform: aws` with bucket/role stubs |
| No architecture | No `.datacoolie/architect/` directory | "Init project basic_proj" | Skill asks for engine and platform interactively, uses defaults |
| Unapproved architecture | Architecture has `Status: Draft` | "Init project draft_proj" | Skill warns architecture not approved, proceeds after user confirmation |

### 5. Scaffold Output Validation

After any scaffold, verify:

- [ ] `metadata/metadata.json` is valid JSON with `connections`, `dataflows`, `schema_hints` arrays
- [ ] `metadata/environments/dev.yaml` exists with commented overlay examples
- [ ] `.datacoolie/config.yaml` has `project_name` and `engine` fields
- [ ] `scripts/run_local.py` has correct imports for the selected engine
- [ ] `functions/__init__.py` exists (can be empty)
- [ ] `functions/sources.py` has the `my_source` function stub with correct kwargs
- [ ] `functions/pyproject.toml` has correct `{project_name}-functions` name
- [ ] `.gitignore` includes `__pycache__/`, `.datacoolie/watermarks/`, `.env`
- [ ] `requirements.txt` lists `datacoolie`
- [ ] No credentials, passwords, or API keys appear in any generated file

### 6. Edge Cases

- [ ] Output directory already exists → AI handles gracefully (warns, continues)
- [ ] Architecture exists but is empty/malformed → falls back to interactive mode
- [ ] User says "mixed" engine but no architecture → AI asks which stages use which engine
- [ ] Project name has spaces or special chars → AI rejects or sanitizes

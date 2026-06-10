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
- [ ] Step 2 (Lazy Scaffold) describes variable substitution from architecture and user input
- [ ] Step 3 (Confirm) shows architecture-linked summary example
- [ ] Supported concrete engines listed: polars, spark
- [ ] Engine strategy handling described as env+stage matrix, with each value `polars` or `spark`
- [ ] Config.yaml is documented as workspace control file in AGENTS.md and the init project-structure template
- [ ] Input contracts reference architecture document (optional)
- [ ] Output contracts distinguish minimal bootstrap from on-demand artifacts
- [ ] Scaffold target flexibility documented (current directory vs subdirectory)
- [ ] Scope clarifies introspection belongs to discover, not init

### 2. Template Completeness

Check `datacoolie-init/templates/project-structure.md`:

- [ ] Variables table documents all substitution points with source and defaults
- [ ] Directory tree documents minimum bootstrap and potential on-demand layout separately
- [ ] Template states future phase folders and placeholder files must not be created during initial bootstrap
- [ ] Config.yaml has minimal (no-arch) and architecture-aware variants
- [ ] Config.yaml examples include only `schema_version`, `project.name`, `project.workspace_name`, and `environments`
- [ ] Config.yaml architecture-aware variant shows Fabric and AWS environment examples
- [ ] Modular metadata skeleton includes `connections.json`, `schema_hints.json`, and stage files under `dataflows/`
- [ ] Environment overlay templates (`dev.yaml`, `prod.yaml`) have commented examples
- [ ] Runner script has concrete polars and spark variants only; env+stage engine strategy is resolved before runner generation
- [ ] Functions package structure includes `pyproject.toml`, `__init__.py`, `sources.py`

### 3. Manual Workflow Testing — Basic Scaffolding (No Architecture)

Ask the AI to scaffold a new project without an architecture document and verify:

| Test Case | Prompt | Verify |
|-----------|--------|--------|
| Minimal defaults | "Create a datacoolie project called my_project" | Minimal bootstrap exists: AGENTS.md, config.yaml, project_management/status.md; no future phase folders are created |
| Polars runner requested | "Init project acme_etl with polars runner" | Runner is created only if runner generation is explicitly requested; config.yaml has no top-level `engine` |
| Spark runner requested | "Scaffold spark_proj with spark runner" | Runner imports SparkEngine only when runner generation is explicitly requested; config.yaml has no top-level `engine` |
| Chosen parent directory | "Create proj under ./work" | Workspace is created as `./work/{project_name}_dcws/` |
| Default parent directory | "Init project at current directory" | Workspace is created as `./{project_name}_dcws/` |

### 4. Manual Workflow Testing — Architecture Consumption

Create a mock `{project_name}_dcws/architecture/current.md` plus an approved architecture gate journal with known values, then test init:

| Test Case | Architecture Setup | Prompt | Verify |
|-----------|-------------------|--------|--------|
| Single engine (polars) | Engine Strategy: all polars | "Init project acme_etl with runner" | Runner imports PolarsEngine only, no engine question asked, config.yaml has no top-level `engine` |
| Env/stage engine strategy | Engine Strategy: dev all polars, prod silver/gold spark | "Init project staged_proj with prod silver2gold runner" | Generated runner is concrete for the selected target env/stage; config.yaml has no top-level `engine` |
| Multi-environment | Environment Differences: dev/test/prod | "Init project cloud_proj" | config.yaml has dev + test + prod blocks; overlay files are created only when overrides are needed |
| Fabric platform | Platform: Fabric, Infrastructure: workspace names | "Init project fabric_proj" | config.yaml prod environment has `platform: fabric`; workspace/lakehouse values are not written to config |
| AWS platform | Platform: AWS, Infrastructure: bucket/role | "Init project aws_proj" | config.yaml prod environment has `platform: aws`; bucket/role values are not written to config |
| No architecture | No `{project_name}_dcws/architecture/current.md` file | "Init project basic_proj" | Skill asks only for project name and uses `dev/local`; it does not ask for engine unless runner generation is requested |
| Unapproved architecture | Architecture gate journal missing or not `status: approved` | "Init project draft_proj" | Skill stops and requests architecture review before gated scaffolding |

### 5. Scaffold Output Validation

After any scaffold, verify:

- [ ] `{project_name}_dcws/AGENTS.md` exists and was not overwritten if pre-existing
- [ ] `{project_name}_dcws/config.yaml` has `schema_version: 1`, `project.name`, `project.workspace_name`, and `environments`, and no `defaults`, `artifacts`, or top-level `engine` field
- [ ] `{project_name}_dcws/config.yaml` contains no dataflow definitions, workflow defaults, artifact directory names, runtime paths, generated metadata paths, gate status, passwords, API keys, or secret values
- [ ] `{project_name}_dcws/project_management/status.md` exists with YAML frontmatter
- [ ] `{project_name}_dcws/metadata/`, `{project_name}_dcws/generated/`, `{project_name}_dcws/functions/`, `{project_name}_dcws/runners/`, and future phase folders are absent unless explicitly requested or needed
- [ ] When a phase is active, only that phase has `scope.md`, `notes.md`, `evidence.md`, and `gate-reviews/`
- [ ] Project-management Markdown files start with YAML frontmatter, including `notes.md` as an audit/handoff artifact
- [ ] If dependency/code scaffolding is requested, `.gitignore` includes `__pycache__/`, `{project_name}_dcws/watermarks/`, `.env`
- [ ] If dependency/code scaffolding is requested, `requirements.txt` lists `datacoolie`
- [ ] No credentials, passwords, or API keys appear in any generated file

### 6. Edge Cases

- [ ] Output directory already exists → AI handles gracefully (warns, continues)
- [ ] Architecture exists but is empty/malformed → falls back to interactive mode
- [ ] User asks to use both engines but no architecture → AI asks for env+stage engine strategy; it does not create a third engine value
- [ ] Project name has spaces or special chars → AI rejects or sanitizes

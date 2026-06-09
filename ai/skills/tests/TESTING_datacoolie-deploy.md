# datacoolie-deploy — Testing Guide

This skill is knowledge-based. The AI reads SKILL.md rules and reference examples, then runs platform CLI commands or generates files directly.

---

## What to Test

### 1. SKILL.md Content Validation

Open `datacoolie-deploy/SKILL.md` and verify:

- [ ] Step 0 reads upstream artifacts (`config.yaml`, metadata, optional architecture)
- [ ] Step 1 preflight checklist covers CLI, auth, metadata, secrets, infra, and packaging readiness
- [ ] Step 2 generates runner from reference examples
- [ ] Step 3 packages functions (wheel if `pyproject.toml`, zip fallback)
- [ ] Step 4 deploys via platform CLI
- [ ] Step 5 promotion includes explicit confirmation gate for prod
- [ ] Step 6 generates CI/CD workflows
- [ ] Configuration section uses `schema_version`, `project`, `defaults`, `artifacts`, and `environments`
- [ ] Configuration section states `config.yaml` follows the documented init project-structure template
- [ ] Input Contracts table exists and matches required inputs
- [ ] Output Contracts table exists and matches generated artifacts
- [ ] Script Status clarifies this skill is knowledge-based

### 2. Reference File Completeness

Check `datacoolie-deploy/references/`:

| File | Verify |
|------|--------|
| `run_local.py.example` | Local runner pattern and DataCoolieDriver usage |
| `run_aws_glue.py.example` | Glue runner pattern and Spark engine usage |
| `run_fabric.ipynb.example` | Notebook JSON structure, 4-cell pattern |
| `run_databricks.ipynb.example` | Notebook JSON structure and volume-root pattern |
| `github-actions-aws.yml.example` | AWS deploy pipeline structure |
| `github-actions-databricks.yml.example` | Databricks deploy pipeline structure |
| `github-actions-fabric.yml.example` | Fabric deploy pipeline structure |

- [ ] Python examples compile
- [ ] YAML examples parse
- [ ] Notebook JSON examples parse
- [ ] Placeholder values map to `{project_name}_dcws/config.yaml` structure

### 3. Manual Workflow Testing (Actionable)

Ask the AI to execute workflow prompts and verify outputs.

#### 3.1 Preflight

| Test Case | Setup | Expected |
|-----------|-------|----------|
| Local passes | Project with metadata | All checks pass |
| Missing CLI | No `aws` or `fab` or `databricks` installed | AI reports install guidance and stops |
| Missing metadata | Empty `{project_name}_dcws/metadata/` | AI stops and recommends running `datacoolie-metadata` |
| Missing secret env var | Metadata includes `secrets_ref` | AI lists missing variables and stops |
| No functions dir | Remove `{project_name}_dcws/functions/` | AI warns and continues |

#### 3.2 Runner Generation

| Platform | Prompt | Verify |
|----------|--------|--------|
| Local | Generate local runner | `{project_name}_dcws/generated/run_local.py` exists and is valid Python |
| AWS Glue | Generate AWS Glue runner for prod | `run_aws_glue.py` contains environment values from config |
| Fabric | Generate Fabric runner for staging | `run_fabric.ipynb` valid JSON and expected cells |
| Databricks | Generate Databricks runner | `run_databricks.ipynb` valid JSON and expected root path pattern |

#### 3.3 Functions Packaging

| Test Case | Verify |
|-----------|--------|
| `pyproject.toml` present | Wheel artifact generated |
| No `pyproject.toml`, has `__init__.py` | Zip artifact generated |
| No functions directory | Packaging skipped gracefully |

#### 3.4 CI/CD Generation

- [ ] Generate AWS workflow → valid YAML under `.github/workflows/`
- [ ] Generate Fabric workflow → valid YAML under `.github/workflows/`
- [ ] Generate Databricks workflow → valid YAML under `.github/workflows/`

---

## 4. Architecture Consumption (SKILL.md AI Workflow)

Prompt-level checks for Step 0 behavior.

### 4.1 Config + metadata present

- Setup: `{project_name}_dcws/config.yaml` and at least one metadata file exist
- Prompt: Deploy to prod
- Verify: AI reads existing config and metadata without asking for platform basics

### 4.2 Missing config.yaml

- Setup: metadata exists, `{project_name}_dcws/config.yaml` absent
- Prompt: Deploy
- Verify: AI asks for project name, workspace name, platform, and environment config interactively; it does not ask for top-level config engine

### 4.3 Missing metadata

- Setup: config exists, metadata directory empty
- Prompt: Deploy
- Verify: AI stops and recommends running `datacoolie-metadata`

### 4.4 Architecture mismatch

- Setup: architecture document platform conflicts with config platform
- Prompt: Deploy to prod
- Verify: AI warns about mismatch and requests confirmation

### 4.5 Missing functions directory

- Setup: config + metadata exist, `{project_name}_dcws/functions/` absent
- Prompt: Deploy to dev
- Verify: AI skips packaging and continues remaining deploy flow

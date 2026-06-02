# datacoolie-provision — Testing Guide

This skill is **knowledge-based** — the AI reads SKILL.md rules and Terraform reference examples, then generates platform CLI commands or `.tf` files directly. There are no scripts to execute.

---

## What to Test

### 1. SKILL.md Content Validation

Open `datacoolie-provision/SKILL.md` and verify:

- [ ] Step 0 reads architecture document (`.datacoolie/architect/*_architecture.md`)
- [ ] Step 0 extracts Infrastructure Requirements table → resource list
- [ ] Step 1 is conditional: arch consumed → ask env/mode only; no arch → ask everything
- [ ] Step 2 preflight checks: CLI mode → platform CLI; Terraform mode → terraform binary; Local → skip
- [ ] Step 3 generates CLI script OR Terraform files (parallel modes, not sequential)
- [ ] Dry-run is default — `--confirm` required for actual execution
- [ ] Security policy: no credentials in generated files, idempotent operations
- [ ] Provision log format documented (template reference)
- [ ] Env suffix rules: `_dev`/`_test` appended for non-prod, prod uses base name
- [ ] Input Contracts table present (architecture optional, user input required)
- [ ] Output Contracts table present (script, log, .tf files)
- [ ] All 4 platforms documented: Local, AWS, Fabric, Databricks

### 2. Reference File Completeness

Check `datacoolie-provision/references/`:

| File | Verify |
|------|--------|
| `aws.tf.example` | S3 buckets, Glue databases, IAM roles; uses variables for sensitive values |
| `fabric.tf.example` | Workspace, lakehouse resources; `workspace_id` variable |
| `databricks.tf.example` | Catalog, schema, volume resources; `databricks_host`/`databricks_token` variables (sensitive) |
| `platform-cli-reference.md` | CLI docs URLs for all 4 platforms + Terraform providers |

Check `datacoolie-provision/templates/`:

| File | Verify |
|------|--------|
| `provision-log.tpl.md` | Status table with resource name, type, status (created/failed/skipped), summary counts |

### 3. Manual Workflow Testing — Local Platform

Ask the AI to provision for local platform:

| Test Case | Prompt | Verify |
|-----------|--------|--------|
| Dry-run | "Provision local resources for dev" | AI shows what directories would be created, does not create |
| Confirm | "Provision local resources for dev, confirm" | Directories actually created |
| Idempotent | Run confirm twice | Second run reports "already exists", no errors |
| Provision log | After any run | `.datacoolie/provision/YYMMDD_provision-log.md` created with status table |

### 4. Manual Workflow Testing — Cloud Platforms (dry-run)

These tests verify the AI generates correct CLI commands without executing them:

| Platform | Prompt | Verify AI Output |
|----------|--------|-----------------|
| AWS | "Provision AWS resources, dry-run" | Shows `aws s3 mb`, `aws glue create-database` commands |
| Fabric | "Provision Fabric resources, dry-run" | Shows `fab lakehouse create` or equivalent commands |
| Databricks | "Provision Databricks resources, dry-run" | Shows `databricks` CLI commands |

### 5. Manual Workflow Testing — Terraform Mode

Ask the AI to generate Terraform files:

| Test Case | Prompt | Verify |
|-----------|--------|--------|
| AWS TF | "Generate Terraform for AWS" | `main.tf` has `aws_s3_bucket` resources, `variables.tf` has region/bucket vars |
| Fabric TF | "Generate Terraform for Fabric" | `variables.tf` has `workspace_id` |
| Databricks TF | "Generate Terraform for Databricks" | `variables.tf` has `databricks_host`, `databricks_token` (sensitive) |

---

## 6. Architecture Consumption (SKILL.md AI Workflow)

Prompt-level tests — verify AI behavior follows Step 0 logic.

### 6.1 Architecture present + approved

- **Setup**: `.datacoolie/architect/260531_architecture.md` with `Status: Approved` and Infrastructure Requirements table (3 resources: bronze lakehouse, silver lakehouse, gold warehouse)
- **Prompt**: "Provision resources for dev"
- **Verify**: AI pre-populates resource list from Infrastructure Requirements; does NOT ask for resource names/types; asks for environment confirmation and mode (CLI/Terraform)

### 6.2 No architecture — interactive fallback

- **Setup**: Empty or missing `.datacoolie/` directory
- **Prompt**: "Provision resources"
- **Verify**: AI asks for all details (resource names, types, platform, environment, mode)

### 6.3 Draft architecture — warning

- **Setup**: Architecture with `Status: Draft`
- **Prompt**: "Provision resources"
- **Verify**: AI warns architecture is not approved; asks for confirmation; if confirmed, extracts Infrastructure Requirements

### 6.4 Architecture has no Infrastructure Requirements table

- **Setup**: Architecture approved but Infrastructure Requirements section is missing or empty
- **Prompt**: "Provision resources"
- **Verify**: AI falls back to interactive mode, asks for resource details

### 6.5 Environment suffix applied correctly

- **Setup**: Architecture with resource name "My Project Bronze"
- **Prompt**: "Provision for dev"
- **Verify**: Generated script/TF uses "My Project Bronze Dev" (dev suffix applied per Environment Naming Convention)

---

## 7. Edge Cases

### 7.1 Missing CLI — AI detects and stops

- **Prompt**: "Provision Fabric resources" (with `fab` CLI not installed)
- **Verify**: AI detects missing CLI, shows install command, stops — does not generate script

### 7.2 Local platform — no CLI needed

- **Prompt**: "Provision local resources"
- **Verify**: AI skips CLI preflight check, generates mkdir commands directly

### 7.3 Idempotent re-run

- **Prompt**: Run "Provision local for dev, confirm" twice
- **Verify**: Second run reports resources "already exist", no errors, no duplicates

### 7.4 Preflight — Terraform mode

- **Prompt**: "Generate Terraform for AWS" (with `terraform` not installed)
- **Verify**: AI warns terraform not found, still generates `.tf` files but skips `terraform validate`

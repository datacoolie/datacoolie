---
name: datacoolie-provision
description: >
  Provision platform infrastructure from an approved architecture design.
  Creates lakehouses, warehouses, S3 buckets, databases, or local directories.
  Supports CLI mode (execute commands) and Terraform mode (generate .tf files).
  Use when user says "provision", "create resources", "setup infrastructure",
  "create lakehouse", "create bucket", "terraform", "infra", "provision resources",
  or after architecture approval.
---

# datacoolie-provision

Provision platform infrastructure based on the approved architecture design document.
Supports four platforms (Fabric, AWS, Databricks, Local) and two execution modes
(CLI direct execution, Terraform file generation).

## Scope

This skill handles: infrastructure resource creation, Terraform generation, provision logging,
idempotent resource checks, dry-run previews.

Does NOT handle: source discovery (use `datacoolie-discover`), architecture design
(use `datacoolie-architect`), metadata generation (use `datacoolie-metadata`),
code deployment (use `datacoolie-deploy`).

## Prerequisites

- Approved architecture at `.datacoolie/architect/yymmdd_architecture.md`
- If no architecture exists, prompt user to run `datacoolie-architect` first
- Platform CLI installed and authenticated (for CLI mode)

## Security Policy

- `--dry-run` is the **default** — never execute without explicit `--confirm`
- Never store credentials, tokens, or connection strings in generated files
- Never log secrets or access keys
- All operations must be idempotent (check-before-create)
- Generated Terraform uses variables for sensitive values

## Scripts

All scripts at `ai/skills/datacoolie-provision/scripts/`.

### provision.py — Main orchestrator

```bash
python scripts/provision.py --architecture <path> --platform {fabric|aws|databricks|local} --mode {cli|terraform} --env {dev|test|prod} [--confirm] [--output <dir>]
```

**Arguments:**
- `--architecture` — Path to approved architecture.md (default: latest in `.datacoolie/architect/`)
- `--platform` — Target platform
- `--mode` — `cli` (execute commands) or `terraform` (generate .tf files)
- `--env` — Target environment (affects naming, resource sizes)
- `--confirm` — Actually execute commands (CLI mode only). Without this, dry-run only.
- `--output` — Output directory (default: `.datacoolie/provision/`)

**Workflow:**
1. Parse architecture.md → extract Infrastructure Requirements table
2. Run preflight checks (CLI installed, authenticated)
3. Select provider module based on `--platform`
4. If `--mode cli`:
   - Generate commands from infra requirements
   - Check if each resource already exists (idempotent)
   - If `--confirm`: execute commands
   - Else: print commands (dry-run)
5. If `--mode terraform`:
   - Render Jinja2 templates with infra requirements
   - Write `.tf` files to output directory
6. Write provision log to `.datacoolie/provision/yymmdd_provision-log.md`

**Exit codes:**
- 0 = success (or dry-run completed)
- 1 = provisioning error (partial failure)
- 2 = preflight failure (CLI missing, auth expired)
- 3 = architecture file not found or unparseable

### Providers

Platform-specific modules at `scripts/providers/`:

#### fabric.py

| Resource Type | CLI Command | Exists Check |
|---|---|---|
| Lakehouse | `fab lakehouse create --display-name {name} --workspace {ws}` | `fab lakehouse list --workspace {ws}` |
| Warehouse | `fab warehouse create --display-name {name} --workspace {ws}` | `fab warehouse list --workspace {ws}` |
| Workspace | `fab workspace create --display-name {name}` | `fab workspace list` |

#### aws.py

| Resource Type | CLI Command | Exists Check |
|---|---|---|
| S3 Bucket | `aws s3 mb s3://{name}` | `aws s3api head-bucket --bucket {name}` |
| Glue Database | `aws glue create-database --database-input '{"Name":"{name}"}'` | `aws glue get-database --name {name}` |
| IAM Role | `aws iam create-role --role-name {name} --assume-role-policy-document file://policy.json` | `aws iam get-role --role-name {name}` |

#### databricks.py

| Resource Type | CLI Command | Exists Check |
|---|---|---|
| Catalog | `databricks unity-catalog create-catalog --name {name}` | `databricks unity-catalog get-catalog --name {name}` |
| Schema | `databricks unity-catalog create-schema --catalog {cat} --name {name}` | `databricks unity-catalog get-schema --full-name {cat}.{name}` |
| Volume | `databricks unity-catalog create-volume --catalog {cat} --schema {schema} --name {name} --volume-type MANAGED` | `databricks unity-catalog get-volume --full-name {cat}.{schema}.{name}` |

#### local.py

| Resource Type | Command | Exists Check |
|---|---|---|
| Directory | `mkdir -p {base_path}/{layer}/` | `os.path.exists()` |
| Metadata dir | `mkdir -p metadata/` | `os.path.exists()` |

### Terraform generator

```bash
python scripts/provision.py --platform aws --mode terraform --env prod
```

Generates `.tf` files using Jinja2 templates at `scripts/terraform/templates/`:
- `{platform}.tf.j2` → `main.tf`
- Always generates `variables.tf` for sensitive values
- Pins provider versions

## Platform-Resource Mapping

| Architecture Layer | Fabric | AWS | Databricks | Local |
|---|---|---|---|---|
| Bronze | Lakehouse | S3 bucket + Glue DB | Unity Volume + Schema | `./data/bronze/` |
| Silver | Lakehouse | S3 bucket + Glue DB | Unity Volume + Schema | `./data/silver/` |
| Gold | Warehouse | S3 bucket + Glue DB (or Redshift) | Unity Schema | `./data/gold/` |
| Secrets | Key Vault (external) | Secrets Manager | Secret Scope | `.env` file |

## Environment Naming Convention

Resources are named with environment suffix:
- Dev: `{name}_dev` or `{name}-dev`
- Test: `{name}_test` or `{name}-test`
- Prod: `{name}` (no suffix — production is the canonical name)

## Output

Provision log written to `.datacoolie/provision/yymmdd_provision-log.md` using
`templates/provision-log.tpl.md`.

## Cross-Skill Dependencies

| Direction | Skill | Interaction |
|---|---|---|
| Input from | `datacoolie-architect` | Reads Infrastructure Requirements from architecture.md |
| Used by | `datacoolie-deploy` | Preflight checks confirm provisioned resources exist |

## Error Handling

| Situation | Action |
|---|---|
| No architecture file found | Prompt user to run `datacoolie-architect` first |
| Platform CLI not installed | Show install command, exit 2 |
| Authentication expired | Show re-auth command, exit 2 |
| Resource already exists | Skip + log "already exists" (idempotent) |
| Insufficient permissions | Clear error + list required permissions |
| Partial failure (some resources created) | Log which succeeded/failed, allow re-run (idempotent) |
| Terraform template missing | Clear error + list available platforms |

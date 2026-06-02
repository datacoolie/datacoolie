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

Provision platform infrastructure. Supports four platforms (Fabric, AWS, Databricks, Local)
and two execution modes (CLI script generation, Terraform file generation).

## Scope

This skill handles: provision script generation, Terraform generation, provision logging,
idempotent resource checks, architecture-to-infrastructure translation (pre-populating resources from approved architecture).

Does NOT handle: source discovery, architecture design, metadata generation, code deployment.

## Prerequisites

- Approved architecture document at `.datacoolie/architect/*_architecture.md` (recommended — falls back to interactive)
- Platform CLI installed and authenticated (for CLI mode)
- Terraform installed (for Terraform mode)

## Security Policy

- `--dry-run` is the **default** — never execute without explicit `--confirm`
- Never store credentials, tokens, or connection strings in generated files
- Never log secrets or access keys
- All operations must be idempotent (check-before-create)
- Generated Terraform uses variables for sensitive values

## AI Workflow

### Step 0: Read Architecture (if available)

Check for an approved architecture document at `.datacoolie/architect/*_architecture.md`.

**If found and status is `Approved`**, extract from the Infrastructure Requirements table:

| Architecture Section | Extract | Maps To |
|---------------------|---------|---------|
| Infrastructure Requirements → Platform | Platform name | `--platform` argument |
| Infrastructure Requirements → Resource Type | Lakehouse, S3 bucket, schema, directory, etc. | Resource type list |
| Infrastructure Requirements → Name | Resource display names | Resource name list |
| Infrastructure Requirements → Purpose | Bronze/Silver/Gold/Secrets | Environment naming + layer mapping |
| Environment Differences table | Dev/Test/Prod config matrix | Environment suffix rules |

**If found but status is `Draft`** → warn user architecture is not approved, proceed after confirmation.

**If not found** → fall back to interactive mode (Step 1 asks everything).

### Step 1: Gather Requirements

**If architecture was consumed (Step 0)** — only ask for:
- Target environment (dev/test/prod) — if not clear from context
- Execution mode: CLI or Terraform
- Any corrections to architecture-derived resource list

**If no architecture** — ask for all details (current behavior):
- Resource names, types, platform, environment
- Execution mode preference

### Step 2: Detect OS and Preflight

Detect user's OS from terminal context → determines script format (`.sh` for Linux/macOS, `.ps1` for Windows).

**For CLI mode:**
- Check platform CLI is installed and authenticated
- If not → show install/auth guidance from [platform-cli-reference.md](./references/platform-cli-reference.md) and stop

**For Terraform mode:**
- Check `terraform` is installed
- If not → show install guidance and stop

**For Local platform:** always passes preflight (no CLI required).

### Step 3: Generate

**CLI mode** → Generate an executable script at `.datacoolie/provision/yymmdd_provision.sh` (or `.ps1`).
The script must:
- Check if each resource already exists before creating (idempotent)
- Print what it will do before doing it
- Exit on first error
- Never contain hardcoded secrets
- Default to `--dry-run` — never execute without explicit `--confirm`

If uncertain about current CLI syntax, read [platform-cli-reference.md](./references/platform-cli-reference.md) for documentation URLs.

**Terraform mode:**
1. Read reference examples from `references/*.tf.example` to understand naming conventions and resource patterns
2. Generate `.tf` files for the user's specific resources
3. Write to `.datacoolie/provision/`
4. Run `terraform validate` if available, otherwise AI reviews for correctness

### Step 4: User Reviews and Approves

Present generated script or `.tf` files. Do NOT execute until user explicitly approves.

### Step 5: Execute

**CLI mode** → Run approved script. Capture stdout/stderr.
**Terraform mode** → `terraform plan` then `terraform apply` after user confirms.

### Step 6: Log

Write provision log to `.datacoolie/provision/yymmdd_provision-log.md` using `templates/provision-log.tpl.md`.

If architecture was consumed, include reference in log:

> Architecture consumed: `.datacoolie/architect/yymmdd_architecture.md`
> - N resources pre-populated from Infrastructure Requirements
> - Environment: {env} (suffix: `_{env}`)

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

## Input Contracts

| Direction | Artifact | Path | Required |
|-----------|----------|------|----------|
| Input | Architecture document | `.datacoolie/architect/*_architecture.md` | No — falls back to interactive |
| Input | User input | Interactive | Yes (if no arch); environment + mode always |

## Output Contracts

| Artifact | Path |
|---|---|
| Provision script | `.datacoolie/provision/yymmdd_provision.sh` (or `.ps1`) |
| Provision log | `.datacoolie/provision/yymmdd_provision-log.md` |
| Terraform files | `.datacoolie/provision/*.tf` (Terraform mode only) |

## Error Handling

| Situation | Action |
|---|---|
| No resource requirements provided | Ask user what resources to provision |
| Platform CLI not installed | Show install command, exit 2 |
| Authentication expired | Show re-auth command, exit 2 |
| Resource already exists | Skip + log "already exists" (idempotent) |
| Insufficient permissions | Clear error + list required permissions |
| Partial failure (some resources created) | Log which succeeded/failed, allow re-run (idempotent) |
| Terraform template missing | Clear error + list available platforms |

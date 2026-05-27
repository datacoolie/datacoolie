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
idempotent resource checks.

Does NOT handle: source discovery, architecture design, metadata generation, code deployment.

## Prerequisites

- User provides resource requirements: resource names, types, platform, and environment
- Platform CLI installed and authenticated (for CLI mode)

## Security Policy

- `--dry-run` is the **default** — never execute without explicit `--confirm`
- Never store credentials, tokens, or connection strings in generated files
- Never log secrets or access keys
- All operations must be idempotent (check-before-create)
- Generated Terraform uses variables for sensitive values

## AI Workflow

### Step 1: Gather Resource Requirements

Collect from user: resource names, types (lakehouse, S3 bucket, schema, directory, etc.), target platform, and environment.

### Step 2: Detect OS and Preflight

Detect user's OS from terminal context → determines script format (`.sh` for Linux/macOS, `.ps1` for Windows).
Check platform CLI is installed and authenticated. If not → show install/auth guidance and stop.

### Step 3: Generate Provision Script

Generate an executable script at `.datacoolie/provision/yymmdd_provision.sh` (or `.ps1`).
The script must:
- Check if each resource already exists before creating (idempotent)
- Print what it will do before doing it
- Exit on first error
- Never contain hardcoded secrets

If uncertain about current CLI syntax, read [platform-cli-reference.md](./references/platform-cli-reference.md) for documentation URLs.

### Step 4: User Reviews and Approves

Present the generated script to user. Do NOT execute until user explicitly approves.

### Step 5: Execute

Run the approved script. Log results.

### Step 6: Terraform Alternative

If user requests Terraform mode:
1. Read reference examples from `references/*.tf.example` to understand naming conventions and resource patterns
2. Generate `.tf` files directly for the user's specific resources
3. Write to `.datacoolie/provision/`
4. Run `terraform validate` if available, otherwise AI reviews for correctness
5. User approves before `terraform apply`

### Step 7: Log

Write provision log to `.datacoolie/provision/yymmdd_provision-log.md` using `templates/provision-log.tpl.md`.

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

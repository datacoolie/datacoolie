# datacoolie-provision — Testing Guide

This skill is **knowledge-based** — the AI reads SKILL.md rules and Terraform templates, then runs platform CLI commands or generates `.tf` files directly. There are no scripts to execute.

---

## What to Test

### 1. SKILL.md Content Validation

Open `datacoolie-provision/SKILL.md` and verify:

- [ ] AI workflow steps are clear: read architecture → preflight → dry-run → confirm → log
- [ ] All platforms documented: Local, AWS, Fabric, Databricks
- [ ] CLI commands listed per platform (e.g., `aws s3 mb`, `fab lakehouse create`, `databricks workspace`)
- [ ] Dry-run is default — `--confirm` required for actual execution
- [ ] Security policy: no credentials in generated files, idempotent operations
- [ ] Preflight check logic: CLI installed → authenticated → architecture exists
- [ ] Provision log format documented
- [ ] Env suffix rules: `_dev`/`_test` appended for non-prod, prod uses base name

### 2. Template Completeness

Check `datacoolie-provision/templates/`:

| Template | Verify |
|----------|--------|
| `aws.tf.j2` | S3 buckets, Glue databases, IAM roles; uses variables for sensitive values |
| `fabric.tf.j2` | Workspace, lakehouse resources; `workspace_id` variable |
| `databricks.tf.j2` | Catalog, schema, volume resources; `databricks_host`/`databricks_token` variables (sensitive) |
| `provision-log.tpl.md` | Status table with resource name, type, status (created/failed/skipped) |

- [ ] All templates use Jinja2 placeholders correctly
- [ ] Terraform templates include `managed_by = "datacoolie"` tags
- [ ] Variables use `sensitive = true` for tokens/passwords

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

### 6. Preflight Checks

- [ ] Missing CLI → AI detects and shows install command, stops
- [ ] Missing architecture file → AI prompts to run `datacoolie-architect` first
- [ ] Local platform → always passes preflight (no CLI required)

## 5. Provision log

### 5.1 Log written and contains expected sections

```sh
python -c "
import sys, tempfile
from pathlib import Path
sys.path.insert(0, 'skills/datacoolie-provision/scripts')
from provision import write_provision_log

results = [
    {'resource': {'platform': 'Local', 'resource_type': 'Directory', 'name': 'bronze', 'purpose': 'Raw'},
     'name': 'bronze_dev', 'action': 'dry_run', 'status': 'pending',
     'command': ['mkdir', '-p', 'data/bronze_dev'], 'output': '[DRY RUN] Would execute.'},
]
with tempfile.TemporaryDirectory() as d:
    log = write_provision_log(results, Path('architecture.md'), 'local', 'cli', 'dev', Path(d))
    content = log.read_text()
    assert 'Dry Run' in content
    assert 'bronze_dev' in content
    assert 'Commands' in content
    print('OK: log structure correct')
    print('Sections:', [s for s in ['Resources', 'Commands', 'Summary', 'Errors'] if s in content])
"
```

### 5.2 Error section present when failures exist

```sh
python -c "
import sys, tempfile
from pathlib import Path
sys.path.insert(0, 'skills/datacoolie-provision/scripts')
from provision import write_provision_log

results = [
    {'resource': {'platform': 'AWS', 'resource_type': 'S3 Bucket', 'name': 'x', 'purpose': ''},
     'name': 'x_dev', 'action': 'create', 'status': 'failed',
     'command': ['aws', 's3', 'mb', 's3://x_dev'], 'output': 'Access Denied'},
]
with tempfile.TemporaryDirectory() as d:
    log = write_provision_log(results, Path('architecture.md'), 'aws', 'cli', 'dev', Path(d))
    content = log.read_text()
    assert 'Errors' in content
    assert 'Access Denied' in content
    print('OK: error section present')
"
```

---

## 6. Edge cases

### 6.1 Architecture file not found — exits 3

```sh
python skills/datacoolie-provision/scripts/provision.py \
  --architecture /tmp/does_not_exist.md \
  --platform local
# ERROR: Architecture file not found: /tmp/does_not_exist.md
# Exit 3
python -c "print('Exit code was:', $LASTEXITCODE)"
```

### 6.2 No Infrastructure Requirements table — exits 3

```sh
python -c "
content = '# Architecture\n\nNo table here.\n'
open('/tmp/no_table.md', 'w').write(content)
" 
python skills/datacoolie-provision/scripts/provision.py \
  --architecture /tmp/no_table.md \
  --platform local
# ERROR: No Infrastructure Requirements table found
# Exit 3
```

### 6.3 Already-existing resources are skipped

```sh
python -c "
import sys, tempfile; from pathlib import Path
sys.path.insert(0, 'skills/datacoolie-provision/scripts')
from provision import provision_cli, apply_env_suffix
from providers import local

with tempfile.TemporaryDirectory() as d:
    local.set_base_path(Path(d))
    Path(d, 'data', 'bronze_dev').mkdir(parents=True)
    resources = [{'platform': 'Local', 'resource_type': 'Directory', 'name': 'data/bronze', 'purpose': ''}]
    results = provision_cli(resources, 'local', 'dev', confirm=True)
    assert results[0]['status'] == 'already_exists'
    print('OK: existing directory skipped')
"
```

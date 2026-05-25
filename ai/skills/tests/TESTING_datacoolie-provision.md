# datacoolie-provision — Testing Guide

## Test Environment

No Docker required — local and dry-run modes work standalone.
For actual CLI provisioning tests, the relevant CLIs must be installed:
- Fabric: `pip install ms-fabric-cli`
- AWS: `pip install awscli`
- Databricks: `pip install databricks-cli`

All commands run from `datacoolie/` (workspace root) with venv activated.

---

## 1. provision.py — Local platform (no CLI required)

### 1.1 Dry-run local — default behaviour (no `--confirm`)

```sh
python skills/datacoolie-provision/scripts/provision.py \
  --architecture usecase-sim/metadata/architecture/local_architecture.md \
  --platform local \
  --env dev
# ✓ [DRY RUN] No resources created. Pass --confirm to apply.
# Provision log written: .datacoolie/provision/YYMMDD_provision-log.md
# Exit 0
```

### 1.2 Confirm local — actually creates directories

```sh
python skills/datacoolie-provision/scripts/provision.py \
  --architecture usecase-sim/metadata/architecture/local_architecture.md \
  --platform local \
  --env dev \
  --confirm
# ✓ 4/4 resources created
# Each directory should exist under the project root
# Exit 0
```

### 1.3 Env suffix applied — dev appends `_dev`

```sh
python -c "
from pathlib import Path; import sys
sys.path.insert(0, 'skills/datacoolie-provision/scripts')
from provision import apply_env_suffix
assert apply_env_suffix('bronze_lake', 'dev') == 'bronze_lake_dev'
assert apply_env_suffix('bronze_lake', 'prod') == 'bronze_lake'
print('OK: env suffix logic correct')
"
```

### 1.4 Custom output dir for log

```sh
python skills/datacoolie-provision/scripts/provision.py \
  --architecture usecase-sim/metadata/architecture/local_architecture.md \
  --platform local \
  --env test \
  --output /tmp/prov_test
# Log written to /tmp/prov_test/YYMMDD_provision-log.md
python -c "import os; files = os.listdir('/tmp/prov_test'); print(files)"
# Should list one file: YYMMDD_provision-log.md
```

---

## 2. provision.py — AWS (dry-run, no real AWS required)

### 2.1 Dry-run AWS — preflight check skipped in dry-run

```sh
python skills/datacoolie-provision/scripts/provision.py \
  --architecture usecase-sim/metadata/architecture/aws_architecture.md \
  --platform aws \
  --env dev
# May warn: aws CLI not found (expected if not installed)
# Shows list of commands that would be executed
# Exit 0
```

### 2.2 Parse AWS architecture — verify resource count

```sh
python -c "
from pathlib import Path; import sys
sys.path.insert(0, 'skills/datacoolie-provision/scripts')
from provision import parse_infra_requirements
resources = parse_infra_requirements(Path('usecase-sim/metadata/architecture/aws_architecture.md'))
print(f'Parsed {len(resources)} resources')
for r in resources:
    print(f'  {r[\"resource_type\"]:20} {r[\"name\"]}')
"
```

### 2.3 AWS provider commands — S3

```sh
python -c "
import sys; sys.path.insert(0, 'skills/datacoolie-provision/scripts')
from providers import aws
cmd = aws.create_command('S3 Bucket', 'my-bucket')
print(cmd)
# ['aws', 's3', 'mb', 's3://my-bucket']
"
```

---

## 3. provision.py — Terraform mode

### 3.1 Generate AWS Terraform

```sh
python skills/datacoolie-provision/scripts/provision.py \
  --architecture usecase-sim/metadata/architecture/aws_architecture.md \
  --platform aws \
  --mode terraform \
  --env dev \
  --output /tmp/tf_aws
# Generated: /tmp/tf_aws/main.tf
# Generated: /tmp/tf_aws/variables.tf
cat /tmp/tf_aws/main.tf
cat /tmp/tf_aws/variables.tf
python -c "import shutil; shutil.rmtree('/tmp/tf_aws')"
```

### 3.2 Generated main.tf contains expected resources

```sh
python -c "
content = open('/tmp/tf_aws/main.tf').read()
assert 'aws_s3_bucket' in content, 'Missing S3 resource'
assert 'managed_by' in content.lower() or 'ManagedBy' in content, 'Missing tag'
print('OK: main.tf structure correct')
"
```

### 3.3 Generate Fabric Terraform

```sh
python skills/datacoolie-provision/scripts/provision.py \
  --architecture usecase-sim/metadata/architecture/fabric_architecture.md \
  --platform fabric \
  --mode terraform \
  --env prod \
  --output /tmp/tf_fabric
cat /tmp/tf_fabric/variables.tf
# Should contain: workspace_id variable
python -c "import shutil; shutil.rmtree('/tmp/tf_fabric')"
```

### 3.4 Generate Databricks Terraform

```sh
python skills/datacoolie-provision/scripts/provision.py \
  --architecture usecase-sim/metadata/architecture/databricks_architecture.md \
  --platform databricks \
  --mode terraform \
  --env test \
  --output /tmp/tf_dbx
cat /tmp/tf_dbx/variables.tf
# Should contain: databricks_host, databricks_token (sensitive = true)
python -c "import shutil; shutil.rmtree('/tmp/tf_dbx')"
```

---

## 4. Preflight checks

### 4.1 Local always passes

```sh
python -c "
import sys; sys.path.insert(0, 'skills/datacoolie-provision/scripts')
from provision import check_preflight
ok, msg = check_preflight('local')
assert ok, f'Expected True, got: {msg}'
print(f'OK: {msg}')
"
```

### 4.2 Missing CLI returns False with install hint

```sh
python -c "
import sys; sys.path.insert(0, 'skills/datacoolie-provision/scripts')
from unittest.mock import patch
from provision import check_preflight
with patch('subprocess.run', side_effect=FileNotFoundError):
    ok, msg = check_preflight('aws')
assert not ok
assert 'not found' in msg.lower()
print(f'OK: {msg}')
"
```

---

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

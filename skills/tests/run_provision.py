"""
datacoolie-provision — Integration test runner.
Exercises provision.py in dry-run and local modes without requiring external CLIs.

Refer to TESTING_datacoolie-provision.md for manual test steps.
"""
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).parent
SCRIPT_DIR = HERE.parent / "datacoolie-provision" / "scripts"
RESULTS = HERE / "test-results" / "provision"
RESULTS.mkdir(parents=True, exist_ok=True)

# Architecture fixtures — generated inline if usecase-sim variants don't exist
ARCH_LOCAL = """\
# Architecture Design — TestProject

## Infrastructure Requirements

| Platform | Resource Type | Name | Purpose |
|---|---|---|---|
| Local | Directory | data/bronze | Raw data landing |
| Local | Directory | data/silver | Cleansed data |
| Local | Directory | data/gold | Serving layer |
| Local | Directory | metadata | Metadata files |
"""

ARCH_AWS = """\
# Architecture Design — AWSProject

## Infrastructure Requirements

| Platform | Resource Type | Name | Purpose |
|---|---|---|---|
| AWS | S3 Bucket | bronze-data | Raw data landing |
| AWS | S3 Bucket | silver-data | Cleansed data |
| AWS | Glue Database | gold_db | Serving layer catalog |
"""

ARCH_FABRIC = """\
# Architecture Design — FabricProject

## Infrastructure Requirements

| Platform | Resource Type | Name | Purpose |
|---|---|---|---|
| Fabric | Lakehouse | bronze_lake | Raw data |
| Fabric | Lakehouse | silver_lake | Cleansed data |
| Fabric | Warehouse | gold_wh | Serving layer |
"""

ARCH_DATABRICKS = """\
# Architecture Design — DatabricksProject

## Infrastructure Requirements

| Platform | Resource Type | Name | Purpose |
|---|---|---|---|
| Databricks | Catalog | main | Unity Catalog root |
| Databricks | Schema | bronze | Raw data schema |
| Databricks | Volume | raw_files | Landing volume |
"""


def _write_arch(tmp_dir: Path, content: str, name: str = "architecture.md") -> Path:
    path = tmp_dir / name
    path.write_text(content, encoding="utf-8")
    return path


def run() -> None:
    summary: list[tuple[str, str]] = []

    def check(name: str, result: subprocess.CompletedProcess, expected_rc: int = 0) -> str:
        status = "✓" if result.returncode == expected_rc else "✗"
        output = (result.stdout + result.stderr).strip()[:200]
        print(f"  {status} {name}")
        if result.returncode != expected_rc:
            print(f"    (expected rc={expected_rc}, got {result.returncode})")
            if output:
                print(f"    {output}")
        summary.append((name, status))
        return status

    # ------------------------------------------------------------------ #
    # 1. Local dry-run (no --confirm)
    # ------------------------------------------------------------------ #
    print("\n[provision] 1. Local dry-run")
    with tempfile.TemporaryDirectory() as tmpdir:
        arch = _write_arch(Path(tmpdir), ARCH_LOCAL)
        result = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "provision.py"),
             "--architecture", str(arch),
             "--platform", "local",
             "--env", "dev",
             "--output", tmpdir],
            capture_output=True, text=True,
        )
        check("local-dry-run", result)
        # Log file should be created
        logs = list(Path(tmpdir).glob("*_provision-log.md"))
        status = "✓" if logs else "✗"
        print(f"  {status} local-dry-run-log-written")
        summary.append(("local-dry-run-log-written", status))

    # ------------------------------------------------------------------ #
    # 2. Local --confirm (actually creates directories)
    # ------------------------------------------------------------------ #
    print("\n[provision] 2. Local --confirm")
    with tempfile.TemporaryDirectory() as tmpdir:
        arch = _write_arch(Path(tmpdir), ARCH_LOCAL)
        result = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "provision.py"),
             "--architecture", str(arch),
             "--platform", "local",
             "--env", "dev",
             "--confirm",
             "--output", tmpdir],
            capture_output=True, text=True,
            cwd=tmpdir,  # local dirs created relative to cwd
        )
        check("local-confirm", result)
        # Verify directories were created (with _dev suffix)
        created = all(
            Path(tmpdir, name).exists()
            for name in ["data/bronze_dev", "data/silver_dev", "data/gold_dev", "metadata_dev"]
        )
        status = "✓" if created else "✗"
        print(f"  {status} local-confirm-dirs-created")
        summary.append(("local-confirm-dirs-created", status))

    # ------------------------------------------------------------------ #
    # 3. Local prod (no suffix)
    # ------------------------------------------------------------------ #
    print("\n[provision] 3. Local prod — no suffix")
    with tempfile.TemporaryDirectory() as tmpdir:
        arch = _write_arch(Path(tmpdir), ARCH_LOCAL)
        result = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "provision.py"),
             "--architecture", str(arch),
             "--platform", "local",
             "--env", "prod",
             "--confirm",
             "--output", tmpdir],
            capture_output=True, text=True,
            cwd=tmpdir,  # local dirs created relative to cwd
        )
        check("local-prod-no-suffix", result)
        created = (Path(tmpdir, "data/bronze").exists() and
                   not Path(tmpdir, "data/bronze_prod").exists())
        status = "✓" if created else "✗"
        print(f"  {status} local-prod-no-env-suffix")
        summary.append(("local-prod-no-env-suffix", status))

    # ------------------------------------------------------------------ #
    # 4. AWS dry-run (no AWS CLI required — dry-run skips actual calls)
    # ------------------------------------------------------------------ #
    print("\n[provision] 4. AWS dry-run (no CLI needed)")
    with tempfile.TemporaryDirectory() as tmpdir:
        arch = _write_arch(Path(tmpdir), ARCH_AWS)
        result = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "provision.py"),
             "--architecture", str(arch),
             "--platform", "aws",
             "--env", "dev",
             "--output", tmpdir],
            capture_output=True, text=True,
        )
        # Dry-run should exit 0 even if aws CLI not installed
        check("aws-dry-run", result)

    # ------------------------------------------------------------------ #
    # 5. Terraform — AWS
    # ------------------------------------------------------------------ #
    print("\n[provision] 5. Terraform mode — AWS")
    with tempfile.TemporaryDirectory() as tmpdir:
        arch = _write_arch(Path(tmpdir), ARCH_AWS)
        result = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "provision.py"),
             "--architecture", str(arch),
             "--platform", "aws",
             "--mode", "terraform",
             "--env", "dev",
             "--output", tmpdir],
            capture_output=True, text=True,
        )
        check("terraform-aws-generate", result)
        tf_dir = Path(tmpdir) / "terraform"
        main_tf = tf_dir / "main.tf"
        variables_tf = tf_dir / "variables.tf"
        status = "✓" if main_tf.exists() and variables_tf.exists() else "✗"
        print(f"  {status} terraform-aws-files-exist")
        summary.append(("terraform-aws-files-exist", status))
        if main_tf.exists():
            content = main_tf.read_text()
            has_s3 = "aws_s3_bucket" in content
            has_glue = "aws_glue_catalog_database" in content
            status2 = "✓" if has_s3 and has_glue else "✗"
            print(f"  {status2} terraform-aws-resource-types (s3={has_s3}, glue={has_glue})")
            summary.append(("terraform-aws-resource-types", status2))

    # ------------------------------------------------------------------ #
    # 6. Terraform — Fabric
    # ------------------------------------------------------------------ #
    print("\n[provision] 6. Terraform mode — Fabric")
    with tempfile.TemporaryDirectory() as tmpdir:
        arch = _write_arch(Path(tmpdir), ARCH_FABRIC)
        result = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "provision.py"),
             "--architecture", str(arch),
             "--platform", "fabric",
             "--mode", "terraform",
             "--env", "prod",
             "--output", tmpdir],
            capture_output=True, text=True,
        )
        check("terraform-fabric-generate", result)
        tf_dir = Path(tmpdir) / "terraform"
        if (tf_dir / "variables.tf").exists():
            vars_content = (tf_dir / "variables.tf").read_text()
            status = "✓" if "workspace_id" in vars_content else "✗"
            print(f"  {status} terraform-fabric-workspace-var")
            summary.append(("terraform-fabric-workspace-var", status))

    # ------------------------------------------------------------------ #
    # 7. Terraform — Databricks
    # ------------------------------------------------------------------ #
    print("\n[provision] 7. Terraform mode — Databricks")
    with tempfile.TemporaryDirectory() as tmpdir:
        arch = _write_arch(Path(tmpdir), ARCH_DATABRICKS)
        result = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "provision.py"),
             "--architecture", str(arch),
             "--platform", "databricks",
             "--mode", "terraform",
             "--env", "test",
             "--output", tmpdir],
            capture_output=True, text=True,
        )
        check("terraform-databricks-generate", result)
        tf_dir = Path(tmpdir) / "terraform"
        if (tf_dir / "variables.tf").exists():
            vars_content = (tf_dir / "variables.tf").read_text()
            has_host = "databricks_host" in vars_content
            has_token = "sensitive" in vars_content
            status = "✓" if has_host and has_token else "✗"
            print(f"  {status} terraform-databricks-vars (host={has_host}, sensitive={has_token})")
            summary.append(("terraform-databricks-vars", status))

    # ------------------------------------------------------------------ #
    # 8. Architecture file not found — exit 3
    # ------------------------------------------------------------------ #
    print("\n[provision] 8. Missing architecture file")
    result = subprocess.run(
        [sys.executable, str(SCRIPT_DIR / "provision.py"),
         "--architecture", "/tmp/does_not_exist_abc123.md",
         "--platform", "local"],
        capture_output=True, text=True,
    )
    check("missing-arch-file-exits-3", result, expected_rc=3)

    # ------------------------------------------------------------------ #
    # 9. No Infrastructure Requirements table — exit 3
    # ------------------------------------------------------------------ #
    print("\n[provision] 9. Architecture with no infra table")
    with tempfile.TemporaryDirectory() as tmpdir:
        arch = _write_arch(Path(tmpdir), "# Architecture\n\nNo table here.\n")
        result = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "provision.py"),
             "--architecture", str(arch),
             "--platform", "local"],
            capture_output=True, text=True,
        )
        check("no-infra-table-exits-3", result, expected_rc=3)

    # ------------------------------------------------------------------ #
    # Summary
    # ------------------------------------------------------------------ #
    print(f"\n{'='*60}")
    print("  PROVISION SUMMARY")
    print(f"{'='*60}")
    failed = sum(1 for _, s in summary if s == "✗")
    for name, status in summary:
        print(f"  {status} {name}")
    if failed:
        print(f"\n  {failed} check(s) failed")
        sys.exit(1)


if __name__ == "__main__":
    run()

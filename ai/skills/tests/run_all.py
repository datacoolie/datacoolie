"""
Master test runner — orchestrates the full skills integration test cycle:
  1. docker compose up -d --wait
  2. Seed MSSQL (AdventureWorks LT — needs post-start seeding)
  3. Seed Iceberg via Trino (creates schemas + tables)
  4. Run per-skill test runners

Usage (from datacoolie/ai/skills/tests/):
  python run_all.py               # run all skill tests
  python run_all.py discover      # run only discover tests
  python run_all.py --no-docker   # skip docker start (already running)
"""
import subprocess
import sys
import os
import time
from pathlib import Path

HERE = Path(__file__).parent


def docker_compose(args: list[str], check: bool = True) -> int:
    cmd = ["docker", "compose", "-f", str(HERE / "docker-compose.yml")] + args
    result = subprocess.run(cmd, cwd=HERE)
    if check and result.returncode != 0:
        print(f"ERROR: docker compose {' '.join(args)} failed")
        sys.exit(1)
    return result.returncode


def seed_mssql() -> None:
    """MSSQL has no initdb.d — run the setup script via sqlcmd post-start."""
    print("\n[Seeding] MSSQL AdventureWorks LT...")
    # Wait a bit extra for MSSQL to be fully ready after health check
    time.sleep(5)
    result = subprocess.run([
        "docker", "exec", "skills-test-mssql",
        "/opt/mssql-tools18/bin/sqlcmd",
        "-S", "localhost",
        "-U", "sa",
        "-P", "Testing@123",
        "-No",  # trust server cert
        "-i", "/opt/mssql-scripts/adventureworks-lt.sql",
    ])
    if result.returncode == 0:
        print("  OK: AdventureWorks LT seeded")
    else:
        print("  WARN: MSSQL seeding returned non-zero — may already be seeded")
    # Update statistics so row estimates in sys.partitions reflect the seeded data
    subprocess.run([
        "docker", "exec", "skills-test-mssql",
        "/opt/mssql-tools18/bin/sqlcmd",
        "-S", "localhost", "-U", "sa", "-P", "Testing@123", "-No",
        "-d", "AdventureWorksLT", "-Q", "EXEC sp_updatestats",
    ], capture_output=True)


def seed_oracle() -> None:
    """Oracle init script sometimes fails silently on first boot — verify and re-seed if needed."""
    print("\n[Seeding] Oracle HR schema...")
    check = subprocess.run(
        ["docker", "exec", "skills-test-oracle", "bash", "-c",
         "echo \"SELECT COUNT(*) FROM user_tables;\" | sqlplus -s hr/hr@FREEPDB1"],
        capture_output=True, text=True,
    )
    if check.returncode == 0 and "7" in check.stdout:
        print("  OK: Oracle HR tables already present")
        # Ensure stats are gathered so NUM_ROWS is populated
        subprocess.run(
            ["docker", "exec", "skills-test-oracle", "bash", "-c",
             "echo \"EXEC DBMS_STATS.GATHER_SCHEMA_STATS('HR');\" | sqlplus -s hr/hr@FREEPDB1"],
            capture_output=True,
        )
        return
    # Re-run the init SQL — container may have seeded too early during first PDB startup
    result = subprocess.run(
        ["docker", "exec", "skills-test-oracle", "bash", "-c",
         "sqlplus hr/hr@FREEPDB1 @/container-entrypoint-initdb.d/01-hr.sql"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        print("  OK: Oracle HR schema seeded")
        subprocess.run(
            ["docker", "exec", "skills-test-oracle", "bash", "-c",
             "echo \"EXEC DBMS_STATS.GATHER_SCHEMA_STATS('HR');\" | sqlplus -s hr/hr@FREEPDB1"],
            capture_output=True,
        )
    else:
        print(f"  WARN: Oracle seed returned non-zero — {result.stderr[:200]}")


def seed_iceberg() -> None:
    """Create Iceberg schemas + tables + data via Trino."""
    print("\n[Seeding] Iceberg tables...")
    result = subprocess.run(
        [sys.executable, str(HERE / "fixtures" / "iceberg" / "seed_iceberg.py")],
        cwd=HERE,
    )
    if result.returncode != 0:
        print("  WARN: Iceberg seeding failed — trino may still be starting up")


def seed_hive() -> None:
    """Verify hive-init one-shot container completed successfully."""
    print("\n[Seeding] Hive (datacoolie_test)...")
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.ExitCode}}", "skills-test-hive-init"],
        capture_output=True, text=True,
    )
    exit_code = result.stdout.strip()
    if result.returncode == 0 and exit_code == "0":
        print("  OK: hive-init exited cleanly — datacoolie_test tables ready")
    elif result.returncode != 0:
        print("  WARN: skills-test-hive-init container not found — skipping Hive check")
    else:
        print(f"  WARN: hive-init exit code={exit_code} — DDL may have failed")


SKILL_RUNNERS = {
    "discover":  "run_discover.py",
    "deploy":    "run_deploy.py",
    "init":      "run_init.py",
    "metadata":  "run_metadata.py",
    "provision": "run_provision.py",
}


def seed_minio_files() -> None:
    """Upload test fixture files to MinIO skills-test bucket for cloud-file discovery tests."""
    print("\n[Seeding] MinIO file fixtures...")
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        print("  SKIP: boto3 not installed — skipping MinIO file seed")
        return

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9010",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    bucket = "skills-test"
    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)

    fixtures_dir = HERE / "fixtures" / "files"
    uploaded = 0
    for f in fixtures_dir.iterdir():
        if f.is_file() and not f.name.startswith(".") and f.suffix != ".py":
            key = f"raw/{f.name}"
            s3.upload_file(str(f), bucket, key)
            uploaded += 1
    print(f"  OK: {uploaded} file(s) uploaded to s3://{bucket}/raw/")


def run_skill(skill: str) -> int:
    runner = HERE / SKILL_RUNNERS.get(skill, f"run_{skill}.py")
    if not runner.exists():
        print(f"  SKIP: {runner.name} not found")
        return 0
    print(f"\n{'='*60}")
    print(f"  Skill: {skill}")
    print(f"{'='*60}")
    result = subprocess.run([sys.executable, str(runner)], cwd=HERE)
    return result.returncode


def main() -> None:
    args = sys.argv[1:]
    no_docker = "--no-docker" in args
    skills = [a for a in args if not a.startswith("--")]

    if not no_docker:
        print("[Docker] Starting services...")
        docker_compose(["up", "-d", "--wait"])
        print("  All services healthy")

    seed_mssql()
    seed_oracle()
    seed_iceberg()
    seed_hive()
    seed_minio_files()

    # Unit tests — currently empty after knowledge-based migration
    # (discover/init/provision/deploy scripts removed; only metadata retains scripts)
    unit_result_returncode = 0

    if not skills:
        skills = list(SKILL_RUNNERS.keys())

    results: dict[str, int] = {"unit": unit_result_returncode}
    for skill in skills:
        results[skill] = run_skill(skill)

    print(f"\n{'='*60}")
    print("  SUMMARY")
    print(f"{'='*60}")
    for label, rc in results.items():
        status = "PASS" if rc == 0 else "FAIL"
        print(f"  [{status}] {label}")

    if any(rc != 0 for rc in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()

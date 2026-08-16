"""
datacoolie-discover — Knowledge-based skill validation + script tests.
Validates SKILL.md sections and runs introspection scripts against local fixtures.

Usage (from datacoolie/ai/skills/tests/):
  python run_discover.py             # SKILL.md validation + script tests
  python run_discover.py --docker    # Also run against Docker databases
"""
import csv
import io
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
SKILL_DIR = HERE.parent / "datacoolie-discover"
SKILL_MD = SKILL_DIR / "SKILL.md"
SCRIPTS_DIR = SKILL_DIR / "scripts"
FIXTURES = HERE / "fixtures"
sys.path.insert(0, str(SCRIPTS_DIR))

from _observation_contract import CSV_HEADER, validate_observations  # noqa: E402

REQUIRED_SECTIONS = [
    "# DataCoolie Discover",
    "## Outcome And Boundary",
    "## Probe Contract",
    "## Resource Routing",
    "## Workflow",
    "## Output And Handoff",
    "observations.csv",
    "assess_watermarks.py",
    "object-summary.json",
    "finalize_watermark_assessment.py",
]

EXPECTED_HEADER = CSV_HEADER

REMOVED_OBSERVATION_FIELDS = {
    "observed_at", "evidence_class", "declared_key", "declared_reference",
}


def _routed_resources() -> list[Path]:
    resources = []
    resources.extend((SKILL_DIR / "references").glob("*.md"))
    resources.extend((SKILL_DIR / "templates").glob("*"))
    resources.extend(
        path for path in (SKILL_DIR / "scripts").glob("*.py")
        if not path.name.startswith("_")
    )
    resources.extend((SKILL_DIR / "scripts").glob("requirements-*.txt"))
    return sorted(path for path in resources if path.is_file())


def _run_script(args: list[str], desc: str) -> tuple[str, bool]:
    """Run a script and return (output, success)."""
    try:
        result = subprocess.run(
            [sys.executable] + args,
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return f"EXIT {result.returncode}: {result.stderr.strip()}", False
        return result.stdout, True
    except Exception as exc:
        return str(exc), False


def _validate_csv(output: str, expected_source: str, min_rows: int = 1) -> tuple[str, bool]:
    """Validate CSV output matches the shared observation contract."""
    reader = csv.DictReader(io.StringIO(output))
    if reader.fieldnames is None:
        return "No output", False
    if reader.fieldnames != EXPECTED_HEADER:
        return "Header does not match the observation contract", False
    try:
        data_rows = validate_observations(reader)
    except ValueError as exc:
        return f"Invalid observation: {exc}", False
    if len(data_rows) < min_rows:
        return f"Only {len(data_rows)} data rows, expected >= {min_rows}", False
    bad = [r for r in data_rows if r["source"] != expected_source]
    if bad:
        return f"Source mismatch: expected '{expected_source}', got '{bad[0]['source']}'", False
    return f"{len(data_rows)} rows, {len(EXPECTED_HEADER)} cols", True


def run(filter_names: list[str] | None = None) -> None:
    docker_mode = filter_names and "--docker" in filter_names

    print(f"\n{'='*60}")
    print("  datacoolie-discover — Validation")
    print(f"{'='*60}")

    summary: list[tuple[str, str]] = []

    # --- Section 1: SKILL.md validation ---
    print("\n  [SKILL.md Sections]")
    if not SKILL_MD.exists():
        print(f"  ✗ SKILL.md not found at {SKILL_MD}")
        sys.exit(1)

    content = SKILL_MD.read_text(encoding="utf-8")
    for section in REQUIRED_SECTIONS:
        found = section in content
        status = "✓" if found else "✗"
        print(f"  {status} {section}")
        summary.append((section, status))
    line_ok = len(content.splitlines()) <= 180
    status = "✓" if line_ok else "✗"
    print(f"  {status} line-budget")
    summary.append(("line-budget", status))
    routed_content = content + "\n" + "\n".join(
        path.read_text(encoding="utf-8")
        for path in (SKILL_DIR / "references").glob("*.md")
    )
    for path in _routed_resources():
        relative_path = path.relative_to(SKILL_DIR).as_posix()
        found = relative_path in routed_content
        status = "✓" if found else "✗"
        print(f"  {status} resource:{relative_path}")
        summary.append((f"resource:{relative_path}", status))
    evals_exist = (SKILL_DIR / "evals/evals.json").is_file()
    status = "✓" if evals_exist else "✗"
    print(f"  {status} evals/evals.json")
    summary.append(("evals/evals.json", status))
    active_contract_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in [SKILL_MD, *(SKILL_DIR / "references").glob("*.md"),
                     *(SKILL_DIR / "templates").glob("*")]
        if path.is_file()
    )
    stale_fields = sorted(
        field for field in REMOVED_OBSERVATION_FIELDS if field in active_contract_text
    )
    status = "✗" if stale_fields else "✓"
    print(f"  {status} no-stale-observation-fields: {stale_fields or 'none'}")
    summary.append(("no-stale-observation-fields", status))
    stale_inventory = any((SKILL_DIR / "templates").glob("schema-inventory.*"))
    status = "✗" if stale_inventory else "✓"
    print(f"  {status} no-duplicate-schema-inventory")
    summary.append(("no-duplicate-schema-inventory", status))

    # --- Section 2: Script smoke tests ---
    print("\n  [Script: introspect_files.py — Parquet]")
    parquet = str(FIXTURES / "files" / "sales.parquet")
    out, ok = _run_script(
        [str(SCRIPTS_DIR / "introspect_files.py"), "schema",
         "--path", parquet, "--source", "test", "--table", "sales"],
        "parquet schema",
    )
    if ok:
        msg, ok = _validate_csv(out, "test")
    else:
        msg = out
    status = "✓" if ok else "✗"
    print(f"  {status} parquet: {msg}")
    summary.append(("parquet-schema", status))

    print("\n  [Script: introspect_files.py — CSV]")
    csv_file = str(FIXTURES / "files" / "products.csv")
    out, ok = _run_script(
        [str(SCRIPTS_DIR / "introspect_files.py"), "schema",
         "--path", csv_file, "--source", "test", "--table", "products"],
        "csv schema",
    )
    if ok:
        msg, ok = _validate_csv(out, "test")
    else:
        msg = out
    status = "✓" if ok else "✗"
    print(f"  {status} csv: {msg}")
    summary.append(("csv-schema", status))

    print("\n  [Script: introspect_files.py — Delta]")
    delta = str(FIXTURES / "files" / "delta_products")
    out, ok = _run_script(
        [str(SCRIPTS_DIR / "introspect_files.py"), "schema",
         "--path", delta, "--format", "delta", "--source", "test", "--table", "delta"],
        "delta schema",
    )
    if ok:
        msg, ok = _validate_csv(out, "test")
    else:
        msg = out
    status = "✓" if ok else "✗"
    print(f"  {status} delta: {msg}")
    summary.append(("delta-schema", status))

    print("\n  [Script: introspect_files.py — Structure]")
    out, ok = _run_script(
        [str(SCRIPTS_DIR / "introspect_files.py"), "structure",
         "--path", str(FIXTURES / "files"), "--source", "test"],
        "structure",
    )
    ok2 = ok and "# Folder Structure" in out and "## Summary" in out
    status = "✓" if ok2 else "✗"
    print(f"  {status} structure report")
    summary.append(("structure-report", status))

    print("\n  [Script: introspect_api.py — OpenAPI]")
    spec = str(FIXTURES / "api" / "openapi-petstore.json")
    out, ok = _run_script(
        [str(SCRIPTS_DIR / "introspect_api.py"), "--spec", spec, "--source", "petstore"],
        "openapi",
    )
    if ok:
        msg, ok = _validate_csv(out, "petstore", min_rows=5)
    else:
        msg = out
    status = "✓" if ok else "✗"
    print(f"  {status} openapi: {msg}")
    summary.append(("openapi-schema", status))

    # --- Section 3: Docker-based tests (optional) ---
    if docker_mode:
        print("\n  [Script: introspect_db.py — Docker databases]")
        db_tests = [
            ("postgres", "DATACOOLIE_TEST_POSTGRES_URL"),
            ("mysql", "DATACOOLIE_TEST_MYSQL_URL"),
            ("mssql", "DATACOOLIE_TEST_MSSQL_URL"),
        ]
        for name, env_name in db_tests:
            if not os.environ.get(env_name):
                print(f"  ✗ {name}: required environment variable {env_name} is missing")
                summary.append((f"db-{name}", "✗"))
                continue
            out, ok = _run_script(
                [str(SCRIPTS_DIR / "introspect_db.py"), "--url-env", env_name, "--source", name],
                f"db-{name}",
            )
            if ok:
                msg, ok = _validate_csv(out, name, min_rows=5)
            else:
                msg = out
            status = "✓" if ok else "✗"
            print(f"  {status} {name}: {msg}")
            summary.append((f"db-{name}", status))

        # Iceberg REST catalog (via Docker)
        print("\n  [Script: introspect_lakehouse.py — Iceberg REST]")
        out, ok = _run_script(
            [str(SCRIPTS_DIR / "introspect_lakehouse.py"),
             "--iceberg", "http://localhost:8182",
             "--source", "iceberg-test"],
            "lakehouse-iceberg",
        )
        if ok:
            msg, ok = _validate_csv(out, "iceberg-test", min_rows=3)
        else:
            msg = out
        status = "✓" if ok else "✗"
        print(f"  {status} iceberg: {msg}")
        summary.append(("lakehouse-iceberg", status))

    # --- Summary ---
    print(f"\n{'='*60}")
    print("  DISCOVER SUMMARY")
    print(f"{'='*60}")
    failed = sum(1 for _, s in summary if s == "✗")
    passed = sum(1 for _, s in summary if s == "✓")
    for name, status in summary:
        print(f"  {status} {name}")
    print(f"\n  {passed}/{len(summary)} checks passed")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    filter_names = sys.argv[1:] if len(sys.argv) > 1 else None
    run(filter_names)

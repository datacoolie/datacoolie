"""
datacoolie-discover — Knowledge-based skill validation + script tests.
Validates SKILL.md sections and runs introspection scripts against local fixtures.

Usage (from datacoolie/ai/skills/tests/):
  python run_discover.py             # SKILL.md validation + script tests
  python run_discover.py --docker    # Also run against Docker databases
"""
import csv
import io
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
SKILL_DIR = HERE.parent / "datacoolie-discover"
SKILL_MD = SKILL_DIR / "SKILL.md"
SCRIPTS_DIR = SKILL_DIR / "scripts"
FIXTURES = HERE / "fixtures"

REQUIRED_SECTIONS = [
    "# datacoolie-discover",
    "## Scope",
    "## Multi-Source Model",
    "## Desired Output",
    "## Retrieval Methods",
    "## Security Policy",
    "## Output Contracts",
    "## Dependencies",
]


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
    """Validate CSV output matches the 14-column contract."""
    reader = csv.reader(io.StringIO(output))
    rows = list(reader)
    if not rows:
        return "No output", False
    if len(rows[0]) != 14:
        return f"Header has {len(rows[0])} cols, expected 14", False
    data_rows = [r for r in rows[1:] if r and len(r) >= 14]
    if len(data_rows) < min_rows:
        return f"Only {len(data_rows)} data rows, expected >= {min_rows}", False
    bad = [r for r in data_rows if r[0] != expected_source]
    if bad:
        return f"Source mismatch: expected '{expected_source}', got '{bad[0][0]}'", False
    return f"{len(data_rows)} rows, 14 cols", True


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
         "--path", str(FIXTURES / "files")],
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
            ("postgres", "postgresql://datacoolie:datacoolie@localhost:5442/pagila"),
            ("mysql", "mysql+pymysql://datacoolie:datacoolie@localhost:3316/sakila"),
            ("mssql", "mssql+pyodbc://sa:Testing%40123@localhost:1444/AdventureWorksLT?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"),
        ]
        for name, url in db_tests:
            out, ok = _run_script(
                [str(SCRIPTS_DIR / "introspect_db.py"), "--url", url, "--source", name],
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

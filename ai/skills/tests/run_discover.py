"""
datacoolie-discover — Integration test runner.
Runs introspect.py against all test data sources and saves output to test-results/discover/.

Usage (from datacoolie/ai/skills/tests/):
  python run_discover.py
  python run_discover.py postgres          # single source
  python run_discover.py postgres mysql    # subset

Output:
  test-results/discover/postgres/catalog.csv
  test-results/discover/mysql/catalog.csv
  test-results/discover/mssql/catalog.csv
  test-results/discover/oracle/catalog.csv
  test-results/discover/files/catalog.csv        (csv/jsonl/parquet/json/avro/xlsx/delta)
  test-results/discover/files-s3/catalog.csv     (MinIO/S3)
  test-results/discover/api/endpoints.csv        (OpenAPI, no auth)
  test-results/discover/api-apikey/endpoints.csv (API Key)
  test-results/discover/api-bearer/endpoints.csv (Bearer token)
  test-results/discover/api-basic/endpoints.csv  (HTTP Basic auth)
  test-results/discover/api-graphql/endpoints.csv
  test-results/discover/api-odata/endpoints.csv
  test-results/discover/api-pagination/endpoints.csv  (cursor/offset/link patterns)
  test-results/discover/api-sample-call/endpoints.csv (live response schema inference)
  test-results/discover/api-yaml/endpoints.csv        (YAML spec from local file)
  test-results/discover/postgres-schema/catalog.csv   (schema filter: sales)
  test-results/discover/iceberg/catalog.csv
  test-results/discover/delta/catalog.csv
"""
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
SCRIPT = HERE.parent / "datacoolie-discover" / "scripts" / "introspect.py"
RESULTS = HERE / "test-results" / "discover"

# Relative to datacoolie/ directory (one level up from tests/)
FILES_PATH = str(HERE / "fixtures" / "files")
API_BASE  = "http://localhost:8092"
API_SPEC   = f"{API_BASE}/openapi.json"
YAML_SPEC  = str(HERE / "fixtures" / "api" / "openapi-inventory.yaml")

TESTS: list[tuple[str, list[str]]] = [
    ("postgres", [
        "--source-type", "db",
        "--connection", "postgresql://datacoolie:datacoolie@localhost:5442/pagila",
        "--format", "csv",
        "--output-dir", str(RESULTS / "postgres"),
    ]),
    ("mysql", [
        "--source-type", "db",
        "--connection", "mysql+pymysql://datacoolie:datacoolie@localhost:3316/sakila",
        "--format", "csv",
        "--output-dir", str(RESULTS / "mysql"),
    ]),
    ("mssql", [
        "--source-type", "db",
        "--connection",
        "mssql+pyodbc://sa:Testing%40123@localhost:1444/AdventureWorksLT"
        "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes",
        "--format", "csv",
        "--output-dir", str(RESULTS / "mssql"),
    ]),
    ("oracle", [
        "--source-type", "db",
        "--connection", "oracle+oracledb://hr:hr@localhost:1522/?service_name=FREEPDB1",
        "--format", "csv",
        "--output-dir", str(RESULTS / "oracle"),
    ]),
    ("files", [
        "--source-type", "file",
        "--path", FILES_PATH,
        "--format", "csv",
        "--output-dir", str(RESULTS / "files"),
    ]),
    ("files-s3", [
        "--source-type", "file",
        "--path", "s3://skills-test/raw/",
        "--storage-options", '{"key":"minioadmin","secret":"minioadmin","endpoint_url":"http://localhost:9010","client_kwargs":{"endpoint_url":"http://localhost:9010"}}',
        "--format", "csv",
        "--output-dir", str(RESULTS / "files-s3"),
    ]),
    ("api", [
        "--source-type", "api",
        "--spec-url", API_SPEC,
        "--format", "csv",
        "--output-dir", str(RESULTS / "api"),
    ]),
    ("api-apikey", [
        "--source-type", "api",
        "--spec-url", f"{API_BASE}/openapi-apikey.json",
        "--api-auth", "api_key",
        "--api-key", "testkey123",
        "--format", "csv",
        "--output-dir", str(RESULTS / "api-apikey"),
    ]),
    ("api-bearer", [
        "--source-type", "api",
        "--spec-url", f"{API_BASE}/openapi-bearer.json",
        "--api-auth", "bearer",
        "--api-token", "testtoken456",
        "--format", "csv",
        "--output-dir", str(RESULTS / "api-bearer"),
    ]),
    ("api-graphql", [
        "--source-type", "api",
        "--spec-url", f"{API_BASE}/graphql",
        "--api-type", "graphql",
        "--format", "csv",
        "--output-dir", str(RESULTS / "api-graphql"),
    ]),
    ("api-odata", [
        "--source-type", "api",
        "--spec-url", f"{API_BASE}/$metadata",
        "--api-type", "odata",
        "--format", "csv",
        "--output-dir", str(RESULTS / "api-odata"),
    ]),
    ("api-pagination", [
        "--source-type", "api",
        "--spec-url", f"{API_BASE}/openapi-paginated.json",
        "--format", "csv",
        "--output-dir", str(RESULTS / "api-pagination"),
    ]),
    ("api-basic", [
        "--source-type", "api",
        "--spec-url", f"{API_BASE}/openapi-basic.json",
        "--api-auth", "basic",
        "--api-token", "testuser:testpass",
        "--format", "csv",
        "--output-dir", str(RESULTS / "api-basic"),
    ]),
    ("api-sample-call", [
        "--source-type", "api",
        "--spec-url", f"{API_BASE}/openapi-sample.json",
        "--sample-call",
        "--format", "csv",
        "--output-dir", str(RESULTS / "api-sample-call"),
    ]),
    ("postgres-schema", [
        "--source-type", "db",
        "--connection", "postgresql://datacoolie:datacoolie@localhost:5442/pagila",
        "--schema", "sales",
        "--format", "csv",
        "--output-dir", str(RESULTS / "postgres-schema"),
    ]),
    ("api-yaml", [
        "--source-type", "api",
        "--spec-url", YAML_SPEC,
        "--format", "csv",
        "--output-dir", str(RESULTS / "api-yaml"),
    ]),
    ("iceberg", [
        "--source-type", "lakehouse",
        "--catalog", "iceberg",
        "--database", "sales",
        "--catalog-uri", "http://localhost:8182",
        "--format", "csv",
        "--output-dir", str(RESULTS / "iceberg"),
    ]),
    ("delta", [
        "--source-type", "lakehouse",
        "--catalog", "delta",
        "--database", str(HERE / "fixtures" / "files"),
        "--format", "csv",
        "--output-dir", str(RESULTS / "delta"),
    ]),
]


def run(filter_names: list[str] | None = None) -> None:
    tests = [(n, a) for n, a in TESTS if not filter_names or n in filter_names]

    if not SCRIPT.exists():
        print(f"ERROR: introspect.py not found at {SCRIPT}")
        sys.exit(1)

    summary: list[tuple[str, str, str]] = []

    for name, args in tests:
        out_dir = RESULTS / name
        out_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n{'─'*60}")
        print(f"  Source: {name}")
        print(f"{'─'*60}")
        cmd = [sys.executable, str(SCRIPT)] + args
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            # Show first few lines of output
            lines = (result.stdout or "").strip().splitlines()
            for line in lines[:8]:
                print(f"  {line}")
            if len(lines) > 8:
                print(f"  ... ({len(lines) - 8} more lines)")
            # Detect output file
            csv_file = next(out_dir.glob("*.csv"), None)
            if csv_file:
                row_count = sum(1 for _ in open(csv_file, encoding="utf-8-sig")) - 1
                summary.append((name, "✓", f"{row_count} data rows → {csv_file.name}"))
            else:
                summary.append((name, "✓", "completed (stdout only)"))
        else:
            print(f"  STDERR: {result.stderr[:400]}")
            summary.append((name, "✗", result.stderr.strip().splitlines()[-1][:80] if result.stderr else "unknown error"))

    print(f"\n{'='*60}")
    print("  DISCOVER SUMMARY")
    print(f"{'='*60}")
    for name, status, detail in summary:
        print(f"  {status} {name:<12}  {detail}")

    print(f"\n  Output: {RESULTS}")
    failures = [n for n, s, _ in summary if s == "✗"]
    if failures:
        print(f"\n  FAILED: {', '.join(failures)}")
        sys.exit(1)


if __name__ == "__main__":
    filter_names = sys.argv[1:] if len(sys.argv) > 1 else None
    run(filter_names)

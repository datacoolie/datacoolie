"""
datacoolie-metadata — Integration test runner stub.
Tests validate.py and convert.py against the shared usecase-sim fixtures.

Refer to TESTING_datacoolie-metadata.md for manual test steps.
"""
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
SKILL_DIR = HERE.parent / "datacoolie-metadata" / "scripts"
USECASE_META = HERE.parent.parent / "usecase-sim" / "metadata" / "file"
RESULTS = HERE / "test-results" / "metadata"
RESULTS.mkdir(parents=True, exist_ok=True)

VALIDATE_FILE = USECASE_META / "local_use_cases.json"


def run() -> None:
    summary = []
    if VALIDATE_FILE.exists():
        cmd = [sys.executable, str(SKILL_DIR / "validate.py"), str(VALIDATE_FILE)]
        print(f"\n  validate: {VALIDATE_FILE.name}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        status = "✓" if result.returncode == 0 else "✗"
        print(f"  {status} {result.stdout.strip()[:120]}")
        summary.append(("validate-local-use-cases", status))
    else:
        print(f"  SKIP: {VALIDATE_FILE} not found")
        summary.append(("validate-local-use-cases", "-"))

    print(f"\n{'='*60}\n  METADATA SUMMARY\n{'='*60}")
    for n, s in summary:
        print(f"  {s} {n}")


if __name__ == "__main__":
    run()

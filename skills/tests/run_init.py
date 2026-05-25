"""
datacoolie-init — Integration test runner stub.
Tests scaffold.py against local filesystem targets.

Refer to TESTING_datacoolie-init.md for manual test steps.
"""
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).parent
SCRIPT_DIR = HERE.parent / "datacoolie-init" / "scripts"
RESULTS = HERE / "test-results" / "init"
RESULTS.mkdir(parents=True, exist_ok=True)


def run() -> None:
    summary = []
    with tempfile.TemporaryDirectory() as tmpdir:
        name = "test_project"
        cmd = [sys.executable, str(SCRIPT_DIR / "scaffold.py"),
               "--name", name, "--output", tmpdir]
        print(f"\n  scaffold --name {name}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        status = "✓" if result.returncode == 0 else "✗"
        print(f"  {status} {result.stdout.strip()[:120]}")
        summary.append(("scaffold-basic", status))

    print(f"\n{'='*60}\n  INIT SUMMARY\n{'='*60}")
    for n, s in summary:
        print(f"  {s} {n}")


if __name__ == "__main__":
    run()

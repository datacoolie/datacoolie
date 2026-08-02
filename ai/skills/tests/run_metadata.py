"""Run required metadata-skill integration checks.

Refer to TESTING_datacoolie-metadata.md for manual test steps.
"""

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[2]
SKILL_DIR = HERE.parent / "datacoolie-metadata" / "scripts"
USECASE_META = REPO_ROOT / "usecase-sim" / "metadata" / "file"

VALIDATION_CHECKS = (
    ("validate-local-use-cases", USECASE_META / "local_use_cases.json"),
    ("validate-transformer-features", USECASE_META / "transformer_features.json"),
)


def run() -> int:
    """Run all required checks and return a process-compatible exit code."""
    summary: list[tuple[str, str]] = []
    failed = False

    for name, metadata_file in VALIDATION_CHECKS:
        if not metadata_file.is_file():
            print(f"\n  ✗ missing required fixture: {metadata_file}")
            summary.append((name, "✗"))
            failed = True
            continue

        cmd = [sys.executable, str(SKILL_DIR / "validate.py"), str(metadata_file)]
        print(f"\n  validate: {metadata_file.name}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        status = "✓" if result.returncode == 0 else "✗"
        details = result.stdout.strip() or result.stderr.strip()
        print(f"  {status} {details}")
        summary.append((name, status))
        failed = failed or result.returncode != 0

    print(f"\n{'=' * 60}\n  METADATA SUMMARY\n{'=' * 60}")
    for name, status in summary:
        print(f"  {status} {name}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(run())

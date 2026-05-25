"""
datacoolie-deploy — Integration test runner stub.
Tests preflight, promote, and deploy scripts against the shared test environment.

Refer to TESTING_datacoolie-deploy.md for manual test steps.
"""
import subprocess
import sys
import tempfile
import json
from pathlib import Path

HERE = Path(__file__).parent
SCRIPT_DIR = HERE.parent / "datacoolie-deploy" / "scripts"
RESULTS = HERE / "test-results" / "deploy"
RESULTS.mkdir(parents=True, exist_ok=True)


def _make_test_project(base: Path) -> Path:
    """Create a minimal project tree for integration checks."""
    meta = base / "metadata"
    meta.mkdir(parents=True)
    dataflows = {"dataflows": [{"name": "sales_load", "load_type": "full_load"}]}
    (meta / "dataflows.json").write_text(json.dumps(dataflows), encoding="utf-8")
    return base


def run() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        project = _make_test_project(Path(tmpdir))

        TESTS: list[tuple[str, list[str], int | None]] = [
            # (label, cmd_args, expected_exit_code)  None = any non-error
            (
                "preflight-local",
                [str(SCRIPT_DIR / "preflight.py"), "--platform", "local", "--skip-auth", "--env", "dev",
                 "--project-dir", str(project)],
                0,
            ),
            (
                "preflight-local-env-prod",
                [str(SCRIPT_DIR / "preflight.py"), "--platform", "local", "--skip-auth", "--env", "prod",
                 "--project-dir", str(project)],
                0,
            ),
            (
                "promote-prod-no-confirm-exits-2",
                [str(SCRIPT_DIR / "promote.py"), "--from", "dev", "--to", "prod",
                 "--platform", "local", "--project-dir", str(project), "--skip-auth"],
                2,
            ),
            (
                "promote-from-eq-to-rejected",
                [str(SCRIPT_DIR / "promote.py"), "--from", "dev", "--to", "dev",
                 "--platform", "local", "--project-dir", str(project)],
                2,
            ),
        ]

        summary: list[tuple[str, str]] = []
        for name, cmd, expected_rc in TESTS:
            print(f"\n  {name}")
            result = subprocess.run([sys.executable] + cmd, capture_output=True, text=True)
            ok = (expected_rc is None and result.returncode == 0) or result.returncode == expected_rc
            status = "✓" if ok else "✗"
            detail = (result.stdout + result.stderr).strip().split("\n")[-1][:100]
            print(f"  {status} rc={result.returncode} | {detail}")
            summary.append((name, status))

    print(f"\n{'='*60}\n  DEPLOY SUMMARY\n{'='*60}")
    passed = sum(1 for _, s in summary if s == "✓")
    for name, status in summary:
        print(f"  {status} {name}")
    print(f"\n  {passed}/{len(summary)} checks passed")


if __name__ == "__main__":
    run()

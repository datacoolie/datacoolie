"""preflight.py — Pre-flight checks for datacoolie deployment.

Validates CLI tools, authentication, metadata, and functions packaging
before attempting a deploy.

Usage:
    python scripts/preflight.py --platform aws
    python scripts/preflight.py --platform fabric --skip-auth
    python scripts/preflight.py --platform databricks
    python scripts/preflight.py --platform local
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PLATFORMS = ("aws", "fabric", "databricks", "local")

# Install hints per platform CLI
CLI_INSTALL_HINTS = {
    "aws": "pip install awscli  OR  https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html",
    "fabric": "pip install ms-fabric-cli",
    "databricks": "pip install databricks-cli",
}

# Auth re-login hints
AUTH_HINTS = {
    "aws": "aws configure  OR  aws sso login",
    "fabric": "fab auth login -u $CLIENT_ID -p $CLIENT_SECRET --tenant $TENANT_ID",
    "databricks": "databricks configure --token  OR  databricks auth login",
}


def _run(cmd: list[str], timeout: int = 30) -> tuple[int, str]:
    """Run a command, return (exit_code, combined_output)."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.returncode, (result.stdout + result.stderr).strip()
    except FileNotFoundError:
        return 127, f"Command not found: {cmd[0]}"
    except subprocess.TimeoutExpired:
        return 1, f"Command timed out: {' '.join(cmd)}"


def check_cli(platform: str) -> tuple[bool, str]:
    """Check if the platform CLI is installed."""
    if platform == "local":
        return True, "skip (local)"

    cmd_map = {
        "aws": ["aws", "--version"],
        "fabric": ["fab", "--version"],
        "databricks": ["databricks", "--version"],
    }
    code, output = _run(cmd_map[platform])
    if code == 0:
        # Extract version from first line
        version_line = output.split("\n")[0]
        return True, version_line
    hint = CLI_INSTALL_HINTS.get(platform, "")
    return False, f"Not found. Install: {hint}"


def check_auth(platform: str) -> tuple[bool, str]:
    """Check if the platform CLI is authenticated."""
    if platform == "local":
        return True, "skip (local)"

    cmd_map = {
        "aws": ["aws", "sts", "get-caller-identity"],
        "fabric": ["fab", "ls"],
        "databricks": ["databricks", "auth", "describe"],
    }
    code, output = _run(cmd_map[platform])
    if code == 0:
        # Extract identity summary
        if platform == "aws":
            # Output is JSON with Arn field
            import json

            try:
                data = json.loads(output)
                return True, data.get("Arn", "authenticated")
            except (json.JSONDecodeError, KeyError):
                return True, "authenticated"
        return True, "authenticated"
    hint = AUTH_HINTS.get(platform, "")
    return False, f"Auth failed. Re-auth: {hint}"


def check_datacoolie() -> tuple[bool, str]:
    """Check if datacoolie is installed (pip package or importable via .pth/src layout)."""
    code, output = _run([sys.executable, "-m", "pip", "show", "datacoolie"])
    if code == 0:
        for line in output.split("\n"):
            if line.startswith("Version:"):
                return True, line.split(":", 1)[1].strip()
        return True, "installed"
    # Fallback: try importing directly (handles editable/src dev installs)
    code2, out2 = _run([sys.executable, "-c", "import datacoolie; print(getattr(datacoolie, '__version__', 'installed'))"])
    if code2 == 0:
        return True, out2.strip()
    return False, "Not installed. Run: pip install datacoolie"


def check_metadata(project_dir: Path) -> tuple[bool, str]:
    """Check if metadata directory exists with at least one JSON/YAML file."""
    metadata_dir = project_dir / "metadata"
    if not metadata_dir.is_dir():
        return False, f"Directory not found: {metadata_dir}"
    meta_files = list(metadata_dir.rglob("*.json")) + list(metadata_dir.rglob("*.yaml")) + list(metadata_dir.rglob("*.yml"))
    if not meta_files:
        return False, f"No metadata files (.json/.yaml) in {metadata_dir}"
    return True, f"{len(meta_files)} file(s) in metadata/"


def check_functions(project_dir: Path) -> tuple[bool, str]:
    """Check if functions/ is packageable."""
    functions_dir = project_dir / "functions"
    if not functions_dir.is_dir():
        return True, "No functions/ directory (optional, skipping)"

    has_pyproject = (functions_dir / "pyproject.toml").is_file()
    has_init = (functions_dir / "__init__.py").is_file()

    if has_pyproject:
        return True, "functions/pyproject.toml found (wheel build)"
    if has_init:
        return True, "functions/__init__.py found (zip package)"
    return False, "functions/ exists but has no pyproject.toml or __init__.py"


def run_preflight(platform: str, project_dir: Path, skip_auth: bool = False) -> bool:
    """Run all preflight checks, print results, return True if all pass."""
    checks: list[tuple[str, tuple[bool, str]]] = []

    checks.append(("Platform CLI installed", check_cli(platform)))

    if skip_auth:
        checks.append(("Authenticated", (True, "skipped (--skip-auth)")))
    else:
        checks.append(("Authenticated", check_auth(platform)))

    checks.append(("datacoolie installed", check_datacoolie()))
    checks.append(("Metadata found", check_metadata(project_dir)))
    checks.append(("Functions packageable", check_functions(project_dir)))

    all_pass = True
    for label, (ok, detail) in checks:
        symbol = "\u2713" if ok else "\u2717"
        print(f"  {symbol} {label} ({detail})")
        if not ok:
            all_pass = False

    return all_pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-flight checks for datacoolie deploy")
    parser.add_argument(
        "--platform",
        required=True,
        choices=PLATFORMS,
        help="Target deployment platform",
    )
    parser.add_argument(
        "--project-dir",
        type=Path,
        default=Path.cwd(),
        help="Project root directory (default: CWD)",
    )
    parser.add_argument(
        "--skip-auth",
        action="store_true",
        help="Skip authentication check (useful in CI lint-only mode)",
    )
    args = parser.parse_args()

    print(f"\nDataCoolie Deploy — Preflight ({args.platform})")
    print("-" * 50)

    passed = run_preflight(args.platform, args.project_dir, args.skip_auth)

    print("-" * 50)
    if passed:
        print("All checks passed. Ready to deploy.\n")
        sys.exit(0)
    else:
        print("Some checks failed. Fix issues above before deploying.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()

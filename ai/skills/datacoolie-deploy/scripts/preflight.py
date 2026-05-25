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


def check_secrets_resolution(project_dir: Path, env_name: str) -> tuple[bool, str]:
    """Check all secrets_ref keys in metadata resolve to real environment variables.

    Scans metadata/ for any field named secrets_ref and verifies the referenced
    env var is present in the current process environment.
    """
    import json
    import os
    import re

    metadata_dir = project_dir / "metadata"
    if not metadata_dir.is_dir():
        return True, "No metadata/ (skip)"

    missing: list[str] = []
    found_refs: list[str] = []

    def _scan(obj: object) -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "secrets_ref" and isinstance(value, str):
                    found_refs.append(value)
                    if not os.environ.get(value):
                        missing.append(value)
                else:
                    _scan(value)
        elif isinstance(obj, list):
            for item in obj:
                _scan(item)

    for meta_file in list(metadata_dir.rglob("*.json")) + list(metadata_dir.rglob("*.yaml")) + list(metadata_dir.rglob("*.yml")):
        try:
            text = meta_file.read_text(encoding="utf-8")
            if "secrets_ref" not in text:
                continue
            if meta_file.suffix == ".json":
                data = json.loads(text)
            else:
                import yaml
                data = yaml.safe_load(text) or {}
            _scan(data)
        except Exception:
            continue

    if not found_refs:
        return True, "No secrets_ref keys found (skip)"
    if missing:
        return False, f"{len(missing)} unresolved secret(s): {', '.join(missing)}"
    return True, f"All {len(found_refs)} secret(s) resolved"


def check_infra_exists(platform: str, project_dir: Path, env_name: str) -> tuple[bool, str]:
    """Check that target infrastructure resources exist on the platform.

    Reads .datacoolie/provision/latest_provision-log.md or falls back to
    architecture.md to determine expected resources, then spot-checks
    a representative resource per platform.
    """
    # Local: infra is always filesystem, nothing to check
    if platform == "local":
        return True, "skip (local)"

    # Look for a provision log that confirms infra was provisioned
    provision_dir = project_dir / ".datacoolie" / "provision"
    if provision_dir.is_dir():
        logs = sorted(provision_dir.glob("*_provision-log.md"), reverse=True)
        if logs:
            content = logs[0].read_text(encoding="utf-8")
            # A log with failures or no 'created' entries is a warning
            created = content.count("| created |")
            failed = content.count("| failed |")
            if created > 0 and failed == 0:
                return True, f"Provision log found ({created} resource(s) created)"
            if failed > 0:
                return False, f"Provision log shows {failed} failed resource(s) — run datacoolie-provision"

    # No provision log — check platform directly via lightweight CLI probe
    probe_map = {
        "aws": ["aws", "sts", "get-caller-identity"],
        "fabric": ["fab", "workspace", "list"],
        "databricks": ["databricks", "unity-catalog", "list-catalogs"],
    }
    probe = probe_map.get(platform)
    if not probe:
        return True, "skip (unknown platform)"

    code, _ = _run(probe)
    if code == 0:
        return True, "Platform reachable (provision log not found — run datacoolie-provision to confirm)"
    return False, "Cannot reach platform — run datacoolie-provision first"


def run_preflight(platform: str, project_dir: Path, skip_auth: bool = False, env_name: str = "dev") -> bool:
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
    checks.append(("Secrets resolution", check_secrets_resolution(project_dir, env_name)))
    checks.append(("Infrastructure exists", check_infra_exists(platform, project_dir, env_name)))

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
    parser.add_argument(
        "--env",
        default="dev",
        help="Target environment name for secrets/infra checks (default: dev)",
    )
    args = parser.parse_args()

    print(f"\nDataCoolie Deploy \u2014 Preflight ({args.platform}, env={args.env})")
    print("-" * 50)

    passed = run_preflight(args.platform, args.project_dir, args.skip_auth, args.env)

    print("-" * 50)
    if passed:
        print("All checks passed. Ready to deploy.\n")
        sys.exit(0)
    else:
        print("Some checks failed. Fix issues above before deploying.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()

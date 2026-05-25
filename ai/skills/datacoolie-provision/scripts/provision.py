"""provision.py — Provision platform infrastructure from architecture design.

Usage:
    python scripts/provision.py --architecture <path> --platform {fabric|aws|databricks|local} --mode {cli|terraform} --env {dev|test|prod} [--confirm] [--output <dir>]

Exit codes:
    0 = success (or dry-run completed)
    1 = provisioning error
    2 = preflight failure
    3 = architecture file not found or unparseable
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

PLATFORMS = ("fabric", "aws", "databricks", "local")
MODES = ("cli", "terraform")
ENVS = ("dev", "test", "prod")


# ---------------------------------------------------------------------------
# Architecture parser
# ---------------------------------------------------------------------------


def parse_infra_requirements(architecture_path: Path) -> list[dict[str, str]]:
    """Parse Infrastructure Requirements table from architecture markdown.

    Returns list of dicts with keys: platform, resource_type, name, purpose.
    """
    text = architecture_path.read_text(encoding="utf-8")

    # Find the Infrastructure Requirements table
    table_pattern = re.compile(
        r"##\s*Infrastructure Requirements.*?\n\s*\n?"
        r"(\|.*\|(?:\n\|.*\|)*)",
        re.IGNORECASE,
    )
    match = table_pattern.search(text)
    if not match:
        return []

    lines = match.group(1).strip().split("\n")
    if len(lines) < 3:  # header + separator + at least one row
        return []

    # Parse header
    header = [h.strip() for h in lines[0].split("|")[1:-1]]
    # Skip separator line (lines[1])
    resources = []
    for line in lines[2:]:
        cells = [c.strip() for c in line.split("|")[1:-1]]
        if len(cells) >= len(header):
            row = dict(zip(header, cells))
            resources.append({
                "platform": row.get("Platform", "").strip(),
                "resource_type": row.get("Resource Type", "").strip(),
                "name": row.get("Name", "").strip(),
                "purpose": row.get("Purpose", "").strip(),
            })
    return resources


def find_latest_architecture(base_dir: Path) -> Path | None:
    """Find the most recent architecture file in .datacoolie/architect/."""
    architect_dir = base_dir / ".datacoolie" / "architect"
    if not architect_dir.exists():
        return None
    files = sorted(architect_dir.glob("*_architecture.md"), reverse=True)
    return files[0] if files else None


# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------

CLI_COMMANDS = {
    "fabric": ["fab", "--version"],
    "aws": ["aws", "--version"],
    "databricks": ["databricks", "--version"],
}

CLI_INSTALL_HINTS = {
    "fabric": "pip install ms-fabric-cli",
    "aws": "pip install awscli  OR  https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html",
    "databricks": "pip install databricks-cli",
}


def check_preflight(platform: str) -> tuple[bool, str]:
    """Check if platform CLI is available. Returns (ok, message)."""
    if platform == "local":
        return True, "Local mode — no CLI required."

    cmd = CLI_COMMANDS.get(platform)
    if not cmd:
        return False, f"Unknown platform: {platform}"

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            version = result.stdout.strip().split("\n")[0]
            return True, f"{platform} CLI found: {version}"
        return False, f"{platform} CLI returned error: {result.stderr.strip()}"
    except FileNotFoundError:
        hint = CLI_INSTALL_HINTS.get(platform, "")
        return False, f"{platform} CLI not found. Install: {hint}"
    except subprocess.TimeoutExpired:
        return False, f"{platform} CLI timed out."


# ---------------------------------------------------------------------------
# Resource naming
# ---------------------------------------------------------------------------


def apply_env_suffix(name: str, env: str) -> str:
    """Apply environment suffix to resource name. Prod has no suffix."""
    if env == "prod":
        return name
    return f"{name}_{env}"


# ---------------------------------------------------------------------------
# Provisioning orchestration
# ---------------------------------------------------------------------------


def provision_cli(
    resources: list[dict[str, str]],
    platform: str,
    env: str,
    confirm: bool,
) -> list[dict[str, Any]]:
    """Provision resources via CLI. Returns list of action results."""
    from providers import fabric, aws, databricks, local

    provider_map = {
        "fabric": fabric,
        "aws": aws,
        "databricks": databricks,
        "local": local,
    }
    provider = provider_map[platform]
    results = []

    for resource in resources:
        name = apply_env_suffix(resource["name"], env)
        resource_type = resource["resource_type"].lower()

        # Check existence
        exists = provider.check_exists(resource_type, name)
        if exists:
            results.append({
                "resource": resource,
                "name": name,
                "action": "skip",
                "status": "already_exists",
                "command": None,
                "output": "Resource already exists.",
            })
            continue

        # Generate command
        cmd = provider.create_command(resource_type, name)
        if not cmd:
            results.append({
                "resource": resource,
                "name": name,
                "action": "skip",
                "status": "unsupported",
                "command": None,
                "output": f"Unsupported resource type: {resource_type}",
            })
            continue

        if not confirm:
            # Dry-run: just record the command
            results.append({
                "resource": resource,
                "name": name,
                "action": "dry_run",
                "status": "pending",
                "command": cmd,
                "output": "[DRY RUN] Would execute.",
            })
        elif platform == "local":
            # Local: use Python-native creation (cross-platform, no subprocess)
            ok, msg = provider.create_directory(name)
            results.append({
                "resource": resource,
                "name": name,
                "action": "create",
                "status": "created" if ok else "failed",
                "command": cmd,
                "output": msg,
            })
        else:
            # Execute via CLI
            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=120
                )
                if result.returncode == 0:
                    results.append({
                        "resource": resource,
                        "name": name,
                        "action": "create",
                        "status": "created",
                        "command": cmd,
                        "output": result.stdout.strip(),
                    })
                else:
                    results.append({
                        "resource": resource,
                        "name": name,
                        "action": "create",
                        "status": "failed",
                        "command": cmd,
                        "output": result.stderr.strip(),
                    })
            except Exception as e:
                results.append({
                    "resource": resource,
                    "name": name,
                    "action": "create",
                    "status": "error",
                    "command": cmd,
                    "output": str(e),
                })

    return results


def provision_terraform(
    resources: list[dict[str, str]],
    platform: str,
    env: str,
    output_dir: Path,
) -> list[dict[str, Any]]:
    """Generate Terraform files from architecture requirements."""
    from terraform.generate import generate_terraform

    return generate_terraform(resources, platform, env, output_dir)


# ---------------------------------------------------------------------------
# Provision log writer
# ---------------------------------------------------------------------------


def write_provision_log(
    results: list[dict[str, Any]],
    architecture_path: Path,
    platform: str,
    mode: str,
    env: str,
    output_dir: Path,
) -> Path:
    """Write provision log markdown file."""
    today = date.today().strftime("%y%m%d")
    log_path = output_dir / f"{today}_provision-log.md"
    output_dir.mkdir(parents=True, exist_ok=True)

    created = sum(1 for r in results if r["status"] == "created")
    skipped = sum(1 for r in results if r["status"] == "already_exists")
    failed = sum(1 for r in results if r["status"] in ("failed", "error"))
    pending = sum(1 for r in results if r["status"] == "pending")

    status = "Dry Run" if pending > 0 else ("Success" if failed == 0 else "Partial Failure")

    lines = [
        f"# Provision Log",
        f"",
        f"**Date:** {date.today().isoformat()}",
        f"**Architecture:** {architecture_path}",
        f"**Platform:** {platform}",
        f"**Mode:** {mode}",
        f"**Environment:** {env}",
        f"**Status:** {status}",
        f"",
        f"---",
        f"",
        f"## Resources",
        f"",
        f"| # | Resource Type | Name | Action | Status | Details |",
        f"|---|---|---|---|---|---|",
    ]

    for i, r in enumerate(results, 1):
        res = r["resource"]

        lines.append(
            f"| {i} | {res['resource_type']} | {r['name']} | {r['action']} | {r['status']} | {r['output'][:80]} |"
        )

    lines.extend([
        f"",
        f"---",
        f"",
        f"## Commands",
        f"",
        f"```bash",
    ])
    for r in results:
        if r["command"]:
            lines.append(" ".join(r["command"]))
    lines.extend([
        f"```",
        f"",
        f"---",
        f"",
        f"## Summary",
        f"",
        f"- **Total resources:** {len(results)}",
        f"- **Created:** {created}",
        f"- **Already existed (skipped):** {skipped}",
        f"- **Failed:** {failed}",
        f"- **Pending (dry-run):** {pending}",
    ])

    if failed > 0:
        lines.extend([f"", f"## Errors", f""])
        for r in results:
            if r["status"] in ("failed", "error"):
                lines.append(f"- **{r['name']}**: {r['output']}")

    log_path.write_text("\n".join(lines), encoding="utf-8")
    return log_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Provision platform infrastructure from architecture design."
    )
    parser.add_argument(
        "--architecture",
        type=Path,
        default=None,
        help="Path to architecture.md (default: latest in .datacoolie/architect/)",
    )
    parser.add_argument(
        "--platform",
        choices=PLATFORMS,
        required=True,
        help="Target platform",
    )
    parser.add_argument(
        "--mode",
        choices=MODES,
        default="cli",
        help="Execution mode: cli or terraform (default: cli)",
    )
    parser.add_argument(
        "--env",
        choices=ENVS,
        default="dev",
        help="Target environment (default: dev)",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually execute commands (CLI mode). Without this flag, dry-run only.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory (default: .datacoolie/provision/)",
    )

    args = parser.parse_args()

    # Resolve architecture path
    arch_path = args.architecture
    if arch_path is None:
        arch_path = find_latest_architecture(Path.cwd())
    if arch_path is None or not arch_path.exists():
        print("ERROR: Architecture file not found.", file=sys.stderr)
        print("Run `datacoolie-architect` first to create an architecture design.", file=sys.stderr)
        return 3

    # Parse infrastructure requirements
    resources = parse_infra_requirements(arch_path)
    if not resources:
        print("ERROR: No Infrastructure Requirements table found in architecture.", file=sys.stderr)
        return 3

    # Filter resources for target platform (or use all if platform matches)
    platform_resources = [
        r for r in resources
        if r["platform"].lower() == args.platform or r["platform"] == "—"
    ]
    if not platform_resources:
        # Use all resources if platform column doesn't match filtering
        platform_resources = resources

    # Preflight — only required when actually executing (--confirm or terraform)
    if args.confirm or args.mode == "terraform":
        ok, msg = check_preflight(args.platform)
        if not ok:
            print(f"PREFLIGHT FAILED: {msg}", file=sys.stderr)
            return 2
        print(f"\u2713 Preflight: {msg}")

    # Output directory
    output_dir = args.output or Path.cwd() / ".datacoolie" / "provision"

    # Execute
    if args.mode == "cli":
        if not args.confirm:
            print(f"\n[DRY RUN] Platform: {args.platform}, Env: {args.env}")
            print("Pass --confirm to execute commands.\n")
        results = provision_cli(platform_resources, args.platform, args.env, args.confirm)
    else:
        tf_dir = output_dir / "terraform"
        results = provision_terraform(platform_resources, args.platform, args.env, tf_dir)

    # Write log
    log_path = write_provision_log(results, arch_path, args.platform, args.mode, args.env, output_dir)
    print(f"\nProvision log: {log_path}")

    # Summary
    failed = sum(1 for r in results if r["status"] in ("failed", "error"))
    if failed > 0:
        print(f"\n⚠ {failed} resource(s) failed. See log for details.", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

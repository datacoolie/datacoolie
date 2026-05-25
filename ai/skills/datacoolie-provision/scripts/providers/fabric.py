"""Fabric CLI provider for datacoolie-provision.

Handles Microsoft Fabric resources: lakehouses, warehouses, workspaces.
"""

from __future__ import annotations

import subprocess


def _run(cmd: list[str], timeout: int = 60) -> tuple[int, str]:
    """Run a CLI command. Returns (exit_code, output)."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result.returncode, (result.stdout + result.stderr).strip()
    except FileNotFoundError:
        return 127, f"Command not found: {cmd[0]}"
    except subprocess.TimeoutExpired:
        return 1, f"Timed out: {' '.join(cmd)}"


# Workspace is required for most Fabric operations; default from env or config
_DEFAULT_WORKSPACE = None


def set_workspace(workspace: str) -> None:
    """Set the default workspace for Fabric operations."""
    global _DEFAULT_WORKSPACE
    _DEFAULT_WORKSPACE = workspace


def check_exists(resource_type: str, name: str) -> bool:
    """Check if a Fabric resource exists."""
    cmd_map = {
        "lakehouse": ["fab", "lakehouse", "list"],
        "warehouse": ["fab", "warehouse", "list"],
        "workspace": ["fab", "workspace", "list"],
    }
    cmd = cmd_map.get(resource_type)
    if not cmd:
        return False

    if resource_type != "workspace" and _DEFAULT_WORKSPACE:
        cmd.extend(["--workspace", _DEFAULT_WORKSPACE])

    code, output = _run(cmd)
    if code != 0:
        return False
    # Simple name check in output (Fabric CLI outputs JSON or table)
    return name.lower() in output.lower()


def create_command(resource_type: str, name: str) -> list[str] | None:
    """Generate the CLI command to create a Fabric resource."""
    cmd_map = {
        "lakehouse": ["fab", "lakehouse", "create", "--display-name", name],
        "warehouse": ["fab", "warehouse", "create", "--display-name", name],
        "workspace": ["fab", "workspace", "create", "--display-name", name],
    }
    cmd = cmd_map.get(resource_type)
    if cmd is None:
        return None

    if resource_type != "workspace" and _DEFAULT_WORKSPACE:
        cmd.extend(["--workspace", _DEFAULT_WORKSPACE])

    return cmd

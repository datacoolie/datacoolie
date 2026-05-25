"""Databricks CLI provider for datacoolie-provision.

Handles Databricks Unity Catalog resources: catalogs, schemas, volumes.
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


# Default catalog for schema/volume creation
_DEFAULT_CATALOG = None


def set_catalog(catalog: str) -> None:
    """Set the default catalog for Databricks operations."""
    global _DEFAULT_CATALOG
    _DEFAULT_CATALOG = catalog


def check_exists(resource_type: str, name: str) -> bool:
    """Check if a Databricks Unity Catalog resource exists."""
    rt = resource_type.lower()

    if rt == "catalog":
        cmd = ["databricks", "unity-catalog", "get-catalog", "--name", name]
    elif rt == "schema":
        full_name = f"{_DEFAULT_CATALOG}.{name}" if _DEFAULT_CATALOG else name
        cmd = ["databricks", "unity-catalog", "get-schema", "--full-name", full_name]
    elif rt == "volume":
        full_name = f"{_DEFAULT_CATALOG}.default.{name}" if _DEFAULT_CATALOG else name
        cmd = ["databricks", "unity-catalog", "get-volume", "--full-name", full_name]
    else:
        return False

    code, _ = _run(cmd)
    return code == 0


def create_command(resource_type: str, name: str) -> list[str] | None:
    """Generate the CLI command to create a Databricks resource."""
    rt = resource_type.lower()

    if rt == "catalog":
        return ["databricks", "unity-catalog", "create-catalog", "--name", name]

    if rt == "schema":
        cmd = ["databricks", "unity-catalog", "create-schema", "--name", name]
        if _DEFAULT_CATALOG:
            cmd.extend(["--catalog", _DEFAULT_CATALOG])
        return cmd

    if rt == "volume":
        cmd = [
            "databricks", "unity-catalog", "create-volume",
            "--name", name,
            "--volume-type", "MANAGED",
        ]
        if _DEFAULT_CATALOG:
            cmd.extend(["--catalog", _DEFAULT_CATALOG, "--schema", "default"])
        return cmd

    return None

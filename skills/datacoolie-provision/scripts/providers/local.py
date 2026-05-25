"""Local filesystem provider for datacoolie-provision.

Handles local resources: directories for bronze/silver/gold/metadata.
"""

from __future__ import annotations

from pathlib import Path


# Base path for local data directories
_BASE_PATH = Path(".")


def set_base_path(base: str | Path) -> None:
    """Set the base path for local directory creation."""
    global _BASE_PATH
    _BASE_PATH = Path(base).resolve()


def _safe_target(name: str) -> Path | None:
    """Resolve target path and reject traversal outside base."""
    base = _BASE_PATH.resolve()
    target = (base / name).resolve()
    try:
        target.relative_to(base)
    except ValueError:
        return None
    return target


_SUPPORTED_TYPES = ("directory", "dir", "folder")


def check_exists(resource_type: str, name: str) -> bool:
    """Check if a local directory exists."""
    target = _safe_target(name)
    if target is None:
        return False
    return target.exists()


def create_command(resource_type: str, name: str) -> list[str] | None:
    """Generate the command to create a local directory.

    Returns a list that can be passed to subprocess or os.makedirs.
    For local, we use Python directly rather than shell commands.
    """
    if resource_type.lower() not in _SUPPORTED_TYPES:
        return None
    target = _safe_target(name)
    if target is None:
        return None
    # Cross-platform: use Python's pathlib in the orchestrator
    # But return a shell command for logging/dry-run display
    return ["mkdir", "-p", str(target)]


def create_directory(name: str) -> tuple[bool, str]:
    """Actually create the local directory (idempotent)."""
    target = _safe_target(name)
    if target is None:
        return False, f"Path traversal rejected: {name}"
    try:
        target.mkdir(parents=True, exist_ok=True)
        return True, f"Created: {target}"
    except OSError as e:
        return False, f"Failed to create {target}: {e}"

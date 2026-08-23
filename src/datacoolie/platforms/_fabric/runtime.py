"""Fabric runtime selection without service or authentication calls."""

from __future__ import annotations

from importlib import import_module
from typing import Any, Literal, cast

from datacoolie.core.exceptions import PlatformError

FabricRuntime = Literal["auto", "fabric", "external"]
EffectiveFabricRuntime = Literal["fabric", "external"]

_VALID_RUNTIMES = frozenset({"auto", "fabric", "external"})


def validate_runtime(runtime: str) -> FabricRuntime:
    """Validate and narrow a public Fabric runtime value."""
    if runtime not in _VALID_RUNTIMES:
        choices = ", ".join(sorted(_VALID_RUNTIMES))
        raise PlatformError(f"Invalid Fabric runtime '{runtime}'. Expected one of: {choices}.")
    return cast(FabricRuntime, runtime)


def try_load_notebookutils() -> Any | None:
    """Import NotebookUtils, returning ``None`` when the module is unavailable."""
    try:
        return import_module("notebookutils")
    except Exception:  # noqa: BLE001 - a broken optional module is not a native runtime
        return None


def require_notebookutils() -> Any:
    """Import NotebookUtils or raise an actionable platform error."""
    module = try_load_notebookutils()
    if module is None:
        raise PlatformError(
            "notebookutils is unavailable. "
            "Use runtime='external' outside Microsoft Fabric, or run this platform "
            "inside a Fabric notebook with NotebookUtils available."
        )
    return module


def resolve_runtime(runtime: FabricRuntime) -> EffectiveFabricRuntime:
    """Resolve an explicit or automatic runtime without performing I/O."""
    if runtime == "external":
        return "external"
    if runtime == "fabric":
        require_notebookutils()
        return "fabric"
    return "fabric" if try_load_notebookutils() is not None else "external"

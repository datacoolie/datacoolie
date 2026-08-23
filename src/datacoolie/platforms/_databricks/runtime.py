"""Databricks runtime selection without service or authentication calls."""

from __future__ import annotations

from typing import Any, Literal, cast

from datacoolie.core.exceptions import PlatformError

DatabricksRuntime = Literal["auto", "databricks", "external"]
EffectiveDatabricksRuntime = Literal["databricks", "external"]

_VALID_RUNTIMES = frozenset({"auto", "databricks", "external"})


def validate_runtime(runtime: str) -> DatabricksRuntime:
    """Validate and narrow a public Databricks runtime value."""
    if runtime not in _VALID_RUNTIMES:
        choices = ", ".join(sorted(_VALID_RUNTIMES))
        raise PlatformError(
            f"Invalid Databricks runtime '{runtime}'. Expected one of: {choices}."
        )
    return cast(DatabricksRuntime, runtime)


def try_resolve_dbutils() -> Any | None:
    """Return native ``dbutils`` when running in a Databricks Python runtime."""
    try:
        from IPython import get_ipython  # type: ignore[import-untyped]

        ipython = get_ipython()
        if ipython is not None:
            dbutils = ipython.user_ns.get("dbutils")
            if dbutils is not None:
                return dbutils
    except Exception:  # noqa: BLE001 - optional runtime detection must be inert
        pass

    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]
        from pyspark.sql import SparkSession  # type: ignore[import-untyped]

        spark = SparkSession.getActiveSession()
        if spark is not None:
            return DBUtils(spark)
    except Exception:  # noqa: BLE001 - optional runtime detection must be inert
        pass
    return None


def require_dbutils() -> Any:
    """Resolve native ``dbutils`` or raise an actionable error."""
    dbutils = try_resolve_dbutils()
    if dbutils is None:
        raise PlatformError(
            "Cannot resolve dbutils. Use runtime='external' outside Databricks, "
            "or run runtime='databricks' inside a Databricks notebook or job."
        )
    return dbutils


def resolve_runtime(
    runtime: DatabricksRuntime,
    *,
    injected_dbutils: Any | None = None,
) -> tuple[EffectiveDatabricksRuntime, Any | None]:
    """Resolve the backend and any discovered native handle without service I/O."""
    if runtime == "external":
        return "external", None
    if injected_dbutils is not None:
        return "databricks", injected_dbutils

    dbutils = try_resolve_dbutils()
    if dbutils is not None:
        return "databricks", dbutils
    if runtime == "databricks":
        raise PlatformError(
            "Cannot resolve dbutils for runtime='databricks'. Run inside a "
            "Databricks notebook/job or inject the native dbutils handle."
        )
    return "external", None

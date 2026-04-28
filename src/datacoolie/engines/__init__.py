"""Engine abstraction — DataFrame-engine-agnostic read / write / transform."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datacoolie.engines.base import BaseEngine

if TYPE_CHECKING:
    from datacoolie.engines.spark_engine import SparkEngine
    from datacoolie.engines.polars_engine import PolarsEngine


def __getattr__(name: str):
    if name == "SparkEngine":
        from datacoolie.engines.spark_engine import SparkEngine
        return SparkEngine
    if name == "PolarsEngine":
        from datacoolie.engines.polars_engine import PolarsEngine
        return PolarsEngine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BaseEngine",
    "PolarsEngine",
    "SparkEngine",
]

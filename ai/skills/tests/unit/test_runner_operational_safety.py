"""Executable safety checks for operational runner lifecycle behavior."""

from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest


AI_DIR = Path(__file__).resolve().parents[3]
RUNNERS = AI_DIR / "skills" / "datacoolie-build" / "templates" / "runners"


def _module(name: str, **attributes: object) -> types.ModuleType:
    module = types.ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


def test_local_spark_stops_session_when_engine_construction_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = SimpleNamespace(stop_calls=0)
    spark.stop = lambda: setattr(spark, "stop_calls", spark.stop_calls + 1)
    builder = SimpleNamespace()
    builder.appName = lambda _name: builder
    builder.getOrCreate = lambda: spark

    class FailingEngine:
        def __init__(self, **_kwargs: object) -> None:
            raise RuntimeError("engine failed")

    modules = {
        "pyspark": _module("pyspark"),
        "pyspark.sql": _module("pyspark.sql", SparkSession=SimpleNamespace(builder=builder)),
        "datacoolie": _module("datacoolie"),
        "datacoolie.core": _module("datacoolie.core"),
        "datacoolie.core.models": _module("datacoolie.core.models", DataCoolieRunConfig=object),
        "datacoolie.engines": _module("datacoolie.engines"),
        "datacoolie.engines.spark_engine": _module("datacoolie.engines.spark_engine", SparkEngine=FailingEngine),
        "datacoolie.metadata": _module("datacoolie.metadata"),
        "datacoolie.metadata.file_provider": _module("datacoolie.metadata.file_provider", FileProvider=object),
        "datacoolie.orchestration": _module("datacoolie.orchestration"),
        "datacoolie.orchestration.driver": _module("datacoolie.orchestration.driver", DataCoolieDriver=object),
        "datacoolie.platforms": _module("datacoolie.platforms"),
        "datacoolie.platforms.local_platform": _module("datacoolie.platforms.local_platform", LocalPlatform=object),
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    path = RUNNERS / "run_local_spark.py.example"
    namespace = {"__name__": "runner_test"}
    exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), namespace)
    namespace["parse_args"] = lambda: SimpleNamespace(
        stage=None,
        metadata_path="metadata.json",
        watermark_base_path=".runtime/dev/watermarks",
        base_log_path=".runtime/dev/logs",
        dry_run=False,
        max_workers=1,
    )
    with pytest.raises(RuntimeError, match="engine failed"):
        namespace["main"]()
    assert spark.stop_calls == 1

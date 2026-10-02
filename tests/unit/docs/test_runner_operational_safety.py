"""Executable safety checks for operational runner lifecycle behavior."""

from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
RUNNERS = PRODUCT_ROOT / "docs" / "examples" / "files" / "runners"


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
    builder.config = lambda *_args: builder
    builder.getOrCreate = lambda: spark

    class FailingEngine:
        def __init__(self, **_kwargs: object) -> None:
            raise RuntimeError("engine failed")

    modules = {
        "pyspark": _module("pyspark"),
        "pyspark.sql": _module("pyspark.sql", SparkSession=SimpleNamespace(builder=builder)),
        "datacoolie": _module("datacoolie"),
        "datacoolie.core": _module("datacoolie.core"),
        "datacoolie.core.models.run_config": _module(
            "datacoolie.core.models.run_config", DataCoolieRunConfig=object
        ),
        "datacoolie.logging": _module("datacoolie.logging", LogConfig=object),
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

    path = RUNNERS / "local" / "run_spark.py"
    namespace = {"__name__": "runner_test"}
    exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), namespace)
    namespace["parse_args"] = lambda: SimpleNamespace(
        stage=None,
        metadata_path="metadata.json",
        watermark_base_path=".runtime/dev/watermarks",
        log_base_path=".runtime/dev/logs",
        artifact_base_path=None,
        metadata_base_path=None,
        connections_path=None,
        schema_hints_path=None,
        sql_base_path=[],
        state_base_path=None,
        log_persistence_mode="snapshot",
        log_flush_interval_seconds=300.0,
        log_flush_batch_bytes=4 * 1024 * 1024,
        log_console_color="auto",
        working_directory=None,
        run_attributes_json={},
        dry_run=False,
        max_workers=1,
        job_num=1,
        job_index=0,
    )
    with pytest.raises(RuntimeError, match="engine failed"):
        namespace["main"]()
    assert spark.stop_calls == 1

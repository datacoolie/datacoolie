"""Contracts for the private Spark session builder."""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from typing import Any

import pytest


ENGINES_DIR = Path(__file__).resolve().parents[4] / "src" / "datacoolie" / "engines"
MODULE_NAME = "datacoolie.engines._spark.session_builder"


class _Builder:
    def __init__(self) -> None:
        self.app_name: str | None = None
        self.configs: list[tuple[str, str]] = []
        self.session = object()

    def appName(self, value: str) -> _Builder:  # noqa: N802
        self.app_name = value
        return self

    def config(self, key: str, value: str) -> _Builder:
        self.configs.append((key, value))
        return self

    def getOrCreate(self) -> object:  # noqa: N802
        return self.session


def _load_module(monkeypatch: pytest.MonkeyPatch, builder: _Builder) -> Any:
    fake_pyspark = types.ModuleType("pyspark")
    fake_sql = types.ModuleType("pyspark.sql")

    class FakeSparkSession:
        pass

    FakeSparkSession.builder = builder  # type: ignore[attr-defined]
    fake_sql.SparkSession = FakeSparkSession  # type: ignore[attr-defined]
    fake_pyspark.sql = fake_sql  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.delitem(sys.modules, MODULE_NAME, raising=False)
    return importlib.import_module(MODULE_NAME)


def test_new_session_applies_defaults_then_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = _Builder()
    module = _load_module(monkeypatch, builder)

    result = module.get_or_create_spark_session(
        app_name="tests",
        config={"spark.sql.adaptive.enabled": "false", "custom.key": "value"},
    )

    assert result is builder.session
    assert builder.app_name == "tests"
    assert dict(builder.configs)["spark.sql.adaptive.enabled"] == "false"
    assert dict(builder.configs)["custom.key"] == "value"
    assert "spark.sql.parquet.int96RebaseModeInRead" in dict(builder.configs)


def test_existing_session_is_reused_and_config_failures_are_ignored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_module(monkeypatch, _Builder())
    applied: list[tuple[str, str]] = []

    class Conf:
        def set(self, key: str, value: str) -> None:
            applied.append((key, value))
            if key == "custom.fail":
                raise RuntimeError("immutable setting")

    existing = types.SimpleNamespace(conf=Conf())

    result = module.get_or_create_spark_session(
        config={"custom.fail": "x", "custom.ok": "y"},
        existing_session=existing,
    )

    assert result is existing
    assert ("custom.fail", "x") in applied
    assert ("custom.ok", "y") in applied


def test_flat_session_builder_module_is_removed() -> None:
    assert not (ENGINES_DIR / "spark_session_builder.py").exists()
    sys.modules.pop("datacoolie.engines.spark_session_builder", None)
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("datacoolie.engines.spark_session_builder")

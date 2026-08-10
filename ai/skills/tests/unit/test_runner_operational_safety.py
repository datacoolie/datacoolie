"""Executable safety checks for maintenance preview and local Spark lifecycle."""

from __future__ import annotations

import ast
import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest


AI_DIR = Path(__file__).resolve().parents[3]
RUNNERS = AI_DIR / "skills" / "datacoolie-build" / "templates" / "runners"


def _function(path: Path, name: str) -> object:
    if path.name.endswith(".ipynb.example"):
        notebook = json.loads(path.read_text(encoding="utf-8"))
        source = "\n".join(
            "".join(cell["source"])
            for cell in notebook["cells"]
            if cell["cell_type"] == "code"
        )
    else:
        source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )
    namespace: dict[str, object] = {}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
    return namespace[name]


def _target(key: str, name: str, *, secret: str) -> SimpleNamespace:
    connection = SimpleNamespace(name="lakehouse", format="delta", configure={"token": secret})
    destination = SimpleNamespace(
        connection=connection,
        destination_key=key,
        full_table_name=f"catalog.schema.{name}",
        path=f"abfss://container/{name}",
    )
    return SimpleNamespace(name=name, dataflow_id=f"id-{name}", destination=destination)


@pytest.mark.parametrize(
    "path",
    [
        RUNNERS / "maintenance_local_polars.py.example",
        RUNNERS / "maintenance_databricks_spark.ipynb.example",
    ],
    ids=lambda path: path.name,
)
def test_maintenance_preview_is_sorted_physical_and_non_secret(path: Path) -> None:
    build = _function(path, "build_maintenance_preview")
    preview = build(
        [_target("table:z", "z", secret="do-not-print"), _target("table:a", "a", secret="hidden")],
        do_compact=True,
        do_cleanup=False,
        retention_hours=168,
    )
    assert [item["destination_key"] for item in preview] == ["table:a", "table:z"]
    assert preview[0]["operations"] == ["compact"]
    assert preview[0]["table"] == "catalog.schema.a"
    serialized = json.dumps(preview)
    assert "secret" not in serialized
    assert "do-not-print" not in serialized


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
        stage_groups=None,
        metadata_path="metadata.json",
        watermark_base_path=".runtime/dev/watermarks",
        base_log_path=".runtime/dev/logs",
        dry_run=False,
        max_workers=1,
    )
    with pytest.raises(RuntimeError, match="engine failed"):
        namespace["main"]()
    assert spark.stop_calls == 1

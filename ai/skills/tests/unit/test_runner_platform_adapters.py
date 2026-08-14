"""Executable checks for platform-specific runner parameter adapters."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest


AI_DIR = Path(__file__).resolve().parents[3]
RUNNERS = AI_DIR / "skills" / "datacoolie-build" / "templates" / "runners"
NOTEBOOKS = tuple(sorted(RUNNERS.glob("*.ipynb.example")))
RUNNER_TEMPLATES = tuple(sorted(RUNNERS.glob("*.example")))


def _notebook(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _notebook_code(path: Path) -> str:
    return "\n".join(
        "".join(cell["source"])
        for cell in _notebook(path)["cells"]
        if cell["cell_type"] == "code"
    )


def _notebook_functions(path: Path, names: set[str], **extra: object) -> dict[str, object]:
    tree = ast.parse(_notebook_code(path))
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    namespace: dict[str, object] = {"json": json, "re": re, **extra}
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(path), "exec"), namespace)
    return namespace


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda path: path.name)
def test_notebooks_parse_have_one_parameter_cell_and_do_not_install(path: Path) -> None:
    notebook = _notebook(path)
    parameter_cells = [
        cell
        for cell in notebook["cells"]
        if "parameters" in cell.get("metadata", {}).get("tags", [])
    ]
    assert len(parameter_cells) == 1
    code = _notebook_code(path)
    ast.parse(code)
    assert "%pip" not in code
    assert "force-reinstall" not in code
    assert "restartPython" not in code


@pytest.mark.parametrize("path", RUNNER_TEMPLATES, ids=lambda path: path.name)
def test_runners_delegate_framework_owned_validation(path: Path) -> None:
    content = _notebook_code(path) if ".ipynb." in path.name else path.read_text(encoding="utf-8")
    for stale in (
        "non_empty_stage",
        "require_persistent_path",
        "PurePosixPath",
        "urlparse",
        "stage must not be blank",
        "mutable state path must be outside",
        "parse_chunk_interval",
        "CHUNK_KEYS",
        "positive_int",
        "parse_connections",
        "CONNECTIONS_JSON",
        "build_maintenance_preview",
        "load_maintenance_dataflows",
        "inspect-only",
        "INSPECT_ONLY",
    ):
        assert stale not in content


@pytest.mark.parametrize(
    "path",
    [
        RUNNERS / "maintenance_local_polars.py.example",
        RUNNERS / "maintenance_databricks_spark.ipynb.example",
    ],
    ids=lambda path: path.name,
)
def test_maintenance_runner_delegates_once(path: Path) -> None:
    content = (
        _notebook_code(path)
        if ".ipynb." in path.name
        else path.read_text(encoding="utf-8")
    )
    assert content.count("driver.run_maintenance(") == 1


class _Widgets:
    def __init__(self, supplied: dict[str, str]) -> None:
        self.values = dict(supplied)

    def text(self, name: str, default: str) -> None:
        self.values.setdefault(name, default)

    def get(self, name: str) -> str:
        return self.values[name]


def test_databricks_widget_preserves_one_stage_string() -> None:
    widgets = _Widgets({"STAGE": "a,b"})
    functions = _notebook_functions(
        RUNNERS / "run_databricks_spark.ipynb.example",
        {"widget_value"},
        dbutils=SimpleNamespace(widgets=widgets),
    )
    assert functions["widget_value"]("STAGE", "") == "a,b"
    assert functions["widget_value"]("METADATA_PATH", "metadata.json") == "metadata.json"


def test_fabric_parameter_cell_exposes_one_optional_stage() -> None:
    path = RUNNERS / "run_fabric_spark.ipynb.example"
    parameter_cell = next(
        cell
        for cell in _notebook(path)["cells"]
        if "parameters" in cell.get("metadata", {}).get("tags", [])
    )
    values: dict[str, object] = {}
    exec("".join(parameter_cell["source"]), values)
    assert values["STAGE"] == ""
    code = _notebook_code(path)
    assert "driver.run(stage=STAGE)" in code
    assert "parse_stage_plan" not in code


def test_replay_widget_decodes_transport_and_delegates_replay_semantics() -> None:
    path = RUNNERS / "replay_databricks_spark.ipynb.example"
    functions = _notebook_functions(
        path,
        {
            "parse_bool",
            "decode_boundary",
            "validate_watermark_request",
        },
    )
    assert functions["parse_bool"]("true") is True
    assert functions["decode_boundary"]("42") == 42
    assert functions["decode_boundary"]("2026-01-01") == "2026-01-01"
    with pytest.raises(ValueError, match="requires CONFIRM_SAVE_WATERMARK"):
        functions["validate_watermark_request"](True, False)
    code = _notebook_code(path)
    assert 'start = decode_boundary(widget_value("START", START))' in code
    assert 'end = decode_boundary(widget_value("END", END))' in code
    assert 'chunk_interval = json.loads(widget_value("CHUNK_INTERVAL_JSON"' in code
    assert "parse_chunk_interval" not in code


def test_maintenance_widget_decoders_and_mutation_gate() -> None:
    path = RUNNERS / "maintenance_databricks_spark.ipynb.example"
    functions = _notebook_functions(
        path,
        {
            "parse_bool",
            "validate_maintenance_request",
        },
    )
    with pytest.raises(ValueError, match="requires CONFIRM_MAINTENANCE"):
        functions["validate_maintenance_request"](True, True, False)
    with pytest.raises(ValueError, match="compact, cleanup, or both"):
        functions["validate_maintenance_request"](False, False, True)
    code = _notebook_code(path)
    assert 'connection = widget_value("CONNECTION", CONNECTION) or None' in code
    assert 'retention_hours = int(widget_value("RETENTION_HOURS"' in code
    assert "parse_connections" not in code
    assert "parse_positive_int" not in code
    assert "build_maintenance_preview" not in code
    assert "load_maintenance_dataflows" not in code
    assert "INSPECT_ONLY" not in code


def test_glue_stage_is_optional() -> None:
    requested: list[list[str]] = []

    def fake_resolve(_argv: list[str], options: list[str]) -> dict[str, str]:
        requested.append(options)
        return {name: "value" for name in options}

    path = RUNNERS / "run_aws_glue_spark.py.example"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "resolve_options"
    )
    namespace = {"getResolvedOptions": fake_resolve}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
    namespace["resolve_options"](["job.py"])
    assert "STAGE" not in requested[-1]
    namespace["resolve_options"](["job.py", "--STAGE", "bronze,silver"])
    assert "STAGE" in requested[-1]
    assert "JOB_NAME" not in requested[-1]

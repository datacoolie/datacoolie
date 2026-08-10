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


class _Widgets:
    def __init__(self, supplied: dict[str, str]) -> None:
        self.values = dict(supplied)

    def text(self, name: str, default: str) -> None:
        self.values.setdefault(name, default)

    def get(self, name: str) -> str:
        return self.values[name]


def test_databricks_widget_and_stage_json_adapters_preserve_groups() -> None:
    widgets = _Widgets({"STAGE_GROUPS_JSON": '["a,b", ["c", "d"]]'})
    functions = _notebook_functions(
        RUNNERS / "run_databricks_spark.ipynb.example",
        {"widget_value", "parse_stage_plan"},
        dbutils=SimpleNamespace(widgets=widgets),
    )
    value = functions["widget_value"]("STAGE_GROUPS_JSON", "[]")
    assert functions["parse_stage_plan"](value) == ["a,b", ["c", "d"]]
    assert functions["widget_value"]("METADATA_PATH", "metadata.json") == "metadata.json"
    with pytest.raises(ValueError, match="invalid stage group"):
        functions["parse_stage_plan"]('[" "]')


def test_fabric_parameter_cell_uses_json_string_for_stage_groups() -> None:
    path = RUNNERS / "run_fabric_spark.ipynb.example"
    parameter_cell = next(
        cell
        for cell in _notebook(path)["cells"]
        if "parameters" in cell.get("metadata", {}).get("tags", [])
    )
    values: dict[str, object] = {}
    exec("".join(parameter_cell["source"]), values)
    assert values["STAGE_GROUPS_JSON"] == "[]"
    parse = _notebook_functions(path, {"parse_stage_plan"})["parse_stage_plan"]
    assert parse('["bronze", ["silver_a", "silver_b"]]') == [
        "bronze",
        ["silver_a", "silver_b"],
    ]


def test_replay_widget_decoders_preserve_types_and_validate_chunks() -> None:
    functions = _notebook_functions(
        RUNNERS / "replay_databricks_spark.ipynb.example",
        {
            "parse_bool",
            "parse_boundary",
            "parse_stage_plan",
            "parse_chunk_interval",
            "validate_watermark_request",
        },
        CHUNK_KEYS={"years", "months", "weeks", "days", "hours", "minutes", "step"},
    )
    assert functions["parse_bool"]("true") is True
    assert functions["parse_boundary"]("42") == 42
    assert functions["parse_boundary"]("2026-01-01") == "2026-01-01"
    assert functions["parse_chunk_interval"]('{"days": 1}') == {"days": 1}
    assert functions["parse_chunk_interval"]("null") is None
    with pytest.raises(ValueError, match="positive integer"):
        functions["parse_chunk_interval"]('{"days": 0}')
    with pytest.raises(ValueError, match="requires CONFIRM_SAVE_WATERMARK"):
        functions["validate_watermark_request"](True, False)


def test_maintenance_widget_decoders_and_mutation_gate() -> None:
    functions = _notebook_functions(
        RUNNERS / "maintenance_databricks_spark.ipynb.example",
        {
            "parse_bool",
            "parse_positive_int",
            "parse_connections",
            "validate_maintenance_request",
        },
    )
    assert functions["parse_connections"]('["lakehouse_a", "lakehouse_b"]') == [
        "lakehouse_a",
        "lakehouse_b",
    ]
    assert functions["parse_positive_int"]("168", "RETENTION_HOURS") == 168
    with pytest.raises(ValueError, match="requires CONFIRM_MAINTENANCE"):
        functions["validate_maintenance_request"](True, True, False, False)
    with pytest.raises(ValueError, match="compact, cleanup, or both"):
        functions["validate_maintenance_request"](False, False, True, False)


def test_glue_stage_groups_are_optional() -> None:
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
    assert "STAGE_GROUPS_JSON" not in requested[-1]
    namespace["resolve_options"](["job.py", "--STAGE_GROUPS_JSON", "[]"])
    assert "STAGE_GROUPS_JSON" in requested[-1]
    assert "JOB_NAME" not in requested[-1]

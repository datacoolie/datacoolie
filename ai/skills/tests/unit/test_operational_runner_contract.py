"""Contract checks for replay and maintenance runner templates."""

from __future__ import annotations

import ast
import argparse
import json
import re
import sys
from pathlib import Path

import pytest


AI_DIR = Path(__file__).resolve().parents[3]
RUNNERS = AI_DIR / "skills" / "datacoolie-build" / "templates" / "runners"


def _load_functions(path: Path, names: set[str]) -> dict[str, object]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    namespace: dict[str, object] = {
        "argparse": argparse,
        "json": json,
        "re": re,
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(path), "exec"), namespace)
    return namespace


def _notebook_code(path: Path) -> str:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    assert notebook["nbformat"] == 4
    parameter_cells = [
        cell
        for cell in notebook["cells"]
        if "parameters" in cell.get("metadata", {}).get("tags", [])
    ]
    assert len(parameter_cells) == 1
    sources = [
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    ]
    return "\n".join(source for source in sources if not source.lstrip().startswith("%"))


@pytest.mark.parametrize(
    ("name", "required"),
    [
        (
            "replay_local_polars.py.example",
            (
                "ReplayConfig",
                "--chunk-interval-json",
                "driver.load_dataflows(stage=args.stage)",
                "driver.run_replay(dataflows=dataflows, replay=replay)",
                "--confirm-save-watermark",
                "--watermark-base-path",
                "--base-log-path",
            ),
        ),
        (
            "maintenance_local_polars.py.example",
            (
                "driver.run_maintenance(",
                "--confirm-maintenance",
                "--retention-hours",
                "--watermark-base-path",
                "--base-log-path",
            ),
        ),
    ],
)
def test_python_operation_templates_parse_and_use_framework(
    name: str, required: tuple[str, ...]
) -> None:
    content = (RUNNERS / name).read_text(encoding="utf-8")
    ast.parse(content)
    for token in required:
        assert token in content
    assert "--env" not in content
    assert "--engine" not in content


@pytest.mark.parametrize(
    ("name", "required"),
    [
        (
            "replay_databricks_spark.ipynb.example",
            (
                "CONFIRM_SAVE_WATERMARK = \"false\"",
                "CHUNK_INTERVAL_JSON",
                "dbutils.widgets.get",
                "driver.load_dataflows(stage=stage)",
                "driver.run_replay(dataflows=dataflows, replay=replay)",
                "WATERMARK_BASE_PATH",
                "BASE_LOG_PATH",
            ),
        ),
        (
            "maintenance_databricks_spark.ipynb.example",
            (
                "CONFIRM_MAINTENANCE = \"false\"",
                "CONNECTION = \"\"",
                "driver.run_maintenance(",
                "RETENTION_HOURS",
                "WATERMARK_BASE_PATH",
                "BASE_LOG_PATH",
            ),
        ),
    ],
)
def test_notebook_operation_templates_parse_and_gate_mutation(
    name: str, required: tuple[str, ...]
) -> None:
    content = _notebook_code(RUNNERS / name)
    ast.parse(content)
    for token in required:
        assert token in content
    assert "DRY_RUN" not in content


def test_operations_contract_requires_direct_confirmed_maintenance() -> None:
    contract = (
        AI_DIR
        / "skills"
        / "datacoolie-build"
        / "references"
        / "operations-contract.md"
    ).read_text(encoding="utf-8")
    assert "maintenance mutation confirmation" in contract
    assert "Do not add runner-owned preview" in contract


def test_replay_watermark_mutation_requires_separate_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = RUNNERS / "replay_local_polars.py.example"
    namespace = _load_functions(path, {"decode_boundary", "parse_args"})
    parse_args = namespace["parse_args"]
    base = [
        "replay",
        "--metadata-path",
        "metadata.json",
        "--watermark-base-path",
        ".runtime/dev/watermarks",
        "--base-log-path",
        ".runtime/dev/logs",
        "--start",
        "1",
        "--end",
        "10",
        "--chunk-interval-json",
        '{"days": 1}',
        "--save-watermark",
    ]
    monkeypatch.setattr(sys, "argv", base)
    with pytest.raises(SystemExit, match="2"):
        parse_args()  # type: ignore[operator]

    monkeypatch.setattr(sys, "argv", [*base, "--confirm-save-watermark"])
    args = parse_args()  # type: ignore[operator]
    assert args.start == 1
    assert args.end == 10
    assert args.chunk_interval == {"days": 1}
    assert args.save_watermark is True


def test_replay_passes_blank_stage_to_framework_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = RUNNERS / "replay_local_polars.py.example"
    namespace = _load_functions(path, {"decode_boundary", "parse_args"})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "replay",
            "--metadata-path",
            "metadata.json",
            "--watermark-base-path",
            ".runtime/dev/watermarks",
            "--base-log-path",
            ".runtime/dev/logs",
            "--start",
            "1",
            "--end",
            "10",
            "--stage",
            "   ",
        ],
    )
    assert namespace["parse_args"]().stage == "   "


def test_replay_parser_only_decodes_chunk_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = RUNNERS / "replay_local_polars.py.example"
    parse_args = _load_functions(path, {"decode_boundary", "parse_args"})["parse_args"]
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "replay",
            "--metadata-path",
            "metadata.json",
            "--watermark-base-path",
            ".runtime/dev/watermarks",
            "--base-log-path",
            ".runtime/dev/logs",
            "--start",
            "1",
            "--end",
            "10",
            "--chunk-interval-json",
            '{"days": 0}',
        ],
    )
    args = parse_args()  # type: ignore[operator]
    assert args.start == 1
    assert args.end == 10
    assert args.chunk_interval == {"days": 0}


def test_maintenance_mutation_requires_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = RUNNERS / "maintenance_local_polars.py.example"
    namespace = _load_functions(path, {"parse_args"})
    parse_args = namespace["parse_args"]
    base = [
        "maintenance",
        "--metadata-path",
        "metadata.json",
        "--watermark-base-path",
        ".runtime/dev/watermarks",
        "--base-log-path",
        ".runtime/dev/logs",
    ]
    monkeypatch.setattr(sys, "argv", base)
    with pytest.raises(SystemExit, match="2"):
        parse_args()  # type: ignore[operator]

    monkeypatch.setattr(
        sys,
        "argv",
        [*base, "--confirm-maintenance", "--retention-hours", "0", "--connection", "a,b"],
    )
    args = parse_args()  # type: ignore[operator]
    assert args.confirm_maintenance is True
    assert args.retention_hours == 0
    assert args.connection == "a,b"

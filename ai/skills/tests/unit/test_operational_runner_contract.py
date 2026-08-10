"""Contract checks for replay and maintenance runner templates."""

from __future__ import annotations

import ast
import argparse
import json
import re
import sys
from pathlib import Path
from pathlib import PurePosixPath
from urllib.parse import urlparse

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
        "re": re,
        "PurePosixPath": PurePosixPath,
        "urlparse": urlparse,
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
                "driver.load_dataflows(stage=stage_group)",
                "driver.run_replay(dataflows=dataflows, replay=replay)",
                "--confirm-save-watermark",
                "--watermark-base-path",
                "--base-log-path",
            ),
        ),
        (
            "maintenance_local_polars.py.example",
            (
                "driver.load_maintenance_dataflows(connection=args.connection)",
                "driver.run_maintenance(",
                "--inspect-only",
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
                "driver.load_dataflows(stage=stage_group)",
                "driver.run_replay(dataflows=dataflows, replay=replay)",
                "WATERMARK_BASE_PATH",
                "BASE_LOG_PATH",
            ),
        ),
        (
            "maintenance_databricks_spark.ipynb.example",
            (
                "INSPECT_ONLY = \"true\"",
                "CONFIRM_MAINTENANCE = \"false\"",
                "CONNECTIONS_JSON",
                "build_maintenance_preview",
                "driver.load_maintenance_dataflows(",
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


def test_operations_contract_does_not_claim_maintenance_dry_run() -> None:
    contract = (
        AI_DIR
        / "skills"
        / "datacoolie-build"
        / "references"
        / "operations-contract.md"
    ).read_text(encoding="utf-8")
    assert "load_maintenance_dataflows" in contract
    assert "requires an explicit confirmation" in contract
    assert "Do not expose or describe `dry_run` as maintenance protection" in contract


def test_replay_watermark_mutation_requires_separate_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = RUNNERS / "replay_local_polars.py.example"
    namespace = _load_functions(
        path,
        {
            "non_empty_stage",
            "parse_boundary",
            "parse_chunk_interval",
            "require_persistent_path",
            "parse_args",
        },
    )
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
        "--save-watermark",
    ]
    monkeypatch.setattr(sys, "argv", base)
    with pytest.raises(SystemExit, match="2"):
        parse_args()  # type: ignore[operator]

    monkeypatch.setattr(sys, "argv", [*base, "--confirm-save-watermark"])
    args = parse_args()  # type: ignore[operator]
    assert args.start == 1
    assert args.end == 10
    assert args.save_watermark is True


def test_replay_rejects_blank_stage_before_driver_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = RUNNERS / "replay_local_polars.py.example"
    namespace = _load_functions(
        path,
        {
            "non_empty_stage",
            "parse_boundary",
            "parse_chunk_interval",
            "require_persistent_path",
            "parse_args",
        },
    )
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
    with pytest.raises(SystemExit, match="2"):
        namespace["parse_args"]()


def test_maintenance_inspection_is_safe_default_path_and_mutation_is_gated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = RUNNERS / "maintenance_local_polars.py.example"
    namespace = _load_functions(
        path, {"positive_int", "require_persistent_path", "parse_args"}
    )
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

    monkeypatch.setattr(sys, "argv", [*base, "--inspect-only"])
    args = parse_args()  # type: ignore[operator]
    assert args.inspect_only is True
    assert args.confirm_maintenance is False

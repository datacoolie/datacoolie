"""Focused contracts for flat authored and in-memory case metadata."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys

FEATURES_ROOT = Path(__file__).resolve().parents[3] / "usecase-sim/scripts/features"
if str(FEATURES_ROOT) not in sys.path:
    sys.path.insert(0, str(FEATURES_ROOT))

from _stateful_support import materialize_fixture  # noqa: E402
import _stateful_support as support  # noqa: E402
import pytest  # noqa: E402
from _fixture_support import compile_existing  # noqa: E402


SIM_ROOT = Path(__file__).resolve().parents[3] / "usecase-sim"


def _case(tmp_path: Path, group: str) -> SimpleNamespace:
    return SimpleNamespace(
        group=group,
        root=tmp_path / "output-case",
        input=tmp_path / "input-case",
        output=tmp_path / "output-case",
        document={},
        sources=(),
    )


def test_authored_recipe_binds_case_paths_without_runtime_metadata_file(tmp_path: Path) -> None:
    case = _case(tmp_path, "write_basic")

    materialize_fixture(case, "append_csv.json")

    assert not (case.root / "metadata.json").exists()
    assert case.sources == (
        str((SIM_ROOT / "metadata/file/write_basic.json").resolve()),
    )
    connections = {row["name"]: row for row in case.document["connections"]}
    assert connections["flat_append_source"]["configure"]["base_path"] == case.input.as_posix()
    assert connections["flat_append_csv_destination"]["configure"]["base_path"] == case.output.as_posix()
    assert [row["name"] for row in case.document["dataflows"]] == ["flat-append-csv"]


def test_existing_flow_projection_keeps_authored_identity_and_source(tmp_path: Path) -> None:
    case = _case(tmp_path, "transform")
    document, identity = compile_existing(case, "transform", "row_filter__on_computed_col")

    assert identity["authored_flow"] == "row_filter__on_computed_col"
    assert identity["runtime_flow"] == identity["authored_flow"]
    assert identity["runtime_stage"] == document["dataflows"][0]["stage"]
    assert case.sources == (
        str((SIM_ROOT / "metadata/file/transform.json").resolve()),
    )
    assert case.document == document
    assert not list(tmp_path.rglob("metadata.json"))


def test_case_rejects_resolved_receipt_escape_before_creating_data(tmp_path, monkeypatch):
    data = tmp_path / "runtime/data"
    monkeypatch.setattr(support, "DATA_DIR", data)
    monkeypatch.setattr(support, "OUTPUT_DIR", data / "output")
    monkeypatch.setattr(support, "INPUT_DIR", data / "input")
    monkeypatch.setattr(support, "STATE_DIR", tmp_path / "runtime/state")
    monkeypatch.setattr(support, "LOG_DIR", tmp_path / "runtime/logs")
    monkeypatch.setattr(support, "WATERMARKS_DIR", tmp_path / "runtime/watermarks")
    original = support._mapped_runtime_root

    def redirected(root, base, *, scenario_logs=False):
        return tmp_path / "escaped" if scenario_logs else original(root, base)

    monkeypatch.setattr(support, "_mapped_runtime_root", redirected)
    with pytest.raises(ValueError, match="receipt root escaped"):
        support.resolve_case_root("demo")
    assert not data.exists()
    assert not (tmp_path / "escaped").exists()


def test_maintenance_case_provider_selects_only_the_requested_stage(tmp_path):
    from datacoolie.platforms.local_platform import LocalPlatform
    from runner.metadata_sources import make_case_provider

    case = _case(tmp_path, "maintenance")
    materialize_fixture(case, "maintenance.json")
    provider = make_case_provider(case, stage="maintenance_eligible", maintenance=True,
                                 platform=LocalPlatform(), watermark_base_path=str(tmp_path / "watermarks"))
    try:
        provider.initialize()
        flows = provider.get_dataflows(stage="maintenance_eligible")
        assert len(flows) == 1
        assert flows[0].stage == "maintenance_eligible"
    finally:
        provider.close()

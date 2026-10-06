"""Focused mutation checks for the date/mtime replay oracle."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest


_VALID_WATERMARK = {
    "__date_folder_partition__": {
        "__datetime__": "2024-01-16T00:00:00+00:00"
    },
    "__file_modification_time": {
        "__datetime__": "2024-01-16T12:00:00+00:00"
    },
}


def _load_oracle() -> ModuleType:
    script = (
        Path(__file__).resolve().parents[3]
        / "usecase-sim"
        / "scripts"
        / "features"
        / "replay"
        / "validate_date_mtime_replay_output.py"
    )
    spec = importlib.util.spec_from_file_location("date_mtime_replay_oracle", script)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Cannot load replay oracle from {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ORACLE = _load_oracle()


def _write_watermark(state_root: Path, payload: object) -> Path:
    path = ORACLE._case_watermark_path(state_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_case_watermark_accepts_exact_normalized_expected_value(tmp_path: Path) -> None:
    _write_watermark(tmp_path, _VALID_WATERMARK)
    actual = ORACLE._assert_case_watermark(tmp_path)

    assert actual == {
        "__date_folder_partition__": "2024-01-16T00:00:00+00:00",
        "__file_modification_time": "2024-01-16T12:00:00+00:00",
    }


def test_case_watermark_rejects_absent_owner_even_when_decoy_has_expected_value(
    tmp_path: Path,
) -> None:
    decoy = tmp_path / "watermarks" / "unrelated" / "watermark_value.json"
    decoy.parent.mkdir(parents=True)
    decoy.write_text(json.dumps(_VALID_WATERMARK), encoding="utf-8")

    with pytest.raises(AssertionError, match="case-owned"):
        ORACLE._assert_case_watermark(tmp_path)


@pytest.mark.parametrize(
    "mtime",
    [
        pytest.param(None, id="null"),
        pytest.param(
            {"__datetime__": "2024-01-16T11:59:59+00:00"},
            id="stale",
        ),
        pytest.param(
            {"__datetime__": "2024-01-16T12:00:01+00:00"},
            id="wrong",
        ),
    ],
)
def test_case_watermark_rejects_null_stale_or_wrong_checkpoint(
    tmp_path: Path, mtime: object
) -> None:
    payload = {
        "__date_folder_partition__": _VALID_WATERMARK["__date_folder_partition__"],
        "__file_modification_time": mtime,
    }
    _write_watermark(tmp_path, payload)

    with pytest.raises(AssertionError, match="Unexpected case-owned"):
        ORACLE._assert_case_watermark(tmp_path)

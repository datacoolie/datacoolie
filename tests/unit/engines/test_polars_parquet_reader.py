"""Regression coverage for Spark directory markers in Polars Parquet reads."""

from __future__ import annotations

from pathlib import Path

import pytest


def test_read_parquet_directory_ignores_checksum_markers(tmp_path: Path) -> None:
    polars = pytest.importorskip("polars")
    from datacoolie.engines.polars_engine import PolarsEngine

    root = tmp_path / "spark-output"
    root.mkdir()
    polars.DataFrame({"id": [1, 2]}).write_parquet(root / "part-00000.parquet")
    (root / ".part-00000.parquet.crc").write_bytes(b"checksum")
    (root / "_SUCCESS").write_bytes(b"")

    result = PolarsEngine().read_parquet(str(root)).collect()

    assert result["id"].to_list() == [1, 2]

"""Tests for named Polars Iceberg option routing."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from datacoolie.core.exceptions import EngineError
from datacoolie.engines._polars.iceberg import operations
from datacoolie.engines.polars_engine import PolarsEngine


def test_named_iceberg_write_rejects_unmapped_options_before_backend_call() -> None:
    engine = PolarsEngine()
    engine.set_iceberg_catalog(MagicMock())
    frame = pl.DataFrame({"id": [1]}).lazy()

    with patch.object(operations, "write_table") as write_table:
        with pytest.raises(EngineError, match="does not support write_options"):
            engine.write_to_table(
                frame,
                "namespace.table",
                mode="append",
                fmt="iceberg",
                options={"schema_mode": "merge"},
            )

    write_table.assert_not_called()

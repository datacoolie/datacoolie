from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.exceptions import EngineError
from datacoolie.engines._spark import delta


def test_merge_to_path_rejects_non_delta_format() -> None:
    with pytest.raises(EngineError, match="only supports delta"):
        delta.merge_to_path(MagicMock(), MagicMock(), "/table", ["id"], "parquet")


def test_merge_overwrite_always_unpersists() -> None:
    df = MagicMock()
    with (
        patch.object(delta.runtime, "safe_cache", return_value=df),
        patch.object(delta, "delta_table", side_effect=RuntimeError("boom")),
        patch.object(delta.runtime, "safe_unpersist") as unpersist,
        pytest.raises(RuntimeError, match="boom"),
    ):
        delta.merge_overwrite_to_path(
            MagicMock(), df, "/table", ["id"], "delta", None, None
        )
    unpersist.assert_called_once_with(df)

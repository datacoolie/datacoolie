"""Constructor contracts for plugin leaves that must reject unknown options."""

from __future__ import annotations

import pytest

from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.platforms.aws_platform import AWSPlatform
from datacoolie.platforms.databricks_platform import DatabricksPlatform
from datacoolie.platforms.fabric_platform import FabricPlatform
from datacoolie.platforms.local_platform import LocalPlatform


@pytest.mark.parametrize(
    "constructor",
    [LocalPlatform, AWSPlatform, FabricPlatform, DatabricksPlatform, PolarsEngine],
)
def test_leaf_constructor_rejects_unknown_keyword(constructor) -> None:
    with pytest.raises(TypeError, match="unsupported_option"):
        constructor(unsupported_option=True)

"""Opt-in contract test intended to run inside a Databricks notebook or job."""

from __future__ import annotations

from pathlib import Path

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms.databricks_platform import DatabricksPlatform
from tests.integration.cloud_config import DatabricksIntegrationConfig
from tests.integration.platforms.databricks._contract import exercise_file_contract

pytestmark = [
    pytest.mark.integration,
    pytest.mark.cloud_integration,
    pytest.mark.cloud_platform("databricks"),
]


def test_native_dbutils_file_contract(
    databricks_integration_config: DatabricksIntegrationConfig,
    tmp_path: Path,
) -> None:
    try:
        platform = DatabricksPlatform(runtime="databricks")
        _ = platform.dbutils
    except PlatformError as exc:
        pytest.skip(f"Native Databricks runtime is unavailable: {exc}")
    exercise_file_contract(
        platform,
        databricks_integration_config.volume_root,
        databricks_integration_config.prefix,
        tmp_path,
    )

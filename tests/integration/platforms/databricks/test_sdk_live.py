"""Opt-in live contract tests for Databricks SDK Volume access."""

from __future__ import annotations

from pathlib import Path

import pytest

from datacoolie.platforms.databricks_platform import DatabricksPlatform
from tests.integration.cloud_config import DatabricksIntegrationConfig
from tests.integration.platforms.databricks._contract import exercise_file_contract

pytestmark = [
    pytest.mark.integration,
    pytest.mark.cloud_integration,
    pytest.mark.cloud_platform("databricks"),
]


def _external_platform(config: DatabricksIntegrationConfig) -> DatabricksPlatform:
    from databricks.sdk import WorkspaceClient

    return DatabricksPlatform(
        runtime="external",
        workspace_client=WorkspaceClient(host=config.host),
    )


def test_external_sdk_file_contract(
    databricks_integration_config: DatabricksIntegrationConfig,
    tmp_path: Path,
) -> None:
    exercise_file_contract(
        _external_platform(databricks_integration_config),
        databricks_integration_config.volume_root,
        databricks_integration_config.prefix,
        tmp_path,
    )


def test_external_sdk_secret_contract(
    databricks_integration_config: DatabricksIntegrationConfig,
) -> None:
    scope = databricks_integration_config.secret_scope
    key = databricks_integration_config.secret_key
    if not scope or not key:
        pytest.skip("Databricks secret live-test coordinates are not configured")
    value = _external_platform(databricks_integration_config).get_secret(key, scope)
    assert isinstance(value, str)
    assert value

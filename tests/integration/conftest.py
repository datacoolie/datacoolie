"""Shared resource-coordinate fixtures for cloud integration tests."""

from __future__ import annotations

import pytest

from tests.integration.cloud_config import (
    AwsIntegrationConfig,
    DatabricksIntegrationConfig,
    FabricIntegrationConfig,
    MissingCloudVariables,
    resolve_aws_config,
    resolve_databricks_config,
    resolve_fabric_config,
)


def _resolve_or_skip(factory):
    try:
        return factory()
    except MissingCloudVariables as exc:
        pytest.skip(str(exc))


@pytest.fixture(scope="session")
def fabric_integration_config() -> FabricIntegrationConfig:
    return _resolve_or_skip(resolve_fabric_config)


@pytest.fixture(scope="session")
def aws_integration_config() -> AwsIntegrationConfig:
    return _resolve_or_skip(resolve_aws_config)


@pytest.fixture(scope="session")
def databricks_integration_config() -> DatabricksIntegrationConfig:
    return _resolve_or_skip(resolve_databricks_config)

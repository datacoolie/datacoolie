"""Public DatabricksPlatform facade tests."""

from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.core.secret_provider import BaseSecretProvider
from datacoolie.platforms._databricks.dbutils_backend import DbutilsBackend
from datacoolie.platforms._databricks.sdk_backend import DatabricksSdkBackend
from datacoolie.platforms.databricks_platform import DatabricksPlatform
from tests.unit.platforms.databricks._fakes import FakeWorkspaceClient


def test_auto_prefers_injected_native_backend() -> None:
    dbutils = MagicMock()
    platform = DatabricksPlatform(dbutils=dbutils)
    assert isinstance(platform._get_backend(), DbutilsBackend)
    assert platform.dbutils is dbutils
    assert platform.fs is dbutils.fs


def test_auto_selects_external_without_native_runtime() -> None:
    client = FakeWorkspaceClient()
    with patch(
        "datacoolie.platforms._databricks.runtime.try_resolve_dbutils",
        return_value=None,
    ):
        platform = DatabricksPlatform(workspace_client=client)
        assert isinstance(platform._get_backend(), DatabricksSdkBackend)
    with pytest.raises(PlatformError, match="runtime='databricks'"):
        _ = platform.dbutils


def test_explicit_external_does_not_probe_native() -> None:
    client = FakeWorkspaceClient()
    with patch(
        "datacoolie.platforms._databricks.runtime.try_resolve_dbutils"
    ) as resolver:
        platform = DatabricksPlatform(runtime="external", workspace_client=client)
        platform.write_file(
            "/Volumes/main/default/logs/a.txt",
            "value",
        )
    resolver.assert_not_called()


def test_external_secret_uses_workspace_client_dbutils() -> None:
    platform = DatabricksPlatform(
        runtime="external",
        workspace_client=FakeWorkspaceClient(),
    )
    assert platform.get_secret("key", "scope") == "scope:key"
    assert isinstance(platform, BaseSecretProvider)


def test_registry_still_exposes_public_platform() -> None:
    from datacoolie import platform_registry

    assert platform_registry.is_available("databricks")

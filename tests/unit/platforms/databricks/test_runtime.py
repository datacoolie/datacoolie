"""Databricks runtime selection tests."""

from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._databricks.runtime import resolve_runtime, validate_runtime


def test_validate_runtime_rejects_unknown_value() -> None:
    with pytest.raises(PlatformError, match="Invalid Databricks runtime"):
        validate_runtime("local")


def test_external_never_probes_native_runtime() -> None:
    with patch(
        "datacoolie.platforms._databricks.runtime.try_resolve_dbutils"
    ) as resolver:
        assert resolve_runtime("external") == ("external", None)
    resolver.assert_not_called()


def test_auto_prefers_injected_dbutils_without_probing() -> None:
    dbutils = MagicMock()
    with patch(
        "datacoolie.platforms._databricks.runtime.try_resolve_dbutils"
    ) as resolver:
        assert resolve_runtime("auto", injected_dbutils=dbutils) == (
            "databricks",
            dbutils,
        )
    resolver.assert_not_called()


def test_auto_uses_discovered_dbutils() -> None:
    dbutils = MagicMock()
    with patch(
        "datacoolie.platforms._databricks.runtime.try_resolve_dbutils",
        return_value=dbutils,
    ):
        assert resolve_runtime("auto") == ("databricks", dbutils)


def test_auto_uses_external_when_dbutils_is_unavailable() -> None:
    with patch(
        "datacoolie.platforms._databricks.runtime.try_resolve_dbutils",
        return_value=None,
    ):
        assert resolve_runtime("auto") == ("external", None)


def test_explicit_native_requires_dbutils() -> None:
    with (
        patch(
            "datacoolie.platforms._databricks.runtime.try_resolve_dbutils",
            return_value=None,
        ),
        pytest.raises(PlatformError, match="Cannot resolve dbutils"),
    ):
        resolve_runtime("databricks")

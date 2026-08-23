"""Runtime selection tests for the portable Fabric platform."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._fabric.runtime import (
    resolve_runtime,
    validate_runtime,
)


def _notebookutils() -> SimpleNamespace:
    fs = SimpleNamespace(
        append=MagicMock(),
        cp=MagicMock(),
        exists=MagicMock(),
        ls=MagicMock(),
        mkdirs=MagicMock(),
        mv=MagicMock(),
        put=MagicMock(),
        rm=MagicMock(),
    )
    return SimpleNamespace(fs=fs, credentials=SimpleNamespace(getSecret=MagicMock()))


def test_validate_runtime_rejects_unknown_value() -> None:
    with pytest.raises(PlatformError, match="Invalid Fabric runtime"):
        validate_runtime("azure")


def test_import_success_is_sufficient_for_native_runtime() -> None:
    incomplete_module = SimpleNamespace()
    with patch(
        "datacoolie.platforms._fabric.runtime.import_module",
        return_value=incomplete_module,
    ):
        assert resolve_runtime("auto") == "fabric"


def test_auto_prefers_usable_notebookutils_without_calling_services() -> None:
    module = _notebookutils()
    with patch(
        "datacoolie.platforms._fabric.runtime.import_module",
        return_value=module,
    ):
        assert resolve_runtime("auto") == "fabric"
    for value in vars(module.fs).values():
        value.assert_not_called()
    module.credentials.getSecret.assert_not_called()


def test_auto_uses_external_when_notebookutils_is_missing() -> None:
    with patch(
        "datacoolie.platforms._fabric.runtime.import_module",
        side_effect=ImportError,
    ):
        assert resolve_runtime("auto") == "external"


def test_explicit_external_does_not_probe_notebookutils() -> None:
    with patch("datacoolie.platforms._fabric.runtime.import_module") as importer:
        assert resolve_runtime("external") == "external"
    importer.assert_not_called()


def test_explicit_fabric_requires_notebookutils() -> None:
    with (
        patch(
            "datacoolie.platforms._fabric.runtime.import_module",
            side_effect=ImportError,
        ),
        pytest.raises(PlatformError, match="notebookutils"),
    ):
        resolve_runtime("fabric")

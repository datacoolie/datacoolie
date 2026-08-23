from unittest.mock import MagicMock

import polars as pl
import pytest

from datacoolie.core.exceptions import EngineError
from datacoolie.engines._polars.registration import register_delta_tables
from datacoolie.engines._polars.relations import PolarsRelationRegistry


def test_delta_discovery_indexes_without_loading_by_default() -> None:
    platform = MagicMock()
    platform.list_folders.return_value = ["/root/orders"]
    platform.folder_exists.return_value = True
    loader = MagicMock(return_value=pl.DataFrame({"id": [1]}).lazy())
    registry = PolarsRelationRegistry()

    names = register_delta_tables(
        platform=platform,
        sql_context=pl.SQLContext(),
        registry=registry,
        loader_factory=loader,
        base_path="/root",
    )

    assert names == ["orders"]
    assert registry.registered_tables() == ["orders"]
    loader.assert_not_called()


def test_delta_preload_skip_reports_loader_failure() -> None:
    platform = MagicMock()
    platform.list_folders.return_value = ["/root/orders"]
    platform.folder_exists.return_value = True
    registry = PolarsRelationRegistry()

    names = register_delta_tables(
        platform=platform,
        sql_context=pl.SQLContext(),
        registry=registry,
        loader_factory=MagicMock(side_effect=RuntimeError("bad scan")),
        base_path="/root",
        preload=True,
        on_error="skip",
    )

    assert names == ["orders"]
    assert registry.last_report.failed


def test_delta_registration_requires_platform() -> None:
    with pytest.raises(EngineError, match="requires a platform"):
        register_delta_tables(
            platform=None,
            sql_context=pl.SQLContext(),
            registry=PolarsRelationRegistry(),
            loader_factory=MagicMock(),
            base_path="/root",
        )

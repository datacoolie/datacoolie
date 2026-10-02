"""Tests for the engine-owned physical destination target contract."""

from __future__ import annotations

import pytest

from datacoolie.core.constants import Format
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.destination import Destination
from datacoolie.destinations.resolution.target import resolve_destination_target
from datacoolie.engines.contracts.windows import WindowSpec, normalize_window


def _destination(
    *,
    fmt: str,
    base_path: str | None = None,
    catalog: str | None = None,
    database: str | None = None,
) -> Destination:
    configure = {}
    if base_path is not None:
        configure["base_path"] = base_path
    return Destination(
        connection=Connection(
            name="target",
            format=fmt,
            configure=configure,
            catalog=catalog,
            database=database,
        ),
        schema_name="curated",
        table="Orders",
    )


def test_path_target_preserves_case_and_uses_path_addressing() -> None:
    upper = resolve_destination_target(
        _destination(fmt=Format.PARQUET.value, base_path="/Lake/Orders")
    )
    lower = resolve_destination_target(
        _destination(fmt=Format.PARQUET.value, base_path="/lake/orders")
    )

    assert upper.addressing == "path"
    assert upper.table_name is None
    assert upper.path == "/Lake/Orders/curated/Orders"
    assert upper.identity != lower.identity


def test_delta_without_catalog_uses_path_when_path_is_configured() -> None:
    target = resolve_destination_target(
        _destination(fmt=Format.DELTA.value, base_path="/lake")
    )

    assert target.addressing == "path"
    assert target.table_name is None
    assert target.path == "/lake/curated/Orders"


def test_delta_without_path_can_still_use_table_addressing() -> None:
    target = resolve_destination_target(_destination(fmt=Format.DELTA.value))

    assert target.addressing == "table"
    assert target.table_name == "`curated`.`Orders`"
    assert target.path is None


def test_path_format_requires_path_for_strict_identity() -> None:
    with pytest.raises(ConfigurationError, match="destination identity"):
        resolve_destination_target(_destination(fmt=Format.PARQUET.value))


def test_named_targets_include_connection_scope() -> None:
    left = Destination(
        connection=Connection(
            name="warehouse-a",
            connection_type="database",
            format="sql",
            configure={"host": "db-a"},
            database="analytics",
        ),
        schema_name="public",
        table="Orders",
    )
    right = Destination(
        connection=Connection(
            name="warehouse-b",
            connection_type="database",
            format="sql",
            configure={"host": "db-b"},
            database="analytics",
        ),
        schema_name="public",
        table="Orders",
    )

    assert resolve_destination_target(left).identity != resolve_destination_target(right).identity


def test_physical_path_aliases_share_identity_without_connection_scope() -> None:
    left = _destination(fmt=Format.PARQUET.value, base_path="/lake")
    right = _destination(fmt=Format.PARQUET.value, base_path="/lake/")

    assert resolve_destination_target(left).identity == resolve_destination_target(right).identity


def test_window_contract_rejects_legacy_mapping() -> None:
    with pytest.raises(ConfigurationError, match="require a WindowSpec"):
        normalize_window({"updated_at": (1, 2)})  # type: ignore[arg-type]


def test_window_equality_includes_operators() -> None:
    lower = WindowSpec(bounds={"updated_at": (1, 2)}, lower_operator=">")
    replay = WindowSpec(bounds={"updated_at": (1, 2)}, lower_operator=">=")

    assert lower != replay


@pytest.mark.parametrize(
    "bounds",
    [None, {"updated_at": (1,)}, {"updated_at": (1, 2, 3)}, {"": (1, 2)}],
)
def test_window_rejects_malformed_bounds(bounds) -> None:
    with pytest.raises(ConfigurationError, match="WindowSpec"):
        WindowSpec(bounds=bounds)  # type: ignore[arg-type]

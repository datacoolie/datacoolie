"""Tests for the engine-owned physical destination target contract."""

from __future__ import annotations

import pytest

from datacoolie.core.constants import Format
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.destination import Destination
from datacoolie.destinations.resolution.target import resolve_destination_target
from datacoolie.engines.contracts.windows import WindowSpec, normalize_window


def test_effective_polars_delta_paths_and_same_path_aliases():
    from datacoolie.engines.polars_engine import PolarsEngine
    engine = PolarsEngine()
    left = _destination(fmt="delta", base_path="/one", catalog="catalog", database="db")
    right = _destination(fmt="delta", base_path="/two", catalog="catalog", database="db")
    alias = _destination(fmt="delta", base_path="/one/", catalog="alias", database="other")
    a = resolve_destination_target(left, engine=engine, require_identity=True)
    b = resolve_destination_target(right, engine=engine, require_identity=True)
    c = resolve_destination_target(alias, engine=engine, require_identity=True)
    assert a.table_name is None
    assert a.path == a.storage_path == "/one/curated/Orders"
    assert a.identity != b.identity
    assert a.identity == c.identity


def test_forced_paths_override_engine_table_preference_and_preserve_auxiliary_path():
    class TableEngine:
        def preferred_target_addressing(self, **_kwargs):
            return "table"

        def target_name_identity(self, *, fmt, table_name):
            return "synthetic-backend", table_name

    engine = TableEngine()
    forced = resolve_destination_target(_destination(fmt="delta", base_path="/lake"), engine=engine)
    assert forced.table_name is None
    assert forced.path == "/lake/curated/Orders"
    named = resolve_destination_target(_destination(fmt="delta", base_path="/lake", catalog="catalog"), engine=engine)
    assert named.path is None
    assert named.table_name == "`catalog`.`curated`.`Orders`"
    assert named.storage_path == "/lake/curated/Orders"


def test_unqualified_identity_fails_maintenance_but_keeps_valid_write_handle():
    class UnqualifiedEngine:
        def preferred_target_addressing(self, **_kwargs):
            return "table"

        def target_name_identity(self, **_kwargs):
            return None

    destination = _destination(fmt="iceberg", catalog="remote")
    target = resolve_destination_target(destination, engine=UnqualifiedEngine())
    assert target.table_name == destination.full_table_name
    assert target.identity is None
    with pytest.raises(ConfigurationError, match="[Uu]nqualified"):
        resolve_destination_target(destination, engine=UnqualifiedEngine(), require_identity=True)


@pytest.mark.parametrize("preference, identity", [("invalid", None), ("table", ("", "name")),
                                                ("table", "not-a-tuple")])
def test_native_target_hook_contract_rejects_invalid_values(preference, identity):
    class InvalidEngine:
        def preferred_target_addressing(self, **_kwargs):
            return preference

        def target_name_identity(self, **_kwargs):
            return identity

    with pytest.raises(ConfigurationError):
        resolve_destination_target(_destination(fmt="iceberg", catalog="local"), engine=InvalidEngine())


@pytest.mark.parametrize("fmt,engine_kind,catalog,path,expected", [
    ("delta", "polars", "logical", "/lake", "path"),
    ("delta", "polars", "logical", None, "missing"),
    ("delta", "table", "logical", "/lake", "table"),
    ("delta", "table", None, "/lake", "path"),
    ("iceberg", "polars", "local", "/lake", "table"),
    ("iceberg", "table", None, "/lake", "path"),
    ("parquet", "table", "logical", "/lake", "path"),
])
def test_effective_route_matrix(fmt, engine_kind, catalog, path, expected):
    from datacoolie.engines.polars_engine import PolarsEngine
    from tests.unit.destinations.support import MockEngine
    engine = PolarsEngine() if engine_kind == "polars" else MockEngine()
    dest = _destination(fmt=fmt, base_path=path, catalog=catalog)
    if expected == "missing":
        with pytest.raises(ConfigurationError, match="storage path is required"):
            resolve_destination_target(dest, engine=engine)
        return
    target = resolve_destination_target(dest, engine=engine)
    assert target.addressing == expected
    assert (target.table_name is None) == (expected == "path")
    assert (target.path is None) == (expected == "table")


@pytest.mark.parametrize("operation", ["write", "merge", "merge_overwrite", "replace_window", "exists", "get_history", "compact", "cleanup"])
@pytest.mark.parametrize("kind", ["polars", "table"])
def test_selected_handle_agrees_across_engine_operations(kind, operation):
    from unittest.mock import patch
    from datacoolie.engines.polars_engine import PolarsEngine
    from tests.unit.destinations.support import MockEngine
    engine = PolarsEngine() if kind == "polars" else MockEngine()
    target = resolve_destination_target(_destination(fmt="delta", catalog="logical", base_path="/lake"), engine=engine)
    suffix = "path" if kind == "polars" else "table"
    primitive = {"write": "write_to_", "merge": "merge_to_", "merge_overwrite": "merge_overwrite_to_",
                 "replace_window": "delete_by_window_", "exists": "table_exists_by_",
                 "get_history": "get_history_by_", "compact": "compact_by_", "cleanup": "cleanup_by_"}[operation] + suffix
    if suffix == "table" and operation in ("exists", "get_history", "compact", "cleanup"):
        primitive = primitive.removesuffix("table") + "name"
    args = []
    kwargs = {"table_name": target.table_name, "path": target.path, "fmt": "delta"}
    if operation in ("write", "merge", "merge_overwrite", "replace_window"):
        args = [object()]
    if operation == "write":
        kwargs["mode"] = "append"
    if operation in ("merge", "merge_overwrite"):
        kwargs["merge_keys"] = ["id"]
    if operation == "replace_window":
        # Exercise BaseEngine's actual delete/append sequencing; MockEngine's
        # override is only a recorder. Polars stable-input handling is isolated.
        from datacoolie.engines.base import BaseEngine
        kwargs["window"] = WindowSpec(bounds={"id": (1, 2)})
        with patch.object(engine, "get_columns", return_value=["id"]), patch.object(engine, "_prepare_replace_window_input", return_value=args[0]), patch.object(engine, "_release_replace_window_input"), patch.object(engine, "is_empty", return_value=True), patch.object(engine, primitive) as call:
            BaseEngine.replace_window(engine, *args, **kwargs)
    else:
        with patch.object(engine, primitive) as call:
            getattr(engine, operation)(*args, **kwargs)
    assert call.call_args.args[0 if operation not in ("write", "merge", "merge_overwrite") else 1] == (target.path or target.table_name)


def test_aws_auxiliary_storage_path_survives_table_routing():
    from datacoolie.destinations._delta.aws_catalog import AwsDeltaCatalogCoordinator
    from tests.unit.destinations.support import MockEngine
    from types import SimpleNamespace
    dest = _destination(fmt="delta", catalog="logical", base_path="s3://bucket/lake")
    engine = MockEngine()
    target = resolve_destination_target(dest, engine=engine)
    assert target.path is None
    assert AwsDeltaCatalogCoordinator(engine)._resolve_path(SimpleNamespace(destination=dest)) == target.storage_path


@pytest.mark.parametrize("name", ["local.ns.``Orders``", "local.ns.`Orders", "local.ns.Orders`", "local.ns.`a.b`", "local..Orders"])
def test_spark_unqualified_identifier_never_reads_session_state(name):
    from datacoolie.engines._spark.target_identity import target_name_identity
    class Uninitialized:
        version = "3.5.9"
        @property
        def _jsparkSession(self):
            raise AssertionError("invalid name reached session state")
    assert target_name_identity(Uninitialized(), fmt="iceberg", table_name=name) is None


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

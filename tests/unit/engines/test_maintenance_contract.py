from uuid import UUID, uuid4

import pytest

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.engines.contracts.maintenance import (
    CLEANUP_ACTIONS, ICEBERG_ACTIONS, RestMaintenanceDeployment,
    native_identifier, selected_actions,
)


def test_default_and_partial_selection_preserve_other_enabled_groups():
    assert set(selected_actions(do_compact=True, do_cleanup=True)) == ICEBERG_ACTIONS
    assert selected_actions(do_compact=True, do_cleanup=True,
        compact_options={"rewrite_data_files": False, "rewrite_position_delete_files": False,
                         "rewrite_manifests": False}) == CLEANUP_ACTIONS
    assert selected_actions(do_compact=False, do_cleanup=True,
        cleanup_options={"remove_orphan_files": False}) == ("expire_snapshots",)
    assert selected_actions(do_compact=False, do_cleanup=False) == ()


@pytest.mark.parametrize("options", [{"typo": True}, {"expire_snapshots": 1}, [True]])
def test_invalid_selection_rejected(options):
    with pytest.raises(ConfigurationError):
        selected_actions(do_compact=False, do_cleanup=True, cleanup_options=options)


def test_disabled_group_options_reject_contradictory_intent():
    with pytest.raises(ConfigurationError):
        selected_actions(do_compact=False, do_cleanup=True, compact_options={"rewrite_data_files": False})


def deployment(**overrides):
    return RestMaintenanceDeployment(**{
        "deployment_identity": ("docker", "backend-instance-verified"),
        "routes": ("http://host:8181", "http://service:8181"),
        "warehouse": "s3://synthetic/warehouse", "sentinel_identifier": ("probe", "scope"),
        "sentinel_uuid": str(uuid4()), "catalog_label": "catalog", **overrides})


def test_scope_uses_attested_backend_and_preserves_aliases():
    one = deployment()
    alias = deployment(routes=("http://alias:8181",), catalog_label="alias")
    other = deployment(deployment_identity=("docker", "distinct-backend-instance"))
    assert one.scope == alias.scope
    assert one.scope != other.scope
    one.validate_properties({"uri": "http://service:8181"})
    one.validate_location("s3://synthetic/warehouse/namespace/table")
    for properties in ({"uri": "http://unknown:8181"},
                       {"uri": "http://service:8181", "token": "synthetic"},
                       {"uri": "http://service:8181", "auth": {"type": "custom"}},
                       {"uri": "http://service:8181", "rest.sigv4-enabled": True},
                       {"uri": "http://service:8181", "prefix": "tenant"},
                       {"uri": "http://service:8181", "warehouse": "s3://other"}):
        with pytest.raises(ConfigurationError):
            one.validate_properties(properties)
    with pytest.raises(ConfigurationError):
        one.validate_location("s3://synthetic/warehouse-other/table")


def test_native_identifier_preserves_explicit_catalog_namespace_boundaries():
    from datacoolie.engines.contracts.maintenance import MaintenanceTableAddress
    assert native_identifier("display", catalog_label="catalog",
        table_address=MaintenanceTableAddress("catalog", ("nested", "schema"), "Orders")) == ("nested", "schema", "Orders")
    assert native_identifier("display", catalog_label="catalog",
        table_address=MaintenanceTableAddress(None, ("catalog", "schema"), "Orders")) == ("catalog", "schema", "Orders")
    assert native_identifier("catalog.Orders", catalog_label="catalog") == ("catalog", "Orders")
    with pytest.raises(ConfigurationError, match="requested catalog"):
        native_identifier("other.ns.Orders", catalog_label="catalog",
            table_address=MaintenanceTableAddress("other", ("ns",), "Orders"))
    for name in ("table", "catalog.ns.Orders", "other.ns.Orders", "namespace..table", "namespace.`dotted.table`", "namespace.table;DROP"):
        with pytest.raises(ConfigurationError):
            native_identifier(name, catalog_label="catalog")


@pytest.mark.parametrize("auth", [{"auth": {"type": "custom"}}, {"auth-manager": "custom"},
                                  {"rest.sigv4-enabled": True}])
def test_auth_variants_rejected_before_any_native_lookup(auth):
    from datacoolie.engines._polars.iceberg.maintenance import validate_deployment
    from pyiceberg.catalog.rest import RestCatalog
    from unittest.mock import MagicMock
    # Construct without RestCatalog's HTTP initialization; only property
    # preflight is under test and a native lookup would fail this assertion.
    catalog = object.__new__(RestCatalog)
    catalog.properties = {"uri": "http://service:8181", **auth}
    catalog.uri = "http://service:8181"
    catalog.load_table = MagicMock()
    with pytest.raises(ConfigurationError, match="tenant-routed"):
        validate_deployment(catalog, deployment())
    catalog.load_table.assert_not_called()


def test_initialized_catalog_route_cannot_diverge_from_attested_properties():
    from datacoolie.engines._polars.iceberg.maintenance import validate_deployment
    from pyiceberg.catalog.rest import RestCatalog
    from unittest.mock import MagicMock
    catalog = object.__new__(RestCatalog)
    catalog.properties = {"uri": "http://service:8181"}
    catalog.uri = "http://unknown:8181"
    catalog.load_table = MagicMock()
    with pytest.raises(ConfigurationError, match="operative route"):
        validate_deployment(catalog, deployment())
    catalog.load_table.assert_not_called()


def test_base_preparation_preserves_legacy_hook_keyword_contract():
    from datacoolie.engines.contracts.maintenance import MaintenanceTableAddress
    from tests.unit.engines.test_base_engine import StubEngine
    calls = []
    class Legacy(StubEngine):
        def prepare_maintenance_target(self, *, fmt, table_name=None, path=None):
            calls.append((fmt, table_name, path))
            return "legacy-target"
    assert Legacy().maintenance_preparation()(fmt="delta", table_name="c.ns.table",
        table_address=MaintenanceTableAddress("c", ("ns",), "table")) == "legacy-target"
    assert calls == [("delta", "c.ns.table", None)]


@pytest.mark.parametrize("profile", ["polars", "spark"])
def test_rest_preparation_rejects_foreign_catalog_before_lookup_and_keeps_namespace(profile, monkeypatch):
    from unittest.mock import MagicMock
    from datacoolie.engines.contracts.maintenance import MaintenanceTableAddress
    bound = deployment()
    native = MagicMock()
    native.metadata.table_uuid = UUID("22222222-2222-2222-2222-222222222222")
    native.uuid.return_value = str(native.metadata.table_uuid)
    native.location.return_value = bound.warehouse + "/catalog/ns/Orders"
    if profile == "polars":
        from pyiceberg.catalog.rest import RestCatalog
        from datacoolie.engines.polars_engine import PolarsEngine
        from datacoolie.engines._polars.iceberg import maintenance
        catalog = object.__new__(RestCatalog)
        catalog.load_table = MagicMock(return_value=native)
        preflight = MagicMock()
        monkeypatch.setattr(maintenance, "validate_deployment", preflight)
        engine = PolarsEngine(iceberg_catalog=catalog, maintenance_deployment=bound)
        prepare = engine.maintenance_preparation()
        lookup = catalog.load_table
    else:
        from datacoolie.engines.spark_engine import SparkEngine
        from datacoolie.engines._spark.iceberg import maintenance
        spark = MagicMock()
        manager = spark._jsparkSession.sessionState().catalogManager()
        plugin = manager.catalog.return_value
        plugin.getClass().getName.return_value = "org.apache.iceberg.spark.SparkCatalog"
        catalog = plugin.icebergCatalog.return_value
        catalog.getClass().getName.return_value = "org.apache.iceberg.rest.RESTCatalog"
        catalog.properties.return_value = {"uri": bound.routes[0]}
        sentinel = MagicMock()
        sentinel.uuid.return_value = bound.sentinel_uuid
        sentinel.location.return_value = bound.warehouse + "/sentinel"
        plugin.loadTable.side_effect = [MagicMock(table=lambda: sentinel), MagicMock(table=lambda: native)]
        preflight = MagicMock()
        monkeypatch.setattr(maintenance, "verify_runtime_provenance", preflight)
        monkeypatch.setattr(maintenance, "_identifier", lambda _spark, parts: parts)
        engine = object.__new__(SparkEngine)
        engine._spark, engine._maintenance_deployment, engine._maintenance_guard_manifest = spark, bound, object()
        prepare = engine.maintenance_preparation()
        lookup = plugin.loadTable
        spark._jsparkSession.sessionState.reset_mock()
    with pytest.raises(ConfigurationError, match="requested catalog"):
        prepare(fmt="iceberg", table_name="other.ns.Orders",
                table_address=MaintenanceTableAddress("other", ("ns",), "Orders"))
    lookup.assert_not_called()
    preflight.assert_not_called()
    if profile == "spark":
        spark._jsparkSession.sessionState.assert_not_called()
    result = prepare(fmt="iceberg", table_name="catalog.ns.Orders",
                     table_address=MaintenanceTableAddress(None, ("catalog", "ns"), "Orders"))
    lookup.assert_called_with(("catalog", "ns", "Orders"))
    assert result.expected_uuid == str(native.metadata.table_uuid)

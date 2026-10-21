from unittest.mock import MagicMock

from datacoolie.destinations.iceberg_writer import IcebergWriter
from datacoolie.destinations.base import BaseDestinationWriter
from datacoolie.engines.contracts.maintenance import ICEBERG_ACTIONS, PreparedMaintenanceTarget
from tests.unit.orchestration.test_maintenance import _maint_df


def test_real_legacy_writer_keeps_exact_default_hook_and_rejects_explicit_options():
    from tests.unit.orchestration.test_maintenance import _make_driver
    class Legacy(BaseDestinationWriter):
        def __init__(self, engine):
            super().__init__(engine)
            self.calls = []
        def _write_internal(self, *args, **kwargs):
            raise AssertionError("Unexpected write")
        def _maintain_internal(self, dataflow, *, do_compact, do_cleanup, retention_hours):
            self.calls.append((do_compact, do_cleanup, retention_hours))
            return [{"operation": "compact", "status": "succeeded", "metrics": {}}], []
    for options, expected_status, calls in [({}, "succeeded", [(True, True, 168)]),
        ({"compact_options": {}, "cleanup_options": {}}, "succeeded", [(True, True, 168)]),
        ({"compact_options": {}}, "succeeded", [(True, True, 168)]),
        ({"cleanup_options": {}}, "succeeded", [(True, True, 168)]),
        ({"compact_options": {"rewrite_data_files": True}}, "failed", []),
        ({"do_compact": False, "do_cleanup": False}, "succeeded", []),
        ({"do_compact": False, "do_cleanup": False, "compact_options": {}, "cleanup_options": {}}, "succeeded", [])]:
        driver, engine, _, _ = _make_driver()
        writer = Legacy(engine)
        driver._create_destination_writer = lambda _fmt: writer
        result = driver.run_maintenance(dataflows=[_maint_df("legacy")], **options)
        assert getattr(result, expected_status) == 1 and writer.calls == calls


def prepared(capabilities=ICEBERG_ACTIONS, **kwargs):
    return PreparedMaintenanceTarget("iceberg", "catalog.ns.table", None, "verified:uuid",
                                     capabilities=capabilities, binding=object(), **kwargs)


def test_unsupported_capabilities_fail_before_all_actions_or_history():
    engine = MagicMock()
    result = IcebergWriter(engine).run_selected_maintenance(_maint_df("unsupported"),
        prepared=prepared(frozenset({"expire_snapshots"})))
    assert result.status == "failed"
    assert "Unsupported" in result.message
    engine.execute_maintenance_action.assert_not_called()
    engine.maintenance_history.assert_not_called()


def test_acknowledged_expiration_survives_replacement_history_diagnostic_failure():
    engine = MagicMock()
    engine.execute_maintenance_action.return_value = {"action": "expire_snapshots", "status": "succeeded",
        "metrics": {"snapshots_expired": 1}, "expired_snapshot_ids": [123]}
    engine.maintenance_history.side_effect = ValueError("UUID changed after acknowledged action")
    result = IcebergWriter(engine).run_selected_maintenance(_maint_df("diagnostic"),
        prepared=prepared(), do_compact=False, cleanup_options={"remove_orphan_files": False})
    assert result.status == "succeeded" and result.files_removed is None
    assert result.operation_details[0]["actions"][0]["expired_snapshot_ids"] == [123]
    assert result.operation_details[-1] == {"operation": "history", "reason": "UUID changed after acknowledged action"}
    engine.execute_maintenance_action.assert_called_once()


def test_all_disabled_one_group_does_not_disable_other_group():
    engine = MagicMock()
    engine.execute_maintenance_action.side_effect = lambda _target, **kwargs: {
        "action": kwargs["action"], "status": "succeeded", "metrics": {"files_removed": 2}}
    engine.maintenance_history.return_value = []
    result = IcebergWriter(engine).run_selected_maintenance(_maint_df("selection"), prepared=prepared(),
        compact_options={"rewrite_data_files": False, "rewrite_position_delete_files": False, "rewrite_manifests": False})
    assert result.status == "succeeded"
    assert [call.kwargs["action"] for call in engine.execute_maintenance_action.call_args_list] == ["expire_snapshots", "remove_orphan_files"]
    assert result.operation_details[0]["operation"] == "cleanup"
    assert result.operation_details[0]["metrics"]["files_removed"] == result.files_removed == 4
    assert result.bytes_removed is None


def test_partial_failure_retains_completed_actions_and_unknown_group_metrics():
    engine = MagicMock()
    engine.execute_maintenance_action.side_effect = [
        {"action": "rewrite_data_files", "status": "succeeded", "metrics": {"files_removed": 3}},
        RuntimeError("native action failed")]
    result = IcebergWriter(engine).run_selected_maintenance(_maint_df("partial"), prepared=prepared(), do_cleanup=False)
    assert result.status == "failed"
    group = result.operation_details[0]
    assert group["status"] == "failed"
    assert [action["status"] for action in group["actions"]] == ["succeeded", "failed"]
    assert group["actions"][0]["metrics"]["files_removed"] == 3
    assert group["metrics"]["files_removed"] == 3 and result.files_removed == 3


def test_no_selected_actions_succeeds_without_effect_metrics_or_native_calls():
    engine = MagicMock()
    result = IcebergWriter(engine).run_selected_maintenance(_maint_df("noop"), prepared=prepared(),
                                                            do_compact=False, do_cleanup=False)
    assert result.status == "succeeded" and result.files_removed is None
    assert result.operation_details[0]["status"] == "skipped"
    engine.execute_maintenance_action.assert_not_called()
    engine.maintenance_history.assert_not_called()


def test_missing_binding_stays_skipped():
    engine = MagicMock()
    target = PreparedMaintenanceTarget("iceberg", "missing", None, None, missing=True)
    result = IcebergWriter(engine).run_selected_maintenance(_maint_df("missing"), prepared=target)
    assert result.status == "skipped"
    engine.execute_maintenance_action.assert_not_called()

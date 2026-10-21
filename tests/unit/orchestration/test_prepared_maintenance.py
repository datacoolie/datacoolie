import pytest

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.engines.contracts.maintenance import PreparedMaintenanceTarget
from datacoolie.orchestration.preparation.dataflow import PreparedDataFlow
from tests.unit.orchestration.test_maintenance import _make_driver, _maint_df


class Borrowed:
    def __deepcopy__(self, memo):
        raise AssertionError("Borrowed native binding was deep-copied")


def test_hydration_precedes_identity_and_execution_retains_snapshot_and_binding():
    driver, engine, _, _ = _make_driver()
    a, b = _maint_df("a", table="one"), _maint_df("b", table="two")
    native = Borrowed()
    calls = []
    def hydrate(metadata, **_kwargs):
        effective = metadata.model_copy(deep=True)
        effective.destination.table = "effective"
        effective.destination.connection.catalog = "catalog"
        effective.destination.connection.database = "catalog"
        effective.destination.schema_name = "nested"
        calls.append(metadata.dataflow_id)
        return PreparedDataFlow(metadata, effective)
    driver._prepare_execution_dataflow = hydrate
    def prepare(**kwargs):
        assert "effective" in kwargs["table_name"]
        from datacoolie.engines.contracts.maintenance import MaintenanceTableAddress
        assert kwargs["table_address"] == MaintenanceTableAddress("catalog", ("catalog", "nested"), "effective")
        return PreparedMaintenanceTarget("iceberg", kwargs["table_name"], None,
            "verified:uuid", expected_uuid="uuid", binding=native, capabilities=frozenset())
    engine.prepare_maintenance_target.side_effect = prepare
    class Writer:
        def run_maintenance(self, **kwargs):
            raise AssertionError("Prepared invocation bypassed extension")
        def run_selected_maintenance(self, **kwargs):
            assert kwargs["prepared"].binding is native
            assert kwargs["dataflow"].destination.table == "effective"
            return DestinationRuntimeInfo(status="succeeded")
    driver._create_destination_writer = lambda _fmt: Writer()
    result = driver.run_maintenance(dataflows=[b, a])
    assert (result.total, result.succeeded) == (1, 1)
    assert calls == ["a", "b"]
    engine.prepare_maintenance_target.assert_called_once()
    engine.release_maintenance_target.assert_called_once()
    assert a.destination.table == "one" and b.destination.table == "two"


def test_preparation_failure_releases_previous_bindings_without_writer_dispatch():
    driver, engine, _, _ = _make_driver()
    engine.prepare_maintenance_target.side_effect = [
        PreparedMaintenanceTarget("iceberg", "one", None, "verified:one", binding=Borrowed()),
        ConfigurationError("transport failed")]
    driver._create_destination_writer = lambda _fmt: pytest.fail("Mutation dispatched before selection completed")
    with pytest.raises(ConfigurationError, match="transport failed"):
        driver.run_maintenance(dataflows=[_maint_df("a", table="one"), _maint_df("b", table="two")])
    engine.release_maintenance_target.assert_called_once()


def test_public_load_releases_then_run_freshly_prepares():
    driver, engine, metadata, _ = _make_driver()
    metadata.get_maintenance_dataflows.return_value = [_maint_df("a")]
    flows = driver.load_maintenance_dataflows()
    assert engine.prepare_maintenance_target.call_count == 1
    assert engine.release_maintenance_target.call_count == 1
    from unittest.mock import MagicMock
    writer = MagicMock()
    writer.run_maintenance.return_value = DestinationRuntimeInfo(status="succeeded")
    driver._create_destination_writer = lambda _fmt: writer
    assert driver.run_maintenance(dataflows=flows).succeeded == 1
    assert engine.prepare_maintenance_target.call_count == 2
    assert engine.release_maintenance_target.call_count == 2


def test_inactive_selection_needs_no_native_preparation():
    driver, engine, _, _ = _make_driver()
    flow = _maint_df("inactive")
    flow.is_active = False
    assert driver.run_maintenance(dataflows=[flow]).skipped == 1
    engine.prepare_maintenance_target.assert_not_called()


def test_cancellation_during_preparation_releases_prior_binding():
    driver, engine, _, _ = _make_driver()
    engine.prepare_maintenance_target.side_effect = [
        PreparedMaintenanceTarget("iceberg", "one", None, "verified:one", binding=Borrowed()),
        KeyboardInterrupt()]
    with pytest.raises(KeyboardInterrupt):
        driver.run_maintenance(dataflows=[_maint_df("a", table="one"), _maint_df("b", table="two")])
    engine.release_maintenance_target.assert_called_once()


def test_worker_cancellation_drains_other_worker_before_releasing_bindings():
    from threading import Event
    from datacoolie.orchestration.scheduling.parallel_executor import ParallelExecutor
    started, drained, cancel_ready = Event(), Event(), Event()
    events = []
    driver, engine, _, _ = _make_driver()
    driver._executor = ParallelExecutor(max_workers=2)
    engine.prepare_maintenance_target.side_effect = lambda **kwargs: PreparedMaintenanceTarget(
        "iceberg", kwargs["table_name"], None, kwargs["table_name"], binding=Borrowed())
    def release(target):
        assert drained.is_set(), "Borrowed binding released before worker drained"
        events.append(target.table_name)
    engine.release_maintenance_target.side_effect = release
    class Writer:
        def run_maintenance(self, **kwargs):
            raise AssertionError("Expected prepared dispatch")
        def run_selected_maintenance(self, *, dataflow, **kwargs):
            if dataflow.dataflow_id == "a":
                assert started.wait(5)
                cancel_ready.set()
                raise KeyboardInterrupt("worker cancellation")
            started.set()
            assert cancel_ready.wait(5)
            drained.set()
            events.append("drained")
            return DestinationRuntimeInfo(status="succeeded")
    driver._create_destination_writer = lambda _fmt: Writer()
    with pytest.raises(KeyboardInterrupt, match="worker cancellation"):
        driver.run_maintenance(dataflows=[_maint_df("a", table="one"), _maint_df("b", table="two")])
    assert events[0] == "drained" and engine.release_maintenance_target.call_count == 2


def test_release_cancellation_attempts_every_resource_and_preserves_active_error():
    from datacoolie.orchestration.preparation.maintenance import release_selection
    from unittest.mock import MagicMock
    engine = MagicMock()
    engine.release_maintenance_target.side_effect = [KeyboardInterrupt(), None]
    targets = [object(), object()]
    with pytest.raises(KeyboardInterrupt):
        release_selection(engine, targets)
    assert engine.release_maintenance_target.call_count == 2
    engine.release_maintenance_target.reset_mock(side_effect=True)
    engine.release_maintenance_target.side_effect = [KeyboardInterrupt(), None]
    try:
        raise ValueError("active failure")
    except ValueError:
        release_selection(engine, targets)
    assert engine.release_maintenance_target.call_count == 2


def test_metadata_sharding_follows_dedupe_but_explicit_list_keeps_selection():
    from datacoolie.orchestration.scheduling.job_distributor import JobDistributor
    from unittest.mock import MagicMock
    driver, engine, metadata, _ = _make_driver()
    flows = [_maint_df("a", table="shared"), _maint_df("b", table="shared"), _maint_df("c", table="other")]
    metadata.get_maintenance_dataflows.return_value = flows
    writer = MagicMock()
    writer.run_maintenance.return_value = DestinationRuntimeInfo(status="succeeded")
    driver._create_destination_writer = lambda _fmt: writer
    counts = []
    for index in (0, 1):
        driver._distributor = JobDistributor(job_num=2, job_index=index)
        counts.append(driver.run_maintenance().total)
    assert sum(counts) == 2
    assert driver.run_maintenance(dataflows=flows).total == 2


def test_whole_flow_retry_keeps_borrowed_binding_and_distinct_attempt_details():
    from datacoolie.utils.retry import RetryHandler
    driver, engine, _, _ = _make_driver()
    driver._retry_handler = RetryHandler(retry_count=1, retry_delay=0)
    borrowed = Borrowed()
    options = {"rewrite_data_files": True}
    engine.prepare_maintenance_target.side_effect = lambda **kwargs: PreparedMaintenanceTarget(
        "iceberg", kwargs["table_name"], None, "verified:uuid", binding=borrowed)
    invocations = []
    class Writer:
        def run_maintenance(self, **kwargs):
            raise AssertionError("Expected native extension")
        def run_selected_maintenance(self, **kwargs):
            invocations.append(kwargs["prepared"].binding)
            assert kwargs["compact_options"]["rewrite_data_files"] is True
            options["rewrite_data_files"] = False
            return DestinationRuntimeInfo(status="failed" if len(invocations) == 1 else "succeeded",
                message="conflict" if len(invocations) == 1 else None,
                operation_details=[{"operation": "compact", "status": "failed" if len(invocations) == 1 else "succeeded"}])
    driver._create_destination_writer = lambda _fmt: Writer()
    result = driver.run_maintenance(dataflows=[_maint_df("retry")], compact_options=options)
    assert result.succeeded == 1
    assert invocations == [borrowed, borrowed]
    engine.prepare_maintenance_target.assert_called_once()
    engine.release_maintenance_target.assert_called_once()

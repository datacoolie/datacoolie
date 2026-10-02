"""Selection boundaries through real metadata, driver and scheduling code."""

import json
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.core.models.runtime import DataFlowRuntimeInfo
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver


def _flow(name, stage, *, group=None, order=None, active=True, source="raw"):
    return {
        "dataflow_id": name, "stage": stage, "group_number": group,
        "execution_order": order, "is_active": active,
        "source": {"connection_name": "test", "table": source, "watermark_columns": ["id"]},
        "destination": {"connection_name": "test", "table": name},
    }


def _driver(flows, *, connection_active=True, **config):
    document = {"connections": [{"name": "test", "format": "delta",
                                 "is_active": connection_active,
                                 "configure": {"base_path": "/data"}}], "dataflows": flows}
    # Real FileProvider reads only this in-memory JSON; no filesystem/cloud I/O.
    platform = SimpleNamespace(read_file=Mock(return_value=json.dumps(document)))
    metadata = FileProvider("/memory/metadata.json", platform=platform)
    metadata.get_dataflows = Mock(wraps=metadata.get_dataflows)
    return DataCoolieDriver(
        engine=SimpleNamespace(platform=platform), metadata_provider=metadata,
        config=DataCoolieRunConfig(max_workers=2, **config),
    ), metadata


def _success(flow):
    return DataFlowRuntimeInfo(dataflow_id=flow.dataflow_id, status="succeeded")


@pytest.mark.parametrize("stage", [["extract_orders", "enrich_customers"],
                                  "extract_orders,enrich_customers"])
def test_combined_stages_select_union_without_a_stage_execution_barrier(stage, monkeypatch):
    driver, metadata = _driver([
        _flow("producer", "extract_orders", order=0),
        _flow("consumer", "enrich_customers", order=99, source="producer"),
        _flow("unselected", "publish_invoices"),
    ])
    rendezvous = Barrier(2, timeout=5)
    visited = []

    def process(flow):
        visited.append(flow.dataflow_id)
        rendezvous.wait()  # Sequential stage runs would deadlock here.
        return _success(flow)

    monkeypatch.setattr(driver, "_process_dataflow", process)
    with driver:
        result = driver.run(stage=stage)
    assert (result.total, result.succeeded, result.failed) == (2, 2, 0), result.errors
    assert sorted(visited) == ["consumer", "producer"]
    metadata.get_dataflows.assert_called_once_with(
        stage=stage, active_only=True, attach_schema_hints=True,
    )


@pytest.mark.parametrize("producer_active", [True, False], ids=["consumer-only", "inactive-producer"])
def test_selection_does_not_expand_missing_prerequisites(producer_active, monkeypatch):
    driver, _ = _driver([
        _flow("producer", "extract_orders", group=0, order=0, active=producer_active),
        _flow("consumer", "enrich_customers", group=0, order=1, source="producer"),
    ])
    process = Mock(side_effect=_success)
    monkeypatch.setattr(driver, "_process_dataflow", process)
    stage = "enrich_customers" if producer_active else ["extract_orders", "enrich_customers"]
    with driver:
        result = driver.run(stage=stage)
    assert (result.total, result.succeeded, result.failed) == (1, 1, 0), result.errors
    process.assert_called_once()
    assert process.call_args.args[0].dataflow_id == "consumer"


def test_explicit_dataflows_bypass_shard_and_stage_selection_but_not_activation(monkeypatch):
    driver, metadata = _driver([
        _flow("eligible", "extract_orders", group=1),
        _flow("other-shard", "extract_orders", group=2),
        _flow("inactive", "extract_orders", group=1, active=False),
    ], job_num=3, job_index=1)
    actual_process = driver._process_dataflow
    process = Mock(side_effect=lambda flow: actual_process(flow) if not flow.is_active else _success(flow))
    monkeypatch.setattr(driver, "_process_dataflow", process)
    explicit = metadata.get_dataflows(active_only=False)
    metadata.get_dataflows.reset_mock()
    with driver:
        normal = driver.run(stage="extract_orders")
        assert (normal.total, normal.succeeded) == (1, 1)
        assert [call.args[0].dataflow_id for call in process.call_args_list] == ["eligible"]
        process.reset_mock()
        supplied = driver.run(dataflows=explicit, stage="unmatched-stage")
    assert (supplied.total, supplied.succeeded, supplied.failed, supplied.skipped) == (
        3, 2, 0, 1,
    ), supplied.errors
    assert sorted(call.args[0].dataflow_id for call in process.call_args_list) == [
        "eligible", "inactive", "other-shard",
    ]
    metadata.get_dataflows.assert_called_once_with(
        stage="extract_orders", active_only=True, attach_schema_hints=True,
    )


def test_inactive_connection_remains_visible_but_blocks_normal_and_explicit_runs():
    driver, metadata = _driver([_flow("orders", "extract_orders")], connection_active=False)
    connections = metadata.get_connections(active_only=False)
    assert len(connections) == 1 and connections[0].is_active is False
    assert metadata.get_connections(active_only=True) == []
    explicit = metadata.get_dataflows(active_only=False)
    assert len(explicit) == 1 and explicit[0].is_active is True
    assert explicit[0].source.connection.is_active is False
    with driver:
        selected = driver.run(stage="extract_orders")
        supplied = driver.run(dataflows=explicit)
        replayed = driver.run_replay(dataflows=explicit, replay=ReplayConfig(start=1, end=2))
    for result in (selected, supplied, replayed):
        assert (result.total, result.skipped, result.failed) == (1, 1, 0), result.errors


def test_inactive_replay_skips_before_missing_chunk_column_validation():
    driver, metadata = _driver([_flow("orders", "extract_orders")], connection_active=False)
    flow = metadata.get_dataflows(active_only=False)[0]
    flow.source.watermark_columns = []
    with driver:
        result = driver.run_replay(flow, ReplayConfig(start=1, end=2))
    assert (result.total, result.skipped, result.failed) == (1, 1, 0), result.errors


def test_replay_dataflows_overlap_despite_same_group_and_different_orders(monkeypatch):
    driver, _ = _driver([
        _flow("producer", "extract_orders", group=0, order=0),
        _flow("consumer", "enrich_customers", group=0, order=99, source="producer"),
    ])
    replay = ReplayConfig(start=1, end=10)
    rendezvous = Barrier(2, timeout=5)
    visited = []

    def process(flow, *, replay):
        assert (replay.start, replay.end) == (1, 10)
        visited.append(flow.dataflow_id)
        rendezvous.wait()  # Replay's outer dataflow pool ignores group/order.
        return _success(flow)

    monkeypatch.setattr(driver, "_process_replay", process)
    with driver:
        result = driver.run_replay(dataflows=driver.load_dataflows(), replay=replay)
    assert (result.total, result.succeeded, result.failed) == (2, 2, 0), result.errors
    assert sorted(visited) == ["consumer", "producer"]

"""Selection boundaries through real metadata, driver and scheduling code."""

import json
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from datacoolie.core.models import DataCoolieRunConfig, DataFlowRuntimeInfo, ReplayConfig
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver


def _flow(name, stage, *, group=None, order=None, active=True, source="raw"):
    return {
        "dataflow_id": name, "stage": stage, "group_number": group,
        "execution_order": order, "is_active": active,
        "source": {"connection_name": "test", "table": source, "watermark_columns": ["id"]},
        "destination": {"connection_name": "test", "table": name},
    }


def _driver(flows, **config):
    document = {"connections": [{"name": "test", "format": "delta",
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


def test_explicit_dataflows_bypass_shard_active_and_stage_selection(monkeypatch):
    driver, metadata = _driver([
        _flow("eligible", "extract_orders", group=1),
        _flow("other-shard", "extract_orders", group=2),
        _flow("inactive", "extract_orders", group=1, active=False),
    ], job_num=3, job_index=1)
    process = Mock(side_effect=_success)
    monkeypatch.setattr(driver, "_process_dataflow", process)
    explicit = metadata.get_dataflows(active_only=False)
    metadata.get_dataflows.reset_mock()
    with driver:
        normal = driver.run(stage="extract_orders")
        assert (normal.total, normal.succeeded) == (1, 1)
        assert [call.args[0].dataflow_id for call in process.call_args_list] == ["eligible"]
        process.reset_mock()
        supplied = driver.run(dataflows=explicit, stage="unmatched-stage")
    assert (supplied.total, supplied.succeeded, supplied.failed) == (3, 3, 0), supplied.errors
    assert sorted(call.args[0].dataflow_id for call in process.call_args_list) == [
        "eligible", "inactive", "other-shard",
    ]
    metadata.get_dataflows.assert_called_once_with(
        stage="extract_orders", active_only=True, attach_schema_hints=True,
    )


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

"""Local FileProvider-to-Driver activation behavior with one real file write."""

from __future__ import annotations

import json

import polars as pl
import pytest

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform


pytestmark = pytest.mark.integration


def test_inactive_metadata_remains_visible_and_only_active_flow_writes(tmp_path):
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    metadata_root = tmp_path / "metadata"
    (input_root / "orders").mkdir(parents=True)
    metadata_root.mkdir()
    (input_root / "orders" / "orders.csv").write_text(
        "order_id,amount\n1,10\n2,20\n", encoding="utf-8"
    )
    metadata_file = metadata_root / "metadata.json"
    metadata_file.write_text(
        json.dumps({
            "connections": [
                {"name": "source", "format": "csv", "configure": {"base_path": str(input_root)}},
                {"name": "inactive_source", "format": "csv", "is_active": False,
                 "configure": {"base_path": str(input_root)}},
                {"name": "destination", "format": "parquet",
                 "configure": {"base_path": str(output_root)}},
                {"name": "inactive_destination", "format": "parquet", "is_active": False,
                 "configure": {"base_path": str(output_root)}},
            ],
            "dataflows": [
                {"name": "healthy", "stage": "bronze",
                 "source": {"connection_name": "source", "table": "orders"},
                 "destination": {"connection_name": "destination", "table": "healthy",
                                 "load_type": "overwrite"}},
                {"name": "source_blocked", "stage": "bronze",
                 "source": {"connection_name": "inactive_source", "table": "orders"},
                 "destination": {"connection_name": "destination", "table": "source_blocked",
                                 "load_type": "overwrite"}},
                {"name": "destination_blocked", "stage": "bronze",
                 "source": {"connection_name": "source", "table": "orders"},
                 "destination": {"connection_name": "inactive_destination",
                                 "table": "destination_blocked", "load_type": "overwrite"}},
                {"name": "flow_blocked", "stage": "bronze", "is_active": False,
                 "source": {"connection_name": "source", "table": "orders"},
                 "destination": {"connection_name": "destination", "table": "flow_blocked",
                                 "load_type": "overwrite"}},
            ],
        }),
        encoding="utf-8",
    )

    platform = LocalPlatform()
    provider = FileProvider(config_path=str(metadata_file), platform=platform)
    assert len(provider.get_connections(active_only=False)) == 4
    assert len(provider.get_connections(active_only=True)) == 2
    assert len(provider.get_dataflows(active_only=False)) == 4
    assert len(provider.get_dataflows(active_only=True)) == 3

    # Driver binds runtime paths during startup; use a fresh, uninitialized
    # provider after the standalone visibility checks above.
    provider = FileProvider(config_path=str(metadata_file), platform=platform)
    with DataCoolieDriver(
        engine=PolarsEngine(platform=platform),
        platform=platform,
        metadata_provider=provider,
        state_base_path=str(tmp_path / "state"),
        log_base_path=str(tmp_path / "logs"),
        config=DataCoolieRunConfig(job_id="activation-contract"),
    ) as driver:
        result = driver.run(stage="bronze")
        explicitly_selected = driver.run(dataflows=provider.get_dataflows(active_only=False))

    assert (result.total, result.succeeded, result.skipped, result.failed) == (3, 1, 2, 0), result.errors
    assert (explicitly_selected.total, explicitly_selected.skipped, explicitly_selected.failed) == (
        4, 3, 0,
    ), explicitly_selected.errors
    assert pl.read_parquet(sorted((output_root / "healthy").glob("*.parquet"))).height == 2
    for blocked_name in ("source_blocked", "destination_blocked", "flow_blocked"):
        assert not (output_root / blocked_name).exists()

    records = [
        json.loads(line)
        for path in (tmp_path / "logs").rglob("dataflow_*.json")
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    blocked = [row for row in records if row["status"] == "skipped" and row["message"]]
    assert blocked
    assert all(row["message"] and row["retry_attempts"] == 0 for row in blocked)
    system_records = [
        json.loads(line)
        for path in (tmp_path / "logs").rglob("system_*.json")
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    assert any(
        "Dataflow skipped: source connection" in row["msg"]
        and row["dataflow_id"] is not None
        and row["dataflow_run_id"] is not None
        for row in system_records
    )

"""Current processing-mode boundary for the built-in driver."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.core.models.runtime import DataFlowRuntimeInfo
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver


def test_non_batch_processing_modes_still_use_the_builtin_normal_etl_path(monkeypatch):
    document = {
        "connections": [
            {"name": "test", "format": "delta", "configure": {"base_path": "/data"}},
        ],
        "dataflows": [
            {
                "name": "streaming-labelled-flow",
                "stage": "extract",
                "processing_mode": "streaming",
                "source": {"connection_name": "test", "table": "source"},
                "destination": {"connection_name": "test", "table": "target"},
            },
        ],
    }
    platform = SimpleNamespace(read_file=Mock(return_value=json.dumps(document)))
    metadata = FileProvider("/memory/metadata.json", platform=platform)
    driver = DataCoolieDriver(
        engine=SimpleNamespace(platform=platform),
        metadata_provider=metadata,
        config=DataCoolieRunConfig(max_workers=1),
    )
    process = Mock(
        return_value=DataFlowRuntimeInfo(
            dataflow_id="streaming-labelled-flow",
            status="succeeded",
        )
    )
    monkeypatch.setattr(driver, "_process_dataflow", process)

    with driver:
        result = driver.run(stage="extract")

    assert (result.total, result.succeeded, result.failed) == (1, 1, 0)
    process.assert_called_once()
    assert process.call_args.args[0].processing_mode == "streaming"

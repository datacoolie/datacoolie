"""Native target/state remain untouched when a bounded source cannot read safely."""

from __future__ import annotations

import json

import polars as pl
import pytest

from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform

pytestmark = pytest.mark.integration
_LEGACY_CALLS = []


def legacy_loader(engine, source, watermark_start=None, watermark_end=None):
    _LEGACY_CALLS.append(source)
    return pl.DataFrame({"id": [1], "updated_seq": [1]}).lazy()


@pytest.mark.parametrize("case", ["missing-column", "legacy-function"])
def test_bounded_source_rejects_before_mutating_seeded_destination(tmp_path, case):
    _LEGACY_CALLS.clear()
    input_root, output_root = tmp_path / "input", tmp_path / "output"
    table = output_root / "events"
    table.mkdir(parents=True)
    pl.DataFrame({"id": [99]}).write_parquet(table / "seed.parquet")
    prior_target = {path.name: path.read_bytes() for path in table.iterdir()}
    if case == "missing-column":
        source_table = input_root / "events"
        source_table.mkdir(parents=True)
        pl.DataFrame({"updated_seq": [1, 2]}).write_parquet(source_table / "part.parquet")
    metadata = tmp_path / "metadata.json"
    source_config = ({"python_function": __name__ + ".legacy_loader"}
                     if case == "legacy-function" else {})
    metadata.write_text(json.dumps({
        "connections": [
            {"name": "source", "connection_type": "function" if source_config else "file",
             "format": "function" if source_config else "parquet",
             "configure": {"base_path": str(input_root)}},
            {"name": "destination", "format": "parquet", "connection_type": "file",
             "configure": {"base_path": str(output_root)}},
        ],
        "dataflows": [{"name": "bounded-failure", "stage": "test",
                       "source": {"connection_name": "source", "table": "events",
                                  "watermark_columns": ["updated_seq"], **source_config},
                       "destination": {"connection_name": "destination", "table": "events",
                                       "load_type": "append"}}],
    }), encoding="utf-8")
    platform = LocalPlatform()
    state = tmp_path / "watermarks"
    provider = FileProvider(config_path=str(metadata), platform=platform, watermark_base_path=str(state))
    with DataCoolieDriver(
        engine=PolarsEngine(platform=platform), platform=platform, metadata_provider=provider,
        log_base_path=str(tmp_path / "logs"),
        config=DataCoolieRunConfig(retry_count=0, allowed_function_prefixes=[__name__]),
    ) as driver:
        flow, = driver.load_dataflows()
        provider.update_watermark(flow.dataflow_id, '{"updated_seq":100,"aux":"retained"}')
        state_file, = state.rglob("watermark_value.json")
        prior_state = state_file.read_bytes()
        result = driver.run_replay(flow, ReplayConfig(start=1, end=3, chunk_column="id", save_watermark=True))
        assert (result.total, result.succeeded, result.failed) == (1, 0, 1)
        expected_error = "read_range" if source_config else "id"
        assert expected_error in str(result.errors)
        assert state_file.read_bytes() == prior_state
    assert {path.name: path.read_bytes() for path in table.iterdir()} == prior_target
    assert _LEGACY_CALLS == []

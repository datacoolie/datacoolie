"""Format-contract tests replacing the removed Parquet logger path."""

from __future__ import annotations

import json
from pathlib import Path

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.runtime.manager import LogManager
from datacoolie.logging.execution_logger import ExecutionLogger
from datacoolie.platforms.local_platform import LocalPlatform

from tests.unit.logging.support import make_dataflow, make_runtime


class TestExecutionLoggerJsonFormat:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_v4_fixtures_use_one_session_identity_across_streams(self):
        fixture_root = Path(__file__).parents[2] / "fixtures" / "logging" / "v4"
        manifest = json.loads((fixture_root / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["schema_version"] == 4
        expected_types = {
            "job_snapshot.json": "job_run_log",
            "dataflow_snapshot.json": "dataflow_run_log",
            "dataflow_part_00000001.json": "dataflow_run_log",
            "system_snapshot.json": "system_log",
            "system_part_00000001.json": "system_log",
        }
        for filename in manifest["streams"]:
            for line in (fixture_root / filename).read_text(encoding="utf-8").splitlines():
                pairs = json.loads(line, object_pairs_hook=list)
                row = dict(pairs)
                assert row["log_schema_version"] == 4
                assert [key for key, _ in pairs[:2]] == [
                    "log_schema_version",
                    "_type",
                ]
                assert row["_type"] == expected_types[filename]
                assert row["job_id"] == "fixture-job"
                assert row["log_session_id"] == "fixture-session"

    def test_nullable_metadata_and_numeric_types_are_preserved(self, tmp_path):
        platform = LocalPlatform(base_path=str(tmp_path))
        dataflow = make_dataflow("nullable")
        dataflow.source.schema_name = None
        dataflow.destination.schema_name = None
        runtime = make_runtime("nullable", rows_read=7, rows_written=3)

        logger = ExecutionLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
        logger.set_run_config(DataCoolieRunConfig(job_id="job-1"))
        logger.activate()
        logger.log(dataflow, runtime)
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        logger.close()

        path = next(tmp_path.rglob("dataflow_*.json"))
        row = json.loads(path.read_text(encoding="utf-8"))
        assert row["source_schema"] is None
        assert row["destination_schema"] is None
        assert row["source_rows_read"] == 7
        assert row["destination_rows_written"] == 3

    def test_every_persisted_line_is_one_compact_json_object(self, tmp_path):
        platform = LocalPlatform(base_path=str(tmp_path))
        logger = ExecutionLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
        logger.set_run_config(DataCoolieRunConfig(job_id="job-1"))
        logger.activate()
        logger.log(make_dataflow(), make_runtime())
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        logger.close()

        for path in tmp_path.rglob("*.json"):
            raw = path.read_bytes()
            assert not raw.startswith(b"\xef\xbb\xbf")
            assert raw.endswith(b"\n")
            for line in raw.splitlines():
                value = json.loads(line)
                assert isinstance(value, dict)

    def test_logger_never_creates_removed_parquet_debug_or_analyst_outputs(self, tmp_path):
        platform = LocalPlatform(base_path=str(tmp_path))
        logger = ExecutionLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
        logger.set_run_config(DataCoolieRunConfig(job_id="job-1"))
        logger.activate()
        logger.log(make_dataflow(), make_runtime())
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        logger.close()

        assert not list(tmp_path.rglob("*.parquet"))
        assert not list(tmp_path.rglob("*.jsonl"))
        assert not any(part in {"debug_json", "analyst"} for path in tmp_path.rglob("*") for part in path.parts)

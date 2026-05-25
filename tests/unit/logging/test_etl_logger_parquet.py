"""Parquet-focused tests for ETLLogger output and schema behavior."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from datacoolie.core.constants import ExecutionType
from datacoolie.logging.base import LogConfig, LogManager
from datacoolie.logging.etl_logger import ETLLogger
from datacoolie.platforms.local_platform import LocalPlatform

from tests.unit.logging.support import (
    make_dataflow,
    make_maintenance_runtime,
    make_real_logger,
    make_runtime,
)


class TestAnalystParquetOutput:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_writes_dataflow_parquet_and_job_jsonl(self, tmp_path):
        pyarrow = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        logger, _ = make_real_logger(tmp_path)
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger.log(make_dataflow("b"), make_maintenance_runtime("b"))
        logger.close()

        # dataflow_run_log: per-run parquet
        parquet_files = sorted(tmp_path.rglob("*.parquet"))
        dataflow_files = [f for f in parquet_files if "dataflow_" in f.name]
        assert len(dataflow_files) == 1

        # No job_run_log parquet (replaced by JSONL)
        job_parquet_files = [f for f in parquet_files if f.name.startswith("job_")]
        assert len(job_parquet_files) == 0

        dtable = pq.read_table(dataflow_files[0])
        non_ts_names = [f.name for f in dtable.schema if not pyarrow.types.is_timestamp(f.type)]
        rows = dtable.select(non_ts_names).to_pydict()
        assert "job_status" not in rows
        assert None in set(rows["operation_type"])
        assert ExecutionType.MAINTENANCE.value in set(rows["operation_type"])
        assert dtable.schema.field("source_rows_read").type == pyarrow.int64()
        assert dtable.schema.field("start_time").type == pyarrow.timestamp("us", tz="UTC")

        # job_run_log: JSONL in analyst folder
        analyst_jsonl = [f for f in tmp_path.rglob("*.jsonl") if "analyst" in str(f)]
        assert len(analyst_jsonl) == 1
        lines = [json.loads(l) for l in analyst_jsonl[0].read_text(encoding="utf-8").strip().split("\n") if l.strip()]
        assert len(lines) == 1
        assert lines[0]["_type"] == "job_run_log"
        assert lines[0]["total_dataflows"] == 2

    def test_job_run_log_path_hive_partitioned(self, tmp_path):
        """analyst/job_run_log/__run_date=.../job_run_log.jsonl structure."""
        logger, _ = make_real_logger(tmp_path)
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger.close()

        analyst_jsonl = [f for f in tmp_path.rglob("*.jsonl") if "analyst" in str(f)]
        assert len(analyst_jsonl) == 1
        path_str = str(analyst_jsonl[0])
        assert "analyst" in path_str
        assert "job_run_log" in path_str
        assert "run_date=" in path_str
        # filename is date-based: job_run_log_YYYYMMDD.jsonl
        import re
        assert re.match(r"job_run_log_\d{8}\.jsonl$", analyst_jsonl[0].name)

    def test_partition_by_date_false_has_no_run_date_folder(self, tmp_path):
        pytest.importorskip("pyarrow")

        platform = LocalPlatform(base_path=str(tmp_path))
        logger = ETLLogger(LogConfig(output_path="logs", partition_by_date=False), platform)
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger.close()

        parquet_files = list(tmp_path.rglob("*.parquet"))
        assert len(parquet_files) >= 1
        assert all("run_date=" not in str(path) for path in parquet_files)

    def test_write_parquet_file_without_schema_handles_dict_list_columns(self):
        pyarrow = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        import tempfile
        from pathlib import Path

        base = Path(tempfile.mkdtemp())
        platform = LocalPlatform(base_path=str(base))
        logger = ETLLogger(LogConfig(output_path="logs"), platform)
        payload = [{"k": "v", "obj": {"a": 1}, "arr": [1, 2, 3]}]
        logger._write_parquet_file(payload, "logs/analyst/custom/test.parquet", pyarrow, pq, schema=None)

        paths = list(base.rglob("test.parquet"))
        assert len(paths) == 1
        table = pq.read_table(paths[0])
        rows = table.to_pydict()
        assert rows["obj"][0] == '{"a": 1}'
        assert rows["arr"][0] == "[1, 2, 3]"
        logger.close()


# ============================================================================
# Additional edge cases (merged from test_etl_logger_edge_cases.py)
# ============================================================================


class TestWriteHelpersEdgeCases:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_write_debug_jsonl_fallback_uses_append_file(self):
        from tests.unit.logging.support import make_logger

        logger, platform = make_logger()
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger._remove_debug_temp()  # force fallback (no temp file)

        logger._write_debug_jsonl({"job_id": "j1"})
        platform.append_file.assert_called()
        logger.close()

    def test_write_analyst_outputs_only_job_jsonl_when_no_runtime_logs(self, tmp_path):
        platform = LocalPlatform(base_path=str(tmp_path))
        logger = ETLLogger(LogConfig(output_path="logs"), platform)
        logger._runtime_logs = []

        summary = {"job_id": "j1", "total_dataflows": 0}
        logger._write_analyst_outputs(summary, "stem", datetime.now(timezone.utc))

        # No dataflow parquet when there are no runtime logs.
        parquet_files = list(tmp_path.rglob("*.parquet"))
        assert not any(path.name.startswith("dataflow_") for path in parquet_files)

        # job_run_log JSONL should still be written.
        analyst_jsonl = [f for f in tmp_path.rglob("*.jsonl") if "analyst" in str(f)]
        assert len(analyst_jsonl) == 1
        logger.close()

    def test_write_parquet_file_handles_write_exception(self):
        pa = pytest.importorskip("pyarrow")

        class BadPQ:
            @staticmethod
            def write_table(table, path, compression="snappy"):
                raise RuntimeError("pq write error")

        import tempfile
        from pathlib import Path

        base = Path(tempfile.mkdtemp())
        platform = LocalPlatform(base_path=str(base))
        logger = ETLLogger(LogConfig(output_path="logs"), platform)
        logger._write_parquet_file([{"a": 1}], "logs/analyst/x.parquet", pa, BadPQ(), schema=None)
        logger.close()

    def test_write_parquet_file_finally_branch_when_tmp_already_removed(self):
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        import tempfile
        from pathlib import Path

        base = Path(tempfile.mkdtemp())
        platform = LocalPlatform(base_path=str(base))

        def _remove_before_upload(local_path, remote_path, overwrite=False):
            if os.path.exists(local_path):
                os.remove(local_path)
            return None

        platform.upload_file = MagicMock(side_effect=_remove_before_upload)

        logger = ETLLogger(LogConfig(output_path="logs"), platform)
        logger._write_parquet_file([{"a": 1}], "logs/analyst/x.parquet", pa, pq, schema=None)
        logger.close()

    def test_remove_debug_temp_swallow_remove_error(self, tmp_path, monkeypatch):
        logger = ETLLogger(LogConfig(output_path="/logs"), platform=MagicMock())
        fp = tmp_path / "debug.jsonl"
        fp.write_text("{}\n", encoding="utf-8")
        logger._debug_temp_file = str(fp)

        monkeypatch.setattr("os.remove", lambda *_: (_ for _ in ()).throw(OSError("denied")))
        logger._remove_debug_temp()
        assert logger._debug_temp_file is None
        logger.close()

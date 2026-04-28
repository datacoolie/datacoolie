"""Flush behavior tests for ETLLogger (JSONL and periodic upload)."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

from datacoolie.core.constants import DataFlowStatus
from datacoolie.logging.base import LogConfig, LogManager
from datacoolie.logging.etl_logger import ETLLogger

from tests.unit.logging.support import make_dataflow, make_logger, make_real_logger, make_runtime


class TestDebugJsonlFlush:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_close_writes_jsonl_dataflow_entries_plus_summary(self, tmp_path):
        logger, _ = make_real_logger(tmp_path)
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger.log(make_dataflow("b"), make_runtime("b", status=DataFlowStatus.FAILED.value))
        logger.close()

        jsonl_files = list(tmp_path.rglob("*.jsonl"))
        assert len(jsonl_files) == 1
        lines = [json.loads(line) for line in jsonl_files[0].read_text(encoding="utf-8").strip().split("\n") if line.strip()]
        assert lines[0]["_type"] == "dataflow_run_log"
        assert lines[1]["_type"] == "dataflow_run_log"
        assert lines[-1]["_type"] == "job_run_log"
        assert lines[-1]["total_dataflows"] == 2

    def test_jsonl_path_uses_debug_json_and_job_run_log(self, tmp_path):
        logger, _ = make_real_logger(tmp_path)
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger.close()

        path = str(next(tmp_path.rglob("*.jsonl")))
        assert "debug_json" in path
        assert "job_run_log" in path

    def test_flush_falls_back_to_in_memory_when_no_temp_file(self):
        logger, platform = make_logger()
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger._remove_debug_temp()  # force fallback path in _write_debug_jsonl

        logger.flush()

        platform.upload_file.assert_called()
        uploads = [str(call.args[1]) for call in platform.upload_file.call_args_list]
        assert any(path.endswith(".jsonl") for path in uploads)
        logger.close()

    def test_no_pyarrow_only_jsonl_uploaded(self):
        logger, platform = make_logger()
        logger.log(make_dataflow("a"), make_runtime("a"))

        with patch.dict("sys.modules", {"pyarrow": None, "pyarrow.parquet": None}):
            logger.close()

        uploads = [str(call.args[1]) for call in platform.upload_file.call_args_list]
        assert any(path.endswith(".jsonl") for path in uploads)
        assert not any(path.endswith(".parquet") for path in uploads)


class TestPeriodicFlush:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_periodic_flush_uploads_after_interval(self):
        logger, platform = make_logger(flush_interval_seconds=1)

        with patch("datacoolie.logging.etl_logger.time.monotonic", side_effect=[100.0, 100.5, 101.6]):
            logger.log(make_dataflow("a"), make_runtime("a"))  # initializes last flush time only
            logger.log(make_dataflow("b"), make_runtime("b"))  # below threshold
            logger.log(make_dataflow("c"), make_runtime("c"))  # triggers periodic flush

        assert platform.upload_file.call_count >= 1
        logger.close()

    def test_periodic_flush_noop_without_platform_or_output(self):
        logger = ETLLogger(LogConfig(output_path=None), platform=None)
        logger._periodic_flush()  # should do nothing and not raise
        logger.close()

    def test_periodic_flush_noop_when_temp_file_missing(self):
        logger, _ = make_logger()
        logger._debug_temp_file = os.path.join(os.getcwd(), "definitely_missing.jsonl")
        logger._periodic_flush()  # should do nothing and not raise
        logger.close()


# ============================================================================
# Additional edge cases (merged from test_etl_logger_edge_cases.py)
# ============================================================================


class TestStreamAndPeriodicErrorPaths:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_stream_write_entry_handles_file_creation_error(self, monkeypatch):
        logger, _ = make_logger()

        monkeypatch.setattr("tempfile.mkstemp", lambda **kwargs: (_ for _ in ()).throw(OSError("no temp")))
        logger._stream_write_entry({"k": "v"})
        logger.close()

    def test_periodic_flush_returns_when_platform_none(self, tmp_path):
        logger = ETLLogger(LogConfig(output_path="/logs"), platform=None)
        fp = tmp_path / "debug.jsonl"
        fp.write_text("{}\n", encoding="utf-8")
        logger._debug_temp_file = str(fp)
        logger._periodic_flush()
        logger.close()

    def test_periodic_flush_handles_upload_error(self, tmp_path):
        platform = MagicMock()
        platform.upload_file.side_effect = RuntimeError("upload failed")
        logger = ETLLogger(LogConfig(output_path="/logs"), platform=platform)

        fp = tmp_path / "debug.jsonl"
        fp.write_text("{}\n", encoding="utf-8")
        logger._debug_temp_file = str(fp)

        logger._periodic_flush()
        logger.close()

    def test_flush_handles_internal_exception(self):
        logger, _ = make_logger()
        logger.log(make_dataflow("a"), make_runtime("a"))

        logger._write_debug_jsonl = MagicMock(side_effect=RuntimeError("flush boom"))  # type: ignore[method-assign]
        logger.flush()
        logger.close()

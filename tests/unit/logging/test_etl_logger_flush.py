"""Flush behavior tests for ETLLogger (JSONL and periodic upload)."""

from __future__ import annotations

import json
import threading
import time
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

        # Debug JSONL is under debug_json/
        debug_files = [f for f in tmp_path.rglob("*.jsonl") if "debug_json" in str(f)]
        assert len(debug_files) == 1
        lines = [json.loads(line) for line in debug_files[0].read_text(encoding="utf-8").strip().split("\n") if line.strip()]
        assert lines[0]["_type"] == "dataflow_run_log"
        assert lines[1]["_type"] == "dataflow_run_log"
        assert lines[-1]["_type"] == "job_run_log"
        assert lines[-1]["total_dataflows"] == 2

    def test_jsonl_path_uses_debug_json_and_job_run_log(self, tmp_path):
        logger, _ = make_real_logger(tmp_path)
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger.close()

        debug_files = [f for f in tmp_path.rglob("*.jsonl") if "debug_json" in str(f)]
        assert len(debug_files) == 1
        path = str(debug_files[0])
        assert "debug_json" in path
        assert "job_run_log" in path

    def test_close_builds_debug_jsonl_from_memory(self):
        logger, platform = make_logger()
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger.close()

        append_paths = [str(call.args[0]) for call in platform.append_file.call_args_list]
        assert any(path.endswith(".jsonl") for path in append_paths)

    def test_no_pyarrow_only_jsonl_appended(self):
        logger, platform = make_logger()
        logger.log(make_dataflow("a"), make_runtime("a"))

        with patch.dict("sys.modules", {"pyarrow": None, "pyarrow.parquet": None}):
            logger.close()

        # Debug JSONL and analyst job_run_log use append_file.
        append_paths = [str(call.args[0]) for call in platform.append_file.call_args_list]
        assert any(path.endswith(".jsonl") for path in append_paths)
        # No parquet uploaded.
        upload_paths = [str(call.args[1]) for call in platform.upload_file.call_args_list]
        assert not any(path.endswith(".parquet") for path in upload_paths)

    def test_repeated_close_is_idempotent(self):
        logger, platform = make_logger(flush_interval_seconds=0)
        logger.log(make_dataflow("a"), make_runtime("a"))

        logger.close()
        first_append_count = platform.append_file.call_count
        first_upload_count = platform.upload_file.call_count
        logger.close()

        assert platform.append_file.call_count == first_append_count
        assert platform.upload_file.call_count == first_upload_count

    def test_terminal_sink_failure_does_not_suppress_other_sinks(self):
        logger, platform = make_logger(flush_interval_seconds=0)
        logger.log(make_dataflow("a"), make_runtime("a"))

        def append_file(path, content):
            if "/analyst/job_run_log/" in path:
                raise RuntimeError("analyst unavailable")

        platform.append_file.side_effect = append_file

        logger.close()
        assert isinstance(logger.last_flush_error, RuntimeError)

        append_paths = [
            str(call.args[0]) for call in platform.append_file.call_args_list
        ]
        assert sum("/debug_json/" in path for path in append_paths) == 1
        assert sum("/analyst/job_run_log/" in path for path in append_paths) == 1
        assert platform.upload_file.call_count == 1
        assert {item.name: item.status for item in logger.terminal_outcomes} == {
            "debug_jsonl": "succeeded",
            "analyst_job_jsonl": "failed",
            "analyst_dataflow_parquet": "succeeded",
        }


class TestPeriodicFlush:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_periodic_flush_appends_after_interval(self):
        logger, platform = make_logger(flush_interval_seconds=0)

        # Log entries to create an in-memory checkpoint batch.
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger.log(make_dataflow("b"), make_runtime("b"))

        # Directly invoke the periodic flush hook (same as timer would call).
        with patch("datacoolie.logging.etl_logger._logger") as internal_log:
            logger._on_periodic_flush()

        assert platform.append_file.call_count >= 1
        periodic_path = platform.append_file.call_args_list[0].args[0]
        internal_log.debug.assert_called_once_with(
            "ETL log pushed: %s",
            periodic_path,
        )
        internal_log.info.assert_not_called()
        logger.close()

    def test_periodic_flush_noop_without_platform_or_output(self):
        logger = ETLLogger(LogConfig(output_path=None), platform=None)
        logger._on_periodic_flush()  # should do nothing and not raise
        logger.close()

    def test_periodic_flush_noop_without_records(self):
        logger, _ = make_logger()
        logger._on_periodic_flush()  # should do nothing and not raise
        logger.close()

    def test_slow_periodic_storage_does_not_block_new_etl_records(self):
        logger, platform = make_logger(flush_interval_seconds=0)
        logger.log(make_dataflow("a"), make_runtime("a"))
        append_started = threading.Event()
        release_append = threading.Event()

        def append_file(_path, _content):
            append_started.set()
            assert release_append.wait(timeout=2)

        platform.append_file.side_effect = append_file
        periodic = threading.Thread(target=logger._on_periodic_flush)
        periodic.start()
        assert append_started.wait(timeout=2)

        logger.log(make_dataflow("b"), make_runtime("b"))
        assert len(logger._runtime_logs) == 2

        release_append.set()
        periodic.join(timeout=2)
        logger.close()

    def test_periodic_success_during_close_does_not_duplicate_records(self):
        logger, platform = make_logger(
            flush_interval_seconds=0.01,
            close_timeout_seconds=0.5,
        )
        append_started = threading.Event()
        release_append = threading.Event()
        first_debug = True

        def append_file(path, _content):
            nonlocal first_debug
            if "/debug_json/" in path and first_debug:
                first_debug = False
                append_started.set()
                assert release_append.wait(timeout=2)

        platform.append_file.side_effect = append_file
        logger.activate()
        logger.log(make_dataflow("a"), make_runtime("a"))
        assert append_started.wait(timeout=2)

        closing = threading.Thread(target=logger.close)
        closing.start()
        release_append.set()
        closing.join(timeout=2)

        debug_payload = "".join(
            call.args[1]
            for call in platform.append_file.call_args_list
            if "/debug_json/" in call.args[0]
        )
        assert debug_payload.count('"dataflow_id": "a"') == 1
        assert debug_payload.count('"_type": "job_run_log"') == 1

    def test_blocked_periodic_skips_only_ambiguous_debug_sink(self):
        logger, platform = make_logger(
            flush_interval_seconds=0.01,
            close_timeout_seconds=0.05,
        )
        append_started = threading.Event()
        release_append = threading.Event()

        def append_file(path, _content):
            if "/debug_json/" in path:
                append_started.set()
                release_append.wait(timeout=2)

        platform.append_file.side_effect = append_file
        logger.activate()
        logger.log(make_dataflow("a"), make_runtime("a"))
        assert append_started.wait(timeout=2)

        started = time.monotonic()
        logger.close()
        elapsed = time.monotonic() - started
        release_append.set()

        append_paths = [
            call.args[0] for call in platform.append_file.call_args_list
        ]
        assert elapsed < 0.3
        assert sum("/debug_json/" in path for path in append_paths) == 1
        assert any("/analyst/job_run_log/" in path for path in append_paths)
        assert platform.upload_file.call_count == 1
        outcomes = {item.name: item.status for item in logger.terminal_outcomes}
        assert outcomes["debug_jsonl"] == "timed_out"
        assert outcomes["analyst_job_jsonl"] == "succeeded"
        assert outcomes["analyst_dataflow_parquet"] == "succeeded"


# ============================================================================
# Additional edge cases (merged from test_etl_logger_edge_cases.py)
# ============================================================================


class TestStreamAndPeriodicErrorPaths:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_log_hot_path_does_not_create_temp_file(self):
        logger, _ = make_logger()
        create_temp = MagicMock(side_effect=AssertionError("unexpected temp I/O"))
        with patch(
            "datacoolie.logging.etl_logger.tempfile.NamedTemporaryFile",
            create_temp,
        ):
            logger.log(make_dataflow("a"), make_runtime("a"))
        create_temp.assert_not_called()
        logger.close()

    def test_periodic_flush_returns_when_platform_none(self):
        logger = ETLLogger(LogConfig(output_path="/logs"), platform=None)
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger._on_periodic_flush()
        logger.close()

    def test_periodic_flush_failure_keeps_record_cursor(self):
        platform = MagicMock()
        platform.append_file.side_effect = RuntimeError("append failed")
        logger = ETLLogger(LogConfig(output_path="/logs"), platform=platform)
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger._on_periodic_flush()
        assert logger._debug_flushed_count == 0
        assert isinstance(logger.last_flush_error, RuntimeError)
        platform.append_file.side_effect = None
        logger._on_periodic_flush()
        assert logger._debug_flushed_count == 1
        logger.close()

    def test_debug_failure_does_not_dump_or_block_analyst_outputs(self):
        logger, platform = make_logger()
        logger.log(make_dataflow("a"), make_runtime("a"))

        def append_file(path, content):
            if "/debug_json/" in path:
                raise RuntimeError("flush boom")

        platform.append_file.side_effect = append_file
        logger.close()
        assert platform.upload_file.call_count == 1
        outcomes = {item.name: item.status for item in logger.terminal_outcomes}
        assert outcomes["debug_jsonl"] == "failed"
        assert outcomes["analyst_job_jsonl"] == "succeeded"
        assert outcomes["analyst_dataflow_parquet"] == "succeeded"

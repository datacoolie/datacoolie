"""Tests for logging constants, configuration, and path layout."""

from __future__ import annotations

from datetime import datetime

import pytest

from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.configuration.constants import LogCategory, LogLevel, LogType, StorageMode
from datacoolie.logging.persistence.layout import (
    build_job_stem,
    format_partition_path,
    safe_filename_token,
)


class TestEnums:
    def test_log_levels(self):
        assert LogLevel.DEBUG.value == "DEBUG"
        assert LogLevel.INFO.value == "INFO"
        assert LogLevel.WARNING.value == "WARNING"

    def test_log_types(self):
        assert LogType.JOB_RUN_LOG.value == "job_run_log"
        assert LogType.DATAFLOW_RUN_LOG.value == "dataflow_run_log"
        assert LogType.SYSTEM_LOG.value == "system_log"
        assert LogLevel.ERROR.value == "ERROR"
        assert LogLevel.CRITICAL.value == "CRITICAL"

    def test_storage_modes(self):
        assert StorageMode.MEMORY.value == "memory"
        assert StorageMode.FILE.value == "file"

    def test_log_categories_are_stable_layout_names(self):
        assert LogCategory.EXECUTION.value == "execution_logs"
        assert LogCategory.SYSTEM.value == "system_logs"


# ============================================================================
# LogConfig


class TestLogConfig:
    def test_defaults(self):
        cfg = LogConfig()
        assert cfg.log_level == "INFO"
        assert cfg.file_level == "DEBUG"
        assert cfg.storage_mode == StorageMode.MEMORY.value
        assert cfg.output_path is None
        assert cfg.partition_by_date is True
        assert cfg.flush_interval_seconds == 300
        assert cfg.persistence_mode == "snapshot"
        assert cfg.flush_batch_bytes == 4 * 1024 * 1024

    def test_level_uppercased(self):
        cfg = LogConfig(log_level="debug")
        assert cfg.log_level == "DEBUG"

    def test_file_level_uppercased(self):
        cfg = LogConfig(file_level="warning")
        assert cfg.file_level == "WARNING"

    def test_custom(self):
        cfg = LogConfig(
            log_level="WARNING",
            file_level="INFO",
            storage_mode="file",
            output_path="/logs",
            partition_by_date=False,
        )
        assert cfg.log_level == "WARNING"
        assert cfg.file_level == "INFO"
        assert cfg.output_path == "/logs"
        assert cfg.partition_by_date is False

    @pytest.mark.parametrize("value", ["true", 1, 0, None])
    def test_partition_by_date_requires_real_boolean(self, value):
        with pytest.raises(ValueError, match="partition_by_date"):
            LogConfig(partition_by_date=value)

    @pytest.mark.parametrize("field", ["output_path", "spool_directory"])
    def test_optional_paths_reject_blank_strings(self, field):
        with pytest.raises(ValueError, match=field):
            LogConfig(**{field: "  "})


# ============================================================================
# Partition configuration and formatting


class TestLogConfigPartitionPattern:
    def test_default_partition_pattern(self):
        cfg = LogConfig()
        assert cfg.partition_pattern == "__run_date={year}-{month}-{day}"

    def test_custom_partition_pattern(self):
        cfg = LogConfig(partition_pattern="year={year}/month={month}/day={day}/hour={hour}")
        assert cfg.partition_pattern == "year={year}/month={month}/day={day}/hour={hour}"

    @pytest.mark.parametrize(
        "pattern",
        [
            "{year}/{day}",
            "{month}/{year}",
            "{year}/{month}/{month}",
            "{year}/{month}/{week}",
            "v2-{year}",
            "{year}/calendar/{month}",
            "{year}\\calendar\\{month}",
            "{year}-%m",
            "{year}{",
            "{{year}}",
            "{year:04d}",
            "{year!s}",
            "{year.__class__}",
            "",
        ],
    )
    def test_invalid_partition_pattern_fails_at_configuration(self, pattern):
        with pytest.raises(ValueError, match="partition_pattern"):
            LogConfig(partition_pattern=pattern)

    @pytest.mark.parametrize(
        "pattern",
        [
            "logs_{year}",
            "logs_{year}--m_{month}",
            "logs_{year}--m_{month}__d_{day}",
            "logs_{year}--m_{month}__d_{day}++h_{hour}",
            "{year}{month}{day}{hour}",
        ],
    )
    def test_ordered_partition_patterns_accept_arbitrary_literals(self, pattern):
        assert LogConfig(partition_pattern=pattern).partition_pattern == pattern

    def test_partition_formatter_zero_pads_year_to_contract_width(self):
        assert format_partition_path("logs", datetime(9, 2, 3), "{year}{month}{day}") == (
            "logs/00090203"
        )

    def test_partition_formatter_rejects_invalid_pattern_when_called_directly(self):
        with pytest.raises(ValueError, match="partition_pattern"):
            format_partition_path("/base", pattern="{year}/{day}")

    @pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan")])
    def test_close_timeout_must_be_positive_and_finite(self, value):
        with pytest.raises(ValueError, match="close_timeout_seconds"):
            LogConfig(close_timeout_seconds=value)


class TestLogFilenameTokens:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("safe-job_01", "safe-job_01"),
            ("a/b", "a%2Fb"),
            ("a_b", "a_b"),
            ("a%2Fb", "a%252Fb"),
            (".hidden.", "%2Ehidden%2E"),
        ],
    )
    def test_unsafe_tokens_are_reversible_and_distinct(self, value, expected):
        assert safe_filename_token(value) == expected

    def test_common_stem_uses_same_encoded_job_identity(self):
        started = datetime(2024, 1, 2, 3, 4, 5)
        stem = build_job_stem(started, job_id="a/b", job_num=1, job_index=0)
        assert stem.endswith("_a%2Fb")


# ============================================================================
# Additional edge cases (merged from test_logging_edge_cases.py)

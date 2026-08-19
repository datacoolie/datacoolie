"""Parquet-focused tests for ETLLogger output behavior."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import pytest

from datacoolie.core.constants import ExecutionType
from datacoolie.logging.base import LogConfig, LogManager
from datacoolie.logging.etl_logger import ETLLogger, _build_dataflow_schema
from datacoolie.platforms.local_platform import LocalPlatform

from tests.unit.logging.support import (
    make_dataflow,
    make_maintenance_runtime,
    make_real_logger,
    make_runtime,
    make_transform_dataflow,
)


class TestAnalystParquetOutput:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_writes_dataflow_parquet_and_job_jsonl(self, tmp_path):
        pytest.importorskip("pyarrow")
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

        rows = pq.read_table(dataflow_files[0]).to_pylist()
        assert len(rows) == 2
        assert "job_status" not in rows[0]
        operation_types = {row["operation_type"] for row in rows}
        assert None in operation_types
        assert ExecutionType.MAINTENANCE.value in operation_types

        # job_run_log: JSONL in analyst folder
        analyst_jsonl = [f for f in tmp_path.rglob("*.jsonl") if "analyst" in str(f)]
        assert len(analyst_jsonl) == 1
        lines = [
            json.loads(line)
            for line in analyst_jsonl[0]
            .read_text(encoding="utf-8")
            .strip()
            .split("\n")
            if line.strip()
        ]
        assert len(lines) == 1
        assert lines[0]["_type"] == "job_run_log"
        assert lines[0]["total_dataflows"] == 2

    def test_dataflow_parquet_keeps_nullable_integer_contract(self, tmp_path):
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        logger, _ = make_real_logger(tmp_path)
        dataflow = make_dataflow("typed")
        dataflow.group_number = 2
        dataflow.execution_order = 10
        logger.log(dataflow, make_runtime("typed"))
        logger.close()

        dataflow_file = next(
            path for path in tmp_path.rglob("*.parquet") if "dataflow_" in path.name
        )
        parquet_schema = pq.ParquetFile(dataflow_file).schema_arrow

        expected_schema = _build_dataflow_schema(pa)
        for field in expected_schema:
            assert parquet_schema.field(field.name).type == field.type

    def test_parquet_projects_typed_transform_metadata(self, tmp_path):
        pq = pytest.importorskip("pyarrow.parquet")

        logger, _ = make_real_logger(tmp_path)
        select_dataflow = make_transform_dataflow("select-transform")
        drop_dataflow = make_transform_dataflow(
            "drop-transform",
            use_drop_projection=True,
        )
        logger.log(select_dataflow, make_runtime(select_dataflow.dataflow_id))
        logger.log(drop_dataflow, make_runtime(drop_dataflow.dataflow_id))
        logger.close()

        dataflow_file = next(
            path for path in tmp_path.rglob("*.parquet") if "dataflow_" in path.name
        )
        table = pq.read_table(dataflow_file)
        expected_columns = {
            "transform_select_columns",
            "transform_drop_columns",
            "transform_rename_columns",
            "transform_value_rules",
            "transform_hash_columns",
            "transform_masking_rules",
            "transform_configure",
        }
        assert expected_columns.issubset(table.schema.names)
        assert "transform_missing_column_policy" not in table.schema.names

        rows = {row["dataflow_id"]: row for row in table.to_pylist()}
        select_row = rows["select-transform"]
        drop_row = rows["drop-transform"]
        assert json.loads(select_row["transform_select_columns"]) == ["customer_id", "email"]
        assert json.loads(drop_row["transform_drop_columns"]) == ["internal_note"]
        assert json.loads(select_row["transform_rename_columns"]) == {
            "email": "contact_email"
        }
        assert json.loads(select_row["transform_value_rules"])[0]["mapping"] == {
            "A": "active",
            "I": "inactive",
        }
        assert json.loads(select_row["transform_masking_rules"])[0]["value"] == "[PRIVATE]"
        assert json.loads(select_row["transform_configure"])["missing_column_policy"] == "ignore"

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
        assert analyst_jsonl[0].name.startswith("job_")
        assert analyst_jsonl[0].name.endswith("_1_0_j1.jsonl")

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

    def test_write_parquet_file_surfaces_write_exception_to_flush_boundary(self):
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
        with pytest.raises(RuntimeError, match="pq write error"):
            logger._write_parquet_file(
                [{"a": 1}],
                "logs/analyst/x.parquet",
                pa,
                BadPQ(),
                schema=None,
            )
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

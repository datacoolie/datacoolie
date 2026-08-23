"""PySpark engine facade backed by private Spark capability modules."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession

from datacoolie.core.constants import Format
from datacoolie.core.exceptions import EngineError
from datacoolie.core.models import HashColumn, MaskingRule, ValueRule
from datacoolie.engines._spark import database as spark_database
from datacoolie.engines._spark import delta as spark_delta
from datacoolie.engines._spark import file_io as spark_file_io
from datacoolie.engines._spark import metrics as spark_metrics
from datacoolie.engines._spark import table_operations as spark_table_operations
from datacoolie.engines._spark import transforms as spark_transforms
from datacoolie.engines._spark import type_mapping as spark_type_mapping
from datacoolie.engines._spark.session_builder import get_or_create_spark_session
from datacoolie.engines._spark.iceberg import operations as spark_iceberg_operations
from datacoolie.engines.base import BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.platforms.base import BasePlatform

logger = get_logger(__name__)


class SparkEngine(BaseEngine[DataFrame]):
    """PySpark implementation of :class:`BaseEngine`.

    Args:
        spark_session: Existing session for managed or notebook runtimes. A
            configured session is created when omitted.
        config: Spark configuration overrides used only when creating or
            updating the active session.
        platform: Optional platform attached to this engine.

    Implementation details are delegated to the private ``_spark`` capability
    modules; this class remains the stable public facade and runtime-state owner.
    """

    def __init__(
        self,
        spark_session: Optional[SparkSession] = None,
        config: Optional[Dict[str, str]] = None,
        platform: Optional[BasePlatform] = None,
    ) -> None:
        super().__init__(platform=platform)
        self._spark = get_or_create_spark_session(
            app_name="DataCoolie_SparkEngine",
            config=config,
            existing_session=spark_session,
        )

    @property
    def spark(self) -> SparkSession:
        """Return the underlying ``SparkSession``."""
        return self._spark

    @property
    def spark_major_version(self) -> int:
        """Return the active Spark runtime's major version."""
        return int(self._spark.version.split(".")[0])

    def read_parquet(
        self, path: str | list[str], options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        return spark_file_io.read_parquet(self._spark, path, options)

    def read_delta(
        self, path: str, options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        return spark_file_io.read_delta(self._spark, path, options)

    def read_iceberg(
        self, path: str, options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        return spark_file_io.read_iceberg(self._spark, path, options)

    def read_csv(
        self, path: str | list[str], options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        return spark_file_io.read_csv(self._spark, path, options)

    def read_json(
        self, path: str | list[str], options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        return spark_file_io.read_json(self._spark, path, options, multiline=True)

    def read_jsonl(
        self, path: str | list[str], options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        return spark_file_io.read_json(self._spark, path, options, multiline=False)

    def read_avro(
        self, path: str | list[str], options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        return spark_file_io.read_avro(self._spark, path, options)

    def read_excel(
        self, path: str | list[str], options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        extension = self.FORMAT_EXTENSIONS.get(Format.EXCEL.value, ".xlsx")
        return spark_file_io.read_excel(
            self._spark, self._resolve_file_paths(path, extension), path, options
        )

    def read_database(
        self,
        *,
        table: Optional[str] = None,
        query: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        return spark_database.read_database(
            self._spark,
            table=table,
            query=query,
            options=options,
            driver_connection_keys=self.DRIVER_CONNECTION_KEYS,
        )

    def create_dataframe(self, records: List[Dict[str, Any]]) -> DataFrame:
        if not records:
            return self._spark.createDataFrame([], schema="string")
        keys: list[str] = []
        seen: set[str] = set()
        for record in records:
            for key in record:
                if key not in seen:
                    keys.append(key)
                    seen.add(key)
        return self._spark.createDataFrame(
            [{key: row.get(key) for key in keys} for row in records]
        )

    def execute_sql(
        self, sql: str, parameters: Optional[Dict[Any, Any]] = None
    ) -> DataFrame:
        return self._spark.sql(sql, parameters) if parameters else self._spark.sql(sql)

    def read_table(
        self,
        table_name: str,
        fmt: str = "delta",
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        return self._spark.table(table_name)

    def read_path(
        self, path: str | list[str], fmt: str, options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        return spark_file_io.read_path(self._spark, path, fmt, options)

    def write_to_path(
        self,
        df: DataFrame,
        path: str,
        mode: str,
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        spark_file_io.write_to_path(df, path, mode, fmt, partition_columns, options)

    def write_to_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str,
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
        _skip_iceberg_evolution: bool = False,
    ) -> None:
        spark_table_operations.write_to_table(
            self._spark,
            df,
            table_name,
            mode,
            fmt,
            partition_columns,
            options,
            skip_iceberg_evolution=_skip_iceberg_evolution,
        )

    def merge_to_path(
        self,
        df: DataFrame,
        path: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        spark_delta.merge_to_path(self._spark, df, path, merge_keys, fmt)

    def merge_overwrite_to_path(
        self,
        df: DataFrame,
        path: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        spark_delta.merge_overwrite_to_path(
            self._spark, df, path, merge_keys, fmt, partition_columns, options
        )

    def merge_to_table(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: List[str],
        fmt: str,
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        spark_table_operations.merge_to_table(
            self._spark, df, table_name, merge_keys, fmt, partition_columns
        )

    def merge_overwrite_to_table(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        spark_table_operations.merge_overwrite_to_table(
            self._spark, df, table_name, merge_keys, fmt, partition_columns, options
        )

    def delete_by_window_path(
        self, path: str, window: Dict[str, tuple], fmt: str = "delta"
    ) -> None:
        spark_delta.delete_by_window_path(path, window, fmt, self._spark)

    def delete_by_window_table(
        self, table_name: str, window: Dict[str, tuple], fmt: str = "delta"
    ) -> None:
        spark_table_operations.delete_by_window_table(
            self._spark, table_name, window, fmt
        )

    def scd2_to_path(
        self,
        df: DataFrame,
        path: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        spark_delta.scd2_to_path(
            self._spark, df, path, merge_keys, fmt, partition_columns, options
        )

    def scd2_to_table(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: List[str],
        fmt: str = "delta",
        partition_columns: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        spark_table_operations.scd2_to_table(
            self._spark, df, table_name, merge_keys, fmt, partition_columns, options
        )

    def add_column(self, df: DataFrame, column_name: str, expression: str) -> DataFrame:
        return spark_transforms.add_column(df, column_name, expression)

    def drop_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        return spark_transforms.drop_columns(df, columns)

    def select_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        return spark_transforms.select_columns(df, columns)

    def rename_column(self, df: DataFrame, old_name: str, new_name: str) -> DataFrame:
        return spark_transforms.rename_column(df, old_name, new_name)

    def rename_columns(self, df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
        return spark_transforms.rename_columns(df, mapping)

    def apply_value_rule(
        self, df: DataFrame, rule: ValueRule, *, missing_column_policy: str = "error"
    ) -> DataFrame:
        resolved: List[str] = []
        for requested in rule.columns:
            try:
                resolved.append(self._resolve_column_name(df.columns, requested))
            except EngineError:
                if missing_column_policy != "ignore":
                    raise
        return spark_transforms.apply_value_rule(df, rule, resolved)

    def apply_masking_rule(
        self, df: DataFrame, rule: MaskingRule, *, missing_column_policy: str = "error"
    ) -> DataFrame:
        resolved: List[str] = []
        for requested in rule.columns:
            try:
                resolved.append(self._resolve_column_name(df.columns, requested))
            except EngineError:
                if missing_column_policy != "ignore":
                    raise
        return spark_transforms.apply_masking_rule(df, rule, resolved)

    def add_hash_column(self, df: DataFrame, definition: HashColumn) -> DataFrame:
        resolved = [
            self._resolve_column_name(df.columns, requested)
            for requested in definition.columns
        ]
        return spark_transforms.add_hash_column(df, definition, resolved)

    def filter_rows(self, df: DataFrame, condition: str) -> DataFrame:
        return spark_transforms.filter_rows(df, condition)

    def apply_watermark_filter(
        self,
        df: DataFrame,
        watermark_columns: List[str],
        watermark_start: Dict[str, Any],
        *,
        start_operator: str = ">",
        watermark_end: Optional[Dict[str, Any]] = None,
        end_operator: str = "<",
    ) -> DataFrame:
        return spark_transforms.apply_watermark_filter(
            df,
            watermark_columns,
            watermark_start,
            start_operator=start_operator,
            watermark_end=watermark_end,
            end_operator=end_operator,
        )

    def deduplicate(
        self,
        df: DataFrame,
        partition_columns: List[str],
        order_columns: Optional[List[str]] = None,
        order: str = "desc",
    ) -> DataFrame:
        return spark_transforms.deduplicate(
            df, partition_columns, order_columns, order, rank=False
        )

    def deduplicate_by_rank(
        self,
        df: DataFrame,
        partition_columns: List[str],
        order_columns: List[str],
        order: str = "desc",
    ) -> DataFrame:
        return spark_transforms.deduplicate(
            df, partition_columns, order_columns, order, rank=True
        )

    def cast_column(
        self,
        df: DataFrame,
        column_name: str,
        target_type: str,
        fmt: Optional[str] = None,
    ) -> DataFrame:
        return spark_type_mapping.cast_column(df, column_name, target_type, fmt)

    def add_system_columns(
        self,
        df: DataFrame,
        author: Optional[str] = None,
        dataflow_run_id: Optional[str] = None,
    ) -> DataFrame:
        return spark_transforms.add_system_columns(df, author, dataflow_run_id)

    def add_file_info_columns(self, df: DataFrame, file_infos=None) -> DataFrame:
        return spark_file_io.add_file_info_columns(self._spark, df, file_infos)

    def convert_timestamp_ntz_to_timestamp(self, df: DataFrame) -> DataFrame:
        return spark_transforms.convert_timestamp_ntz_to_timestamp(df)

    def count_rows(self, df: DataFrame) -> int:
        return spark_metrics.count_rows(df)

    def is_empty(self, df: DataFrame) -> bool:
        return spark_metrics.is_empty(df)

    def get_columns(self, df: DataFrame) -> List[str]:
        return spark_metrics.get_columns(df)

    def get_schema(self, df: DataFrame) -> Dict[str, str]:
        return spark_metrics.get_schema(df)

    def get_hive_schema(self, df: DataFrame) -> Dict[str, str]:
        return spark_metrics.get_hive_schema(df)

    def get_max_values(self, df: DataFrame, columns: List[str]) -> Dict[str, Any]:
        return spark_metrics.get_max_values(df, columns)

    def get_count_and_max_values(
        self, df: DataFrame, columns: List[str]
    ) -> Tuple[int, Dict[str, Any]]:
        return spark_metrics.get_count_and_max_values(df, columns)

    def table_exists_by_path(self, path: str, *, fmt: str = "delta") -> bool:
        try:
            if fmt.lower() == Format.ICEBERG.value:
                return spark_iceberg_operations.table_exists_by_path(
                    self._spark, self._platform, path
                )
            if fmt.lower() == Format.DELTA.value:
                return spark_delta.table_exists_by_path(
                    self._spark, self._platform, path
                )
            return self._platform.folder_exists(path)
        except Exception:  # noqa: BLE001
            return False

    def table_exists_by_name(self, table_name: str, *, fmt: str = "delta") -> bool:
        try:
            return self._spark.catalog.tableExists(table_name)
        except Exception:  # noqa: BLE001
            return False

    def get_history_by_path(
        self,
        path: str,
        limit: int = 1,
        start_time: Optional[datetime] = None,
        *,
        end_time: Optional[datetime] = None,
        fmt: str = "delta",
    ) -> List[Dict[str, Any]]:
        if fmt.lower() == Format.ICEBERG.value:
            return spark_iceberg_operations.get_history(
                self._spark, path, limit, start_time, end_time, is_path=True
            )
        if fmt.lower() == Format.DELTA.value:
            return spark_delta.get_history(
                self._spark, path, limit, start_time, end_time, is_path=True
            )
        return []

    def get_history_by_name(
        self,
        table_name: str,
        limit: int = 1,
        start_time: Optional[datetime] = None,
        *,
        end_time: Optional[datetime] = None,
        fmt: str = "delta",
    ) -> List[Dict[str, Any]]:
        if fmt.lower() == Format.ICEBERG.value:
            return spark_iceberg_operations.get_history(
                self._spark, table_name, limit, start_time, end_time, is_path=False
            )
        if fmt.lower() == Format.DELTA.value:
            return spark_delta.get_history(
                self._spark, table_name, limit, start_time, end_time, is_path=False
            )
        return []

    def compact_by_path(
        self, path: str, *, fmt: str = "delta", options: Optional[Dict[str, Any]] = None
    ) -> None:
        if fmt.lower() != Format.DELTA.value:
            logger.warning(
                "compact_by_path: compaction by path is only supported for Delta, skipping %s",
                fmt,
            )
            return
        spark_delta.compact_by_path(self._spark, path)

    def compact_by_name(
        self,
        table_name: str,
        *,
        fmt: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        if fmt.lower() == Format.ICEBERG.value:
            spark_iceberg_operations.compact_by_name(
                self._spark, table_name, options
            )
        elif fmt.lower() == Format.DELTA.value:
            spark_delta.compact_by_name(self._spark, table_name)
        else:
            logger.warning(
                "compact_by_name: compaction by name is only supported for Delta and Iceberg, skipping %s",
                fmt,
            )

    def cleanup_by_path(
        self,
        path: str,
        retention_hours: int = 168,
        *,
        fmt: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        if fmt.lower() != Format.DELTA.value:
            logger.warning(
                "cleanup_by_path: cleanup by path is only supported for Delta, skipping %s",
                fmt,
            )
            return
        spark_delta.cleanup_by_path(self._spark, path, retention_hours)

    def cleanup_by_name(
        self,
        table_name: str,
        retention_hours: int = 168,
        *,
        fmt: str = "delta",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        if fmt.lower() == Format.ICEBERG.value:
            spark_iceberg_operations.cleanup_by_name(
                self._spark, table_name, retention_hours, options
            )
        elif fmt.lower() == Format.DELTA.value:
            spark_delta.cleanup_by_name(self._spark, table_name, retention_hours)
        else:
            logger.warning(
                "cleanup_by_name: cleanup by name is only supported for Delta and Iceberg, skipping %s",
                fmt,
            )

    def generate_symlink_manifest(self, path: str) -> None:
        logger.debug("SparkEngine: generating symlink manifest for %s", path)
        spark_delta.generate_symlink_manifest(self._spark, path)

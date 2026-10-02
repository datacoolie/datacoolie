"""Stateless Spark file and path I/O helpers."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    DataType,
    MapType,
    StructType,
    TimestampType,
)
from pyspark.sql import functions as sf

from datacoolie.core.constants import FileInfoColumn, Format, LoadType
from datacoolie.core.exceptions import EngineError


def apply_options(target: Any, options: Optional[Dict[str, Any]]) -> Any:
    """Apply reader or writer options in insertion order."""
    if options:
        for key, value in options.items():
            target = target.option(key, value)
    return target


def path_is_absent_or_empty(spark: SparkSession, platform: Any, path: str) -> bool:
    """Return whether a target path is absent or is an empty directory."""

    if platform is not None:
        if platform.file_exists(path):
            return False
        if not platform.folder_exists(path):
            return True
        return not platform.list_files(path) and not platform.list_folders(path)

    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not filesystem.exists(hadoop_path):
        return True
    if filesystem.getFileStatus(hadoop_path).isFile():
        return False
    return len(filesystem.listStatus(hadoop_path)) == 0


def validate_write_options(
    options: Optional[Dict[str, Any]], *, mode: str
) -> Dict[str, Any]:
    """Validate framework-owned Spark writer controls without mutating input."""

    validated = dict(options or {})
    if mode not in {
        LoadType.APPEND.value,
        LoadType.OVERWRITE.value,
        LoadType.FULL_LOAD.value,
    }:
        raise EngineError(f"Unsupported Spark write mode: {mode!r}")
    for key in ("mergeSchema", "overwriteSchema"):
        if key not in validated:
            continue
        value = validated[key]
        if isinstance(value, bool):
            validated[key] = str(value).lower()
        elif isinstance(value, str) and value.lower() in {"true", "false"}:
            validated[key] = value.lower()
        else:
            raise EngineError(
                f"Spark write option {key!r} must be boolean, got {value!r}"
            )
    if "mergeSchema" in validated and "overwriteSchema" in validated:
        raise EngineError(
            "Spark write options 'mergeSchema' and 'overwriteSchema' "
            "cannot be supplied together"
        )
    if mode == LoadType.APPEND.value and "overwriteSchema" in validated:
        raise EngineError(
            "Spark write option 'overwriteSchema' is only valid for overwrite/full_load"
        )
    return validated


def _with_base_path(options: Optional[Dict[str, str]]) -> Dict[str, str]:
    merged = dict(options or {})
    base_path = merged.pop("use_hive_partitioning", None)
    if base_path:
        merged.setdefault("basePath", base_path)
    return merged


def read_parquet(
    spark: SparkSession, path: str | list[str], options: Optional[Dict[str, str]]
) -> DataFrame:
    merged = {"mergeSchema": "true", **_with_base_path(options)}
    return apply_options(spark.read.format("parquet"), merged).load(path)


def read_delta(
    spark: SparkSession, path: str, options: Optional[Dict[str, str]]
) -> DataFrame:
    return apply_options(spark.read.format("delta"), options).load(path)


def read_iceberg(
    spark: SparkSession, path: str, options: Optional[Dict[str, str]]
) -> DataFrame:
    return apply_options(spark.read.format("iceberg"), options).load(path)


def read_csv(
    spark: SparkSession, path: str | list[str], options: Optional[Dict[str, str]]
) -> DataFrame:
    merged = {
        "header": "true",
        "quote": '"',
        "escape": '"',
        "escapeQuotes": "true",
        "multiLine": "true",
        # CSV has no trustworthy type contract. Keep tokens intact so the
        # shared schema-hint resolver can perform the deliberate conversion
        # after read; callers may opt into Spark inference explicitly.
        "inferSchema": "false",
        **_with_base_path(options),
    }
    return apply_options(spark.read.format("csv"), merged).load(path)


def read_json(
    spark: SparkSession,
    path: str | list[str],
    options: Optional[Dict[str, str]],
    *,
    multiline: bool,
) -> DataFrame:
    merged = {"multiLine": str(multiline).lower(), **_with_base_path(options)}
    return apply_options(spark.read.format("json"), merged).load(path)


def read_avro(
    spark: SparkSession, path: str | list[str], options: Optional[Dict[str, str]]
) -> DataFrame:
    return apply_options(spark.read.format("avro"), _with_base_path(options)).load(path)


def read_excel(
    spark: SparkSession,
    resolved_paths: Sequence[str],
    requested_path: str | list[str],
    options: Optional[Dict[str, str]],
) -> DataFrame:
    import pandas as pd  # noqa: PLC0415

    merged: Dict[str, Any] = dict(options or {})
    merged.pop("use_hive_partitioning", None)
    pd_kwargs: Dict[str, Any] = {}
    if merged.get("header", "true").lower() == "false":
        pd_kwargs["header"] = None
    if "sheet_name" in merged:
        pd_kwargs["sheet_name"] = merged["sheet_name"]
    if not resolved_paths:
        raise FileNotFoundError(f"No Excel files found at: {requested_path}")

    frames = []
    for path in resolved_paths:
        frame = pd.read_excel(path, **pd_kwargs)
        frame[FileInfoColumn.FILE_PATH.value] = path
        frames.append(frame)
    return spark.createDataFrame(pd.concat(frames, ignore_index=True))


def read_path(
    spark: SparkSession,
    path: str | list[str],
    fmt: str,
    options: Optional[Dict[str, str]],
) -> DataFrame:
    return apply_options(spark.read.format(fmt), options).load(path)


def add_file_metadata_columns(df: DataFrame) -> DataFrame:
    return df.selectExpr(
        "*",
        f"_metadata.file_path as {FileInfoColumn.FILE_PATH.value}",
        f"_metadata.file_name as {FileInfoColumn.FILE_NAME.value}",
        f"_metadata.file_modification_time as {FileInfoColumn.FILE_MODIFICATION_TIME.value}",
    )


def add_file_info_columns(
    spark: SparkSession,
    df: DataFrame,
    file_infos: Optional[Sequence[Any]],
) -> DataFrame:
    path_col = FileInfoColumn.FILE_PATH.value
    name_col = FileInfoColumn.FILE_NAME.value
    mtime_col = FileInfoColumn.FILE_MODIFICATION_TIME.value
    if path_col in df.columns:
        if file_infos:
            from pyspark.sql import Row  # noqa: PLC0415

            rows = [
                Row(
                    **{
                        path_col: info.path,
                        name_col: info.name,
                        mtime_col: info.modification_time,
                    }
                )
                for info in file_infos
            ]
            return df.join(
                sf.broadcast(spark.createDataFrame(rows)), on=path_col, how="left"
            )
        return df.withColumn(
            name_col, sf.element_at(sf.split(sf.col(path_col), r"[/\\]"), -1)
        ).withColumn(mtime_col, sf.lit(None).cast("timestamp"))
    return add_file_metadata_columns(df)


def _contains_instant_timestamp(data_type: DataType) -> bool:
    """Return whether a Spark schema contains an instant timestamp."""

    if isinstance(data_type, TimestampType):
        return True
    if isinstance(data_type, ArrayType):
        return _contains_instant_timestamp(data_type.elementType)
    if isinstance(data_type, MapType):
        return _contains_instant_timestamp(
            data_type.keyType
        ) or _contains_instant_timestamp(data_type.valueType)
    if isinstance(data_type, StructType):
        return any(_contains_instant_timestamp(field.dataType) for field in data_type.fields)
    return False


def require_portable_timestamp_output(df: DataFrame, fmt: str) -> None:
    """Reject Spark's unannotated INT96 output for instant timestamps.

    A caller-owned Spark session must opt into ``TIMESTAMP_MICROS`` itself;
    changing its SQLConf from a framework writer would leak state to other
    workloads.  Without the check, Polars/Arrow readers lose the instant
    annotation and silently observe a different logical type.
    """

    if fmt.lower() not in {Format.PARQUET.value, Format.DELTA.value}:
        return
    if not _contains_instant_timestamp(df.schema):
        return
    try:
        output_type = df.sparkSession.conf.get(
            "spark.sql.parquet.outputTimestampType"
        )
    except Exception as exc:  # noqa: BLE001
        raise EngineError(
            "Cannot verify Spark timestamp output configuration; "
            "set spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS"
        ) from exc
    if str(output_type).upper() != "TIMESTAMP_MICROS":
        raise EngineError(
            "Portable Parquet/Delta output for instant timestamps requires "
            "spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS. "
            f"The active Spark session uses {output_type!r}; configure the "
            "caller-owned session explicitly before writing."
        )


def write_to_path(
    df: DataFrame,
    path: str,
    mode: str,
    fmt: str,
    partition_columns: Optional[List[str]],
    options: Optional[Dict[str, str]],
) -> None:
    fmt = Format.JSON.value if fmt.lower() == Format.JSONL.value else fmt
    options = validate_write_options(options, mode=mode)
    require_portable_timestamp_output(df, fmt)
    writer = df.write.format(fmt).mode(mode)
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    mode_options = {
        "overwriteSchema"
        if mode in (LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value)
        else "mergeSchema": "true"
    }
    if options:
        mode_options.update(options)
    apply_options(writer, mode_options).save(path)

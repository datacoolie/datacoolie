"""Stateless Spark file and path I/O helpers."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf

from datacoolie.core.constants import FileInfoColumn, Format, LoadType


def apply_options(target: Any, options: Optional[Dict[str, Any]]) -> Any:
    """Apply reader or writer options in insertion order."""
    if options:
        for key, value in options.items():
            target = target.option(key, value)
    return target


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


def write_to_path(
    df: DataFrame,
    path: str,
    mode: str,
    fmt: str,
    partition_columns: Optional[List[str]],
    options: Optional[Dict[str, str]],
) -> None:
    fmt = Format.JSON.value if fmt.lower() == Format.JSONL.value else fmt
    writer = df.write.format(fmt).mode(mode)
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    mode_options = {
        "overwriteSchema" if mode == LoadType.OVERWRITE.value else "mergeSchema": "true"
    }
    if options:
        mode_options.update(options)
    apply_options(writer, mode_options).save(path)

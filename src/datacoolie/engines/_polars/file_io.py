"""Flat-file read and write helpers for Polars."""

from __future__ import annotations

import inspect
import io
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import polars as pl

from datacoolie.core.constants import FileInfoColumn, Format, LoadType
from datacoolie.core.exceptions import EngineError
from datacoolie.engines.base import FileInfo
from datacoolie.platforms.base import BasePlatform
from datacoolie.utils.path_utils import normalize_path

_SCAN_CSV_PARAMS = frozenset(inspect.signature(pl.scan_csv).parameters)
_PARTITION_SEGMENT_PATTERN = re.compile(r"^\d+$|^[^=]+=.+$")
_DEFAULT_BYTES_PER_FILE = 256 * 1024 * 1024

try:
    import fastexcel as _fastexcel  # noqa: F401

    _FASTEXCEL_AVAILABLE = True
except ImportError:
    _FASTEXCEL_AVAILABLE = False


def scan_parquet(
    path: str | list[str],
    options: Optional[Dict[str, Any]],
    storage_options: Dict[str, Any],
) -> pl.LazyFrame:
    merged = dict(options or {})
    if merged.pop("use_hive_partitioning", None):
        merged["hive_partitioning"] = True
    merged.setdefault("missing_columns", "insert")
    merged.setdefault("extra_columns", "ignore")
    if storage_options:
        merged["storage_options"] = storage_options
    return pl.scan_parquet(
        path, include_file_paths=FileInfoColumn.FILE_PATH, **merged
    )


def scan_csv(
    path: str | list[str],
    options: Optional[Dict[str, Any]],
    storage_options: Dict[str, Any],
) -> pl.LazyFrame:
    merged = dict(options or {})
    merged.pop("use_hive_partitioning", None)
    if "sep" in merged:
        merged.setdefault("separator", merged.pop("sep"))
    else:
        merged.setdefault("separator", ",")
    if "header" in merged:
        merged.setdefault("has_header", str(merged.pop("header")).lower() == "true")
    else:
        merged.setdefault("has_header", True)
    if "quote" in merged:
        merged.setdefault("quote_char", merged.pop("quote"))
    else:
        merged.setdefault("quote_char", '"')
    if "inferSchema" in merged:
        merged.setdefault(
            "infer_schema", str(merged.pop("inferSchema")).lower() == "true"
        )
    else:
        merged.setdefault("infer_schema", True)
    if "missing_columns" in _SCAN_CSV_PARAMS:
        merged.setdefault("missing_columns", "insert")
    if storage_options:
        merged["storage_options"] = storage_options
    return pl.scan_csv(path, include_file_paths=FileInfoColumn.FILE_PATH, **merged)


def read_json_files(paths: Sequence[str], platform: BasePlatform) -> pl.LazyFrame:
    frames = [
        pl.read_json(io.BytesIO(platform.read_bytes(path))).with_columns(
            pl.lit(path).alias(FileInfoColumn.FILE_PATH)
        )
        for path in paths
    ]
    return pl.concat(frames).lazy()


def scan_jsonl(
    path: str | list[str],
    options: Optional[Dict[str, Any]],
    storage_options: Dict[str, Any],
) -> pl.LazyFrame:
    merged = dict(options or {})
    merged.pop("use_hive_partitioning", None)
    if storage_options:
        merged["storage_options"] = storage_options
    return pl.scan_ndjson(
        path, include_file_paths=FileInfoColumn.FILE_PATH, **merged
    )


def read_avro_files(
    paths: Sequence[str],
    platform: BasePlatform,
    options: Optional[Dict[str, Any]],
) -> pl.LazyFrame:
    merged = dict(options or {})
    merged.pop("use_hive_partitioning", None)
    frames = [
        pl.read_avro(io.BytesIO(platform.read_bytes(path)), **merged).with_columns(
            pl.lit(path).alias(FileInfoColumn.FILE_PATH)
        )
        for path in paths
    ]
    return pl.concat(frames).lazy()


def read_excel_files(
    paths: Sequence[str],
    platform: BasePlatform,
    options: Optional[Dict[str, Any]],
) -> pl.LazyFrame:
    merged = dict(options or {})
    merged.pop("use_hive_partitioning", None)
    merged.setdefault("engine", "calamine" if _FASTEXCEL_AVAILABLE else "openpyxl")
    frames: list[pl.DataFrame] = []
    for path in paths:
        result = pl.read_excel(io.BytesIO(platform.read_bytes(path)), **merged)
        frame = pl.concat(result.values()) if isinstance(result, dict) else result
        frames.append(frame.with_columns(pl.lit(path).alias(FileInfoColumn.FILE_PATH)))
    return pl.concat(frames).lazy()


def add_file_info_columns(
    df: pl.LazyFrame,
    file_infos: Optional[List[FileInfo]],
) -> pl.LazyFrame:
    path_column = FileInfoColumn.FILE_PATH
    name_column = FileInfoColumn.FILE_NAME
    modification_column = FileInfoColumn.FILE_MODIFICATION_TIME
    if file_infos:
        mapping = pl.LazyFrame(
            {
                path_column: [info.path.replace("\\", "/") for info in file_infos],
                name_column: [info.name for info in file_infos],
                modification_column: [info.modification_time for info in file_infos],
            }
        )
        return df.join(mapping, on=path_column, how="left")
    return df.with_columns(
        pl.col(path_column).str.split("/").list.last().alias(name_column),
        pl.lit(None).cast(pl.Datetime(time_zone="UTC")).alias(modification_column),
    )


def make_file_name(path: str, fmt: str, is_overwrite: bool) -> str:
    """Build a stable overwrite name or timestamped append name."""
    parts = [part for part in normalize_path(path).split("/") if part]
    while len(parts) > 1 and _PARTITION_SEGMENT_PATTERN.match(parts[-1]):
        parts.pop()
    folder_name = parts[-1] if parts else "data"
    if is_overwrite:
        return f"{folder_name}.{fmt}"
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{folder_name}_{timestamp}.{fmt}"


def write_flat_sink(
    df: pl.LazyFrame,
    path: str,
    mode: str,
    fmt: str,
    partition_columns: Optional[list[str]],
    options: Dict[str, Any],
    *,
    platform: Optional[BasePlatform],
    storage_options: Dict[str, Any],
) -> None:
    """Write Parquet, CSV, or JSONL through a streaming Polars sink."""
    is_overwrite = mode in (LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value)
    if is_overwrite and platform is not None:
        try:
            platform.delete_folder(path, recursive=True)
        except Exception:  # noqa: BLE001
            pass

    sink_options = dict(options)
    if storage_options:
        sink_options["storage_options"] = storage_options
    sink_options.setdefault("mkdir", True)
    target: str | pl.PartitionBy
    if partition_columns:
        target = pl.PartitionBy(
            path,
            key=partition_columns,
            approximate_bytes_per_file=_DEFAULT_BYTES_PER_FILE,
        )
    else:
        target = f"{normalize_path(path)}/{make_file_name(path, fmt, is_overwrite)}"

    if fmt == Format.PARQUET.value:
        df.sink_parquet(target, **sink_options)
    elif fmt == Format.CSV.value:
        df.sink_csv(target, **sink_options)
    elif fmt == Format.JSONL.value:
        df.sink_ndjson(target, **sink_options)
    else:
        raise EngineError(f"PolarsEngine._write_flat_sink: unsupported format {fmt!r}")


def write_flat_eager(
    df: pl.LazyFrame,
    path: str,
    mode: str,
    fmt: str,
    options: Dict[str, Any],
    *,
    platform: BasePlatform,
) -> None:
    """Write JSON or Avro through an explicit eager boundary."""
    is_overwrite = mode in (LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value)
    if is_overwrite:
        try:
            platform.delete_folder(path, recursive=True)
        except Exception:  # noqa: BLE001
            pass

    file_path = f"{normalize_path(path)}/{make_file_name(path, fmt, is_overwrite)}"
    platform.create_folder(path)
    write_options = dict(options)
    if fmt == Format.AVRO.value:
        datetime_casts = [
            pl.col(name).cast(pl.String)
            for name, dtype in df.collect_schema().items()
            if isinstance(dtype, pl.Datetime)
        ]
        if datetime_casts:
            df = df.with_columns(datetime_casts)
        write_options.setdefault("name", normalize_path(path).rsplit("/", 1)[-1])

    collected = df.collect()
    buffer = io.BytesIO()
    if fmt == Format.JSON.value:
        collected.write_json(buffer)
    elif fmt == Format.AVRO.value:
        collected.write_avro(buffer, **write_options)
    platform.write_bytes(file_path, buffer.getvalue(), overwrite=True)

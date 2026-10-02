"""Flat-file read and write helpers for Polars."""

from __future__ import annotations

import inspect
import io
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence
from uuid import uuid4

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


def _coerce_bool_option(value: Any, *, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
        return value.strip().lower() == "true"
    raise EngineError(f"Polars option {name!r} must be boolean, got {value!r}")


def _map_alias(
    options: Dict[str, Any], *, canonical: str, native: str
) -> None:
    if canonical in options and native in options:
        raise EngineError(
            f"Options {canonical!r} and {native!r} cannot be supplied together"
        )
    if canonical in options:
        options[native] = options.pop(canonical)


def _csv_read_options(options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged = dict(options or {})
    _map_alias(merged, canonical="header", native="has_header")
    _map_alias(merged, canonical="sep", native="separator")
    _map_alias(merged, canonical="quote", native="quote_char")
    _map_alias(merged, canonical="inferSchema", native="infer_schema")
    if "has_header" in merged:
        merged["has_header"] = _coerce_bool_option(
            merged["has_header"], name="header/has_header"
        )
    if "infer_schema" in merged:
        merged["infer_schema"] = _coerce_bool_option(
            merged["infer_schema"], name="inferSchema/infer_schema"
        )
    for name in ("separator", "quote_char"):
        if name not in merged or merged[name] is None:
            continue
        if not isinstance(merged[name], str) or (
            name == "separator" and len(merged[name]) != 1
        ) or (name == "quote_char" and len(merged[name]) > 1):
            raise EngineError(f"Polars CSV option {name!r} has an invalid value")
    return merged


def _csv_write_options(options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged = dict(options or {})
    _map_alias(merged, canonical="header", native="include_header")
    _map_alias(merged, canonical="sep", native="separator")
    _map_alias(merged, canonical="quote", native="quote_char")
    if "include_header" in merged:
        merged["include_header"] = _coerce_bool_option(
            merged["include_header"], name="header/include_header"
        )
    for name in ("separator", "quote_char"):
        if name not in merged or merged[name] is None:
            continue
        if not isinstance(merged[name], str) or (
            name == "separator" and len(merged[name]) != 1
        ) or (name == "quote_char" and len(merged[name]) > 1):
            raise EngineError(f"Polars CSV option {name!r} has an invalid value")
    return merged


def scan_parquet(
    path: str | list[str],
    options: Optional[Dict[str, Any]],
    storage_options: Dict[str, Any],
) -> pl.LazyFrame:
    # Spark writes Hadoop marker/checksum files (for example ``_SUCCESS`` and
    # ``part-....parquet.crc``) beside data files.  Passing a directory to
    # Polars makes it inspect those markers and can fail before a frame is
    # produced.  A format-qualified glob keeps the reader portable for local
    # and object-store roots while preserving explicit file/glob inputs.
    def parquet_glob(value: str) -> str:
        if value.lower().endswith(".parquet") or any(
            token in value for token in ("*", "?", "[")
        ):
            return value
        return value.rstrip("/\\") + "/**/*.parquet"

    resolved_path = (
        [parquet_glob(value) for value in path]
        if isinstance(path, list)
        else parquet_glob(path)
    )
    merged = dict(options or {})
    if merged.pop("use_hive_partitioning", None):
        merged["hive_partitioning"] = True
    merged.setdefault("missing_columns", "insert")
    merged.setdefault("extra_columns", "ignore")
    if storage_options:
        merged["storage_options"] = storage_options
    return pl.scan_parquet(
        resolved_path, include_file_paths=FileInfoColumn.FILE_PATH, **merged
    )


def scan_csv(
    path: str | list[str],
    options: Optional[Dict[str, Any]],
    storage_options: Dict[str, Any],
) -> pl.LazyFrame:
    merged = _csv_read_options(options)
    merged.pop("use_hive_partitioning", None)
    if "separator" not in merged:
        merged.setdefault("separator", ",")
    if "has_header" not in merged:
        merged.setdefault("has_header", True)
    if "quote_char" not in merged:
        merged.setdefault("quote_char", '"')
    if "infer_schema" not in merged:
        # Keep CSV tokens lossless by default. Schema hints are applied later
        # by the shared resolver; inference remains an explicit opt-in.
        merged.setdefault("infer_schema", False)
    if "missing_columns" in _SCAN_CSV_PARAMS:
        merged.setdefault("missing_columns", "insert")
    if storage_options:
        merged["storage_options"] = storage_options
    return pl.scan_csv(path, include_file_paths=FileInfoColumn.FILE_PATH, **merged)


def _json_read_options(options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Translate portable JSON read options to the Polars reader contract.

    ``multiLine`` is a Spark reader option.  Polars' JSON reader already
    accepts a complete JSON document, so there is no equivalent switch.  The
    remaining supported options are passed explicitly instead of silently
    dropping every option at the engine facade.
    """

    raw = dict(options or {})
    translated: Dict[str, Any] = {}
    for source_name, target_name in (
        ("schema", "schema"),
        ("schema_overrides", "schema_overrides"),
        ("schemaOverrides", "schema_overrides"),
        ("infer_schema_length", "infer_schema_length"),
        ("inferSchemaLength", "infer_schema_length"),
    ):
        if source_name not in raw:
            continue
        value = raw[source_name]
        if target_name == "infer_schema_length" and value is not None:
            value = int(value)
        translated[target_name] = value
    if "inferSchema" in raw and "infer_schema_length" not in translated:
        # Spark's boolean option has no exact Polars spelling.  Full-scan
        # inference is the closest equivalent for true; zero disables value
        # inference for weak JSON inputs and preserves the explicit policy.
        translated["infer_schema_length"] = (
            None if str(raw["inferSchema"]).lower() == "true" else 0
        )
    return translated


def read_json_files(
    paths: Sequence[str],
    platform: BasePlatform,
    options: Optional[Dict[str, Any]] = None,
) -> pl.LazyFrame:
    """Read JSON files while preserving the engine-specific read options."""

    read_options = _json_read_options(options)
    frames = [
        pl.read_json(
            io.BytesIO(platform.read_bytes(path)), **read_options
        ).with_columns(pl.lit(path).alias(FileInfoColumn.FILE_PATH))
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
    """Build a stable overwrite name or collision-safe append name.

    Append writes can happen several times within one second (for example,
    replay chunks or concurrent jobs). A timestamp by itself is therefore
    not a sufficient object name: the later write could replace the earlier
    one on a local filesystem or object-store upload. Keep the timestamp for
    operator readability and add a short UUID suffix for uniqueness.
    """
    parts = [part for part in normalize_path(path).split("/") if part]
    while len(parts) > 1 and _PARTITION_SEGMENT_PATTERN.match(parts[-1]):
        parts.pop()
    folder_name = parts[-1] if parts else "data"
    if is_overwrite:
        return f"{folder_name}.{fmt}"
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{folder_name}_{timestamp}_{uuid4().hex[:12]}.{fmt}"


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
    sink_options = (
        _csv_write_options(options)
        if fmt == Format.CSV.value
        else dict(options)
    )
    is_overwrite = mode in (LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value)
    if is_overwrite and platform is not None:
        # Platform adapters make deletion idempotent for an absent folder.
        # Any other delete failure must stop the overwrite before the sink
        # can publish a new file beside stale output.
        platform.delete_folder(path, recursive=True)

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
        # Adapters treat an absent folder as a successful no-op.  Preserve
        # real deletion errors so this overwrite cannot continue incorrectly.
        platform.delete_folder(path, recursive=True)

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

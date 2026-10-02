"""Spark WriterV2 and named-table mutation operations."""

from __future__ import annotations

from functools import reduce
from operator import and_
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as sf

from datacoolie.core.constants import Format, LoadType, SCD2Column, SystemColumn
from datacoolie.core.exceptions import EngineError
from datacoolie.engines._spark import file_io
from datacoolie.engines._spark import runtime
from datacoolie.engines._spark.iceberg import schema as iceberg_schema
from datacoolie.engines._spark.temporal import build_window_predicate
from datacoolie.engines.contracts.windows import WindowSpec


def build_table_writer(
    df: DataFrame,
    table_name: str,
    fmt: str,
    *,
    overwrite: bool,
    options: Optional[Dict[str, str]],
) -> Any:
    mode_options = {"overwriteSchema" if overwrite else "mergeSchema": "true"}
    if options:
        mode_options.update(options)
    writer = df.writeTo(table_name).using(fmt)
    for key, value in mode_options.items():
        writer = writer.option(key, value)
    return writer


def validate_write_options(
    options: Optional[Dict[str, str]], *, mode: str
) -> Dict[str, str]:
    """Validate canonical Spark table writer controls before table mutation."""
    return file_io.validate_write_options(options, mode=mode)


def write_to_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    mode: str,
    fmt: str,
    partition_columns: Optional[List[str]],
    options: Optional[Dict[str, str]],
    *,
    skip_iceberg_evolution: bool = False,
) -> None:
    fmt = Format.JSON.value if fmt.lower() == Format.JSONL.value else fmt
    overwrite = mode in (LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value)
    append = mode == LoadType.APPEND.value
    if not (overwrite or append):
        raise EngineError(f"Unsupported write_to_table mode: {mode!r}")
    options = validate_write_options(options, mode=mode)
    file_io.require_portable_timestamp_output(df, fmt)
    if overwrite or not spark.catalog.tableExists(table_name):
        writer = build_table_writer(
            df, table_name, fmt, overwrite=True, options=options
        )
        if partition_columns:
            writer = writer.partitionedBy(
                *[sf.col(column) for column in partition_columns]
            )
        writer.createOrReplace()
        return
    if fmt.lower() == Format.ICEBERG.value:
        if not skip_iceberg_evolution:
            iceberg_schema.prepare_table(spark, df, table_name, partition_columns)
        df = iceberg_schema.align_df_to_table_schema(spark, df, table_name)
    build_table_writer(df, table_name, fmt, overwrite=False, options=options).append()


def delete_by_window_table(
    spark: SparkSession,
    table_name: str,
    window: WindowSpec,
    fmt: str,
) -> None:
    predicate = build_window_predicate(window)
    if fmt.lower() == Format.DELTA.value:
        from delta import DeltaTable  # noqa: PLC0415

        DeltaTable.forName(spark, table_name).delete(predicate)
    else:
        spark.sql(f"DELETE FROM {table_name} WHERE {predicate}")


def maybe_evolve_iceberg(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    fmt: str,
    partition_columns: Optional[Sequence[str]],
) -> bool:
    if fmt.lower() != Format.ICEBERG.value:
        return False
    iceberg_schema.prepare_table(spark, df, table_name, partition_columns)
    return True


def _merge_parts(
    merge_keys: Sequence[str], options: Optional[Dict[str, str]] = None
) -> Tuple[str, str, str]:
    """Resolve aliases and an optional predicate for SQL MERGE operations."""

    merged = dict(options or {})
    source_alias = merged.get("source_alias", "source")
    target_alias = merged.get("target_alias", "target")
    predicate = merged.get("predicate")
    if not isinstance(source_alias, str) or not isinstance(target_alias, str):
        raise EngineError("Spark table merge aliases must be strings")
    if not source_alias.strip() or not target_alias.strip():
        raise EngineError("Spark table merge aliases must be non-empty strings")
    if predicate is None:
        predicate = " AND ".join(
            f"{target_alias}.`{column}` = {source_alias}.`{column}`"
            for column in merge_keys
        )
    elif not isinstance(predicate, str):
        raise EngineError("Spark table merge predicate must be a string")
    elif not predicate.strip():
        raise EngineError("Spark table merge predicate must be non-empty")
    return source_alias, target_alias, predicate


def _requires_sql_merge(options: Optional[Dict[str, str]]) -> bool:
    """Whether options require the SQL MERGE path instead of mergeInto."""

    return bool(
        options
        and any(key in options for key in ("source_alias", "target_alias", "predicate"))
    )


def build_merge_condition(
    merge_keys: Sequence[str], options: Optional[Dict[str, str]] = None
) -> str:
    return _merge_parts(merge_keys, options)[2]


def build_merge_condition_col(
    df: DataFrame,
    table_name: str,
    merge_keys: Sequence[str],
) -> Column:
    return reduce(
        and_,
        [df[key].eqNullSafe(sf.col(f"{table_name}.`{key}`")) for key in merge_keys],
    )


def update_columns(df_columns: Sequence[str], merge_keys: Sequence[str]) -> List[str]:
    excluded = {column.lower() for column in merge_keys}
    excluded.add(SystemColumn.CREATED_AT.lower())
    return [column for column in df_columns if column.lower() not in excluded]


def scd2_merge_parts(
    merge_keys: Sequence[str], options: Optional[Dict[str, str]] = None
) -> Tuple[str, Dict[str, str], str]:
    source_alias, target_alias, predicate = _merge_parts(merge_keys, options)
    merge_condition = (
        f"({predicate}) "
        f"AND {target_alias}.`{SCD2Column.IS_CURRENT.value}` = true"
    )
    updates = {
        f"`{SCD2Column.VALID_TO.value}`": (
            f"{source_alias}.`{SCD2Column.VALID_FROM.value}`"
        ),
        f"`{SCD2Column.IS_CURRENT.value}`": "false",
    }
    late_guard = (
        f"{source_alias}.`{SCD2Column.VALID_FROM.value}` > "
        f"{target_alias}.`{SCD2Column.VALID_FROM.value}`"
    )
    return merge_condition, updates, late_guard


def scd2_merge_parts_col(
    df: DataFrame,
    table_name: str,
    merge_keys: Sequence[str],
) -> Tuple[Column, Dict[str, Column], Column]:
    key_condition = build_merge_condition_col(df, table_name, merge_keys)
    merge_condition = key_condition & (
        sf.col(f"{table_name}.`{SCD2Column.IS_CURRENT.value}`") == sf.lit(True)
    )
    updates = {
        f"`{SCD2Column.VALID_TO.value}`": df[SCD2Column.VALID_FROM.value],
        f"`{SCD2Column.IS_CURRENT.value}`": sf.lit(False),
    }
    late_guard = df[SCD2Column.VALID_FROM.value] > sf.col(
        f"{table_name}.`{SCD2Column.VALID_FROM.value}`"
    )
    return merge_condition, updates, late_guard


def merge_sql_upsert(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    merge_keys: Sequence[str],
    options: Optional[Dict[str, str]] = None,
) -> None:
    source_alias, target_alias, condition = _merge_parts(merge_keys, options)
    updates = ", ".join(
        f"{target_alias}.`{column}` = {source_alias}.`{column}`"
        for column in update_columns(df.columns, merge_keys)
    )
    insert_columns = ", ".join(f"`{column}`" for column in df.columns)
    insert_values = ", ".join(
        f"{source_alias}.`{column}`" for column in df.columns
    )
    spark.sql(
        f"MERGE INTO {table_name} AS {target_alias} "
        f"USING {{source_df}} AS {source_alias} "
        f"ON {condition} WHEN MATCHED THEN UPDATE SET {updates} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})",
        source_df=df,
    )


def merge_into_upsert(
    df: DataFrame, table_name: str, merge_keys: Sequence[str]
) -> None:
    condition = build_merge_condition_col(df, table_name, merge_keys)
    updates = {column: df[column] for column in update_columns(df.columns, merge_keys)}
    (
        df.mergeInto(table_name, condition)
        .whenMatched()
        .update(updates)
        .whenNotMatched()
        .insertAll()
        .merge()
    )


def merge_to_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    merge_keys: List[str],
    fmt: str,
    partition_columns: Optional[List[str]],
    options: Optional[Dict[str, str]] = None,
) -> None:
    file_io.require_portable_timestamp_output(df, fmt)
    _merge_parts(merge_keys, options)
    maybe_evolve_iceberg(spark, df, table_name, fmt, partition_columns)
    if _requires_sql_merge(options) or not runtime.supports_merge_into():
        merge_sql_upsert(spark, df, table_name, merge_keys, options)
    else:
        merge_into_upsert(df, table_name, merge_keys)


def merge_sql_overwrite(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    merge_keys: List[str],
    fmt: str,
    partition_columns: Optional[List[str]],
    options: Optional[Dict[str, str]],
    *,
    skip_iceberg_evolution: bool,
    write_options: Optional[Dict[str, str]] = None,
) -> None:
    keys = df.select(merge_keys).dropDuplicates()
    source_alias, target_alias, predicate = _merge_parts(merge_keys, options)
    spark.sql(
        f"MERGE INTO {table_name} AS {target_alias} "
        f"USING {{keys_df}} AS {source_alias} "
        f"ON {predicate} WHEN MATCHED THEN DELETE",
        keys_df=keys,
    )
    write_to_table(
        spark,
        df,
        table_name,
        LoadType.APPEND.value,
        fmt,
        partition_columns,
        write_options,
        skip_iceberg_evolution=skip_iceberg_evolution,
    )


def merge_into_overwrite(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    merge_keys: List[str],
    fmt: str,
    partition_columns: Optional[List[str]],
    options: Optional[Dict[str, str]],
    *,
    skip_iceberg_evolution: bool,
    write_options: Optional[Dict[str, str]] = None,
) -> None:
    if _requires_sql_merge(options):
        raise EngineError(
            "Spark merge aliases/predicate require the SQL MERGE path"
        )
    keys = df.select(merge_keys).dropDuplicates()
    condition = build_merge_condition_col(keys, table_name, merge_keys)
    keys.mergeInto(table_name, condition).whenMatched().delete().merge()
    write_to_table(
        spark,
        df,
        table_name,
        LoadType.APPEND.value,
        fmt,
        partition_columns,
        write_options,
        skip_iceberg_evolution=skip_iceberg_evolution,
    )


def merge_overwrite_to_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    merge_keys: List[str],
    fmt: str,
    partition_columns: Optional[List[str]],
    options: Optional[Dict[str, str]],
    *,
    write_options: Optional[Dict[str, str]] = None,
) -> None:
    _merge_parts(merge_keys, options)
    write_options = validate_write_options(
        write_options, mode=LoadType.APPEND.value
    )
    evolved = maybe_evolve_iceberg(spark, df, table_name, fmt, partition_columns)
    df = runtime.safe_cache(df)
    try:
        operation = (
            merge_into_overwrite
            if runtime.supports_merge_into() and not _requires_sql_merge(options)
            else merge_sql_overwrite
        )
        operation(
            spark,
            df,
            table_name,
            merge_keys,
            fmt,
            partition_columns,
            options,
            skip_iceberg_evolution=evolved,
            write_options=write_options,
        )
    finally:
        runtime.safe_unpersist(df)


def scd2_to_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    merge_keys: List[str],
    fmt: str,
    partition_columns: Optional[List[str]],
    options: Optional[Dict[str, str]],
    *,
    write_options: Optional[Dict[str, str]] = None,
) -> None:
    _merge_parts(merge_keys, options)
    write_options = validate_write_options(
        write_options, mode=LoadType.APPEND.value
    )
    evolved = maybe_evolve_iceberg(spark, df, table_name, fmt, partition_columns)
    df = runtime.safe_cache(df)
    try:
        if runtime.supports_merge_into() and not _requires_sql_merge(options):
            condition, updates, late_guard = scd2_merge_parts_col(
                df, table_name, merge_keys
            )
            (
                df.mergeInto(table_name, condition)
                .whenMatched(condition=late_guard)
                .update(updates)
                .merge()
            )
        else:
            source_alias, target_alias, _ = _merge_parts(merge_keys, options)
            condition, updates, late_guard = scd2_merge_parts(merge_keys, options)
            update_clause = ", ".join(
                f"{target_alias}.{key} = {value}" for key, value in updates.items()
            )
            spark.sql(
                f"MERGE INTO {table_name} AS {target_alias} "
                f"USING {{source_df}} AS {source_alias} "
                f"ON {condition} WHEN MATCHED AND {late_guard} THEN UPDATE SET {update_clause}",
                source_df=df,
            )
        write_to_table(
            spark,
            df,
            table_name,
            LoadType.APPEND.value,
            fmt,
            partition_columns,
            write_options,
            skip_iceberg_evolution=evolved,
        )
    finally:
        runtime.safe_unpersist(df)

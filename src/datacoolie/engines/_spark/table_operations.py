"""Spark WriterV2 and named-table mutation operations."""

from __future__ import annotations

from functools import reduce
from operator import and_
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as sf

from datacoolie.core.constants import Format, LoadType, SCD2Column, SystemColumn
from datacoolie.core.exceptions import EngineError
from datacoolie.engines._spark import runtime
from datacoolie.engines._spark.iceberg import schema as iceberg_schema
from datacoolie.engines._spark.temporal import build_window_predicate


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
    window: Dict[str, tuple],
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


def build_merge_condition(merge_keys: Sequence[str]) -> str:
    return " AND ".join(
        f"target.`{column}` = source.`{column}`" for column in merge_keys
    )


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


def scd2_merge_parts(merge_keys: Sequence[str]) -> Tuple[str, Dict[str, str], str]:
    merge_condition = (
        f"{build_merge_condition(merge_keys)} "
        f"AND target.`{SCD2Column.IS_CURRENT.value}` = true"
    )
    updates = {
        f"`{SCD2Column.VALID_TO.value}`": f"source.`{SCD2Column.VALID_FROM.value}`",
        f"`{SCD2Column.IS_CURRENT.value}`": "false",
    }
    late_guard = (
        f"source.`{SCD2Column.VALID_FROM.value}` > "
        f"target.`{SCD2Column.VALID_FROM.value}`"
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
) -> None:
    condition = build_merge_condition(merge_keys)
    updates = ", ".join(
        f"target.`{column}` = source.`{column}`"
        for column in update_columns(df.columns, merge_keys)
    )
    insert_columns = ", ".join(f"`{column}`" for column in df.columns)
    insert_values = ", ".join(f"source.`{column}`" for column in df.columns)
    spark.sql(
        f"MERGE INTO {table_name} AS target USING {{source_df}} AS source "
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
) -> None:
    maybe_evolve_iceberg(spark, df, table_name, fmt, partition_columns)
    if runtime.supports_merge_into():
        merge_into_upsert(df, table_name, merge_keys)
    else:
        merge_sql_upsert(spark, df, table_name, merge_keys)


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
) -> None:
    keys = df.select(merge_keys).dropDuplicates()
    spark.sql(
        f"MERGE INTO {table_name} AS target USING {{keys_df}} AS source "
        f"ON {build_merge_condition(merge_keys)} WHEN MATCHED THEN DELETE",
        keys_df=keys,
    )
    write_to_table(
        spark,
        df,
        table_name,
        LoadType.APPEND.value,
        fmt,
        partition_columns,
        options,
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
) -> None:
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
        options,
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
) -> None:
    evolved = maybe_evolve_iceberg(spark, df, table_name, fmt, partition_columns)
    df = runtime.safe_cache(df)
    try:
        operation = (
            merge_into_overwrite
            if runtime.supports_merge_into()
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
) -> None:
    evolved = maybe_evolve_iceberg(spark, df, table_name, fmt, partition_columns)
    df = runtime.safe_cache(df)
    try:
        if runtime.supports_merge_into():
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
            condition, updates, late_guard = scd2_merge_parts(merge_keys)
            update_clause = ", ".join(
                f"target.{key} = {value}" for key, value in updates.items()
            )
            spark.sql(
                f"MERGE INTO {table_name} AS target USING {{source_df}} AS source "
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
            options,
            skip_iceberg_evolution=evolved,
        )
    finally:
        runtime.safe_unpersist(df)

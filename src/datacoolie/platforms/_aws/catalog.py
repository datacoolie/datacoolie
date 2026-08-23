"""Glue and Athena helpers used by AWSPlatform."""

from __future__ import annotations

import time
from typing import Any, Callable

from datacoolie.core.exceptions import PlatformError
from datacoolie.logging.base import get_logger
from datacoolie.platforms._aws.errors import error_code, failure_detail

logger = get_logger(__name__)


def _is_entity_not_found(exc: BaseException) -> bool:
    code = error_code(exc)
    if code == "EntityNotFoundException":
        return True
    if exc.__class__.__name__ == "EntityNotFoundException":
        return True
    return str(exc).strip() in {"EntityNotFound", "EntityNotFoundException"}


def _sql_string_literal(value: str) -> str:
    """Quote a generated SQL string literal without touching caller DDL."""
    return "'" + value.replace("'", "''") + "'"


class CatalogBackend:
    """Implement AWS Glue Catalog and Athena table helpers."""

    def __init__(
        self,
        client_factory: Callable[..., Any],
        *,
        delete_table: Callable[..., None] | None = None,
        execute_ddl: Callable[..., str] | None = None,
        repair_partitions: Callable[..., None] | None = None,
    ) -> None:
        self._client_factory = client_factory
        self._delete_table_callback = delete_table
        self._execute_ddl_callback = execute_ddl
        self._repair_partitions_callback = repair_partitions

    def delete_glue_table(self, database: str, table_name: str) -> None:
        try:
            glue = self._client_factory("glue")
            glue.delete_table(DatabaseName=database, Name=table_name)
            logger.debug(
                "Removed stale Glue catalog entry: %s.%s", database, table_name
            )
        except Exception as exc:  # noqa: BLE001
            if _is_entity_not_found(exc):
                return
            raise PlatformError(
                f"Failed to delete Glue table {database}.{table_name}: "
                f"{failure_detail(exc)}"
            ) from exc

    def glue_table_exists(self, database: str, table_name: str) -> bool:
        try:
            glue = self._client_factory("glue")
            glue.get_table(DatabaseName=database, Name=table_name)
            return True
        except Exception as exc:  # noqa: BLE001
            if _is_entity_not_found(exc):
                return False
            raise PlatformError(
                f"Failed to check Glue table {database}.{table_name}: "
                f"{failure_detail(exc)}"
            ) from exc

    def execute_athena_ddl(
        self,
        sql: str,
        *,
        database: str | None = None,
        output_location: str = "",
    ) -> str:
        if not output_location:
            raise PlatformError(
                "athena_output_location is required for execute_athena_ddl. "
                "Set athena_output_location in configure or pass output_location=."
            )
        athena = self._client_factory("athena")
        start_kwargs: dict[str, Any] = {
            "QueryString": sql,
            "ResultConfiguration": {"OutputLocation": output_location},
        }
        if database:
            start_kwargs["QueryExecutionContext"] = {"Database": database}

        try:
            response = athena.start_query_execution(**start_kwargs)
            query_id: str = response["QueryExecutionId"]
        except Exception as exc:
            raise PlatformError(f"Failed to start Athena query: {exc}") from exc

        deadline = time.monotonic() + 60.0
        while True:
            try:
                status_resp = athena.get_query_execution(QueryExecutionId=query_id)
                state = status_resp["QueryExecution"]["Status"]["State"]
            except Exception as exc:
                raise PlatformError(
                    f"Failed to poll Athena query {query_id}: {exc}"
                ) from exc

            if state == "SUCCEEDED":
                logger.debug("Athena query %s succeeded", query_id)
                return query_id
            if state in ("FAILED", "CANCELLED"):
                reason = status_resp["QueryExecution"]["Status"].get(
                    "StateChangeReason", "unknown"
                )
                raise PlatformError(f"Athena query {query_id} {state}: {reason}")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PlatformError(
                    f"Athena query {query_id} timed out after 60s; "
                    "the remote query was not cancelled"
                )
            time.sleep(min(1.0, remaining))

    def register_delta_table(
        self,
        table_name: str,
        path: str,
        *,
        database: str,
        output_location: str = "",
        recreate: bool = False,
    ) -> None:
        if recreate:
            self._delete_table(database, table_name)

        create_sql = (
            f"CREATE EXTERNAL TABLE IF NOT EXISTS `{database}`.`{table_name}` "
            f"LOCATION {_sql_string_literal(path)} "
            "TBLPROPERTIES ('table_type'='DELTA')"
        )
        logger.info(
            "%s native Delta table: %s.%s",
            "Recreating" if recreate else "Registering",
            database,
            table_name,
        )
        self._execute_ddl(
            create_sql, database=database, output_location=output_location
        )

    def register_symlink_table(
        self,
        table_name: str,
        path: str,
        *,
        database: str,
        output_location: str = "",
        schema_ddl: str = "",
        partition_ddl: str = "",
        recreate: bool = False,
        run_msck: bool = True,
    ) -> None:
        manifest_location = path.rstrip("/") + "/_symlink_format_manifest/"

        if recreate:
            self._delete_table(database, table_name)

        partitioned_by = f"\n{partition_ddl}" if partition_ddl else ""
        create_sql = (
            f"CREATE EXTERNAL TABLE IF NOT EXISTS `{database}`.`{table_name}` (\n"
            f"  {schema_ddl}\n"
            f")\n"
            f"{partitioned_by}"
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\n"
            "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'\n"
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n"
            f"LOCATION {_sql_string_literal(manifest_location)}"
        )
        logger.info(
            "%s symlink table: %s.%s",
            "Recreating" if recreate else "Registering",
            database,
            table_name,
        )
        self._execute_ddl(
            create_sql, database=database, output_location=output_location
        )

        if partition_ddl and run_msck:
            self._repair_partitions(
                table_name,
                database=database,
                output_location=output_location,
            )

    def repair_table_partitions(
        self,
        table_name: str,
        *,
        database: str,
        output_location: str = "",
    ) -> None:
        repair_sql = f"MSCK REPAIR TABLE `{database}`.`{table_name}`"
        logger.info("Repairing partitions: %s.%s", database, table_name)
        self._execute_ddl(
            repair_sql, database=database, output_location=output_location
        )

    def _delete_table(self, database: str, table_name: str) -> None:
        callback = self._delete_table_callback
        if callback is None:
            self.delete_glue_table(database, table_name)
        else:
            callback(database, table_name)

    def _execute_ddl(self, sql: str, **kwargs: Any) -> str:
        callback = self._execute_ddl_callback
        if callback is None:
            return self.execute_athena_ddl(sql, **kwargs)
        return callback(sql, **kwargs)

    def _repair_partitions(self, table_name: str, **kwargs: Any) -> None:
        callback = self._repair_partitions_callback
        if callback is None:
            self.repair_table_partitions(table_name, **kwargs)
        else:
            callback(table_name, **kwargs)

"""AWS-specific catalog coordination for Delta destinations.

The Delta writer owns format/load-strategy orchestration.  This collaborator
owns the optional AWS Glue/Athena side effects that happen after a successful
Delta operation.  Keeping the integration here makes the non-AWS Delta path
independent from AWS catalog capabilities while preserving the existing
post-commit warning policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional

from datacoolie.core.constants import Format
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.destinations.resolution.target import resolve_destination_target
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.platforms.aws_platform import AWSPlatform

logger = get_logger(__name__)


def _struct_field_names(hive_type: str) -> frozenset[str]:
    """Return top-level field names from a Hive ``STRUCT<...>`` type."""
    trimmed = hive_type.strip()
    if not trimmed.upper().startswith("STRUCT<") or not trimmed.endswith(">"):
        return frozenset()
    inner = trimmed[7:-1]
    fields: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(inner):
        if char in "<(":
            depth += 1
        elif char in ">)":
            depth -= 1
        elif char == "," and depth == 0:
            fields.append(inner[start:index].strip())
            start = index + 1
    fields.append(inner[start:].strip())
    result: set[str] = set()
    for field in fields:
        colon = field.find(":")
        if colon > 0:
            result.add(field[:colon].lower())
    return frozenset(result)


def _has_new_columns(
    pre: Mapping[str, str],
    post: Mapping[str, str],
    *,
    strict: bool = False,
) -> bool:
    """Return whether the post-write schema requires catalog recreation."""
    pre_lower = {key.lower(): value for key, value in pre.items()}
    post_lower = {key.lower(): value for key, value in post.items()}

    if set(post_lower) - set(pre_lower):
        return True

    if strict:
        for column, pre_type in pre_lower.items():
            if post_lower.get(column, pre_type) != pre_type:
                return True

    for column in pre_lower:
        before = _struct_field_names(pre_lower[column])
        after = _struct_field_names(post_lower.get(column, ""))
        if after - before:
            return True
    return False


@dataclass(frozen=True, slots=True)
class AwsDeltaState:
    """Typed pre-operation state captured for one AWS catalog lifecycle."""

    platform: AWSPlatform
    native_exists: bool
    symlink_exists: bool
    pre_schema: Mapping[str, str]

    def __post_init__(self) -> None:
        # Capture a stable snapshot rather than retaining a mutable schema
        # mapping owned by a platform/engine adapter.
        object.__setattr__(self, "pre_schema", MappingProxyType(dict(self.pre_schema)))


_UNSET = object()


class AwsDeltaCatalogCoordinator:
    """Coordinate optional Glue/Athena actions for a Delta destination.

    The engine is deliberately the only dependency used for Delta reads and
    manifest generation.  AWS operations are delegated to ``AWSPlatform``;
    no driver-level capability probing or generic catalog bus is introduced.
    """

    def __init__(self, engine: Any) -> None:
        self._engine = engine

    def _resolve_path(self, dataflow: DataFlow) -> Optional[str]:
        return resolve_destination_target(dataflow.destination).path

    def read_delta_hive_schema(
        self,
        path: Optional[str],
        *,
        log_context: str,
    ) -> Dict[str, str]:
        if not path or not self._engine.exists(path=path, fmt=Format.DELTA.value):
            return {}
        try:
            frame = self._engine.read(fmt=Format.DELTA.value, path=path)
            return self._engine.get_hive_schema(frame)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not read %s: %s", log_context, exc)
            return {}

    def capture_state(self, dataflow: DataFlow) -> Optional[AwsDeltaState]:
        """Capture pre-write Glue state, or ``None`` when AWS is not active."""
        platform = getattr(self._engine, "platform", None)
        if not isinstance(platform, AWSPlatform):
            return None

        dest = dataflow.destination
        if not dest.connection.athena_output_location:
            if dest.connection.generate_manifest or dest.connection.register_symlink_table:
                logger.warning(
                    "generate_manifest/register_symlink_table is set for table %r "
                    "but athena_output_location is missing — skipping catalog registration",
                    dest.table,
                )
            return None

        path = self._resolve_path(dataflow)
        if not path or not dest.connection.database:
            return None

        database = dest.connection.database
        table = dest.table
        prefix = dest.connection.symlink_database_prefix
        symlink_database = f"{prefix}{database}" if prefix and database else None
        native_exists = platform.glue_table_exists(database, table)
        symlink_exists = (
            platform.glue_table_exists(symlink_database, table)
            if dest.connection.register_symlink_table and symlink_database
            else False
        )
        pre_schema = (
            self.read_delta_hive_schema(path, log_context="pre-write Delta schema")
            if native_exists or symlink_exists
            else {}
        )
        return AwsDeltaState(
            platform=platform,
            native_exists=native_exists,
            symlink_exists=symlink_exists,
            pre_schema=pre_schema,
        )

    def post_write(
        self,
        dataflow: DataFlow,
        *,
        aws_state: AwsDeltaState | None | object = _UNSET,
    ) -> None:
        """Apply the minimum Glue/manifest actions after a Delta write."""
        state = self.capture_state(dataflow) if aws_state is _UNSET else aws_state
        if not state:
            return

        assert isinstance(state, AwsDeltaState)
        platform = state.platform
        native_exists = state.native_exists
        symlink_exists = state.symlink_exists
        pre_schema = dict(state.pre_schema)

        dest = dataflow.destination
        path = self._resolve_path(dataflow)
        database = dest.connection.database
        table = dest.table
        output_location = dest.connection.athena_output_location
        prefix = dest.connection.symlink_database_prefix
        symlink_database = f"{prefix}{database}" if prefix and database else None

        needs_post_schema = native_exists or dest.connection.register_symlink_table
        post_schema = (
            self.read_delta_hive_schema(path, log_context="post-write Delta schema")
            if needs_post_schema
            else {}
        )

        if native_exists:
            native_recreate = _has_new_columns(pre_schema, post_schema, strict=True)
            if not native_recreate:
                logger.debug(
                    "Native Glue entry unchanged for %s.%s — skipping", database, table
                )
        else:
            native_recreate = False

        if not native_exists or native_recreate:
            try:
                platform.register_delta_table(
                    table,
                    path,
                    database=database,
                    output_location=output_location,
                    recreate=native_recreate,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to register native Delta table: %s", exc)

        if not (dest.connection.generate_manifest or dest.connection.register_symlink_table):
            return

        try:
            self._engine.generate_symlink_manifest(path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to generate symlink manifest: %s", exc)
            return

        if not dest.connection.register_symlink_table:
            return

        is_partitioned = bool(dest.partition_columns)
        try:
            symlink_recreate = symlink_exists and _has_new_columns(
                pre_schema, post_schema, strict=True
            )
            if not symlink_exists or symlink_recreate:
                schema_ddl = self.build_schema_ddl(dataflow, post_schema)
                partition_ddl = self.build_partition_ddl(dataflow, post_schema)
                platform.register_symlink_table(
                    table,
                    path,
                    database=symlink_database,
                    output_location=output_location,
                    schema_ddl=schema_ddl,
                    partition_ddl=partition_ddl,
                    recreate=symlink_recreate,
                    run_msck=is_partitioned,
                )
            elif is_partitioned:
                platform.repair_table_partitions(
                    table,
                    database=symlink_database,
                    output_location=output_location,
                )
            else:
                logger.debug(
                    "Symlink Glue entry unchanged for %s.%s — skipping",
                    symlink_database,
                    table,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to register/update symlink table: %s", exc)

    def post_maintenance(
        self,
        dataflow: DataFlow,
        *,
        aws_state: AwsDeltaState | None | object = _UNSET,
    ) -> None:
        """Repair catalog entries and regenerate symlinks after maintenance."""
        state = self.capture_state(dataflow) if aws_state is _UNSET else aws_state
        if not state:
            return

        assert isinstance(state, AwsDeltaState)
        platform = state.platform
        native_exists = state.native_exists
        symlink_exists = state.symlink_exists

        dest = dataflow.destination
        path = self._resolve_path(dataflow)
        database = dest.connection.database
        table = dest.table
        output_location = dest.connection.athena_output_location
        prefix = dest.connection.symlink_database_prefix
        symlink_database = f"{prefix}{database}" if prefix and database else None
        is_partitioned = bool(dest.partition_columns)

        if not native_exists:
            try:
                platform.register_delta_table(
                    table,
                    path,
                    database=database,
                    output_location=output_location,
                    recreate=False,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to register native Delta table after maintenance: %s", exc
                )

        if not (dest.connection.generate_manifest or dest.connection.register_symlink_table):
            return

        try:
            self._engine.generate_symlink_manifest(path)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to generate symlink manifest after maintenance: %s", exc
            )
            return

        if not dest.connection.register_symlink_table:
            return

        try:
            if not symlink_exists:
                post_schema = self.read_delta_hive_schema(
                    path,
                    log_context="schema for maintenance symlink DDL",
                )
                platform.register_symlink_table(
                    table,
                    path,
                    database=symlink_database,
                    output_location=output_location,
                    schema_ddl=self.build_schema_ddl(dataflow, post_schema),
                    partition_ddl=self.build_partition_ddl(dataflow, post_schema),
                    recreate=False,
                    run_msck=is_partitioned,
                )
            elif is_partitioned:
                platform.repair_table_partitions(
                    table,
                    database=symlink_database,
                    output_location=output_location,
                )
            else:
                logger.debug(
                    "Symlink Glue entry unchanged after maintenance for %s.%s — skipping",
                    symlink_database,
                    table,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to update symlink table after maintenance: %s", exc)

    @staticmethod
    def build_schema_ddl(dataflow: DataFlow, hive_schema: Mapping[str, str]) -> str:
        """Build non-partition column DDL from a Hive schema."""
        partition_cols = {
            partition.column.lower() for partition in dataflow.destination.partition_columns
        }
        columns = [
            f"`{column}` {hive_type}"
            for column, hive_type in hive_schema.items()
            if column.lower() not in partition_cols
        ]
        return ",\n  ".join(columns)

    @staticmethod
    def build_partition_ddl(dataflow: DataFlow, hive_schema: Mapping[str, str]) -> str:
        """Build the optional ``PARTITIONED BY`` clause."""
        partitions = dataflow.destination.partition_columns
        if not partitions:
            return ""
        values: List[str] = []
        for partition in partitions:
            hive_type = (
                hive_schema.get(partition.column)
                or hive_schema.get(partition.column.lower())
                or "STRING"
            )
            values.append(f"`{partition.column}` {hive_type}")
        return f"PARTITIONED BY ({', '.join(values)})\n"


__all__ = [
    "AwsDeltaState",
    "AwsDeltaCatalogCoordinator",
    "_has_new_columns",
    "_struct_field_names",
]

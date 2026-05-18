"""Main ETL driver coordinating all framework components.

``DataCoolieDriver`` ties together metadata, watermark, engine, platform, loggers,
job distribution, parallel execution, and retry handling through constructor
injection.

Typical usage::

    driver = create_driver(
        engine=spark_engine,
        platform=local_platform,
        metadata_provider=file_provider,
        job_num=4, job_index=0, max_workers=4,
    )
    result = driver.run(stage="bronze2silver")
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    from datacoolie.core.secret_resolver import BaseSecretResolver

from datacoolie.core.constants import (
    DEFAULT_MAX_WORKERS,
    ColumnCaseMode,
    DataFlowStatus,
    ExecutionType,
    Format,
)
from datacoolie.core.exceptions import DataCoolieError, PipelineError
from datacoolie.core.models import (
    DataCoolieRunConfig,
    DataFlow,
    DataFlowRuntimeInfo,
    DestinationRuntimeInfo,
    ReplayConfig,
    SourceRuntimeInfo,
    TransformRuntimeInfo,
)
from datacoolie.engines.base import BaseEngine
from datacoolie.metadata.base import BaseMetadataProvider
from datacoolie.platforms.base import BasePlatform
from datacoolie.watermark.base import BaseWatermarkManager

# Source readers
from datacoolie.sources import BaseSourceReader

# Transformers
from datacoolie.transformers import TransformerPipeline

# Destination writers
from datacoolie.destinations import BaseDestinationWriter

from datacoolie.orchestration.job_distributor import JobDistributor
from datacoolie.orchestration.utils import dedupe_by_destination
from datacoolie.orchestration.parallel_executor import ExecutionResult, ParallelExecutor
from datacoolie.orchestration.retry_handler import RetryHandler
from dataclasses import replace as _dc_replace

from datacoolie.core.secret_provider import BaseSecretProvider, resolve_secrets
from datacoolie.logging import ETLLogger, LogConfig, SystemLogger, create_etl_logger, create_system_logger
from datacoolie.logging.base import get_logger
from datacoolie.logging.context import clear_dataflow_id, set_dataflow_id
from datacoolie.utils.helpers import generate_unique_id, utc_now
from datacoolie.utils.datetime_utils import generate_chunk_boundaries

logger = get_logger(__name__)

# Keys in source.configure / connection.configure that trigger date_backward logic.
# Cleared on the deep-copied dataflow during replay so chunk boundaries are exact.
_BACKWARD_KEYS: tuple[str, ...] = (
    "backward_days", "backward_months", "backward_hours",
    "backward_years", "backward_closing_day", "backward",
)

# Default transformer pipeline — names correspond to transformer_registry keys.
# Order matters: each transformer sees the output of all preceding ones.
# Override by subclassing DataCoolieDriver and replacing _create_transformer_pipeline.
DEFAULT_TRANSFORMERS: list[str] = [
    "schema_converter",       # 10. Cast to target schema types first
    "deduplicator",           # 20. Remove duplicate source rows early
    "column_adder",           # 30. User-configured calculated columns
    "row_filter",             # 35. Discard unwanted rows (post-column_adder, pre-scd2)
    "scd2_column_adder",      # 60. SCD2 validity columns from source effective-date
    "system_column_adder",    # 70. Framework audit columns (__created_at, etc.)
    "partition_handler",      # 80. Derive partition values from final columns
    "column_name_sanitizer",  # 90. Normalize column names last
]


class DataCoolieDriver:
    """Main orchestration class for DataCoolie ETL pipelines.

    Uses constructor injection for all dependencies.  Supports:

    * **ETL mode** — read → transform → write with `run` or `run_dataflow` function.
    * **Maintenance mode** — optimize + vacuum on destinations with `run_maintenance` function.
    * **Dry-run** — logs planned work without side effects.
    * Context manager (``with``) for resource cleanup.

    Args:
        engine: Data operation engine (e.g. PySpark).
        platform: Platform abstraction for file I/O.
        metadata_provider: Provides dataflow / connection metadata.
        watermark_manager: Reads and writes watermarks.  When ``None`` and
            *metadata_provider* is supplied, a :class:`~datacoolie.watermark.
            watermark_manager.WatermarkManager` is created automatically.
        config: Execution parameters (includes ``job_id``).
        secret_provider: Resolves secrets in connection configs.
            If not provided, the resolved platform is used as the default provider.
        system_logger: Optional system-level logger.
        etl_logger: Optional structured ETL logger.
        base_log_path: Base directory for auto-created loggers.  When
            provided, ``SystemLogger`` and ``ETLLogger`` are created under
            ``<base_log_path>/system_logs`` and ``<base_log_path>/etl_logs``.
            Takes precedence over ``log_config.output_path``.
        log_config: Optional :class:`LogConfig` used as the template for
            auto-created loggers.  If ``base_log_path`` is also given it
            overrides ``output_path``; otherwise ``log_config.output_path``
            is used as the base directory.  All other fields (``log_level``,
            ``storage_mode``, ``partition_by_date``, ``partition_pattern``,
            ``flush_interval_seconds``) are always preserved.
    """

    def __init__(
        self,
        engine: BaseEngine,
        platform: Optional[BasePlatform] = None,
        metadata_provider: Optional[BaseMetadataProvider] = None,
        watermark_manager: Optional[BaseWatermarkManager] = None,
        config: Optional[DataCoolieRunConfig] = None,
        secret_provider: Optional[BaseSecretProvider] = None,
        system_logger: Optional[SystemLogger] = None,
        etl_logger: Optional[ETLLogger] = None,
        base_log_path: Optional[str] = None,
        log_config: Optional[LogConfig] = None,
    ) -> None:
        # -- Core dependencies ------------------------------------------
        self._engine = engine

        # -- Platform resolution ----------------------------------------
        # Ensure engine.platform is set, with type-safety when both supplied.
        if platform is not None and engine.platform is not None:
            if type(platform) is not type(engine.platform):
                raise DataCoolieError(
                    f"Platform type mismatch: provided {type(platform).__name__!r} "
                    f"but engine already has {type(engine.platform).__name__!r}"
                )
        elif platform is not None:
            engine.set_platform(platform)
        elif engine.platform is None:
            raise DataCoolieError(
                "A platform is required — pass platform= or set engine.platform "
                "before creating the driver"
            )

        self._metadata_provider = metadata_provider

        # Auto-create WatermarkManager when not explicitly provided.
        if watermark_manager is None and metadata_provider is not None:
            from datacoolie.watermark.watermark_manager import WatermarkManager
            watermark_manager = WatermarkManager(metadata_provider)
        self._watermark_manager = watermark_manager

        self._config = config or DataCoolieRunConfig()
        # Platforms are now BaseSecretProvider subclasses; fall back to the
        # resolved platform itself when no explicit provider is supplied.
        self._secret_provider: BaseSecretProvider = secret_provider or self._engine.platform

        # -- Timing -----------------------------------------------------
        self._start_time = utc_now()

        # -- Loggers ----------------------------------------------------
        # Resolve the effective base path: base_log_path wins over
        # log_config.output_path when both are supplied.
        effective_base = base_log_path
        if effective_base is None and log_config is not None:
            effective_base = log_config.output_path

        if effective_base is not None:
            base = effective_base.rstrip("/")
            if system_logger is None:
                if log_config is not None:
                    sys_cfg = _dc_replace(log_config, output_path=f"{base}/system_logs")
                    system_logger = SystemLogger(sys_cfg, self._engine.platform)
                else:
                    system_logger = create_system_logger(
                        output_path=f"{base}/system_logs",
                        platform=self._engine.platform,
                    )
            if etl_logger is None:
                if log_config is not None:
                    etl_cfg = _dc_replace(log_config, output_path=f"{base}/etl_logs")
                    etl_logger = ETLLogger(etl_cfg, self._engine.platform)
                else:
                    etl_logger = create_etl_logger(
                        output_path=f"{base}/etl_logs",
                        platform=self._engine.platform,
                    )

        self._system_logger = system_logger
        self._etl_logger = etl_logger

        # Always sync run config onto any loggers.
        if self._system_logger:
            self._system_logger.set_run_config(self._config)
        if self._etl_logger:
            self._etl_logger.set_run_config(self._config)
            self._etl_logger.set_component_names(
                engine_name=type(self._engine).__name__,
                platform_name=type(self._engine.platform).__name__,
                metadata_provider_name=type(self._metadata_provider).__name__,
                watermark_manager_name=type(self._watermark_manager).__name__,
            )

        # -- Execution components ---------------------------------------
        self._distributor = JobDistributor(
            job_num=self._config.job_num,
            job_index=self._config.job_index,
        )
        self._executor = ParallelExecutor(
            max_workers=self._config.max_workers,
            stop_on_error=self._config.stop_on_error,
        )
        self._retry_handler = RetryHandler(
            retry_count=self._config.retry_count,
            retry_delay=self._config.retry_delay,
        )

        self._dataflows: List[DataFlow] = []
        self._column_name_mode = ColumnCaseMode.LOWER

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def job_id(self) -> str:
        return self._config.job_id

    @property
    def config(self) -> DataCoolieRunConfig:
        return self._config

    # ------------------------------------------------------------------
    # Dataflow loading
    # ------------------------------------------------------------------

    def load_dataflows(
        self,
        stage: Optional[Union[str, List[str]]] = None,
        active_only: bool = True,
        attach_schema_hints: bool = True,
    ) -> List[DataFlow]:
        """Load and filter dataflows for this job.

        Args:
            stage: Optional stage filter. Accepts a single name
                (``"bronze2silver"``), a comma-separated string
                (``"bronze2silver,silver2gold"``), or a list of names.
            active_only: Skip inactive dataflows.
            attach_schema_hints: Attach schema hints from metadata.

        Returns:
            Filtered list for this job.
        """
        logger.info(
            "Loading dataflows — stage: %s, job: %d/%d",
            stage,
            self._config.job_index + 1,
            self._config.job_num,
        )

        all_dataflows = self._metadata_provider.get_dataflows(
            stage=stage,
            active_only=active_only,
            attach_schema_hints=attach_schema_hints,
        )

        self._dataflows = self._distributor.filter_dataflows(
            all_dataflows, active_only=active_only
        )

        logger.info(
            "Loaded %d dataflows for this job (total: %d)",
            len(self._dataflows),
            len(all_dataflows),
        )
        return self._dataflows

    def load_maintenance_dataflows(
        self,
        connection: Optional[Union[str, List[str]]] = None,
        active_only: bool = True,
    ) -> List[DataFlow]:
        """Load lakehouse dataflows eligible for maintenance.

        Dataflows that share the same physical destination (same
        catalog-qualified table or storage path) are deduplicated
        before job distribution so ``OPTIMIZE`` / ``VACUUM`` runs
        at most once per destination, avoiding concurrent-write
        races in fan-in topologies.  Only the winning dataflow
        produces a maintenance log row; covered dataflows are not
        individually logged.

        Args:
            connection: Optional filter by destination connection id or
                name. Accepts a single value, a comma-separated string,
                or a list.
            active_only: Skip inactive dataflows.

        Returns:
            Filtered list of unique-destination dataflows for this job.
        """
        logger.info(
            "Loading maintenance dataflows — connection: %s, job: %d/%d",
            connection,
            self._config.job_index + 1,
            self._config.job_num,
        )

        all_dataflows = self._metadata_provider.get_maintenance_dataflows(
            connection=connection,
        )

        unique = dedupe_by_destination(all_dataflows)

        self._dataflows = self._distributor.filter_dataflows(
            unique, active_only=active_only
        )

        logger.info(
            "Loaded %d maintenance dataflows for this job (unique: %d, total: %d)",
            len(self._dataflows),
            len(unique),
            len(all_dataflows),
        )
        return self._dataflows

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(
        self,
        stage: Optional[Union[str, List[str]]] = None,
        dataflows: Optional[List[DataFlow]] = None,
        column_name_mode: Union[ColumnCaseMode, str] = ColumnCaseMode.LOWER,
    ) -> ExecutionResult:
        """Alias for :meth:`run_dataflow`."""
        return self.run_dataflow(stage=stage, dataflows=dataflows, column_name_mode=column_name_mode)

    # ------------------------------------------------------------------
    # ETL execution
    # ------------------------------------------------------------------

    def run_dataflow(
        self,
        stage: Optional[Union[str, List[str]]] = None,
        dataflows: Optional[List[DataFlow]] = None,
        column_name_mode: Union[ColumnCaseMode, str] = ColumnCaseMode.LOWER,
    ) -> ExecutionResult:
        """Execute ETL (read → transform → write) for this job.

        Loads dataflows from metadata when *dataflows* is not provided.
        Can be called multiple times within the same job session
        (logs accumulate until :meth:`close`).

        Args:
            stage: Optional stage filter. Accepts a single name
                (``"bronze2silver"``), a comma-separated string
                (``"bronze2silver,silver2gold"``), or a list of names.
            dataflows: Pre-loaded dataflows (skips metadata loading).
            column_name_mode: Column name case-conversion mode.
                ``"lower"`` (default) lowercases without inserting underscores;
                ``"snake"`` converts to ``snake_case``.

        Returns:
            Aggregated execution statistics.
        """
        self._column_name_mode = ColumnCaseMode(column_name_mode)
        if dataflows is None:
            target = self.load_dataflows(stage=stage)
        else:
            target = dataflows

        if not target:
            logger.info("No dataflows to process")
            return ExecutionResult()

        if self._config.dry_run:
            logger.info("Dry-run mode — would process %d dataflows", len(target))
            for df in target:
                logger.info(
                    "  - [%s] %s → %s",
                    df.dataflow_id,
                    df.source.full_table_name or df.source.path,
                    df.destination.full_table_name or df.destination.path,
                )
            return ExecutionResult(total=len(target))

        groups = self._distributor.group_dataflows(target)
        result = self._executor.execute_with_groups(
            groups=groups,
            process_fn=self._process_dataflow,
            callback=self._on_dataflow_complete,
        )

        logger.info(
            "ETL complete — Succeeded: %d, Failed: %d, Skipped: %d",
            result.succeeded,
            result.failed,
            result.skipped,
        )
        return result

    def _process_dataflow(self, dataflow: DataFlow) -> DataFlowRuntimeInfo:
        """Process a single dataflow with retry logic.

        Thin wrapper around :meth:`_run_single_pipeline` with deep copy.
        """
        return self._run_single_pipeline(dataflow.model_copy(deep=True))

    def _run_single_pipeline(
        self,
        dataflow: DataFlow,
        *,
        watermark_start: Optional[Dict[str, Any]] = None,
        watermark_end: Optional[Dict[str, Any]] = None,
        save_watermark: bool = True,
        watermark_operator: str = ">",
        operation_type: str = ExecutionType.ETL.value,
    ) -> DataFlowRuntimeInfo:
        """Execute one pipeline run with timing, retry, context, and logging.

        This is the shared execution wrapper used by both normal ETL
        (:meth:`_process_dataflow`) and replay chunks (:meth:`_process_replay`).

        The *dataflow* should already be deep-copied by the caller.

        Args:
            dataflow: Pre-copied dataflow to execute.
            watermark_start: Override the read watermark (chunk lower bound).
                ``None`` = use the stored watermark (normal ETL).
            watermark_end: Override the value saved after a successful write.
                ``None`` = auto-save the reader-detected new watermark
                (only applies when *save_watermark* is ``True``).
            save_watermark: Whether to persist a watermark at all.
                ``False`` leaves the stored watermark untouched (backfill mode).
            watermark_operator: Comparison operator for the lower-bound
                WHERE clause.  ``">"`` (default, normal ETL) or ``">=>"
                (replay, inclusive lower bound).

        Returns:
            :class:`DataFlowRuntimeInfo` with timing, status, and metrics.
        """
        start_time = utc_now()
        dataflow_run_id = generate_unique_id()
        status = DataFlowStatus.RUNNING
        error_message: Optional[str] = None

        source_runtime: Optional[SourceRuntimeInfo] = None
        transform_runtime: Optional[TransformRuntimeInfo] = None
        dest_runtime: Optional[DestinationRuntimeInfo] = None
        attempts = 1

        ctx_token = set_dataflow_id(dataflow.dataflow_id)
        try:
            (source_runtime, transform_runtime, dest_runtime, status), attempts = (
                self._retry_handler.execute(
                    self._execute_etl_pipeline, dataflow,
                    dataflow_run_id=dataflow_run_id,
                    watermark_start=watermark_start,
                    watermark_end=watermark_end,
                    save_watermark=save_watermark,
                    watermark_operator=watermark_operator,
                )
            )
        except PipelineError as exc:
            status = DataFlowStatus.FAILED
            error_message = str(exc)
            if exc.partial_result:
                source_runtime, transform_runtime, dest_runtime, _ = exc.partial_result
            logger.error("Failed (final): %s", exc, exc_info=exc.__cause__)
        except Exception as exc:
            status = DataFlowStatus.FAILED
            error_message = str(exc)
            logger.error("Failed (final): %s", exc, exc_info=exc.__cause__)
        finally:
            clear_dataflow_id(ctx_token)

        end_time = utc_now()
        runtime = DataFlowRuntimeInfo(
            dataflow_run_id=dataflow_run_id,
            dataflow_id=dataflow.dataflow_id,
            operation_type=operation_type,
            source=source_runtime or SourceRuntimeInfo(),
            transform=transform_runtime or TransformRuntimeInfo(),
            destination=dest_runtime or DestinationRuntimeInfo(),
            start_time=start_time,
            end_time=end_time,
            status=status.value,
            error_message=error_message,
            retry_attempts=max(0, attempts - 1),
        )

        if self._etl_logger:
            try:
                self._etl_logger.log(dataflow=dataflow, runtime_info=runtime)
            except Exception as exc:
                logger.warning("Failed to log ETL result", exc_info=exc.__cause__)

        return runtime

    def _execute_etl_pipeline(
        self,
        dataflow: DataFlow,
        dataflow_run_id: str,
        *,
        watermark_start: Optional[Dict[str, Any]] = None,
        watermark_end: Optional[Dict[str, Any]] = None,
        save_watermark: bool = True,
        watermark_operator: str = ">",
    ) -> tuple:
        """Run read → transform → write for *dataflow*.

        Called by :meth:`RetryHandler.execute`; any exception triggers
        automatic retry with exponential backoff.

        Args:
            dataflow: The dataflow to execute (already deep-copied by caller).
            dataflow_run_id: Unique ID for this execution.
            watermark_start: Override the read watermark (chunk lower bound).
                ``None`` = use the stored watermark (normal ETL).
            watermark_end: Override the value saved after a successful write.
                ``None`` = auto-save the reader-detected new watermark
                (only applies when *save_watermark* is ``True``).
            save_watermark: Whether to persist a watermark at all.
                ``False`` leaves the stored watermark untouched (backfill mode).

        Returns:
            ``(source_runtime, transform_runtime, dest_runtime, status)``
        """
        logger.info(
            "Starting %s: %s → %s",
            dataflow.name,
            dataflow.source.full_table_name or dataflow.source.path,
            dataflow.destination.full_table_name or dataflow.destination.path,
        )

        # Replay uses an explicit start; normal ETL reads from the store.
        watermark = (
            watermark_start
            if watermark_start is not None
            else self._watermark_manager.get_watermark(dataflow_id=dataflow.dataflow_id)
        )

        source_runtime: Optional[SourceRuntimeInfo] = None
        transform_runtime: Optional[TransformRuntimeInfo] = None
        dest_runtime: Optional[DestinationRuntimeInfo] = None

        try:
            # Read source
            # Resolve secrets before creating reader/writer
            self._resolve_connection_secrets(dataflow)

            reader = self._create_source_reader(dataflow)
            df = reader.read(dataflow.source, watermark, watermark_operator=watermark_operator)
            source_runtime = reader.get_runtime_info()

            if df is None or source_runtime.rows_read == 0:
                logger.info("No data to process")
                return source_runtime, None, None, DataFlowStatus.SKIPPED

            # Transform
            pipeline = self._create_transformer_pipeline()
            df = pipeline.transform(df, dataflow)
            transform_runtime = pipeline.get_runtime_info()

            # Write
            # Compute watermark window for replace_by_watermark strategies.
            dataflow.apply_watermark_window(source_runtime)

            writer = self._create_destination_writer(dataflow)
            writer.write(df, dataflow)
            dest_runtime = writer.get_runtime_info()

            # Watermark persistence: save_watermark=False skips entirely.
            # When saving: use explicit watermark_end if provided, otherwise
            # auto-save the reader-detected new watermark.
            if save_watermark and self._watermark_manager:
                wm_to_save = watermark_end if watermark_end is not None else reader.get_new_watermark()
                if wm_to_save:
                    self._watermark_manager.save_watermark(
                        dataflow_id=dataflow.dataflow_id,
                        watermark=wm_to_save,
                        job_id=self._config.job_id,
                        dataflow_run_id=dataflow_run_id,
                    )
        except Exception as exc:
            raise PipelineError(
                str(exc),
                partial_result=(source_runtime, transform_runtime, dest_runtime, DataFlowStatus.FAILED),
            ) from exc

        rows_r = source_runtime.rows_read if source_runtime else 0
        rows_w = dest_runtime.rows_written if dest_runtime else 0
        logger.info(
            "Complete — Read: %d, Written: %d",
            rows_r,
            rows_w,
        )

        return source_runtime, transform_runtime, dest_runtime, DataFlowStatus.SUCCEEDED

    # ------------------------------------------------------------------
    # Replay / backfill
    # ------------------------------------------------------------------

    def run_replay(
        self,
        dataflows: Union[DataFlow, List[DataFlow]],
        replay: ReplayConfig,
        column_name_mode: Union[ColumnCaseMode, str] = ColumnCaseMode.LOWER,
    ) -> ExecutionResult:
        """Replay a bounded range across one or more dataflows in sequential chunks.

        Each dataflow is processed concurrently (bounded by ``max_workers``);
        chunks within a single dataflow always run sequentially.

        The chunk column is resolved automatically from
        ``dataflow.source.watermark_columns[0]`` unless ``replay.chunk_column``
        is set explicitly.

        Args:
            dataflows: One or more dataflows to replay.
            replay: Replay configuration (range, chunking, watermark policy).
            column_name_mode: Column name case-conversion mode.

        Returns:
            :class:`ExecutionResult` where each counter entry represents one
            chunk (not one dataflow).  Use :meth:`_process_replay` directly
            to obtain the per-chunk :class:`DataFlowRuntimeInfo` list.
        """
        logger.info("Starting replay run")

        self._column_name_mode = ColumnCaseMode(column_name_mode)

        target: List[DataFlow] = dataflows if isinstance(dataflows, list) else [dataflows]
        if not target:
            return ExecutionResult()

        # Pre-validate chunk_column resolution for all dataflows before executing.
        if replay.chunk_column is None:
            for df in target:
                if not df.source.watermark_columns:
                    raise DataCoolieError(
                        f"Cannot auto-resolve chunk_column: dataflow {df.dataflow_id!r} "
                        f"has no watermark_columns. Set replay.chunk_column explicitly."
                    )

        result = self._executor.execute(
            dataflows=target,
            process_fn=functools.partial(self._process_replay, replay=replay),
            callback=self._on_replay_complete,
        )

        logger.info(
            "Replay complete — Succeeded: %d, Failed: %d",
            result.succeeded,
            result.failed,
        )
        return result

    def _process_replay(
        self,
        dataflow: DataFlow,
        replay: ReplayConfig,
    ) -> DataFlowRuntimeInfo:
        """Process a full replay for a single dataflow.

        Resolves chunk boundaries and runs sequential chunks via
        :meth:`_run_single_pipeline`.

        Args:
            dataflow: The dataflow to replay.
            replay: Replay configuration (range, chunking, watermark policy).

        Returns:
            Single :class:`DataFlowRuntimeInfo` summarising all chunks.
            Processing stops on the first failed chunk.
        """
        start_time = utc_now()
        dataflow_run_id = generate_unique_id()

        # Resolve chunk column from dataflow metadata
        col = replay.chunk_column
        if col is None:
            wm_cols = dataflow.source.watermark_columns
            if not wm_cols:
                raise DataCoolieError(
                    f"Cannot auto-resolve chunk_column: dataflow {dataflow.dataflow_id!r} "
                    f"has no watermark_columns. Set replay.chunk_column explicitly."
                )
            col = wm_cols[0]

        # Generate chunk boundaries
        if replay.chunk_interval:
            chunks = generate_chunk_boundaries(
                start=replay.start,
                end=replay.end,
                interval=replay.chunk_interval,
            )
        else:
            # Single-shot: one chunk covering the entire range
            chunks = [(replay.start, replay.end)]

        # Resume support: skip completed chunks when save_watermark is enabled
        if replay.save_watermark and self._watermark_manager:
            stored = self._watermark_manager.get_watermark(dataflow.dataflow_id)
            if stored and stored.get(col) is not None:
                stored_val = stored[col]
                chunks = [(lo, hi) for lo, hi in chunks if lo >= stored_val or (lo < stored_val < hi)]
                if not chunks:
                    logger.info(
                        "Replay already complete for %s — stored watermark %s >= range end",
                        dataflow.name,
                        stored_val,
                    )
                    return DataFlowRuntimeInfo(
                        dataflow_run_id=dataflow_run_id,
                        dataflow_id=dataflow.dataflow_id,
                        operation_type=ExecutionType.REPLAY.value,
                        source=SourceRuntimeInfo(),
                        transform=TransformRuntimeInfo(),
                        destination=DestinationRuntimeInfo(),
                        start_time=start_time,
                        end_time=utc_now(),
                        status=DataFlowStatus.SKIPPED.value,
                    )

        total = len(chunks)
        logger.info(
            "Replaying %s — %d chunk(s), range [%s, %s), column=%s",
            dataflow.name,
            total,
            replay.start,
            replay.end,
            col,
        )

        chunk_results: List[DataFlowRuntimeInfo] = []

        for idx, (lower, upper) in enumerate(chunks, 1):
            logger.info("Replay chunk %d/%d: [%s, %s)", idx, total, lower, upper)

            # Deep copy per chunk; disable date_backward so chunk boundaries are exact.
            chunk_df: DataFlow = dataflow.model_copy(deep=True)
            for key in _BACKWARD_KEYS:
                chunk_df.source.configure.pop(key, None)
            if chunk_df.source.connection:
                for key in _BACKWARD_KEYS:
                    chunk_df.source.connection.configure.pop(key, None)

            # Set the upper-bound filter on source so the source reader
            # applies it during read.  Uses strict less-than for [lower, upper)
            # semantics — produces whole calendar-aligned chunks.
            # Numeric bounds are unquoted; date/datetime/str bounds are
            # single-quoted (Python's __str__ produces ISO 8601 for date/datetime).
            _upper = f"{col} < {upper}" if isinstance(upper, (int, float)) else f"{col} < '{upper}'"
            if chunk_df.source.filter_expression:
                chunk_df.source.filter_expression = (
                    f"({chunk_df.source.filter_expression}) AND ({_upper})"
                )
            else:
                chunk_df.source.filter_expression = _upper

            chunk_runtime = self._run_single_pipeline(
                chunk_df,
                watermark_start={col: lower},
                watermark_end={col: upper} if replay.save_watermark else None,
                save_watermark=replay.save_watermark,
                watermark_operator=">=",
                operation_type=ExecutionType.REPLAY.value,
            )

            chunk_results.append(chunk_runtime)
            self._on_replay_complete(chunk_runtime)

            if chunk_runtime.status == DataFlowStatus.FAILED.value:
                logger.error(
                    "Replay stopped for %s at chunk %d/%d due to failure",
                    dataflow.name, idx, total,
                )
                break

        # Summarise all chunk results into one DataFlowRuntimeInfo
        failed = next((r for r in chunk_results if r.status == DataFlowStatus.FAILED.value), None)
        all_skipped = all(r.status == DataFlowStatus.SKIPPED.value for r in chunk_results)
        final_status = (
            DataFlowStatus.FAILED.value if failed
            else DataFlowStatus.SKIPPED.value if all_skipped
            else DataFlowStatus.SUCCEEDED.value
        )
        return DataFlowRuntimeInfo(
            dataflow_run_id=dataflow_run_id,
            dataflow_id=dataflow.dataflow_id,
            operation_type=ExecutionType.REPLAY.value,
            source=SourceRuntimeInfo(rows_read=sum(r.source.rows_read for r in chunk_results)),
            transform=TransformRuntimeInfo(),
            destination=DestinationRuntimeInfo(rows_written=sum(r.destination.rows_written for r in chunk_results)),
            start_time=start_time,
            end_time=utc_now(),
            status=final_status,
            error_message=failed.error_message if failed else None,
        )

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def run_maintenance(
        self,
        connection: Optional[Union[str, List[str]]] = None,
        dataflows: Optional[List[DataFlow]] = None,
        do_compact: bool = True,
        do_cleanup: bool = True,
    ) -> ExecutionResult:
        """Run maintenance operations (optimize + vacuum).

        Only lakehouse destinations (Delta Lake and Iceberg) are eligible.

        Args:
            connection: Optional filter by destination connection id or
                name. Accepts a single value, a comma-separated string,
                or a list.
            dataflows: Pre-loaded dataflows to maintain. When ``None``,
                dataflows are fetched from metadata filtered to lakehouse
                formats only.
            do_compact: Run the compaction (optimize) step.
            do_cleanup: Run the cleanup (vacuum) step.

        Returns:
            Aggregated execution statistics.
        """
        logger.info("Starting maintenance run")

        if dataflows is not None:
            target = dedupe_by_destination(dataflows)
        else:
            target = self.load_maintenance_dataflows(connection=connection)

        if not target:
            logger.info("No lakehouse dataflows for maintenance")
            return ExecutionResult()

        result = self._executor.execute(
            dataflows=target,
            process_fn=functools.partial(
                self._process_maintenance,
                do_compact=do_compact,
                do_cleanup=do_cleanup,
            ),
            callback=self._on_maintenance_complete,
        )

        logger.info(
            "Maintenance complete — Succeeded: %d, Failed: %d",
            result.succeeded,
            result.failed,
        )
        return result

    def _process_maintenance(
        self,
        dataflow: DataFlow,
        do_compact: bool = True,
        do_cleanup: bool = True,
    ) -> DataFlowRuntimeInfo:
        """Process maintenance for a single dataflow with retry logic.

        Delegates to :meth:`RetryHandler.execute` so all retry / backoff
        logic lives in one place.

        Returns:
            Runtime info wrapping the maintenance :class:`DestinationRuntimeInfo`.
        """
        dataflow = dataflow.model_copy(deep=True)
        start_time = utc_now()
        dataflow_run_id = generate_unique_id()
        status: str = DataFlowStatus.RUNNING.value
        error_msg: Optional[str] = None
        dest_runtime: Optional[DestinationRuntimeInfo] = None
        attempts = 1

        ctx_token = set_dataflow_id(dataflow.dataflow_id)
        try:
            dest_runtime, attempts = self._retry_handler.execute(
                self._execute_maintenance_pipeline, dataflow,
                do_compact=do_compact, do_cleanup=do_cleanup,
            )
            status = dest_runtime.status
            error_msg = dest_runtime.error_message
        except PipelineError as exc:
            status = DataFlowStatus.FAILED.value
            error_msg = str(exc)
            if exc.partial_result:
                dest_runtime = exc.partial_result
            logger.error("Maintenance failed (final): %s", exc, exc_info=exc.__cause__)
        except Exception as exc:
            status = DataFlowStatus.FAILED.value
            error_msg = str(exc)
            logger.error("Maintenance failed (final): %s", exc, exc_info=exc.__cause__)
        finally:
            clear_dataflow_id(ctx_token)

        if dest_runtime is None:
            dest_runtime = DestinationRuntimeInfo(
                start_time=start_time,
                end_time=utc_now(),
                status=status,
                error_message=error_msg,
                operation_type=ExecutionType.MAINTENANCE.value,
            )

        retry_attempts = max(0, attempts - 1)
        end_time = utc_now()

        runtime = DataFlowRuntimeInfo(
            dataflow_run_id=dataflow_run_id,
            dataflow_id=dataflow.dataflow_id,
            operation_type=ExecutionType.MAINTENANCE.value,
            destination=dest_runtime,
            start_time=start_time,
            end_time=end_time,
            status=status,
            error_message=error_msg,
            retry_attempts=retry_attempts,
        )

        if self._etl_logger:
            try:
                self._etl_logger.log(
                    dataflow=dataflow,
                    runtime_info=runtime,
                )
            except Exception as exc:
                logger.warning("Failed to log maintenance result", exc_info=exc.__cause__)

        return runtime

    def _execute_maintenance_pipeline(
        self,
        dataflow: DataFlow,
        do_compact: bool = True,
        do_cleanup: bool = True,
    ) -> DestinationRuntimeInfo:
        """Run optimize + vacuum for *dataflow*.

        Called by :meth:`RetryHandler.execute`; any exception triggers
        automatic retry with exponential backoff.

        Args:
            dataflow: The dataflow to maintain.
            do_compact: Run the compaction (optimize) step.
            do_cleanup: Run the cleanup (vacuum) step.

        Returns:
            :class:`DestinationRuntimeInfo` from the writer.
        """
        logger.info(
            "Starting maintenance: %s",
            dataflow.destination.full_table_name or dataflow.destination.path,
        )

        dest_runtime: Optional[DestinationRuntimeInfo] = None
        try:
            writer = self._create_destination_writer(dataflow)
            dest_runtime = writer.run_maintenance(
                dataflow=dataflow,
                do_compact=do_compact,
                do_cleanup=do_cleanup,
                retention_hours=self._config.retention_hours,
            )
        except Exception as exc:
            raise PipelineError(
                str(exc),
                partial_result=dest_runtime,
            ) from exc

        logger.info(
            "Maintenance %s — files_added=%d, files_removed=%d",
            dest_runtime.status,
            dest_runtime.files_added,
            dest_runtime.files_removed,
        )

        return dest_runtime

    # ------------------------------------------------------------------
    # Secret resolution
    # ------------------------------------------------------------------

    def _resolve_connection_secrets(self, dataflow: DataFlow) -> None:
        """Resolve secret references on source and destination connections.

        Uses the explicit ``secret_provider`` when given; otherwise the
        driver's platform is used (platforms now implement
        :class:`~datacoolie.core.secret_provider.BaseSecretProvider`).

        Prefixed sources (e.g. ``"env:APP_"``) are dispatched to the
        matching plugin resolver via :data:`datacoolie.resolver_registry`.
        """
        from datacoolie import resolver_registry

        def _lookup(prefix: str) -> BaseSecretResolver | None:
            if resolver_registry.is_available(prefix):
                return resolver_registry.get_or_create(prefix)
            return None

        if dataflow.source and dataflow.source.connection:
            resolve_secrets(
                dataflow.source.connection,
                self._secret_provider,
                resolver_lookup=_lookup,
            )

        if dataflow.destination and dataflow.destination.connection:
            resolve_secrets(
                dataflow.destination.connection,
                self._secret_provider,
                resolver_lookup=_lookup,
            )

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    def _create_source_reader(self, dataflow: DataFlow) -> BaseSourceReader:
        """Create a source reader based on the connection format."""
        from datacoolie import source_registry

        fmt = dataflow.source.connection.format
        kwargs: Dict[str, Any] = {"engine": self._engine}
        if fmt == Format.FUNCTION.value and self._config.allowed_function_prefixes:
            kwargs["allowed_prefixes"] = self._config.allowed_function_prefixes
        return source_registry.get(fmt, **kwargs)

    def _create_transformer_pipeline(self) -> TransformerPipeline:
        """Create a default transformer pipeline from the registry."""
        from datacoolie import transformer_registry

        pipeline = TransformerPipeline(self._engine)
        # Per-transformer extra kwargs beyond ``engine``.
        extra_kwargs: dict[str, dict[str, object]] = {
            "column_name_sanitizer": {"mode": self._column_name_mode},
        }
        for name in DEFAULT_TRANSFORMERS:
            if transformer_registry.is_available(name):
                kwargs = extra_kwargs.get(name, {})
                pipeline.add_transformer(
                    transformer_registry.get(name, engine=self._engine, **kwargs)
                )
        return pipeline

    def _create_destination_writer(self, dataflow: DataFlow) -> BaseDestinationWriter:
        """Create a destination writer based on the connection format."""
        from datacoolie import destination_registry

        fmt = dataflow.destination.connection.format
        return destination_registry.get(fmt, engine=self._engine)

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    def _on_dataflow_complete(self, result: DataFlowRuntimeInfo) -> None:
        """Callback for dataflow completion (logging handled in _process_dataflow)."""
        pass

    def _on_maintenance_complete(self, result: DataFlowRuntimeInfo) -> None:
        """Callback for maintenance completion."""
        pass

    def _on_replay_complete(self, result: DataFlowRuntimeInfo) -> None:
        """Per-chunk completion hook for replay.

        Called after each chunk finishes (including failures). Override in
        subclasses to add custom monitoring, alerting, or logging.
        """
        pass

    # ------------------------------------------------------------------
    # Logging / cleanup
    # ------------------------------------------------------------------

    def _flush_logs(self) -> None:
        """Flush all loggers."""
        for lgr in (self._etl_logger, self._system_logger):
            if lgr is not None:
                try:
                    lgr.close()
                except Exception as exc:
                    logger.debug("Logger flush failed: %s", exc)

    def close(self) -> None:
        """Close driver and flush logs. Safe to call multiple times."""
        self._flush_logs()
        self._dataflows = []

    def __enter__(self) -> "DataCoolieDriver":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


# ============================================================================
# Factory function
# ============================================================================


def create_driver(
    engine: BaseEngine,
    platform: Optional[BasePlatform] = None,
    metadata_provider: Optional[BaseMetadataProvider] = None,
    watermark_manager: Optional[BaseWatermarkManager] = None,
    job_id: Optional[str] = None,
    job_num: int = 1,
    job_index: int = 0,
    max_workers: int = DEFAULT_MAX_WORKERS,
    secret_provider: Optional[BaseSecretProvider] = None,
    system_logger: Optional[SystemLogger] = None,
    etl_logger: Optional[ETLLogger] = None,
    base_log_path: Optional[str] = None,
    log_config: Optional[LogConfig] = None,
    **kwargs: Any,
) -> DataCoolieDriver:
    """Create a configured :class:`DataCoolieDriver`.

    Args:
        engine: Data operation engine.
        platform: Platform implementation.
        metadata_provider: Metadata provider.
        watermark_manager: Watermark manager.  Auto-created from
            *metadata_provider* when ``None``.
        job_id: Optional job identifier. Auto-generated when ``None``.
        job_num: Total parallel jobs.
        job_index: Current job index (0-based).
        max_workers: Max parallel workers per job.
        secret_provider: Optional secret provider for resolving secrets in
            connection configs.
        system_logger: Explicit system logger (overrides auto-creation).
        etl_logger: Explicit ETL logger (overrides auto-creation).
        base_log_path: Base directory for auto-created loggers.
            Takes precedence over ``log_config.output_path``.
        log_config: Optional :class:`LogConfig` template for auto-created
            loggers.  If ``base_log_path`` is also given it overrides
            ``output_path``; otherwise ``log_config.output_path`` is used.
        **kwargs: Additional :class:`DataCoolieRunConfig` options.

    Returns:
        Ready-to-use driver.
    """
    config = DataCoolieRunConfig(
        job_id=job_id or generate_unique_id(),
        job_num=job_num,
        job_index=job_index,
        max_workers=max_workers,
        **kwargs,
    )
    return DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=metadata_provider,
        watermark_manager=watermark_manager,
        config=config,
        secret_provider=secret_provider,
        system_logger=system_logger,
        etl_logger=etl_logger,
        base_log_path=base_log_path,
        log_config=log_config,
    )

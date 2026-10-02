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

import copy
import functools
import logging
import sys
import threading
from uuid import uuid4
from contextlib import contextmanager
from dataclasses import replace as _dc_replace
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    from datacoolie.core.secrets.resolver import BaseSecretResolver
    from datacoolie.core.models.connection import Connection

from datacoolie.core.constants import (
    ColumnCaseMode,
    DataFlowStatus,
    ExecutionType,
    DATE_FOLDER_PARTITION_KEY,
)
from datacoolie.core.exceptions import ConfigurationError, DataCoolieError
from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.runtime import DataFlowRuntimeInfo, PipelineAttemptResult
from datacoolie.engines.base import BaseEngine
from datacoolie.metadata.base import BaseMetadataProvider
from datacoolie.metadata.contracts.context import MetadataProviderStartupContext
from datacoolie.platforms.base import BasePlatform
from datacoolie.watermark.base import BaseWatermarkManager

# Source readers
from datacoolie.sources import BaseSourceReader, SourceReadRange

# Transformers
from datacoolie.transformers import TransformerPipeline

# Destination writers
from datacoolie.destinations import BaseDestinationWriter

from datacoolie.orchestration.maintenance import dedupe_by_destination
from datacoolie.orchestration.execution.activation import inactive_reason
from datacoolie.orchestration.scheduling.job_distributor import JobDistributor
from datacoolie.orchestration.scheduling.parallel_executor import ExecutionResult, ParallelExecutor
from datacoolie.orchestration.execution.lifecycle import (
    execution_observation_attempted,
    run_dataflow_execution,
    run_dry_run_execution,
    run_prepared_execution,
    log_result_safely,
)
from datacoolie.orchestration.execution.pipeline import (
    build_destination_writer,
    build_source_reader,
    build_transformer_pipeline,
    execute_etl_pipeline,
    execute_maintenance_pipeline,
)
from datacoolie.orchestration.execution.replay import (
    process_replay,
    validate_replay_chunk_column,
)
from datacoolie.utils.retry import RetryHandler
from datacoolie.orchestration.preparation import (
    PreparedDataFlow,
    prepare_execution_dataflow,
    validate_preparation,
)

from datacoolie.core.secrets.provider import BaseSecretProvider, resolve_secrets
from datacoolie.utils.component_paths import ComponentPathError, normalize_component_paths
from datacoolie.logging import (
    ExecutionLogger,
    LogCategory,
    LogConfig,
    SystemLogger,
    create_execution_logger,
    create_system_logger,
)
from datacoolie.logging.base import BaseLogger
from datacoolie.logging.configuration.constants import INTERNAL_LOGGER_NAME, LogEvent
from datacoolie.logging.runtime.diagnostics import emit_safely
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.utils.time import utc_now
from datacoolie.utils.chunking import generate_chunk_boundaries, normalize_chunk_range
from datacoolie.utils.path_utils import join_path, normalize_optional_base_path

logger = get_logger(__name__)
_diagnostic_logger = logging.getLogger(INTERNAL_LOGGER_NAME)


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
        metadata_base_path: Optional metadata directory for an automatically
            created :class:`~datacoolie.metadata.file_provider.FileProvider`.
            When a provider is injected, the path is passed through the typed
            startup context so the provider can accept or reject it explicitly.
        watermark_manager: Reads and writes watermarks.  When ``None`` and a
            metadata provider is resolved (injected or inferred), a
            :class:`~datacoolie.watermark.watermark_manager.WatermarkManager`
            is created automatically.
        config: Execution parameters (includes ``job_id``).
        secret_provider: Resolves secrets in connection configs.
            If not provided, the resolved platform is used as the default provider.
        system_logger: Optional system-level logger.
        execution_logger: Optional structured execution logger.
        artifact_base_path: Optional root for deployed metadata/SQL/artifacts.
            With no injected provider it also selects FileProvider artifact
            mode, whose metadata default is ``<artifact_base_path>/metadata``.
            It remains available for relative SQL references when
            ``sql_base_path`` is absent and for explicit ``artifact:/``
            references.
        state_base_path: Optional root for framework runtime state.  It is
            used to derive logs and file-provider watermarks when their
            component-specific roots are absent.
        sql_base_path: Optional component root for relative SQL references.
            It takes precedence over ``artifact_base_path``.
        log_base_path: Base directory for auto-created loggers.  When
            provided, ``SystemLogger`` and ``ExecutionLogger`` write under
            ``<log_base_path>/system_logs`` and ``<log_base_path>/execution_logs``.
            Takes precedence over ``log_config.output_path``.
        log_config: Optional :class:`LogConfig` used as the template for
            auto-created loggers.  If ``log_base_path`` is also given it
            overrides ``output_path``; otherwise ``log_config.output_path``
            is used as the base directory.  All other persistence/capture
            fields are preserved.
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
        execution_logger: Optional[ExecutionLogger] = None,
        artifact_base_path: Optional[str] = None,
        state_base_path: Optional[str] = None,
        metadata_base_path: Optional[str] = None,
        sql_base_path: str | Sequence[str] | None = None,
        log_base_path: Optional[str] = None,
        log_config: Optional[LogConfig] = None,
    ) -> None:
        # -- Core dependencies ------------------------------------------
        # Capture the Driver lifecycle start before any provider/platform
        # initialization so JobRuntime includes startup and preparation.
        self._started_at = utc_now()
        self._log_session_id = uuid4().hex
        self._config = self._snapshot_run_config(config)
        self._engine = engine
        self._driver_state_lock = threading.Lock()
        self._operation_active = False
        self._operation_owner: Optional[int] = None
        self._closing = False
        self._session_failed = False
        self._session_error_message: Optional[str] = None
        self._last_recorded_session_exception: Optional[BaseException] = None
        self._session_had_success = False
        # Normalize all caller-owned roots before mutating the Engine's
        # platform binding.  Invalid path/config input must fail before the
        # Driver has changed any shared dependency state.
        self._artifact_base_path = self._normalise_root(artifact_base_path, "artifact_base_path")
        self._state_base_path = self._normalise_root(state_base_path, "state_base_path")
        metadata_base_path = self._normalise_root(metadata_base_path, "metadata_base_path")
        self._sql_base_path = self._normalise_component_roots(
            sql_base_path,
            "sql_base_path",
            artifact_base_path=self._artifact_base_path,
        )
        self._log_base_path = self._normalise_root(log_base_path, "log_base_path")
        log_config_base_path = (
            self._normalise_root(log_config.output_path, "log_config.output_path")
            if log_config is not None
            else None
        )

        # Validate caller-owned logger instances before binding the optional
        # platform onto the Engine.  A rejected logger is a pure
        # configuration error and must not leave a shared Engine partially
        # mutated for a caller that catches the exception and retries.
        if (
            system_logger is not None
            and system_logger is execution_logger
        ):
            raise ConfigurationError(
                "system_logger and execution_logger must be distinct session instances"
            )
        for logger_instance, role in (
            (system_logger, "system_logger"),
            (execution_logger, "execution_logger"),
        ):
            if logger_instance is not None:
                self._validate_session_logger(logger_instance, role)

        # -- Platform resolution ----------------------------------------
        # Resolve one execution platform without mutating the Engine yet.
        # Identity matters because platform instances can carry different
        # roots, credentials, or session state.  Delaying the setter keeps
        # provider/context validation below transactional for callers that
        # catch a configuration error and retry with the same Engine.
        if platform is not None and engine.platform is not None:
            if platform is not engine.platform:
                raise DataCoolieError(
                    "Platform type mismatch: Engine and Driver received different platform instances; "
                    "reuse engine.platform or leave platform unset"
                )
            resolved_platform = engine.platform
        elif platform is not None:
            resolved_platform = platform
        elif engine.platform is not None:
            resolved_platform = engine.platform
        else:
            raise DataCoolieError(
                "A platform is required — pass platform= or set engine.platform "
                "before creating the driver"
            )

        self._metadata_provider = metadata_provider
        self._owns_metadata_provider = False
        self._closed = False
        # Completion callbacks run synchronously inside the executor call.
        # Keep the metadata lookup scoped to that call so scheduler-created
        # fallback runtimes can be persisted without making the executor know
        # about Driver/provider concerns.
        self._completion_metadata_by_id: Dict[str, DataFlow] = {}
        self._completion_observed_ids: set[str] = set()

        # ``state_base_path`` supplies the framework's common runtime root;
        # component-specific log configuration remains the higher-priority
        # owner of the actual logger output location.
        effective_base = self._log_base_path
        if effective_base is None:
            effective_base = log_config_base_path
        if effective_base is None and self._state_base_path is not None:
            effective_base = join_path(self._state_base_path, "logs")
        self._effective_log_base_path = effective_base

        # Construct auto loggers and validate every injected logger before
        # metadata I/O.  Construction is intentionally inert; global capture
        # is claimed only by ``activate`` after provider startup succeeds.
        if self._effective_log_base_path is not None:
            base = self._effective_log_base_path
            if system_logger is None:
                if log_config is not None:
                    sys_cfg = _dc_replace(
                        log_config,
                        output_path=join_path(base, LogCategory.SYSTEM.value),
                    )
                    system_logger = SystemLogger(sys_cfg, resolved_platform)
                else:
                    system_logger = create_system_logger(
                        output_path=join_path(base, LogCategory.SYSTEM.value),
                        platform=resolved_platform,
                    )
            if execution_logger is None:
                if log_config is not None:
                    execution_cfg = _dc_replace(
                        log_config,
                        output_path=join_path(base, LogCategory.EXECUTION.value),
                    )
                    execution_logger = ExecutionLogger(execution_cfg, resolved_platform)
                else:
                    execution_logger = create_execution_logger(
                        output_path=join_path(base, LogCategory.EXECUTION.value),
                        platform=resolved_platform,
                    )

        self._system_logger = system_logger
        self._execution_logger = execution_logger
        self._session_loggers = tuple(
            logger_instance
            for logger_instance in (self._execution_logger, self._system_logger)
            if logger_instance is not None
        )

        self._capture_ready = False
        try:
            # Startup is deliberately staged: dependency/context binding,
            # logger activation, then provider initialization.  A missing
            # provider plus a metadata/artifact root selects FileProvider;
            # provider-less Drivers remain valid for explicit dataflows.
            self._bind_session_components(
                resolved_platform=resolved_platform,
                metadata_base_path=metadata_base_path,
                watermark_manager=watermark_manager,
                secret_provider=secret_provider,
            )
            self._activate_session_loggers()

            # Capture startup diagnostics before any provider performs I/O.
            emit_safely(
                logger,
                logging.INFO,
                "DataCoolie session starting: job_id=%s, metadata_provider=%s",
                self._config.job_id,
                (
                    type(self._metadata_provider).__name__
                    if self._metadata_provider is not None
                    else None
                ),
                extra={"event_name": LogEvent.SESSION_STARTING.value},
                catch_base=True,
            )

            # Provider startup is the fail-fast boundary.  BaseMetadataProvider
            # owns context application and initialization; concrete providers own
            # the meaning of values they consume.
            if self._metadata_provider is not None:
                self._metadata_provider.initialize()

            emit_safely(
                logger,
                logging.INFO,
                "DataCoolie session ready: job_id=%s, startup=%.3fs",
                self._config.job_id,
                (utc_now() - self._started_at).total_seconds(),
                extra={"event_name": LogEvent.SESSION_READY.value},
                catch_base=True,
            )
        except BaseException as exc:
            self._record_session_failure(exc)
            startup_logger = logger if self._capture_ready else _diagnostic_logger
            # Before this Driver owns capture, use the noncaptured diagnostic
            # path.  This is especially important when a second Driver is
            # rejected while another session is active.
            emit_safely(
                startup_logger,
                logging.ERROR,
                "DataCoolie session startup failed: job_id=%s",
                self._config.job_id,
                extra={"event_name": LogEvent.SESSION_STARTUP_FAILED.value},
                exc_info=(type(exc), exc, exc.__traceback__),
                catch_base=True,
            )
            # If execution logging reached activation, make startup failure
            # visible as a failed JobRuntime before releasing the logger.  A
            # failure before logger readiness remains best-effort by design.
            if self._execution_logger is not None and self._execution_logger.is_active:
                try:
                    self._execution_logger.finish_job(
                        DataFlowStatus.FAILED.value,
                        message=self._session_error_message,
                    )
                except BaseException as log_exc:
                    self._safe_diagnostic_log(
                        startup_logger,
                        "Startup failure could not finalize JobRuntime: %s",
                        log_exc,
                    )
            # Keep provider cleanup observable while SystemLogger still owns
            # capture, then release the session loggers.
            self._close_owned_metadata_provider(diagnostic_logger=startup_logger)
            self._close_session_loggers(diagnostic_logger=startup_logger)
            raise

        self._dataflows: List[DataFlow] = []

    @staticmethod
    def _snapshot_run_config(
        config: Optional[DataCoolieRunConfig],
    ) -> DataCoolieRunConfig:
        """Validate and detach the run policy owned by one Driver session."""
        if config is None:
            return DataCoolieRunConfig()
        if not isinstance(config, DataCoolieRunConfig):
            raise ConfigurationError(
                "config must be a DataCoolieRunConfig instance"
            )
        try:
            values = copy.deepcopy(config.model_dump())
            return DataCoolieRunConfig(**values)
        except Exception as exc:
            raise ConfigurationError(
                "Invalid DataCoolieRunConfig supplied to Driver"
            ) from exc

    @staticmethod
    def _normalise_root(value: Optional[str], name: str) -> Optional[str]:
        """Normalize an optional runtime root using the shared path contract."""
        try:
            return normalize_optional_base_path(value, name=name)
        except ValueError as exc:
            raise ConfigurationError(str(exc)) from exc

    @staticmethod
    def _normalise_component_roots(
        value: str | Sequence[str] | None,
        name: str,
        *,
        artifact_base_path: Optional[str] = None,
    ) -> str | tuple[str, ...] | None:
        """Normalize one-or-many component roots without probing storage."""
        try:
            roots = normalize_component_paths(
                value,
                name=name,
                artifact_base_path=artifact_base_path,
                allow_empty=False,
            )
        except ComponentPathError as exc:
            raise ConfigurationError(str(exc)) from exc
        if roots is None:
            return None
        values = tuple(root.base_path for root in roots)
        # Keep the scalar representation for the common one-root case while
        # exposing a tuple only when callers actually configure multiple
        # roots.  Preparation accepts either representation.
        return values[0] if len(values) == 1 else values

    @staticmethod
    def _assemble_metadata_provider(
        provider: Optional[BaseMetadataProvider],
        *,
        metadata_base_path: Optional[str],
        artifact_base_path: Optional[str],
    ) -> tuple[Optional[BaseMetadataProvider], bool]:
        """Resolve provider ownership and file-provider inference once.

        Explicit provider injection always wins.  When no provider is
        supplied, either metadata root selects an explicit FileProvider root or
        artifact root selects artifact mode (whose metadata root is bound by
        the provider context).  No root preserves provider-less execution for
        callers that pass dataflows directly.
        """
        if provider is not None:
            return provider, False
        if metadata_base_path is None and artifact_base_path is None:
            return None, False

        from datacoolie.metadata.file_provider import FileProvider

        return FileProvider(metadata_base_path=metadata_base_path), True

    @staticmethod
    def _validate_session_logger(logger_instance: BaseLogger, role: str) -> None:
        """Validate a typed logger contract before startup mutates it."""
        if not isinstance(logger_instance, BaseLogger):
            raise ConfigurationError(
                f"{role} must be a BaseLogger instance"
            )
        if role == "execution_logger" and not isinstance(logger_instance, ExecutionLogger):
            raise ConfigurationError(
                "execution_logger must be an ExecutionLogger instance"
            )
        logger_instance.validate_session_eligibility()

    def _bind_session_components(
        self,
        *,
        resolved_platform: BasePlatform,
        metadata_base_path: Optional[str],
        watermark_manager: Optional[BaseWatermarkManager],
        secret_provider: Optional[BaseSecretProvider],
    ) -> None:
        """Assemble and configure Driver-owned components before activation.

        This phase performs dependency wiring and provider context validation,
        but does not activate logging or perform provider I/O.  Keeping it as
        one typed Driver phase makes ownership and startup ordering explicit
        without introducing a cross-module service container.
        """
        (
            self._metadata_provider,
            self._owns_metadata_provider,
        ) = self._assemble_metadata_provider(
            self._metadata_provider,
            metadata_base_path=metadata_base_path,
            artifact_base_path=self._artifact_base_path,
        )

        metadata_context = MetadataProviderStartupContext(
            platform=resolved_platform,
            metadata_base_path=metadata_base_path,
            sql_base_path=self._sql_base_path,
            artifact_base_path=self._artifact_base_path,
            state_base_path=self._state_base_path,
            log_base_path=self._effective_log_base_path,
        )
        if self._metadata_provider is not None:
            self._metadata_provider.configure_context(metadata_context)
            if isinstance(self._metadata_provider, BaseMetadataProvider):
                self._sql_base_path = self._metadata_provider.resolve_sql_base_path(
                    metadata_context
                )

        if watermark_manager is None and self._metadata_provider is not None:
            from datacoolie.watermark.watermark_manager import WatermarkManager

            watermark_manager = WatermarkManager(self._metadata_provider)
        self._watermark_manager = watermark_manager
        # Platforms are BaseSecretProvider implementations; an explicit
        # provider remains higher priority than the platform fallback.
        self._secret_provider: BaseSecretProvider = secret_provider or resolved_platform

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

        if self._system_logger:
            self._system_logger.set_log_session_id(self._log_session_id)
            self._system_logger.set_run_config(self._config)
        if self._execution_logger:
            self._execution_logger.set_log_session_id(self._log_session_id)
            self._execution_logger.set_run_config(self._config)
            self._execution_logger.set_component_names(
                engine_name=type(self._engine).__name__,
                platform_name=type(resolved_platform).__name__,
                metadata_provider_name=(
                    type(self._metadata_provider).__name__
                    if self._metadata_provider is not None
                    else None
                ),
                watermark_manager_name=(
                    type(self._watermark_manager).__name__
                    if self._watermark_manager is not None
                    else None
                ),
            )

        # Commit the resolved dependency only after all pure provider,
        # logger, and execution-component validation has succeeded.
        if self._engine.platform is None:
            self._engine.set_platform(resolved_platform)

    def _activate_session_loggers(self) -> None:
        """Activate accepted loggers and remember capture ownership readiness."""
        if self._system_logger:
            self._system_logger.activate(started_at=self._started_at)
            # Mark immediately after SystemLogger activation so a later
            # ExecutionLogger failure still routes startup cleanup through the
            # capture-aware diagnostic logger.
            self._capture_ready = True
        if self._execution_logger:
            self._execution_logger.activate(started_at=self._started_at)

    def _close_owned_metadata_provider(
        self,
        *,
        diagnostic_logger: Optional[logging.Logger] = None,
    ) -> Optional[BaseException]:
        """Close a provider created by this Driver during startup/teardown."""
        if not self._owns_metadata_provider or self._metadata_provider is None:
            return None
        provider = self._metadata_provider
        self._owns_metadata_provider = False
        try:
            provider.close()
        except Exception as exc:
            self._safe_diagnostic_log(
                diagnostic_logger,
                "Metadata provider cleanup failed: %s",
                exc,
                exc_info=(type(exc), exc, exc.__traceback__),
            )
            return exc
        except BaseException as exc:
            self._safe_diagnostic_log(
                diagnostic_logger,
                "Metadata provider cleanup interrupted: %s",
                exc,
                exc_info=(type(exc), exc, exc.__traceback__),
            )
            return exc
        return None

    @staticmethod
    def _safe_diagnostic_log(
        diagnostic_logger: Optional[logging.Logger],
        message: str,
        *args: Any,
        exc_info: Any = None,
    ) -> None:
        """Best-effort cleanup diagnostic with no recursive fallback path."""
        target = diagnostic_logger or _diagnostic_logger
        kwargs: Dict[str, Any] = {"catch_base": True}
        if exc_info is not None:
            kwargs["exc_info"] = exc_info
        emit_safely(target, logging.WARNING, message, *args, **kwargs)

    def _record_session_failure(
        self,
        error: BaseException | str | None = None,
        *,
        message: Optional[str] = None,
        error_type: Optional[type[BaseException]] = None,
    ) -> None:
        """Record one session failure without replacing earlier details.

        The Driver owns the session-level status and explanation.  Dataflow
        errors, scheduler contract failures, startup exceptions, and
        interruptions all enter through this method so a later failure cannot
        erase useful context from an earlier operation.
        """
        with self._driver_state_lock:
            self._session_failed = True
            if (
                isinstance(error, BaseException)
                and self._last_recorded_session_exception is error
            ):
                return
            if isinstance(error, BaseException):
                self._last_recorded_session_exception = error
            if message is None:
                if error is not None:
                    # Exception stringification is user-controlled.  A
                    # malformed ``__str__`` must never replace the original
                    # failure while the Driver is recording its summary.
                    try:
                        message = str(error).strip()
                    except BaseException:
                        message = None
                elif error_type is not None:
                    message = error_type.__name__
            if not message:
                message = (
                    type(error).__name__
                    if isinstance(error, BaseException)
                    else error_type.__name__
                    if error_type is not None
                    else "Unknown session failure"
                )
            if self._session_error_message:
                self._session_error_message = (
                    f"{self._session_error_message}; {message}"
                )
            else:
                self._session_error_message = message

    @contextmanager
    def _driver_operation(self, operation_name: Optional[str] = None):
        """Admit one public Driver operation for the lifetime of its work.

        A Driver owns shared execution state (loggers, metadata snapshots,
        and executor resources), so overlapping public operations are not
        safe.  Admission is deliberately kept at the orchestration boundary;
        private helpers used by an admitted operation do not acquire it a
        second time.
        """
        owner = threading.get_ident()
        with self._driver_state_lock:
            if self._closed or self._closing:
                raise RuntimeError("Driver is closed")
            if self._operation_active:
                if self._operation_owner == owner:
                    raise RuntimeError("Driver operation is already active")
                raise RuntimeError("Driver already has an active operation")
            self._operation_active = True
            self._operation_owner = owner

        try:
            if operation_name:
                emit_safely(
                    logger,
                    logging.INFO,
                    "Operation started: %s",
                    operation_name,
                    extra={"event_name": LogEvent.OPERATION_STARTED.value},
                    catch_base=True,
                )
            yield
        except BaseException as exc:
            self._record_session_failure(exc)
            if operation_name:
                emit_safely(
                    logger,
                    logging.ERROR,
                    "Operation failed: %s",
                    operation_name,
                    extra={"event_name": LogEvent.OPERATION_FAILED.value},
                    exc_info=(type(exc), exc, exc.__traceback__),
                    catch_base=True,
                )
            raise
        finally:
            with self._driver_state_lock:
                self._operation_active = False
                self._operation_owner = None

    def _observe_operation_result(self, result: ExecutionResult) -> ExecutionResult:
        """Accumulate business outcome independently from log persistence."""

        with self._driver_state_lock:
            if result.succeeded > 0:
                self._session_had_success = True
            if result.failed > 0:
                self._session_failed = True
            observed_ids = set(self._completion_observed_ids)
        # Terminal dataflow errors are summarized by ExecutionLogger when their
        # observation is attempted. Scheduler/operation failures with no
        # terminal observation still belong to the Driver session summary.
        unobserved_errors = [
            f"{dataflow_id}: {message}"
            for dataflow_id, message in sorted(result.errors.items())
            if dataflow_id not in observed_ids
        ]
        if unobserved_errors:
            self._record_session_failure(message="; ".join(unobserved_errors))
        return result

    @contextmanager
    def _completion_metadata_scope(self, dataflows: List[DataFlow]):
        """Expose declarative snapshots to completion callbacks temporarily."""

        with self._driver_state_lock:
            previous = self._completion_metadata_by_id
            self._completion_observed_ids = set()
            self._completion_metadata_by_id = {
                dataflow.dataflow_id: dataflow
                for dataflow in dataflows
                if dataflow.dataflow_id is not None
            }
        try:
            yield
        finally:
            with self._driver_state_lock:
                self._completion_metadata_by_id = previous

    @staticmethod
    def _log_operation_finished(
        operation_name: str,
        result: ExecutionResult,
    ) -> None:
        """Emit one summary anchor for an admitted public operation."""
        emit_safely(
            logger,
            logging.INFO,
            "Operation finished: %s, total=%d, succeeded=%d, failed=%d, "
            "skipped=%d, pending=%d, duration=%.3fs",
            operation_name,
            result.total,
            result.succeeded,
            result.failed,
            result.skipped,
            result.pending,
            result.duration_seconds,
            extra={"event_name": LogEvent.OPERATION_FINISHED.value},
            catch_base=True,
        )

    def _final_job_status(self) -> str:
        with self._driver_state_lock:
            if self._session_failed:
                return DataFlowStatus.FAILED.value
            if self._session_had_success:
                return DataFlowStatus.SUCCEEDED.value
            return DataFlowStatus.SKIPPED.value

    def _close_session_loggers(
        self,
        *,
        diagnostic_logger: Optional[logging.Logger] = None,
    ) -> Optional[BaseException]:
        """Close loggers accepted for this Driver session, best effort."""
        cleanup_error: Optional[BaseException] = None
        for logger_instance in self._session_loggers:
            try:
                logger_instance.close()
            except Exception as exc:
                self._safe_diagnostic_log(
                    diagnostic_logger,
                    "Logger cleanup failed: %s",
                    exc,
                )
            except BaseException as exc:
                if cleanup_error is None:
                    cleanup_error = exc
                self._safe_diagnostic_log(
                    diagnostic_logger,
                    "Logger cleanup interrupted: %s",
                    exc,
                )
        return cleanup_error

    def _prepare_execution_dataflow(
        self,
        dataflow: DataFlow,
        *,
        operation_type: str = ExecutionType.ETL.value,
    ) -> PreparedDataFlow:
        """Prepare one isolated DataFlow for an execution operation."""
        return prepare_execution_dataflow(
            dataflow,
            platform=self._engine.platform,
            resolve_connection_secrets=self._resolve_secrets_for_connection,
            sql_base_path=self._sql_base_path,
            artifact_base_path=self._artifact_base_path,
            operation_type=operation_type,
        )

    def _validate_execution_preparation(
        self,
        dataflow: DataFlow,
        *,
        operation_type: str,
    ) -> None:
        """Validate file preparation without resolving business secrets."""
        validate_preparation(
            dataflow,
            platform=self._engine.platform,
            sql_base_path=self._sql_base_path,
            artifact_base_path=self._artifact_base_path,
            operation_type=operation_type,
        )

    def _log_dataflow_result(
        self,
        metadata_dataflow: DataFlow,
        runtime: DataFlowRuntimeInfo,
    ) -> None:
        """Log the declarative snapshot together with runtime observations."""
        if self._execution_logger is None:
            return
        self._execution_logger.log(dataflow=metadata_dataflow, runtime_info=runtime)

    def _validate_watermark_storage(
        self,
        dataflow: DataFlow,
        *,
        operation_type: str,
        watermark_start: Optional[Dict[str, Any]],
        watermark_end: Optional[Dict[str, Any]],
        save_watermark: bool,
    ) -> None:
        """Preflight provider-owned watermark storage before business I/O.

        File-backed providers can validate their bound root without touching
        the watermark file.  Other providers keep ownership of their own
        validation and are intentionally not forced to implement a new API.
        """
        if operation_type == ExecutionType.MAINTENANCE.value:
            return
        if operation_type == ExecutionType.REPLAY.value:
            required = save_watermark
        else:
            requires_read = watermark_start is None and dataflow.source.has_watermark_state
            requires_write = save_watermark and (
                dataflow.source.has_watermark_state or watermark_end is not None
            )
            required = requires_read or requires_write
        if not required:
            return

        if self._watermark_manager is None:
            raise ConfigurationError(
                f"{operation_type} execution requires a watermark_manager for "
                "the configured watermark read/save operation"
            )

        self._watermark_manager.validate_ready()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def job_id(self) -> str:
        return self._config.job_id

    @property
    def config(self) -> DataCoolieRunConfig:
        return self._config.model_copy(deep=True)

    # ------------------------------------------------------------------
    # Dataflow loading
    # ------------------------------------------------------------------

    def load_dataflows(
        self,
        stage: Optional[Union[str, List[str]]] = None,
        active_only: bool = True,
        attach_schema_hints: bool = True,
    ) -> List[DataFlow]:
        """Load and filter dataflows for this Driver session."""
        with self._driver_operation():
            return self._load_dataflows(
                stage=stage,
                active_only=active_only,
                attach_schema_hints=attach_schema_hints,
            )

    def _load_dataflows(
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
        emit_safely(
            logger,
            logging.INFO,
            "Loading dataflows — stage: %s, job: %d/%d",
            stage,
            self._config.job_index + 1,
            self._config.job_num,
            catch_base=True,
        )

        if self._metadata_provider is None:
            raise ConfigurationError(
                "metadata_provider is required to load dataflows; pass one or use create_driver "
                "with metadata_base_path/artifact_base_path"
            )

        all_dataflows = self._metadata_provider.get_dataflows(
            stage=stage,
            active_only=active_only,
            attach_schema_hints=attach_schema_hints,
        )

        self._dataflows = self._distributor.filter_dataflows(
            all_dataflows, active_only=active_only
        )

        emit_safely(
            logger,
            logging.INFO,
            "Loaded %d dataflows for this job (total: %d)",
            len(self._dataflows),
            len(all_dataflows),
            catch_base=True,
        )
        return self._dataflows

    def load_maintenance_dataflows(
        self,
        connection: Optional[Union[str, List[str]]] = None,
        active_only: bool = True,
    ) -> List[DataFlow]:
        """Load unique-destination dataflows eligible for maintenance."""
        with self._driver_operation():
            return self._load_maintenance_dataflows(
                connection=connection,
                active_only=active_only,
            )

    def _load_maintenance_dataflows(
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
        emit_safely(
            logger,
            logging.INFO,
            "Loading maintenance dataflows — connection: %s, job: %d/%d",
            connection,
            self._config.job_index + 1,
            self._config.job_num,
            catch_base=True,
        )

        if self._metadata_provider is None:
            raise ConfigurationError(
                "metadata_provider is required to load maintenance dataflows"
            )

        all_dataflows = self._metadata_provider.get_maintenance_dataflows(
            connection=connection,
            active_only=active_only,
        )

        unique = dedupe_by_destination(all_dataflows)

        self._dataflows = self._distributor.filter_dataflows(
            unique, active_only=active_only
        )

        emit_safely(
            logger,
            logging.INFO,
            "Loaded %d maintenance dataflows for this job (unique: %d, total: %d)",
            len(self._dataflows),
            len(unique),
            len(all_dataflows),
            catch_base=True,
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
        """Execute one admitted ETL operation."""
        with self._driver_operation("run_dataflow"):
            result = self._run_dataflow(
                stage=stage,
                dataflows=dataflows,
                column_name_mode=column_name_mode,
            )
            result = self._observe_operation_result(result)
            self._log_operation_finished("run_dataflow", result)
            return result

    def _run_dataflow(
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
        resolved_column_name_mode = ColumnCaseMode(column_name_mode)
        if dataflows is None:
            target = self._load_dataflows(stage=stage)
        else:
            target = dataflows

        if not target:
            emit_safely(logger, logging.INFO, "No dataflows to process", catch_base=True)
            return ExecutionResult()

        if self._config.dry_run:
            return self._dry_run_dataflows(target, operation_type=ExecutionType.ETL.value)

        groups = self._distributor.group_dataflows(target)
        process_fn = self._process_dataflow
        if resolved_column_name_mode != ColumnCaseMode.LOWER:
            process_fn = functools.partial(
                self._process_dataflow,
                column_name_mode=resolved_column_name_mode,
            )
        with self._completion_metadata_scope(target):
            result = self._executor.execute_with_groups(
                groups=groups,
                process_fn=process_fn,
                callback=self._record_dataflow_complete,
                operation_type=ExecutionType.ETL.value,
            )

        emit_safely(
            logger,
            logging.DEBUG,
            "ETL complete — Total: %d, Succeeded: %d, Failed: %d, Skipped: %d, Running: %d, Pending: %d (%.1fs)",
            result.total,
            result.succeeded,
            result.failed,
            result.skipped,
            result.running,
            result.pending,
            result.duration_seconds,
            catch_base=True,
        )
        return result

    def _dry_run_dataflows(
        self,
        dataflows: List[DataFlow],
        *,
        operation_type: str,
        replay: Optional[ReplayConfig] = None,
    ) -> ExecutionResult:
        """Validate selected operations without business or state I/O."""
        started = utc_now()
        result = ExecutionResult(total=len(dataflows))
        emit_safely(
            logger,
            logging.INFO,
            "Dry-run mode — validating %d dataflows",
            len(dataflows),
            catch_base=True,
        )

        for dataflow in dataflows:
            runtime = run_dry_run_execution(
                dataflow,
                operation_type=operation_type,
                validate=functools.partial(
                    self._validate_dry_run_dataflow,
                    operation_type=operation_type,
                    replay=replay,
                ),
                log_result=self._log_dataflow_result,
            )
            if runtime.status == DataFlowStatus.SKIPPED.value:
                result.skipped += 1
            elif runtime.status == DataFlowStatus.FAILED.value:
                result.failed += 1
                if dataflow.dataflow_id:
                    result.errors[dataflow.dataflow_id] = (
                        runtime.message or "Dry-run validation failed"
                    )

        result.pending = result.total - result.succeeded - result.failed - result.skipped
        result.duration_seconds = (utc_now() - started).total_seconds()
        return result

    def _validate_dry_run_dataflow(
        self,
        dataflow: DataFlow,
        *,
        operation_type: str,
        replay: Optional[ReplayConfig],
    ) -> None:
        """Validate one operation-specific dry-run copy without business I/O."""
        if operation_type == ExecutionType.REPLAY.value and replay is not None:
            if replay.save_watermark and self._watermark_manager is None:
                raise ConfigurationError(
                    "Replay save_watermark=True requires a watermark_manager"
                )
            if replay.chunk_column is None and not dataflow.source.watermark_columns:
                raise DataCoolieError(
                    f"Cannot auto-resolve chunk_column: dataflow {dataflow.dataflow_id!r} "
                    "has no watermark_columns. Set replay.chunk_column explicitly."
                )
            resolved_chunk_column = replay.chunk_column or dataflow.source.watermark_columns[0]
            validate_replay_chunk_column(dataflow, resolved_chunk_column)
            if resolved_chunk_column == DATE_FOLDER_PARTITION_KEY:
                raise DataCoolieError(
                    "Date-folder partition is an internal discovery key and cannot "
                    "be used as replay.chunk_column"
                )
            # Validate range/interval arithmetic without consulting the
            # watermark store or constructing a reader.
            normalized_start, normalized_end, _ = normalize_chunk_range(
                replay.start, replay.end
            )
            if replay.chunk_interval is not None:
                generate_chunk_boundaries(
                    start=normalized_start,
                    end=normalized_end,
                    interval=replay.chunk_interval,
                )

        self._validate_execution_preparation(
            dataflow,
            operation_type=operation_type,
        )
        self._validate_watermark_storage(
            dataflow,
            operation_type=operation_type,
            watermark_start=None,
            watermark_end=None,
            save_watermark=(replay.save_watermark if replay is not None else True),
        )
        emit_safely(
            logger,
            logging.INFO,
            "Dry-run validated [%s] %s → %s",
            dataflow.dataflow_id,
            dataflow.source.full_table_name or dataflow.source.path,
            dataflow.destination.full_table_name or dataflow.destination.path,
            catch_base=True,
        )

    def _process_dataflow(
        self,
        dataflow: DataFlow,
        *,
        column_name_mode: ColumnCaseMode = ColumnCaseMode.LOWER,
    ) -> DataFlowRuntimeInfo:
        """Process a single dataflow with retry logic.

        Runtime timing begins when this dataflow enters processing, before
        metadata preparation. Preparation remains outside retryable attempts.
        """
        return run_dataflow_execution(
            dataflow,
            operation_type=ExecutionType.ETL.value,
            prepare_execution_dataflow=self._prepare_execution_dataflow,
            retry_handler=self._retry_handler,
            preflight=functools.partial(
                self._validate_watermark_storage,
                operation_type=ExecutionType.ETL.value,
                watermark_start=None,
                watermark_end=None,
                save_watermark=True,
            ),
            attempt_runner=functools.partial(
                self._execute_etl_pipeline,
                column_name_mode=column_name_mode,
            ),
            log_result=self._log_dataflow_result,
            attempt_kwargs={
                "watermark_start": None,
                "watermark_end": None,
                "save_watermark": True,
                # Omitted operators are resolved by the source reader.  This
                # keeps persisted watermark-kind semantics at the source
                # boundary instead of hard-coding them in Driver.
                "watermark_start_operator": None,
                "watermark_end_operator": None,
            },
        )

    def _run_single_pipeline(
        self,
        prepared_dataflow: PreparedDataFlow,
        *,
        column_name_mode: ColumnCaseMode = ColumnCaseMode.LOWER,
        watermark_start: Optional[Dict[str, Any]] = None,
        watermark_end: Optional[Dict[str, Any]] = None,
        save_watermark: bool = True,
        watermark_start_operator: Optional[str] = None,
        watermark_end_operator: Optional[str] = None,
        read_range: Optional[SourceReadRange] = None,
        operation_type: str = ExecutionType.ETL.value,
    ) -> DataFlowRuntimeInfo:
        """Execute one pipeline run with timing, retry, context, and logging.

        This adapter is used for already-prepared replay chunks. Normal ETL
        and maintenance enter through :func:`run_dataflow_execution` so their
        runtime includes preparation.

        Preparation happens before the retry handler.  The prepared baseline
        is never mutated; each attempt receives a fresh deep copy.

        Args:
            prepared_dataflow: Internal pair containing the declarative
                logging snapshot and isolated execution copy.
            watermark_start: Override the read watermark (chunk lower bound).
                ``None`` = use the stored watermark (normal ETL).
            watermark_end: Upper watermark bound for the source reader AND
                the value saved after a successful write.
                ``None`` = no ceiling (normal ETL) / auto-save reader-detected.
            save_watermark: Whether to persist a watermark at all.
                ``False`` leaves the stored watermark untouched (backfill mode).
            watermark_start_operator: Comparison operator for the lower-bound
                WHERE clause.  ``">"`` (default, normal ETL) or ``">="``
                (replay, inclusive lower bound).
            watermark_end_operator: Comparison operator for the upper-bound
                filter.  ``"<"`` (default, exclusive) or ``"<="`` (inclusive).

        Returns:
            :class:`DataFlowRuntimeInfo` with timing, status, and metrics.
        """
        return run_prepared_execution(
            prepared_dataflow,
            operation_type=operation_type,
            retry_handler=self._retry_handler,
            preflight=functools.partial(
                self._validate_watermark_storage,
                operation_type=operation_type,
                watermark_start=watermark_start,
                watermark_end=watermark_end,
                save_watermark=save_watermark,
            ),
            attempt_runner=functools.partial(
                self._execute_etl_pipeline,
                column_name_mode=column_name_mode,
            ),
            log_result=self._log_dataflow_result,
            attempt_kwargs={
                "watermark_start": watermark_start,
                "watermark_end": watermark_end,
                "save_watermark": save_watermark,
                "watermark_start_operator": watermark_start_operator,
                "watermark_end_operator": watermark_end_operator,
                "read_range": read_range,
            },
        )

    def _execute_etl_pipeline(
        self,
        dataflow: DataFlow,
        dataflow_run_id: str,
        *,
        column_name_mode: ColumnCaseMode = ColumnCaseMode.LOWER,
        watermark_start: Optional[Dict[str, Any]] = None,
        watermark_end: Optional[Dict[str, Any]] = None,
        save_watermark: bool = True,
        watermark_start_operator: Optional[str] = None,
        watermark_end_operator: Optional[str] = None,
        read_range: Optional[SourceReadRange] = None,
    ) -> PipelineAttemptResult:
        """Run read → transform → write for *dataflow*.

        Called by :meth:`RetryHandler.execute`; any exception triggers
        automatic retry with exponential backoff.

        Args:
            dataflow: The dataflow to execute (already deep-copied by caller).
            dataflow_run_id: Unique ID for this execution.
            watermark_start: Override the read watermark (chunk lower bound).
                ``None`` = use the stored watermark (normal ETL).
            watermark_end: Upper watermark bound for the reader AND value
                saved after a successful write.
                ``None`` = no ceiling / auto-save reader-detected.
            save_watermark: Whether to persist a watermark at all.
                ``False`` leaves the stored watermark untouched (backfill mode).
            watermark_start_operator: Comparison operator for the lower-bound
                filter.  ``">"`` (default) or ``">="`` (replay, inclusive).
            watermark_end_operator: Comparison operator for the upper bound
                filter.  ``"<"`` (default) or ``"<="`` (inclusive).

        Returns:
            Terminal phase runtimes and status for this attempt.
        """
        return execute_etl_pipeline(
            dataflow,
            dataflow_run_id,
            column_name_mode=column_name_mode,
            watermark_start=watermark_start,
            watermark_end=watermark_end,
            save_watermark=save_watermark,
            watermark_start_operator=watermark_start_operator,
            watermark_end_operator=watermark_end_operator,
            read_range=read_range,
            watermark_manager=self._watermark_manager,
            job_id=self._config.job_id,
            create_source_reader=self._create_source_reader,
            create_transformer_pipeline=self._create_transformer_pipeline,
            create_destination_writer=self._create_destination_writer,
            validate_watermark_storage=self._validate_watermark_storage,
        )

    # ------------------------------------------------------------------
    # Replay / backfill
    # ------------------------------------------------------------------

    def run_replay(
        self,
        dataflows: Union[DataFlow, List[DataFlow]],
        replay: ReplayConfig,
        column_name_mode: Union[ColumnCaseMode, str] = ColumnCaseMode.LOWER,
    ) -> ExecutionResult:
        """Execute one admitted replay operation."""
        with self._driver_operation("run_replay"):
            result = self._run_replay(
                dataflows=dataflows,
                replay=replay,
                column_name_mode=column_name_mode,
            )
            result = self._observe_operation_result(result)
            self._log_operation_finished("run_replay", result)
            return result

    def _run_replay(
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
        is set explicitly. A source-supported independent bounded-read column
        may be selected explicitly; API sources require a matching
        ``range_param_mapping`` binding.

        Args:
            dataflows: One or more dataflows to replay.
            replay: Replay configuration (range, chunking, watermark policy).
            column_name_mode: Column name case-conversion mode.

        Returns:
            :class:`ExecutionResult` whose counters represent outer
            dataflows. Each dataflow aggregates its sequential chunk results.
        """
        emit_safely(logger, logging.DEBUG, "Starting replay run", catch_base=True)

        resolved_column_name_mode = ColumnCaseMode(column_name_mode)

        target: List[DataFlow] = dataflows if isinstance(dataflows, list) else [dataflows]
        if not target:
            return ExecutionResult()

        normalized_start, normalized_end, _ = normalize_chunk_range(
            replay.start, replay.end
        )
        if replay.chunk_interval is not None:
            generate_chunk_boundaries(
                start=normalized_start,
                end=normalized_end,
                interval=replay.chunk_interval,
            )

        if self._config.dry_run:
            return self._dry_run_dataflows(
                target,
                operation_type=ExecutionType.REPLAY.value,
                replay=replay,
            )

        # Pre-validate chunk_column resolution and source-owned bounded-read
        # capability for all active dataflows before executing any chunk.
        for df in target:
            if inactive_reason(df) is not None:
                continue
            resolved_chunk_column = replay.chunk_column
            if resolved_chunk_column is None:
                if not df.source.watermark_columns:
                    raise DataCoolieError(
                        f"Cannot auto-resolve chunk_column: dataflow {df.dataflow_id!r} "
                        f"has no watermark_columns. Set replay.chunk_column explicitly."
                    )
                resolved_chunk_column = df.source.watermark_columns[0]
            validate_replay_chunk_column(df, resolved_chunk_column)

        replay_process_fn = functools.partial(self._process_replay, replay=replay)
        if resolved_column_name_mode != ColumnCaseMode.LOWER:
            replay_process_fn = functools.partial(
                self._process_replay,
                replay=replay,
                column_name_mode=resolved_column_name_mode,
            )
        with self._completion_metadata_scope(target):
            result = self._executor.execute(
                dataflows=target,
                process_fn=replay_process_fn,
                callback=self._record_replay_complete,
                operation_type=ExecutionType.REPLAY.value,
            )

        emit_safely(
            logger,
            logging.DEBUG,
            "Replay complete — Total: %d, Succeeded: %d, Failed: %d, Skipped: %d, Running: %d, Pending: %d (%.1fs)",
            result.total,
            result.succeeded,
            result.failed,
            result.skipped,
            result.running,
            result.pending,
            result.duration_seconds,
            catch_base=True,
        )
        return result

    def _process_replay(
        self,
        dataflow: DataFlow,
        replay: ReplayConfig,
        *,
        column_name_mode: ColumnCaseMode = ColumnCaseMode.LOWER,
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
        return process_replay(
            dataflow,
            replay,
            column_name_mode=column_name_mode,
            prepare_execution_dataflow=self._prepare_execution_dataflow,
            validate_watermark_storage=self._validate_watermark_storage,
            watermark_manager=self._watermark_manager,
            run_single_pipeline=self._run_single_pipeline,
            log_result=self._log_dataflow_result,
            on_chunk_complete=self._record_replay_chunk_complete,
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
        """Execute one admitted maintenance operation."""
        with self._driver_operation("run_maintenance"):
            result = self._run_maintenance(
                connection=connection,
                dataflows=dataflows,
                do_compact=do_compact,
                do_cleanup=do_cleanup,
            )
            result = self._observe_operation_result(result)
            self._log_operation_finished("run_maintenance", result)
            return result

    def _run_maintenance(
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
        emit_safely(logger, logging.DEBUG, "Starting maintenance run", catch_base=True)

        if dataflows is not None:
            target = dedupe_by_destination(dataflows)
        else:
            if self._metadata_provider is None:
                raise ConfigurationError(
                    "metadata_provider is required to load maintenance dataflows; pass one "
                    "or provide an explicit dataflows list"
                )
            target = self._load_maintenance_dataflows(connection=connection)

        if not target:
            emit_safely(
                logger,
                logging.INFO,
                "No lakehouse dataflows for maintenance",
                catch_base=True,
            )
            return ExecutionResult()

        if self._config.dry_run:
            return self._dry_run_dataflows(
                target,
                operation_type=ExecutionType.MAINTENANCE.value,
            )

        with self._completion_metadata_scope(target):
            result = self._executor.execute(
                dataflows=target,
                process_fn=functools.partial(
                    self._process_maintenance,
                    do_compact=do_compact,
                    do_cleanup=do_cleanup,
                ),
                callback=self._record_maintenance_complete,
                operation_type=ExecutionType.MAINTENANCE.value,
            )

        emit_safely(
            logger,
            logging.DEBUG,
            "Maintenance complete — Total: %d, Succeeded: %d, Failed: %d, Skipped: %d, Running: %d, Pending: %d (%.1fs)",
            result.total,
            result.succeeded,
            result.failed,
            result.skipped,
            result.running,
            result.pending,
            result.duration_seconds,
            catch_base=True,
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
        return run_dataflow_execution(
            dataflow,
            operation_type=ExecutionType.MAINTENANCE.value,
            prepare_execution_dataflow=self._prepare_execution_dataflow,
            retry_handler=self._retry_handler,
            preflight=lambda _execution: None,
            attempt_runner=self._execute_maintenance_pipeline,
            log_result=self._log_dataflow_result,
            attempt_kwargs={
                "do_compact": do_compact,
                "do_cleanup": do_cleanup,
            },
            include_dataflow_run_id=False,
        )

    def _execute_maintenance_pipeline(
        self,
        dataflow: DataFlow,
        do_compact: bool = True,
        do_cleanup: bool = True,
    ) -> PipelineAttemptResult:
        """Run optimize + vacuum for *dataflow*.

        Called by :meth:`RetryHandler.execute`; any exception triggers
        automatic retry with exponential backoff.

        Args:
            dataflow: The dataflow to maintain.
            do_compact: Run the compaction (optimize) step.
            do_cleanup: Run the cleanup (vacuum) step.

        Returns:
            Terminal destination runtime and status for this attempt.
        """
        return execute_maintenance_pipeline(
            dataflow,
            create_destination_writer=self._create_destination_writer,
            retention_hours=self._config.retention_hours,
            do_compact=do_compact,
            do_cleanup=do_cleanup,
        )

    # ------------------------------------------------------------------
    # Secret resolution
    # ------------------------------------------------------------------

    def _resolve_secrets_for_connection(self, connection: Connection) -> None:
        """Resolve secret references for one runtime connection."""
        from datacoolie import resolver_registry

        def _lookup(prefix: str) -> BaseSecretResolver | None:
            if resolver_registry.is_available(prefix):
                return resolver_registry.get_or_create(prefix)
            return None

        resolve_secrets(
            connection,
            self._secret_provider,
            resolver_lookup=_lookup,
        )

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    def _create_source_reader(self, fmt: str) -> BaseSourceReader:
        """Create a registered source reader through the pipeline boundary."""
        return build_source_reader(
            self._engine,
            fmt,
            allowed_prefixes=self._config.allowed_function_prefixes,
        )

    def _create_transformer_pipeline(
        self,
        dataflow_run_id: Optional[str] = None,
        column_name_mode: ColumnCaseMode = ColumnCaseMode.LOWER,
    ) -> TransformerPipeline:
        """Create the default registered transformer pipeline."""
        return build_transformer_pipeline(
            self._engine,
            dataflow_run_id=dataflow_run_id,
            column_name_mode=column_name_mode,
        )

    def _create_destination_writer(self, fmt: str) -> BaseDestinationWriter:
        """Create a registered destination writer through the pipeline boundary."""
        return build_destination_writer(self._engine, fmt)

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    def _on_dataflow_complete(self, result: DataFlowRuntimeInfo) -> None:
        """Hook for ETL completion notifications.

        Mandatory execution observation is owned by
        :meth:`_record_dataflow_complete` and therefore cannot be bypassed by
        a subclass override.  Subclasses may override this hook for
        monitoring/alerting and may safely call ``super()``.
        """
        pass

    def _on_maintenance_complete(self, result: DataFlowRuntimeInfo) -> None:
        """Hook for maintenance completion notifications."""
        pass

    def _record_dataflow_complete(self, result: DataFlowRuntimeInfo) -> None:
        """Persist the terminal ETL observation before notifying user hooks."""
        try:
            self._log_missing_execution_observation(result)
        except Exception as exc:
            # Completion callbacks are observational.  A diagnostic or sink
            # failure must not suppress the user hook or change executor
            # counters; the sink boundary already owns its own best effort.
            self._safe_diagnostic_log(
                None,
                "ETL completion observation failed: %s",
                exc,
            )
        self._notify_completion_hook(
            self._on_dataflow_complete,
            result,
            operation_name="ETL",
        )

    def _record_maintenance_complete(self, result: DataFlowRuntimeInfo) -> None:
        """Persist the terminal maintenance observation before its hook."""
        try:
            self._log_missing_execution_observation(result)
        except Exception as exc:
            self._safe_diagnostic_log(
                None,
                "Maintenance completion observation failed: %s",
                exc,
            )
        self._notify_completion_hook(
            self._on_maintenance_complete,
            result,
            operation_name="maintenance",
        )

    def _notify_completion_hook(
        self,
        hook: Any,
        result: DataFlowRuntimeInfo,
        *,
        operation_name: str,
    ) -> None:
        """Invoke an observational completion hook without changing status."""
        try:
            hook(result)
        except Exception as exc:
            emit_safely(
                logger,
                logging.WARNING,
                "%s completion hook failed",
                operation_name,
                extra={
                    "dataflow_id": result.dataflow_id,
                    "dataflow_run_id": result.dataflow_run_id,
                },
                exc_info=(type(exc), exc, exc.__traceback__),
                catch_base=True,
            )

    def _on_replay_chunk_complete(self, result: DataFlowRuntimeInfo) -> None:
        """Hook for one replay chunk; errors are isolated by the executor."""
        pass

    def _record_replay_chunk_complete(self, result: DataFlowRuntimeInfo) -> None:
        """Persist a missing replay-chunk observation, then notify the hook."""

        try:
            self._log_missing_execution_observation(result)
        except Exception as exc:
            self._safe_diagnostic_log(
                None,
                "Replay completion observation failed: %s",
                exc,
            )
        self._notify_completion_hook(
            self._on_replay_chunk_complete,
            result,
            operation_name="replay chunk",
        )

    def _on_replay_complete(self, result: DataFlowRuntimeInfo) -> None:
        """Hook for the aggregate result of one replay dataflow.

        Per-chunk notifications use :meth:`_on_replay_chunk_complete`. Override
        this hook for dataflow-level monitoring or alerting.
        """
        pass

    def _record_replay_complete(self, result: DataFlowRuntimeInfo) -> None:
        """Notify the replay aggregate hook without affecting the result."""
        self._notify_completion_hook(
            self._on_replay_complete,
            result,
            operation_name="replay",
        )

    def _log_missing_execution_observation(
        self,
        result: DataFlowRuntimeInfo,
    ) -> None:
        """Log one scheduler fallback without adding a second normal row."""

        if execution_observation_attempted(result):
            self._mark_completion_observed(result.dataflow_id)
            return
        with self._driver_state_lock:
            metadata = self._completion_metadata_by_id.get(result.dataflow_id)
        if metadata is None:
            # A result without a declarative snapshot is an orchestration
            # contract failure.  It cannot be safely projected into an
            # execution row, so retain the error only in system diagnostics
            # and the Driver's final JobRuntime explanation.
            message = (
                "Execution result has no metadata snapshot: "
                f"dataflow_id={result.dataflow_id!r}"
            )
            self._record_session_failure(message=message)
            emit_safely(
                logger,
                logging.ERROR,
                message,
                extra={"event_name": LogEvent.SCHEDULER_EXECUTION_FAILED.value},
                catch_base=True,
            )
            return
        log_result_safely(self._log_dataflow_result, metadata, result)
        self._mark_completion_observed(result.dataflow_id)

    def _mark_completion_observed(self, dataflow_id: Optional[str]) -> None:
        """Mark one terminal observation from a possibly parallel callback."""

        if self._execution_logger is None or not dataflow_id:
            return
        with self._driver_state_lock:
            self._completion_observed_ids.add(dataflow_id)

    # ------------------------------------------------------------------
    # Logging / cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close driver and its accepted session loggers once."""
        primary_exception = sys.exception()
        with self._driver_state_lock:
            if self._closed or self._closing:
                return
            if self._operation_active:
                raise RuntimeError("Cannot close Driver while an operation is active")
            self._closing = True

        teardown_errors: List[BaseException] = []

        def remember_teardown_error(error: Optional[BaseException]) -> None:
            if error is None:
                return
            if not teardown_errors:
                teardown_errors.append(error)
            self._record_session_failure(error)

        try:
            # Driver-owned providers are part of the business session and must
            # be released before the final JobRuntime status is committed.
            remember_teardown_error(
                self._close_owned_metadata_provider(diagnostic_logger=logger)
            )

            # The finishing anchor observes the status after owned cleanup.
            # Ordinary handler errors remain logging health issues; an
            # interruption is a teardown failure when no primary is active.
            finishing_error = emit_safely(
                logger,
                logging.INFO,
                "DataCoolie session finishing: job_id=%s, status=%s",
                self._config.job_id,
                self._final_job_status(),
                extra={"event_name": LogEvent.SESSION_FINISHING.value},
                catch_base=True,
            )
            if isinstance(finishing_error, (KeyboardInterrupt, SystemExit)):
                remember_teardown_error(finishing_error)
            elif finishing_error is not None:
                self._safe_diagnostic_log(
                    _diagnostic_logger,
                    "Session finishing diagnostic failed: %s",
                    finishing_error,
                    exc_info=(
                        type(finishing_error),
                        finishing_error,
                        finishing_error.__traceback__,
                    ),
                )

            # Finalize the business summary once all owned non-logger
            # components have reported.  Logger persistence failures remain
            # fail-open, while contract errors and interruptions are surfaced
            # after every accepted logger has had a close attempt.
            if self._execution_logger is not None:
                try:
                    self._execution_logger.finish_job(
                        self._final_job_status(),
                        message=self._session_error_message,
                    )
                except ConfigurationError as exc:
                    remember_teardown_error(exc)
                except Exception as exc:
                    self._safe_diagnostic_log(
                        _diagnostic_logger,
                        "Job-runtime finalization failed: %s",
                        exc,
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )
                except BaseException as exc:
                    remember_teardown_error(exc)

            # Keep SystemLogger last so diagnostics from the earlier cleanup
            # attempts can still be captured.  Ordinary logger/storage errors
            # remain best effort; an interruption is retained for precedence.
            remember_teardown_error(
                self._close_session_loggers(diagnostic_logger=logger)
            )
            self._dataflows = []
        finally:
            with self._driver_state_lock:
                self._closed = True
                self._closing = False
        if primary_exception is None and teardown_errors:
            raise teardown_errors[0]

    def __enter__(self) -> "DataCoolieDriver":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None:
            self._record_session_failure(
                exc_val,
                error_type=exc_type,
            )
        try:
            self.close()
        except BaseException as cleanup_exc:
            # A cleanup/finalization problem must not mask the active business
            # exception raised inside the context manager.
            if exc_type is None:
                raise
            self._safe_diagnostic_log(
                None,
                "Driver cleanup failed while preserving active exception: %s",
                cleanup_exc,
            )

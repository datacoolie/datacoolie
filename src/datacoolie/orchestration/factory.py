"""Convenience factory for constructing a configured DataCoolie Driver."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Optional

from datacoolie.core.constants import DEFAULT_MAX_WORKERS
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.core.secrets.provider import BaseSecretProvider
from datacoolie.engines.base import BaseEngine
from datacoolie.logging import ExecutionLogger, LogConfig, SystemLogger
from datacoolie.metadata.base import BaseMetadataProvider
from datacoolie.platforms.base import BasePlatform
from datacoolie.utils.identity import generate_unique_id
from datacoolie.watermark.base import BaseWatermarkManager

from datacoolie.orchestration.driver import DataCoolieDriver


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
    execution_logger: Optional[ExecutionLogger] = None,
    artifact_base_path: Optional[str] = None,
    state_base_path: Optional[str] = None,
    metadata_base_path: Optional[str] = None,
    sql_base_path: str | Sequence[str] | None = None,
    log_base_path: Optional[str] = None,
    log_config: Optional[LogConfig] = None,
    **kwargs: Any,
) -> DataCoolieDriver:
    """Create a configured, provider-ready :class:`DataCoolieDriver`.

    Metadata and artifact roots are forwarded unchanged to Driver, which owns
    provider inference, conflict validation, and cleanup for both construction
    entry points.  ``sql_base_path`` remains the Driver session fallback;
    provider-declared SQL roots are preferred when present and must agree with
    an explicitly supplied Driver value.
    """
    if "base_log_path" in kwargs:
        raise TypeError(
            "create_driver() got an unexpected keyword argument 'base_log_path'; "
            "use 'log_base_path'"
        )
    allowed_config_fields = set(DataCoolieRunConfig.model_fields)
    unknown_config_fields = sorted(set(kwargs).difference(allowed_config_fields))
    if unknown_config_fields:
        names = ", ".join(unknown_config_fields)
        raise TypeError(
            f"create_driver() got unexpected keyword argument(s): {names}"
        )
    config = DataCoolieRunConfig(
        job_id=job_id if job_id is not None else generate_unique_id(),
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
        execution_logger=execution_logger,
        artifact_base_path=artifact_base_path,
        state_base_path=state_base_path,
        metadata_base_path=metadata_base_path,
        sql_base_path=sql_base_path,
        log_base_path=log_base_path,
        log_config=log_config,
    )


__all__ = ["create_driver"]

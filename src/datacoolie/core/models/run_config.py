"""Replay and run configuration models."""

from __future__ import annotations

import copy
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from datacoolie.core.constants import (
    DEFAULT_MAX_WORKERS,
    DEFAULT_RETRY_COUNT,
    DEFAULT_RETRY_DELAY,
    DEFAULT_RETENTION_HOURS,
)
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.utils.identity import generate_unique_id
from datacoolie.core.models.base import CompatModel


def _validate_json_value(value: Any, *, field_path: str, seen: set[int]) -> None:
    """Validate one JSON value without coercing unsupported objects.

    ``run_attributes`` is an external correlation payload.  Silently turning
    arbitrary objects into strings would make the persisted contract depend on
    object ``repr`` implementations, so validation is deliberately strict.
    """

    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ConfigurationError(
                "run_attributes must contain finite JSON numbers",
                details={"field": field_path},
            )
        return
    value_id = id(value)
    if value_id in seen:
        raise ConfigurationError(
            "run_attributes must not contain cyclic references",
            details={"field": field_path},
        )
    if isinstance(value, Mapping):
        seen.add(value_id)
        try:
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ConfigurationError(
                        "run_attributes object keys must be strings",
                        details={"field": field_path},
                    )
                _validate_json_value(
                    item,
                    field_path=f"{field_path}.{key}",
                    seen=seen,
                )
        finally:
            seen.remove(value_id)
        return
    if isinstance(value, (list, tuple)):
        seen.add(value_id)
        try:
            for index, item in enumerate(value):
                _validate_json_value(
                    item,
                    field_path=f"{field_path}[{index}]",
                    seen=seen,
                )
        finally:
            seen.remove(value_id)
        return
    raise ConfigurationError(
        "run_attributes must contain only JSON-compatible values",
        details={"field": field_path, "type": type(value).__name__},
    )


def validate_json_object(value: Any, *, field_name: str = "run_attributes") -> None:
    """Validate an optional JSON object used as caller-owned context."""

    if value is None:
        return
    if not isinstance(value, Mapping):
        raise ConfigurationError(
            f"{field_name} must be a JSON object or None",
            details={"field": field_name},
        )
    _validate_json_value(value, field_path=field_name, seen=set())


@dataclass
class ReplayConfig:
    """Configuration for replaying a bounded time range in chunks.

    Used by :meth:`DataCoolieDriver.run_replay` to reprocess historical
    data without corrupting the production watermark.

    The range uses the left-closed, right-open ``[start, end)`` convention:
    *start* is **inclusive**, *end* is **exclusive**.  This aligns chunks
    to whole calendar units (days, weeks, months, etc.) and is the
    industry-standard interval convention used by Python’s ``range()``,
    Spark partition pruning, and PostgreSQL range types.

    Example::

        # Replay all of Q1 2025 in monthly chunks:
        ReplayConfig(
            start="2025-01-01",  # inclusive
            end="2025-04-01",    # exclusive (first day NOT included)
            chunk_interval={"months": 1},
        )
        # Produces chunks: [Jan 1, Feb 1), [Feb 1, Mar 1), [Mar 1, Apr 1)

    The chunk column is auto-resolved from
    ``dataflow.source.watermark_columns[0]`` at runtime. Override with
    ``chunk_column`` for a source-supported independent bounded-read column
    or when the first watermark column is not the one to chunk on.

    Type detection is automatic:

    * ``str`` parseable to date/datetime → time-based chunking
    * ``datetime`` / ``date`` objects → time-based chunking
    * ``int`` → integer-based chunking

    Args:
        start: Inclusive lower bound of the replay range.
        end: Exclusive upper bound of the replay range.
        chunk_interval: Chunking interval.  Time-based keys (``months``,
            ``days``, ``hours``, ``minutes``, ``weeks``, ``years``) use
            ``relativedelta``; ``step`` key is for integer watermarks.
            ``None`` disables chunking (single-shot replay).
        save_watermark: When ``True``, persist the source-observed watermark
            after each successful chunk. Replay is always re-runnable: this
            flag does not create a checkpoint or skip chunks on a later run.
            When ``False``, the stored watermark is never touched.
        chunk_column: Override the auto-resolved chunk column. Use this for
            an independent bounded-read column when the selected source
            reader supports it, or when the first watermark column is not the
            desired chunking dimension. API readers require a matching
            ``range_param_mapping`` binding.
    """

    start: Any
    end: Any
    chunk_interval: Optional[Dict[str, int]] = None
    save_watermark: bool = False
    chunk_column: Optional[str] = None

    def __post_init__(self) -> None:
        if self.start is None:
            raise ConfigurationError("ReplayConfig.start must not be None")
        if self.end is None:
            raise ConfigurationError("ReplayConfig.end must not be None")
        if self.chunk_column is not None and (
            not isinstance(self.chunk_column, str) or not self.chunk_column.strip()
        ):
            raise ConfigurationError(
                "ReplayConfig.chunk_column must be a non-empty string or None"
            )
        if self.chunk_interval is not None and not isinstance(self.chunk_interval, dict):
            raise ConfigurationError(
                "ReplayConfig.chunk_interval must be a mapping or None"
            )


@dataclass(init=False)
class DataCoolieRunConfig(CompatModel):
    """Validated execution parameters for a DataCoolie run."""

    job_id: str = field(default_factory=generate_unique_id)
    job_num: int = 1
    job_index: int = 0
    max_workers: int = DEFAULT_MAX_WORKERS
    stop_on_error: bool = False
    retry_count: int = DEFAULT_RETRY_COUNT
    retry_delay: float = DEFAULT_RETRY_DELAY
    dry_run: bool = False
    retention_hours: int = DEFAULT_RETENTION_HOURS
    allowed_function_prefixes: List[str] = field(default_factory=list)
    run_attributes: Optional[Dict[str, Any]] = None

    def _validate_constraints(self) -> "DataCoolieRunConfig":
        if not self.job_id:
            raise ConfigurationError(
                "DataCoolieRunConfig.job_id must be a non-empty string"
            )
        if self.job_num < 1:
            raise ConfigurationError("DataCoolieRunConfig.job_num must be at least 1")
        if self.job_index < 0:
            raise ConfigurationError(
                "DataCoolieRunConfig.job_index must be non-negative"
            )
        if self.job_index >= self.job_num:
            raise ConfigurationError(
                f"DataCoolieRunConfig.job_index ({self.job_index}) must be less than job_num ({self.job_num})"
            )
        if self.max_workers < 1:
            raise ConfigurationError(
                "DataCoolieRunConfig.max_workers must be at least 1"
            )
        if self.retry_count < 0:
            raise ConfigurationError(
                "DataCoolieRunConfig.retry_count must be non-negative"
            )
        if self.retry_delay < 0:
            raise ConfigurationError(
                "DataCoolieRunConfig.retry_delay must be non-negative"
            )
        if self.retention_hours < 0:
            raise ConfigurationError(
                "DataCoolieRunConfig.retention_hours must be non-negative"
            )
        return self

    def __post_init__(self) -> None:
        validate_json_object(self.run_attributes)
        # Detach caller-owned mutable context before it reaches Driver or a
        # logger snapshot.  Validation above rejects cycles, so deepcopy is
        # deterministic and cannot silently stringify arbitrary objects.
        if self.run_attributes is not None:
            self.run_attributes = copy.deepcopy(dict(self.run_attributes))
        self._validate_constraints()

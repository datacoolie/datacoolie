"""Abstract base class for source readers.

``BaseSourceReader[DF]`` uses the **Template Method** pattern:

* Public :meth:`read` handles timing, watermark filtering,
  count/max calculation, file-info columns, and error wrapping.
* Subclasses implement :meth:`_read_internal` (and optionally
  :meth:`_read_data`) with format-specific logic.

The reader delegates all DataFrame operations to a :class:`BaseEngine`.
"""

from __future__ import annotations

import calendar
import copy
from abc import ABC, abstractmethod
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from numbers import Number
from typing import Any, Dict, Generic, List, Optional, Tuple

from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY, DataFlowStatus
from datacoolie.core.exceptions import ConfigurationError, SourceError
from datacoolie.core.models.source import Source
from datacoolie.sources.contracts import (
    SourceReadMode,
    SourceReadRange as _SourceReadRange,
    SourceReadRequest,
    SourceReadResult,
    SourceReadSelection,
    SourceReadUpperOrigin,
)
from datacoolie.sources.runtime import SourceRuntimeInfo
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.utils.time import utc_now
from datacoolie.utils.chunking import normalize_chunk_range
from datacoolie.watermark.base import merge_watermark_values
from datacoolie.watermark.contracts import WatermarkCandidate, WatermarkCandidateKind

logger = get_logger(__name__)


class BaseSourceReader(ABC, Generic[DF]):
    """Abstract source reader with Template Method lifecycle.

    Subclasses must implement :meth:`_read_internal`.  Optionally they
    may also implement :meth:`_read_data` for the raw read step.

    Type parameter *DF* is the concrete DataFrame class.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine
        self._candidate: Optional[WatermarkCandidate] = None
        self._source_action: Dict[str, Any] = {}
        self._watermark_start_operator: str = ">"
        self._watermark_end_operator: str = "<"
        self._read_range: Optional[_SourceReadRange] = None
        self._effective_watermark_start: Optional[Dict[str, Any]] = None
        self._active_source: Optional[Source] = None
        self._rows_read: Optional[int] = None
        self._selection = SourceReadSelection()
        self._runtime_start: Optional[datetime] = None
        self._active_reference_time: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read(
        self,
        source: Source,
        *,
        request: SourceReadRequest,
    ) -> SourceReadResult[DF]:
        """Read data and return the sole typed source contract."""
        self._active_source = source
        self._runtime_start = utc_now()
        self._candidate = None
        self._source_action = {}
        self._rows_read = None
        self._read_range = None
        self._effective_watermark_start = None
        self._selection = SourceReadSelection()
        try:
            if not isinstance(request, SourceReadRequest):
                raise SourceError("source read requires a SourceReadRequest")
            self._active_reference_time = request.reference_time
            read_range = request.read_range
            if read_range is not None and (
                request.watermark_start is not None
                or request.watermark_end is not None
                or request.watermark_start_operator is not None
                or request.watermark_end_operator is not None
            ):
                raise SourceError(
                    "read_range cannot be combined with explicit watermark bounds"
                )
            if read_range is not None and not self._supports_read_range():
                raise SourceError(
                    f"{type(self).__name__} does not support source-owned read_range"
                )
            if read_range is not None:
                try:
                    normalized_start, normalized_end, _ = normalize_chunk_range(
                        read_range.start, read_range.end
                    )
                except ConfigurationError as exc:
                    raise SourceError(str(exc)) from exc
                read_range = _SourceReadRange(
                    column=read_range.column,
                    start=normalized_start,
                    end=normalized_end,
                    lower_operator=read_range.lower_operator,
                    upper_operator=read_range.upper_operator,
                )
                self._validate_read_range(read_range)
                self.validate_watermark(
                    {read_range.column: read_range.start},
                    boundary="range lower bound",
                )
                self.validate_watermark(
                    {read_range.column: read_range.end},
                    boundary="range upper bound",
                )
            self._read_range = read_range
            if read_range is not None:
                self._watermark_start_operator = read_range.lower_operator
                self._watermark_end_operator = read_range.upper_operator
            else:
                self._watermark_start_operator, self._watermark_end_operator = (
                    self._resolve_watermark_operators(source, request)
                )
            self.validate_watermark(request.watermark_start, boundary="lower bound")
            self.validate_watermark(request.watermark_end, boundary="upper bound")
            self._effective_watermark_start = (
                None
                if read_range is not None
                else self._build_watermark_effective(
                    source,
                    request.watermark_start,
                    reference_time=request.reference_time,
                )
            )
            self._selection = SourceReadSelection(
                mode=(
                    SourceReadMode.RANGE
                    if read_range is not None
                    else (
                        SourceReadMode.INCREMENTAL
                        if request.watermark_start is not None
                        or request.watermark_end is not None
                        else SourceReadMode.FULL
                    )
                ),
                lower=copy.deepcopy(
                    {read_range.column: read_range.start}
                    if read_range is not None
                    else self._effective_watermark_start
                ),
                upper=copy.deepcopy(
                    {read_range.column: read_range.end}
                    if read_range is not None
                    else request.watermark_end
                ),
                lower_operator=self._watermark_start_operator,
                upper_operator=self._watermark_end_operator,
                upper_origin=(
                    SourceReadUpperOrigin.EXPLICIT
                    if read_range is not None or request.watermark_end is not None
                    else SourceReadUpperOrigin.NONE
                ),
            )
            frame = self._read_internal(source, request)
            if self._rows_read is None:
                raise SourceError(
                    f"{type(self).__name__} did not record rows_read for its read"
                )
            if frame is None and self._rows_read != 0:
                raise SourceError("A source read returning None must report rows_read=0")
            runtime = self._build_runtime(
                status=DataFlowStatus.SUCCEEDED.value,
                message=None,
                rows_read=self._rows_read,
            )
            return SourceReadResult(
                frame=frame,
                rows_read=self._rows_read,
                selection=copy.deepcopy(self._selection),
                candidate=copy.deepcopy(self._candidate),
                runtime=runtime,
            )
        except SourceError as exc:
            runtime = self._build_runtime(
                status=DataFlowStatus.FAILED.value,
                message=str(exc) or type(exc).__name__,
                rows_read=self._rows_read,
            )
            if getattr(exc, "runtime_info", None) is None:
                exc.runtime_info = runtime
            logger.debug("Source read failed: %s", exc)
            raise
        except Exception as exc:
            runtime = self._build_runtime(
                status=DataFlowStatus.FAILED.value,
                message=str(exc) or type(exc).__name__,
                rows_read=self._rows_read,
            )
            logger.debug("Source read failed: %s", exc)
            raise SourceError(
                f"Failed to read source: {exc}",
                details={
                    "source_table": source.full_table_name,
                    "source_path": source.path,
                },
                runtime_info=runtime,
            ) from exc
        finally:
            self._runtime_start = None
            self._active_reference_time = None
            self._read_range = None
            self._effective_watermark_start = None

    def _resolve_watermark_operators(
        self,
        source: Source,
        request: SourceReadRequest,
    ) -> Tuple[str, str]:
        """Resolve omitted operators at the source boundary."""

        del source
        resolved_start = (
            request.watermark_start_operator
            if request.watermark_start_operator is not None
            else ">"
        )
        resolved_end = (
            request.watermark_end_operator
            if request.watermark_end_operator is not None
            else "<"
        )
        if resolved_start not in {">", ">="}:
            raise SourceError(
                f"Unsupported watermark lower operator: {resolved_start!r}"
            )
        if resolved_end not in {"<", "<="}:
            raise SourceError(f"Unsupported watermark upper operator: {resolved_end!r}")
        return resolved_start, resolved_end

    def _supports_read_range(self) -> bool:
        """Whether this reader implements the source-owned range contract."""

        return False

    @staticmethod
    def _validate_read_range(read_range: _SourceReadRange) -> None:
        """Reject empty or reversed ranges before source I/O."""
        try:
            normalize_chunk_range(read_range.start, read_range.end)
        except ConfigurationError as exc:
            raise SourceError(str(exc)) from exc

    def _get_read_range(self) -> Optional[_SourceReadRange]:
        """Return the exact range active for the current read."""

        return self._read_range

    def _apply_read_range_filter(self, df: DF) -> DF:
        """Apply the active exact range to an already loaded frame."""

        read_range = self._read_range
        if read_range is None:
            return df
        previous_start = self._watermark_start_operator
        previous_end = self._watermark_end_operator
        try:
            self._watermark_start_operator = read_range.lower_operator
            self._watermark_end_operator = read_range.upper_operator
            return self._apply_watermark_filter(
                df,
                [read_range.column],
                {read_range.column: read_range.start},
                {read_range.column: read_range.end},
            )
        finally:
            self._watermark_start_operator = previous_start
            self._watermark_end_operator = previous_end

    def _get_reference_time(self) -> datetime:
        """Return the required reference time for the active read."""

        if self._runtime_start is None:
            raise SourceError("No source read is active")
        if self._active_reference_time is None:
            raise SourceError("No source reference time is active")
        return self._active_reference_time

    def _get_effective_watermark_start(self) -> Optional[Dict[str, Any]]:
        """Return the source-adjusted lower bound for the active read."""

        return copy.deepcopy(self._effective_watermark_start)

    def _build_runtime(
        self,
        *,
        status: str,
        message: Optional[str],
        rows_read: Optional[int],
    ) -> SourceRuntimeInfo:
        """Build a detached runtime projection for the current invocation."""

        return SourceRuntimeInfo(
            start_time=self._runtime_start or utc_now(),
            end_time=utc_now(),
            status=status,
            message=message,
            rows_read=rows_read,
            source_action=copy.deepcopy(self._source_action),
            selection=copy.deepcopy(self._selection),
        )

    def merge_watermark(
        self,
        existing: Optional[Dict[str, Any]],
        candidate: Optional[WatermarkCandidate],
    ) -> Dict[str, Any]:
        """Merge a source observation using this reader's semantics.

        Watermark column membership only controls which source observations may
        be persisted.  It does not grant ordering semantics: unclassified
        values, including cursor-like strings, remain opaque and replace their
        own key. Built-in readers may override
        :meth:`_watermark_ordering_kinds` for source-specific typed fields.
        """

        if candidate is None:
            return copy.deepcopy(existing or {})
        if not isinstance(candidate, WatermarkCandidate):
            raise SourceError(
                "merge_watermark requires a WatermarkCandidate or None"
            )
        self.validate_watermark(candidate.values, boundary="candidate")
        return merge_watermark_values(
            existing,
            candidate.values,
            ordered_keys=self._watermark_ordering_kinds(candidate.values),
        )

    def _watermark_ordering_kinds(
        self,
        candidate: Dict[str, Any],
    ) -> Dict[str, str]:
        """Return source-authorized ordering kinds for typed observations.

        The base/custom reader contract is opaque by default. Built-in readers
        that know their row maxima are ordered may opt in through
        :meth:`_typed_row_watermark_ordering_kinds`; membership in
        ``source.watermark_columns`` alone is not enough.
        """

        return {}

    def _typed_row_watermark_ordering_kinds(
        self,
        candidate: Dict[str, Any],
    ) -> Dict[str, str]:
        """Classify typed row maxima for a reader that explicitly opts in."""

        source = self._active_source
        if source is None:
            return {}
        kinds: Dict[str, str] = {}
        for key, value in candidate.items():
            if key not in (source.watermark_columns or []):
                continue
            if isinstance(value, bool):
                continue
            if isinstance(value, Number) and not isinstance(value, bool):
                kinds[key] = "numeric"
            elif isinstance(value, Decimal):
                kinds[key] = "numeric"
            elif isinstance(value, (date, datetime)):
                kinds[key] = "temporal"
        return kinds

    # ------------------------------------------------------------------
    # Abstract methods (subclass contract)
    # ------------------------------------------------------------------

    @abstractmethod
    def _read_internal(
        self,
        source: Source,
        request: SourceReadRequest,
    ) -> Optional[DF]:
        """Read data from the source (subclass implementation).

        This is the core reading logic.  The base class handles timing,
        error wrapping, and runtime-info population.

        Args:
            source: Pipeline source configuration.
            request: The complete caller-owned read request.

        Returns:
            DataFrame or ``None`` if no data available.
        """

    @abstractmethod
    def _read_data(
        self,
        source: Source,
        configure: Optional[Dict[str, Any]] = None,
    ) -> DF:
        """Perform the raw read operation (format-specific).

        Args:
            source: Pipeline source configuration.
            configure: Additional read configuration.

        Returns:
            Raw DataFrame from the data source.
        """

    # ------------------------------------------------------------------
    # Protected helpers
    # ------------------------------------------------------------------

    def _apply_watermark_filter(
        self,
        df: DF,
        watermark_columns: List[str],
        watermark_start: Dict[str, Any],
        watermark_end: Optional[Dict[str, Any]] = None,
    ) -> DF:
        """Apply incremental watermark filter to the DataFrame.

        Builds an OR condition across watermark columns:
        ``col1 > val1 OR col2 > val2 ...``

        When *watermark_end* is provided, builds per-column windowed
        conditions: ``(col > lower AND col < upper) OR ...``

        Delegates the actual filtering to :meth:`BaseEngine.apply_watermark_filter`
        which uses native DataFrame API.
        ``DATE_FOLDER_PARTITION_KEY`` entries are skipped (handled by
        :class:`~datacoolie.sources.file_reader.FileReader`).
        """
        if not watermark_columns:
            return df
        if not watermark_start and not watermark_end:
            return df

        # Filter out DATE_FOLDER_PARTITION_KEY and columns with no values at all
        filtered_columns = [
            col
            for col in watermark_columns
            if col != DATE_FOLDER_PARTITION_KEY
            and (
                (watermark_start or {}).get(col) is not None
                or (watermark_end or {}).get(col) is not None
            )
        ]

        if not filtered_columns:
            return df

        filtered_watermark = {
            col: (watermark_start or {}).get(col) for col in filtered_columns
        }
        # Remove None entries from lower watermark (columns with only upper bound)
        filtered_watermark = {
            k: v for k, v in filtered_watermark.items() if v is not None
        }

        # Build upper bound dict if provided
        filtered_end: Optional[Dict[str, Any]] = None
        if watermark_end:
            filtered_end = {
                col: watermark_end[col]
                for col in filtered_columns
                if watermark_end.get(col) is not None
            }
            if not filtered_end:
                filtered_end = None

        logger.debug(
            "Applying watermark filter on columns: %s (start=%s, start_operator=%s, end=%s, end_operator=%s)",
            filtered_columns,
            filtered_watermark,
            self._watermark_start_operator,
            filtered_end,
            self._watermark_end_operator,
        )
        return self._engine.apply_watermark_filter(
            df,
            filtered_columns,
            filtered_watermark,
            start_operator=self._watermark_start_operator,
            watermark_end=filtered_end,
            end_operator=self._watermark_end_operator,
        )

    def _apply_filter_expression(self, df: DF, source: Source) -> DF:
        """Apply ``source.filter_expression`` as a post-read row filter.

        Non-database readers cannot push SQL predicates to the storage
        layer, so the expression is evaluated in-memory via
        :meth:`BaseEngine.filter_rows` (Polars ``sql_expr`` / Spark
        ``df.filter``).

        Called after watermark filtering and before count/new-watermark
        calculation so that filtered rows are excluded from ``rows_read``.
        """
        if not source.filter_expression:
            return df
        logger.debug("Applying filter_expression: %s", source.filter_expression)
        return self._engine.filter_rows(df, source.filter_expression)

    def _calculate_count_and_new_watermark(
        self,
        df: DF,
        watermark_columns: List[str],
    ) -> Tuple[int, Dict[str, Any]]:
        """Calculate row count and new watermark values in one pass.

        If *watermark_columns* is empty, only the count is retrieved.

        Returns:
            ``(row_count, {column: max_value})``
        """
        if not watermark_columns:
            return self._engine.count_rows(df), {}

        count, max_values = self._engine.get_count_and_max_values(df, watermark_columns)
        return count, max_values

    def _set_rows_read(self, rows_read: int) -> None:
        """Record the number of rows read."""
        if isinstance(rows_read, bool) or not isinstance(rows_read, int) or rows_read < 0:
            raise SourceError("rows_read must be a non-negative integer")
        self._rows_read = rows_read

    def _set_source_action(self, source_action: Dict[str, Any]) -> None:
        """Record the actual source action performed (path, query, function, etc.)."""
        self._source_action = copy.deepcopy(source_action)

    def _set_candidate(
        self,
        watermark: Optional[Dict[str, Any]],
        *,
        kind: Optional[WatermarkCandidateKind | str] = None,
    ) -> None:
        """Store the source-owned candidate for the active read."""

        if not watermark:
            self._candidate = None
            return
        self.validate_watermark(watermark, boundary="candidate")
        from datacoolie.watermark.base import WatermarkSerializer

        WatermarkSerializer.serialize(watermark)
        resolved_kind = (
            WatermarkCandidateKind(kind)
            if kind is not None
            else self._infer_candidate_kind(watermark)
        )
        self._candidate = WatermarkCandidate(
            values=copy.deepcopy(dict(watermark)),
            kind=resolved_kind,
        )

    def _infer_candidate_kind(
        self,
        candidate: Dict[str, Any],
    ) -> WatermarkCandidateKind:
        """Infer the safest built-in meaning for a source observation."""

        if self._watermark_ordering_kinds(candidate):
            return WatermarkCandidateKind.OBSERVED_MAX
        return WatermarkCandidateKind.OPAQUE

    def _set_generated_upper(self, upper: Dict[str, Any]) -> None:
        """Record a source-generated ceiling without losing observed scopes."""

        if not upper or self._selection.upper_origin is SourceReadUpperOrigin.EXPLICIT:
            return
        current = dict(self._selection.upper or {})
        current.update(copy.deepcopy(upper))
        self._selection = replace(
            self._selection,
            upper=current,
            upper_origin=SourceReadUpperOrigin.GENERATED,
        )

    def validate_watermark(
        self,
        watermark: Optional[Dict[str, Any]],
        *,
        boundary: str = "watermark",
    ) -> None:
        """Reject binary bounds until a backend has qualified ordering.

        The JSON codec can preserve bytes, but comparison semantics belong to
        the source/engine boundary.  A reader that has a qualified native
        ordering may override this protected hook; built-in readers fail
        before a query or destination mutation is started.
        """

        def _find_binary(value: Any, path: str) -> tuple[str, type] | None:
            if isinstance(value, (bytes, bytearray, memoryview)):
                return path, type(value)
            if isinstance(value, dict):
                for key, nested in value.items():
                    found = _find_binary(nested, f"{path}.{key}" if path else str(key))
                    if found is not None:
                        return found
            if isinstance(value, (list, tuple)):
                for index, nested in enumerate(value):
                    found = _find_binary(nested, f"{path}[{index}]")
                    if found is not None:
                        return found
            return None

        if watermark is not None:
            if not isinstance(watermark, dict):
                raise SourceError(
                    f"{boundary} must be a mapping",
                    details={"boundary": boundary},
                )
            if any(not isinstance(key, str) for key in watermark):
                raise SourceError(
                    f"{boundary} keys must be strings",
                    details={"boundary": boundary},
                )

        found = _find_binary(watermark or {}, "")
        if found is not None:
            path, value_type = found
            raise SourceError(
                "Binary watermark comparison requires a qualified backend",
                details={
                    "boundary": boundary,
                    "path": path or "<root>",
                    "value_type": value_type.__name__,
                },
            )

    def _finalize_read(
        self,
        df: DF,
        watermark_columns: List[str],
        reader_name: str,
        context: str,
    ) -> Optional[DF]:
        """Record count/watermark and return *df* or ``None`` on zero rows.

        This is the common epilogue shared by simple readers (Delta,
        Iceberg, Database, PythonFunction).  Readers with extra
        post-processing (FileReader, APIReader) implement the steps
        inline.

        Args:
            df: DataFrame after reading and watermark filtering.
            watermark_columns: Columns to compute new watermark from.
            reader_name: Human label for log messages (e.g. ``"DeltaReader"``).
            context: Identifier for the data source (table name, path, etc.).

        Returns:
            *df* when rows exist, ``None`` otherwise.
        """
        count, new_wm = self._calculate_count_and_new_watermark(
            df,
            watermark_columns,
        )

        self._set_rows_read(count)
        self._set_candidate(new_wm)

        if count == 0:
            logger.debug(
                "%s: 0 rows after filtering — skipping. %s", reader_name, context
            )
            return None

        logger.debug("%s: read %d rows from %s", reader_name, count, context)
        return df

    # ------------------------------------------------------------------
    # Backward offset helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_watermark_effective(
        source: Source,
        watermark: Optional[Dict[str, Any]],
        *,
        reference_time: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return a watermark adjusted by the source's backward offset.

        On first run (*watermark* is ``None``) or when no backward offset is
        configured, the original value is returned unchanged.

        Two offset strategies are supported (configured via ``date_backward``):

        **Fixed offset** — ``{days: 7}`` / ``{months: 1}`` / ``{hours: 6}``.
        Subtracts the offset from the stored watermark value.

        **Closing-day** — ``{closing_day: 10}``.  Computes an absolute
        start boundary from the current date (takes priority over offset keys).

        **File-reader path** (``DATE_FOLDER_PARTITION_KEY`` present):
        The offset is applied *only* to ``DATE_FOLDER_PARTITION_KEY`` (parsed
        from its ISO-8601 string and written back as ISO-8601).  All other
        watermark columns are passed through unchanged so that regular datetime
        filters remain anchored to the original stored value.

        **All other readers**: every ``datetime`` value is adjusted by the
        backward offset; non-datetime values pass through unchanged.
        """
        backward = source.date_backward
        if not watermark or not backward:
            return watermark

        closing_day = backward.get("closing_day")

        def _offset(dt: datetime) -> datetime:
            if closing_day is not None:
                return BaseSourceReader._apply_closing_day_backward(
                    reference_time or utc_now(),
                    int(closing_day),
                    months=int(backward.get("months", 0)),
                    years=int(backward.get("years", 0)),
                )
            return BaseSourceReader._apply_backward(dt, backward)

        # -- file-reader path: only adjust DATE_FOLDER_PARTITION_KEY ----------
        if DATE_FOLDER_PARTITION_KEY in watermark:
            raw = watermark[DATE_FOLDER_PARTITION_KEY]
            dt: Optional[datetime] = None
            if isinstance(raw, datetime):
                dt = raw
            elif isinstance(raw, str):
                try:
                    dt = datetime.fromisoformat(raw)
                except (ValueError, TypeError):
                    pass
            if dt is None:
                return watermark
            result = dict(watermark)
            result[DATE_FOLDER_PARTITION_KEY] = _offset(dt)
            return result

        # -- all other readers: adjust every datetime column ------------------
        adjusted: Dict[str, Any] = {}
        for col, val in watermark.items():
            if isinstance(val, datetime):
                adjusted[col] = _offset(val)
            elif (
                type(val) is date
            ):  # exact check — datetime IS-A date, must come after datetime
                dt_mid = datetime(val.year, val.month, val.day, tzinfo=timezone.utc)
                adjusted[col] = _offset(dt_mid).date()
            elif isinstance(val, str):
                try:
                    dt_val = datetime.fromisoformat(val)
                    adjusted[col] = _offset(dt_val).isoformat()
                except (ValueError, TypeError):
                    adjusted[col] = val
            else:
                adjusted[col] = val
        return adjusted

    @staticmethod
    def _apply_backward(dt: datetime, backward: Dict[str, Any]) -> datetime:
        """Subtract a look-back offset from *dt*.

        Accepted keys in *backward*: ``days``, ``months``, ``years``, ``hours``.
        Month/year subtraction uses calendar-safe arithmetic (no third-party deps).

        Examples::

            _apply_backward(now, {"days": 7})             # 7 days back
            _apply_backward(now, {"months": 1})           # 1 month back
            _apply_backward(now, {"years": 1})            # 1 year back
            _apply_backward(now, {"days": 3, "hours": 6})
        """
        days = int(backward.get("days", 0))
        months = int(backward.get("months", 0))
        years = int(backward.get("years", 0))
        hours = int(backward.get("hours", 0))
        result = dt - timedelta(days=days, hours=hours)
        total_months = months + years * 12
        if total_months:
            total = result.year * 12 + result.month - 1 - total_months
            year, rem = divmod(total, 12)
            month = rem + 1
            max_day = calendar.monthrange(year, month)[1]
            result = result.replace(
                year=year, month=month, day=min(result.day, max_day)
            )
        return result

    @staticmethod
    def _apply_closing_day_backward(
        now: datetime,
        closing_day: int,
        months: int = 0,
        years: int = 0,
    ) -> datetime:
        """Compute the start boundary for a monthly closing-day rule.

        First determines the base boundary month:

        * ``today.day <= closing_day`` → 1st of **previous** month.
        * ``today.day >  closing_day`` → 1st of **current** month.

        Then subtracts additional *months* and *years* from that
        boundary, allowing longer look-back windows.

        Examples (closing_day=10)::

            March  8            → Feb 1   (8 <= 10)
            March 12            → Mar 1   (12 > 10)
            March  8, months=2  → Dec 1 prev year
            March  8, years=1   → Feb 1 prev year

        Args:
            now: The reference datetime (typically *utc_now()*).
            closing_day: Day-of-month that marks the period boundary.
            months: Additional months to subtract from the boundary.
            years: Additional years to subtract from the boundary.

        Returns:
            A :class:`~datetime.datetime` set to midnight UTC on the
            1st of the applicable month.
        """
        tz = now.tzinfo
        if now.day <= closing_day:
            # Previous month
            if now.month == 1:
                base = datetime(now.year - 1, 12, 1, tzinfo=tz)
            else:
                base = datetime(now.year, now.month - 1, 1, tzinfo=tz)
        else:
            # Current month
            base = datetime(now.year, now.month, 1, tzinfo=tz)

        # Apply additional month/year offset
        extra = months + years * 12
        if extra:
            total = base.year * 12 + base.month - 1 - extra
            year, rem = divmod(total, 12)
            month = rem + 1
            base = datetime(year, month, 1, tzinfo=tz)

        return base

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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from numbers import Number
from typing import Any, Dict, Generic, List, Optional, Tuple

from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY, DataFlowStatus
from datacoolie.core.exceptions import ConfigurationError, SourceError
from datacoolie.core.models.source import Source
from datacoolie.core.models.runtime import SourceRuntimeInfo
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.utils.time import utc_now
from datacoolie.utils.chunking import normalize_chunk_range
from datacoolie.watermark.base import merge_watermark_values

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class SourceReadRange:
    """Exact source-owned range for one bounded read."""

    column: str
    start: Any
    end: Any
    lower_operator: str = ">="
    upper_operator: str = "<"

    def __post_init__(self) -> None:
        if not isinstance(self.column, str) or not self.column.strip():
            raise ValueError("SourceReadRange.column must be a non-empty string")
        if self.start is None or self.end is None:
            raise ValueError("SourceReadRange.start and end must not be None")
        if self.lower_operator not in {">", ">="}:
            raise ValueError(
                f"Unsupported SourceReadRange.lower_operator: {self.lower_operator!r}"
            )
        if self.upper_operator not in {"<", "<="}:
            raise ValueError(
                f"Unsupported SourceReadRange.upper_operator: {self.upper_operator!r}"
            )

    @property
    def start_operator(self) -> str:
        """Compatibility alias used by older source adapters."""

        return self.lower_operator

    @property
    def end_operator(self) -> str:
        """Compatibility alias used by older source adapters."""

        return self.upper_operator


class BaseSourceReader(ABC, Generic[DF]):
    """Abstract source reader with Template Method lifecycle.

    Subclasses must implement :meth:`_read_internal`.  Optionally they
    may also implement :meth:`_read_data` for the raw read step.

    Type parameter *DF* is the concrete DataFrame class.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine
        self._new_watermark: Dict[str, Any] = {}
        self._runtime_info = SourceRuntimeInfo()
        self._watermark_start_operator: str = ">"
        self._watermark_end_operator: str = "<"
        self._preserve_empty: bool = False
        self._read_range: Optional[SourceReadRange] = None
        self._pending_watermark_start: Optional[Dict[str, Any]] = None
        self._pending_watermark_end: Optional[Dict[str, Any]] = None
        self._active_source: Optional[Source] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read(
        self,
        source: Source,
        watermark_start: Optional[Dict[str, Any]] = None,
        *,
        watermark_start_operator: Optional[str] = None,
        watermark_end: Optional[Dict[str, Any]] = None,
        watermark_end_operator: Optional[str] = None,
        read_range: Optional[SourceReadRange] = None,
        preserve_empty: bool = False,
    ) -> Optional[DF]:
        """Read data from a source (Template Method).

        1. Initialise runtime info.
        2. Delegate to :meth:`_read_internal`.
        3. Record timing and watermark data.

        Args:
            source: Pipeline source configuration.
            watermark_start: Previous watermark values (``None`` = first run).
                Acts as the lower bound for incremental reads.
            watermark_start_operator: Comparison operator for the lower-bound
                WHERE clause.  ``">"`` (default) for exclusive lower bound,
                ``">=``" for inclusive lower bound (used by replay).
            watermark_end: Upper watermark bound (``None`` = no ceiling).
                When provided, rows are filtered to ``col < end`` for
                each column.  Used by replay to cap reads at chunk
                boundaries.
            watermark_end_operator: Comparison operator for the upper-bound
                WHERE clause.  ``"<"`` (default) for exclusive upper bound.
            read_range: Source-owned exact range.  This is independent from
                persisted watermark columns and is mutually exclusive with
                explicit ``watermark_start``/``watermark_end`` bounds.
            preserve_empty: Keep a typed zero-row frame instead of returning
                ``None``.  Used only for confirmed bounded replacement reads.

        Returns:
            DataFrame with source data, or ``None`` when there is
            no data to process.

        Raises:
            SourceError: On any failure during reading.
        """
        if read_range is not None and (
            watermark_start is not None or watermark_end is not None
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
            read_range = SourceReadRange(
                column=read_range.column,
                start=normalized_start,
                end=normalized_end,
                lower_operator=read_range.lower_operator,
                upper_operator=read_range.upper_operator,
            )
            self._validate_read_range(read_range)
            self._validate_watermark_comparison(
                {read_range.column: read_range.start}, boundary="range lower bound"
            )
            self._validate_watermark_comparison(
                {read_range.column: read_range.end}, boundary="range upper bound"
            )
        self._read_range = read_range
        self._active_source = source
        self._pending_watermark_start = watermark_start
        self._pending_watermark_end = watermark_end
        if read_range is not None:
            self._watermark_start_operator = read_range.lower_operator
            self._watermark_end_operator = read_range.upper_operator
        else:
            self._watermark_start_operator, self._watermark_end_operator = (
                self._resolve_watermark_operators(
                    source,
                    watermark_start_operator,
                    watermark_end_operator,
                )
            )
        self._preserve_empty = bool(preserve_empty)
        self._runtime_info = SourceRuntimeInfo(
            start_time=utc_now(),
            status=DataFlowStatus.RUNNING.value,
            watermark_before=dict(watermark_start) if watermark_start else None,
            watermark_start_operator=self._watermark_start_operator,
            watermark_end_operator=self._watermark_end_operator,
        )
        # This is a result of this read, not persisted checkpoint state.  A
        # reader instance may be reused by callers, so never expose the
        # previous read's value on an empty result or a failed attempt.
        self._new_watermark = {}

        try:
            self._validate_watermark_comparison(
                watermark_start, boundary="lower bound"
            )
            self._validate_watermark_comparison(
                watermark_end, boundary="upper bound"
            )
            # Apply backward offset to datetime watermark columns so
            # late-arriving data is not missed.  First-run
            # (watermark_start=None) is left untouched.  The upper bound is
            # never adjusted: replay boundaries must remain exact.
            watermark_effective = (
                None
                if read_range is not None
                else self._build_watermark_effective(source, watermark_start)
            )
            self._runtime_info.watermark_effective = (
                dict(watermark_effective) if watermark_effective else None
            )
            df = self._read_internal(source, watermark_effective, watermark_end=watermark_end)

            self._runtime_info.end_time = utc_now()

            if df is None:
                self._runtime_info.status = DataFlowStatus.SUCCEEDED.value
                self._runtime_info.watermark_after = dict(self._new_watermark) if self._new_watermark else None
                self._preserve_empty = False
                self._pending_watermark_start = None
                self._pending_watermark_end = None
                return None

            self._runtime_info.status = DataFlowStatus.SUCCEEDED.value
            self._runtime_info.watermark_after = dict(self._new_watermark) if self._new_watermark else None
            self._preserve_empty = False
            self._pending_watermark_start = None
            self._pending_watermark_end = None
            return df

        except SourceError as exc:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.message = str(exc) or type(exc).__name__
            logger.debug("Source read failed: %s", exc)
            self._preserve_empty = False
            self._pending_watermark_start = None
            self._pending_watermark_end = None
            raise
        except Exception as exc:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.message = str(exc) or type(exc).__name__
            logger.debug("Source read failed: %s", exc)
            self._preserve_empty = False
            self._pending_watermark_start = None
            self._pending_watermark_end = None
            raise SourceError(
                f"Failed to read source: {exc}",
                details={"source_table": source.full_table_name, "source_path": source.path},
            ) from exc

    def _resolve_watermark_operators(
        self,
        source: Source,
        start_operator: Optional[str],
        end_operator: Optional[str],
    ) -> Tuple[str, str]:
        """Resolve omitted operators at the source boundary."""

        del source
        resolved_start = start_operator if start_operator is not None else ">"
        resolved_end = end_operator if end_operator is not None else "<"
        if resolved_start not in {">", ">="}:
            raise SourceError(
                f"Unsupported watermark lower operator: {resolved_start!r}"
            )
        if resolved_end not in {"<", "<="}:
            raise SourceError(
                f"Unsupported watermark upper operator: {resolved_end!r}"
            )
        return resolved_start, resolved_end

    def _supports_read_range(self) -> bool:
        """Whether this reader implements the source-owned range contract."""

        return False

    @staticmethod
    def _validate_read_range(read_range: SourceReadRange) -> None:
        """Reject empty or reversed ranges before source I/O."""
        try:
            normalize_chunk_range(read_range.start, read_range.end)
        except ConfigurationError as exc:
            raise SourceError(str(exc)) from exc

    def _get_read_range(self) -> Optional[SourceReadRange]:
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

    def get_runtime_info(self) -> SourceRuntimeInfo:
        """Return runtime information for the most recent read."""
        return self._runtime_info

    def get_new_watermark(self) -> Dict[str, Any]:
        """Return the new watermark values computed during the last read."""
        return self._new_watermark

    def merge_watermark(
        self,
        existing: Optional[Dict[str, Any]],
        candidate: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Merge a source observation using this reader's semantics.

        Watermark column membership only controls which source observations may
        be persisted.  It does not grant ordering semantics: unclassified
        values, including cursor-like strings, remain opaque and replace their
        own key. Built-in readers may override
        :meth:`_watermark_ordering_kinds` for source-specific typed fields.
        """

        return merge_watermark_values(
            existing,
            candidate,
            ordered_keys=self._watermark_ordering_kinds(candidate or {}),
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
        watermark_start: Optional[Dict[str, Any]] = None,
        *,
        watermark_end: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        """Read data from the source (subclass implementation).

        This is the core reading logic.  The base class handles timing,
        error wrapping, and runtime-info population.

        Args:
            source: Pipeline source configuration.
            watermark_start: Previous watermark values (lower bound).
            watermark_end: Upper watermark bound.  When provided, rows
                should be filtered to ``col < end`` for each column.

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
            col for col in watermark_columns
            if col != DATE_FOLDER_PARTITION_KEY
            and ((watermark_start or {}).get(col) is not None or (watermark_end or {}).get(col) is not None)
        ]

        if not filtered_columns:
            return df

        filtered_watermark = {col: (watermark_start or {}).get(col) for col in filtered_columns}
        # Remove None entries from lower watermark (columns with only upper bound)
        filtered_watermark = {k: v for k, v in filtered_watermark.items() if v is not None}

        # Build upper bound dict if provided
        filtered_end: Optional[Dict[str, Any]] = None
        if watermark_end:
            filtered_end = {col: watermark_end[col] for col in filtered_columns if watermark_end.get(col) is not None}
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
            df, filtered_columns, filtered_watermark,
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
        self._runtime_info.rows_read = rows_read

    def _set_source_action(self, source_action: Dict[str, Any]) -> None:
        """Record the actual source action performed (path, query, function, etc.)."""
        self._runtime_info.source_action = source_action

    def _set_new_watermark(self, watermark: Optional[Dict[str, Any]]) -> None:
        """Store the computed new watermark values."""
        self._new_watermark = watermark or {}

    def _validate_watermark_comparison(
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
            df, watermark_columns,
        )

        self._set_new_watermark(new_wm)
        self._set_rows_read(count)

        if count == 0:
            if getattr(self, "_preserve_empty", False):
                logger.debug(
                    "%s: 0 rows after filtering — preserving typed empty frame. %s",
                    reader_name,
                    context,
                )
                return df
            logger.debug("%s: 0 rows after filtering — skipping. %s", reader_name, context)
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
                    utc_now(),
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
            elif type(val) is date:  # exact check — datetime IS-A date, must come after datetime
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

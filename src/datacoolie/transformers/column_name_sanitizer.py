"""Column name sanitizer transformer.

Renames DataFrame columns using the chosen case-conversion mode:

``lower`` (default)
    Lowercases the name without inserting underscores at word boundaries.
    ``"MyColumn"`` → ``"mycolumn"``, ``"HTTPStatus"`` → ``"httpstatus"``.

``snake``
    Converts ``camelCase`` / ``PascalCase`` → ``snake_case``.
    ``"MyColumn"`` → ``"my_column"``, ``"HTTPStatus"`` → ``"http_status"``.

Both modes apply the same clean-up rules:

* Replaces special characters (spaces, hyphens, dots, etc.) with ``_``.
* Collapses consecutive underscores and strips leading / trailing ones.
* Prefixes names that start with a digit with ``_``.
* Columns whose names already start with ``_`` are left unchanged.
"""

from __future__ import annotations

from datacoolie.core.constants import ColumnCaseMode, TRAILING_COLUMNS
from datacoolie.core.models import DataFlow
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.transformers.base import BaseTransformer
from datacoolie.utils.converters import to_lower_case, to_snake_case

logger = get_logger(__name__)

# Re-export so callers can do: from datacoolie.transformers.column_name_sanitizer import to_snake_case
__all__ = ["ColumnNameSanitizer", "to_lower_case", "to_snake_case"]

_SANITIZERS = {
    ColumnCaseMode.LOWER: to_lower_case,
    ColumnCaseMode.SNAKE: to_snake_case,
}


class ColumnNameSanitizer(BaseTransformer[DF]):
    """Rename columns using the chosen case-conversion mode (order = 90).

    Runs after :class:`SystemColumnAdder` so the framework audit columns keep
    their canonical names. All prior transformers (schema conversion, dedup,
    additional columns, partition columns) still work with the original
    business column names. Only columns whose names actually change are
    renamed. Columns that already start with ``_`` (e.g. system columns added
    in the current or a previous run) are skipped.

    Args:
        engine: Data engine instance.
        mode: Case-conversion mode — ``ColumnCaseMode.LOWER`` (default) or
            ``ColumnCaseMode.SNAKE``.
    """

    def __init__(self, engine: BaseEngine[DF], *, mode: ColumnCaseMode = ColumnCaseMode.LOWER) -> None:
        self._engine = engine
        self._mode = ColumnCaseMode(mode)

    @property
    def order(self) -> int:
        return 90

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        """Rename columns that are not already clean, then reorder so that
        system / file-info columns appear last."""
        self._mark_applied(self._mode.value)
        sanitize = _SANITIZERS[self._mode]
        columns = self._engine.get_columns(df)

        for col in columns:
            sanitized = sanitize(col)
            if not sanitized or sanitized == col:
                continue
            logger.debug(
                "ColumnNameSanitizer(%s): renaming %r → %r",
                self._mode.value, col, sanitized,
            )
            df = self._engine.rename_column(df, col, sanitized)

        # Move system / file-info columns to the tail of the DataFrame.
        current = self._engine.get_columns(df)
        trailing_present = [c for c in TRAILING_COLUMNS if c in current]
        if trailing_present:
            leading = [c for c in current if c not in trailing_present]
            df = self._engine.select_columns(df, leading + trailing_present)

        return df

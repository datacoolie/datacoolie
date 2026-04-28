"""Python function source reader.

Reads data by importing and calling a user-defined Python function that
returns a DataFrame.  This is intended for complex source logic that
cannot be expressed as a simple ``source.query``.

The function path is specified on :attr:`Source.python_function` as a
dotted module path, e.g. ``"mypackage.loaders.load_orders"``.

**Two-phase watermark filtering:**

1. *Before* — the previous watermark dict is passed to the function so it
   can apply efficient source-side filtering (e.g. a WHERE clause).
2. *After* — the framework applies :meth:`_apply_watermark_filter` on the
   returned DataFrame for precise, engine-level filtering — identical to
   what other readers do.

The function signature must be::

    def my_loader(engine, source, watermark) -> DataFrame | None

* ``engine`` — the active :class:`BaseEngine` instance.
* ``source`` — the :class:`Source` model.
* ``watermark`` — previous watermark values (``None`` on first run).
"""

from __future__ import annotations

import importlib
from typing import Any, Callable, Dict, List, Optional

from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Source
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.sources.base import BaseSourceReader

logger = get_logger(__name__)


class PythonFunctionReader(BaseSourceReader[DF]):
    """Source reader that delegates to a user-defined Python function.

    The function path is read from ``source.python_function`` and must
    be a fully-qualified dotted path (e.g. ``"my_module.load_data"``).

    The function signature must be::

        def my_loader(engine, source, watermark) -> DataFrame | None

    Watermark handling follows a **before / after** pattern:

    * The ``watermark`` dict is passed to the function so it *can*
      pre-filter at the source (optional — the function may ignore it).
    * After the function returns, the framework applies
      :meth:`_apply_watermark_filter` for precise row-level filtering,
      consistent with other readers.
    """

    def __init__(self, engine: BaseEngine[DF], allowed_prefixes: Optional[List[str]] = None) -> None:
        super().__init__(engine)
        self._allowed_prefixes: List[str] = allowed_prefixes or []

    # ------------------------------------------------------------------
    # Core reading
    # ------------------------------------------------------------------

    def _read_internal(
        self,
        source: Source,
        watermark: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        """Call the user-defined function and process the result.

        Steps:
            1. Resolve and call the Python function (watermark is passed
               so the function can do source-side pre-filtering).
            2. Apply engine-level watermark filter on the returned
               DataFrame (post-filter — consistent with other readers).
            3. Calculate count and new watermark.
            4. Return ``None`` if zero rows.
        """
        self._set_source_action({"reader": type(self).__name__, "function": source.python_function or ""})

        df = self._read_data(source, watermark=watermark)

        if df is None:
            logger.info("PythonFunctionReader: function returned None — skipping.")
            return None

        # Post-function watermark filter (engine-level precision)
        if watermark and source.watermark_columns:
            df = self._apply_watermark_filter(df, source.watermark_columns, watermark)

        context = f"Function: {source.python_function}"
        return self._finalize_read(df, source.watermark_columns, "PythonFunctionReader", context)

    def _read_data(
        self,
        source: Source,
        configure: Optional[Dict[str, Any]] = None,
        *,
        watermark: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        """Import and call the Python function.

        Raises:
            SourceError: If the function path is missing, cannot be
                imported, or raises an exception.
        """
        func_path = source.python_function
        if not func_path:
            raise SourceError(
                "PythonFunctionReader requires source.python_function",
            )

        func = self._resolve_function(func_path, self._allowed_prefixes)

        try:
            return func(engine=self._engine, source=source, watermark=watermark)
        except Exception as exc:
            raise SourceError(
                f"Python function '{func_path}' raised an error: {exc}",
                details={"python_function": func_path},
            ) from exc

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_function(func_path: str, allowed_prefixes: Optional[List[str]] = None) -> Callable[..., Any]:
        """Import and return the callable from a dotted path.

        Args:
            func_path: Fully-qualified path, e.g.
                ``"mypackage.loaders.load_orders"``.
            allowed_prefixes: When non-empty, *func_path* must start with
                one of these prefixes.  This restricts which modules can
                be imported, mitigating arbitrary code execution when
                metadata comes from an untrusted source.

        Raises:
            SourceError: If the module or attribute cannot be found, or
                the function path is not in the allowed prefixes.
        """
        if allowed_prefixes and not any(func_path.startswith(p) for p in allowed_prefixes):
            raise SourceError(
                f"python_function '{func_path}' is not allowed. "
                f"Allowed prefixes: {allowed_prefixes}",
                details={"python_function": func_path, "allowed_prefixes": allowed_prefixes},
            )

        parts = func_path.rsplit(".", 1)
        if len(parts) != 2:
            raise SourceError(
                f"python_function must be a dotted path "
                f"'module.function', got: '{func_path}'",
            )
        module_path, func_name = parts

        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            raise SourceError(
                f"Cannot import module '{module_path}' "
                f"for python_function '{func_path}'",
            ) from exc

        func = getattr(module, func_name, None)
        if func is None:
            raise SourceError(
                f"Module '{module_path}' has no attribute '{func_name}'",
            )
        if not callable(func):
            raise SourceError(
                f"'{func_path}' is not callable",
            )
        return func

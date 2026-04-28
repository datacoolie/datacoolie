"""Abstract base classes for transformers and the transformer pipeline.

``BaseTransformer[DF]`` defines the contract for individual transformers.
``TransformerPipeline`` orchestrates multiple transformers in priority
order, tracking runtime info.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, List

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.exceptions import TransformError
from datacoolie.core.models import DataFlow, TransformRuntimeInfo
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.utils.helpers import utc_now

logger = get_logger(__name__)

_NOT_SET = object()  # sentinel: no tracking call was made


class BaseTransformer(ABC, Generic[DF]):
    """Abstract base class for a single transformation step.

    Each transformer has an :attr:`order` that determines execution
    priority within the pipeline (lower values execute first).

    Tracking
    --------
    Call :meth:`_mark_applied` or :meth:`_mark_skipped` inside
    :meth:`transform` to control what the pipeline records in
    ``transformers_applied``.

    * ``_mark_applied()``           → record ``ClassName``
    * ``_mark_applied("detail")``   → record ``ClassName(detail)``
    * ``_mark_skipped()``           → do **not** record anything
    * *(no call)*                   → default: record ``ClassName``
    """

    _applied_label: object = _NOT_SET

    @property
    @abstractmethod
    def order(self) -> int:
        """Execution priority (lower = earlier)."""

    @abstractmethod
    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        """Apply the transformation to a DataFrame.

        Args:
            df: Input DataFrame.
            dataflow: Full pipeline configuration for context.

        Returns:
            Transformed DataFrame.
        """

    @property
    def name(self) -> str:
        """Human-readable transformer name (class name by default)."""
        return self.__class__.__name__

    # -- tracking helpers ----------------------------------------------

    def _mark_applied(self, detail: str | None = None) -> None:
        """Signal that this transformer did real work.

        Args:
            detail: Optional qualifier appended as ``Name(detail)``.
        """
        if detail:
            self._applied_label = f"{self.name}({detail})"
        else:
            self._applied_label = self.name

    def _mark_skipped(self) -> None:
        """Signal that this transformer was a no-op."""
        self._applied_label = None

    @property
    def applied_label(self) -> str | None:
        """Label resolved after :meth:`transform` returns.

        Returns:
            A string label to record, or ``None`` to skip recording.
        """
        if self._applied_label is _NOT_SET:
            return self.name          # backward-compat default
        return self._applied_label    # type: ignore[return-value]


class TransformerPipeline(Generic[DF]):
    """Ordered pipeline of transformers with runtime tracking.

    Transformers are sorted by :attr:`BaseTransformer.order` and executed
    sequentially.  Runtime info (timing, status, applied transformer names)
    is tracked for observability.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine
        self._transformers: List[BaseTransformer[DF]] = []
        self._runtime_info = TransformRuntimeInfo()

    # ------------------------------------------------------------------
    # Transformer management
    # ------------------------------------------------------------------

    def add_transformer(self, transformer: BaseTransformer[DF]) -> None:
        """Add a transformer to the pipeline."""
        self._transformers.append(transformer)

    def remove_transformer(self, transformer_class: type) -> bool:
        """Remove all transformers of a given class.

        Returns:
            ``True`` if any were removed.
        """
        before = len(self._transformers)
        self._transformers = [
            t for t in self._transformers if not isinstance(t, transformer_class)
        ]
        return len(self._transformers) < before

    def clear(self) -> None:
        """Remove all transformers from the pipeline."""
        self._transformers.clear()

    @property
    def transformers(self) -> List[BaseTransformer[DF]]:
        """Return transformers sorted by execution order."""
        return sorted(self._transformers, key=lambda t: t.order)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        """Run all transformers in order.

        Args:
            df: Input DataFrame.
            dataflow: Full pipeline configuration.

        Returns:
            Transformed DataFrame.

        Raises:
            TransformError: If any transformer fails.
        """
        self._runtime_info = TransformRuntimeInfo(
            start_time=utc_now(),
            status=DataFlowStatus.RUNNING.value,
        )
        applied: List[str] = []

        try:
            result = df
            for transformer in self.transformers:
                logger.debug(
                    "TransformerPipeline: running %s (order=%d)",
                    transformer.name,
                    transformer.order,
                )
                transformer._applied_label = _NOT_SET   # reset
                result = transformer.transform(result, dataflow)

                label = transformer.applied_label
                if label is not None:
                    applied.append(label)

            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.SUCCEEDED.value
            self._runtime_info.transformers_applied = applied
            return result

        except TransformError:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.transformers_applied = applied
            raise
        except Exception as exc:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.transformers_applied = applied
            self._runtime_info.error_message = str(exc)
            raise TransformError(
                f"Transformer pipeline failed: {exc}",
                details={"applied": applied},
            ) from exc

    def get_runtime_info(self) -> TransformRuntimeInfo:
        """Return runtime information from the most recent transform."""
        return self._runtime_info

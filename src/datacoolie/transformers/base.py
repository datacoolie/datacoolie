"""Abstract base classes for transformers and the transformer pipeline.

``BaseTransformer[DF]`` defines the contract for individual transformers.
``TransformerPipeline`` orchestrates multiple transformers in priority
order, tracking runtime info.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, List, Mapping, Optional

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.exceptions import TransformError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.runtime import TransformRuntimeInfo
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.utils.time import utc_now

logger = get_logger(__name__)

_NOT_SET = object()  # sentinel: no tracking call was made


@dataclass(frozen=True, slots=True)
class ColumnMapping:
    """Typed mapping from input columns to output columns.

    ``None`` means a source column was removed. ``known=False`` marks a
    custom transformer that changed names without declaring a mapping; an
    active replacement window must reject that state rather than guess.
    """

    mapping: dict[str, Optional[str]]
    known: bool = True

    @classmethod
    def identity(cls, columns: list[str]) -> "ColumnMapping":
        return cls({column: column for column in columns})

    @classmethod
    def from_columns(
        cls,
        before: list[str],
        after: list[str],
        renames: Mapping[str, str] | None = None,
    ) -> "ColumnMapping":
        """Build a deterministic mapping for a built-in column transform."""

        final = {column.casefold(): column for column in after}
        explicit = {
            source.casefold(): target for source, target in (renames or {}).items()
        }
        return cls(
            {
                source: final.get(
                    explicit.get(source.casefold(), source).casefold()
                )
                for source in before
            }
        )

    @classmethod
    def preserve_or_unknown(cls, before: list[str], after: list[str]) -> "ColumnMapping":
        """Infer only unchanged names; never infer a rename heuristically."""

        final = {column.casefold(): column for column in after}
        mapping = {source: final.get(source.casefold()) for source in before}
        return cls(mapping, known=all(value is not None for value in mapping.values()))

    def resolve(self, source: str) -> Optional[str]:
        source_key = source.casefold()
        for authored, output in self.mapping.items():
            if authored.casefold() == source_key:
                return output
        return None

    def compose(self, step: "ColumnMapping") -> "ColumnMapping":
        """Compose this mapping with the next transform's mapping."""

        return ColumnMapping(
            {
                source: None if output is None else step.resolve(output)
                for source, output in self.mapping.items()
            },
            known=self.known and step.known,
        )


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
    _column_mapping: Optional[ColumnMapping] = None

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

    def _reset_tracking(self) -> None:
        """Reset per-invocation labels and optional column mapping."""
        self._applied_label = _NOT_SET
        self._column_mapping = None

    def _report_column_mapping(self, mapping: ColumnMapping) -> None:
        """Report deterministic name/drop behavior for this invocation."""
        self._column_mapping = mapping

    @property
    def column_mapping(self) -> Optional[ColumnMapping]:
        """Mapping reported by the most recent transform invocation."""
        return self._column_mapping

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
        self._column_mapping: Optional[ColumnMapping] = None
        self._output_columns: list[str] = []

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
        self._column_mapping = None
        self._output_columns = []
        applied: List[str] = []

        try:
            result = df
            current_columns = self._engine.get_columns(df)
            overall_mapping = ColumnMapping.identity(current_columns)
            for transformer in self.transformers:
                logger.debug(
                    "TransformerPipeline: running %s (order=%d)",
                    transformer.name,
                    transformer.order,
                )
                before_columns = current_columns
                transformer._reset_tracking()
                result = transformer.transform(result, dataflow)
                after_columns = self._engine.get_columns(result)
                current_columns = after_columns

                step_mapping = transformer.column_mapping
                if step_mapping is None:
                    step_mapping = ColumnMapping.preserve_or_unknown(
                        before_columns, after_columns
                    )
                overall_mapping = overall_mapping.compose(step_mapping)

                label = transformer.applied_label
                if label is not None:
                    applied.append(label)

            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.SUCCEEDED.value
            self._runtime_info.transformers_applied = applied
            self._column_mapping = overall_mapping
            self._output_columns = list(current_columns)
            return result

        except TransformError as exc:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.transformers_applied = applied
            self._runtime_info.message = str(exc) or type(exc).__name__
            logger.debug("Transformer pipeline failed: %s", exc)
            raise
        except Exception as exc:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.transformers_applied = applied
            self._runtime_info.message = str(exc) or type(exc).__name__
            logger.debug("Transformer pipeline failed: %s", exc)
            raise TransformError(
                f"Transformer pipeline failed: {exc}",
                details={"applied": applied},
            ) from exc

    def get_runtime_info(self) -> TransformRuntimeInfo:
        """Return runtime information from the most recent transform."""
        return self._runtime_info

    def get_column_mapping(self) -> Optional[ColumnMapping]:
        """Return the mapping from source columns to final output columns."""
        return self._column_mapping

    def get_output_columns(self) -> list[str]:
        """Return final output columns from the most recent invocation."""
        return list(self._output_columns)

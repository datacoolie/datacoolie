"""Small custom transformer plugin used by the public extension example."""

from __future__ import annotations

from datacoolie.core.models.dataflow import DataFlow
from datacoolie.transformers.base import BaseTransformer


class PiiMaskerTransformer(BaseTransformer):
    """Mask configured columns while preserving the engine DataFrame type."""

    ORDER = 45

    def __init__(self, engine) -> None:
        self._engine = engine

    @property
    def order(self) -> int:
        return self.ORDER

    def transform(self, df, dataflow: DataFlow):
        config = dataflow.transform.configure.get("pii_mask", {})
        columns = config.get("columns", [])
        if not columns:
            self._mark_skipped()
            return df
        for column in columns:
            df = self._engine.add_column(
                df,
                column,
                f"CASE WHEN {column} IS NULL THEN NULL ELSE '***' END",
            )
        self._mark_applied(f"cols={len(columns)}")
        return df


__all__ = ["PiiMaskerTransformer"]

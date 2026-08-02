"""Ordered, typed value normalization before schema conversion."""

from __future__ import annotations

from datacoolie.core.models import DataFlow
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.transformers.base import BaseTransformer


class ColumnValueTransformer(BaseTransformer[DF]):
    """Apply ``value_rules`` in stable ``(order, declaration)`` order."""

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine

    @property
    def order(self) -> int:
        return 5

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        indexed = enumerate(dataflow.transform.value_rules)
        rules = [
            rule
            for _, rule in sorted(indexed, key=lambda item: (item[1].order, item[0]))
        ]
        if not rules:
            self._mark_skipped()
            return df
        available = {column.lower() for column in self._engine.get_columns(df)}
        applied_rules = sum(
            any(column.lower() in available for column in rule.columns)
            for rule in rules
        )
        for rule in rules:
            df = self._engine.apply_value_rule(
                df, rule, missing_column_policy=dataflow.transform.missing_column_policy
            )
        if applied_rules:
            self._mark_applied(f"{applied_rules} rules")
        else:
            self._mark_skipped()
        return df

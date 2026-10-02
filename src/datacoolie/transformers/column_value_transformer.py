"""Ordered, typed value normalization before schema conversion."""

from __future__ import annotations

from datacoolie.core.models.dataflow import DataFlow
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.transformers.base import BaseTransformer


logger = get_logger(__name__)


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
        ignored_references = (
            sum(
                column.lower() not in available
                for rule in rules
                for column in rule.columns
            )
            if dataflow.transform.missing_column_policy == "ignore"
            else 0
        )
        for rule in rules:
            df = self._engine.apply_value_rule(
                df, rule, missing_column_policy=dataflow.transform.missing_column_policy
            )
        logger.debug(
            "ColumnValueTransformer: configured_rules=%d, matched_rules=%d, "
            "unmatched_rules=%d, ignored_references=%d",
            len(rules),
            applied_rules,
            len(rules) - applied_rules,
            ignored_references,
        )
        if applied_rules:
            self._mark_applied(f"{applied_rules} rules")
        else:
            self._mark_skipped()
        return df

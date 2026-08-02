"""Structured scalar column masking."""

from __future__ import annotations

from datacoolie.core.constants import TRAILING_COLUMNS
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models import DataFlow
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.transformers.base import BaseTransformer


class DataMasker(BaseTransformer[DF]):
    """Apply irreversible masking late in the business pipeline."""

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine

    @property
    def order(self) -> int:
        return 84

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        rules = dataflow.transform.masking_rules
        if not rules:
            self._mark_skipped()
            return df
        protected = {
            *(column.lower() for column in dataflow.merge_keys),
            *(column.lower() for column in dataflow.partition_column_names),
            *(column.lower() for column in TRAILING_COLUMNS),
        }
        targeted = {column.lower() for rule in rules for column in rule.columns}
        overlap = protected.intersection(targeted)
        if overlap:
            raise ConfigurationError(
                "masking_rules cannot target merge, partition, or framework-reserved columns",
                details={"columns": sorted(overlap)},
            )
        available = {column.lower() for column in self._engine.get_columns(df)}
        applied_rules = sum(
            any(column.lower() in available for column in rule.columns)
            for rule in rules
        )
        for rule in rules:
            df = self._engine.apply_masking_rule(
                df, rule, missing_column_policy=dataflow.transform.missing_column_policy
            )
        if applied_rules:
            self._mark_applied(f"{applied_rules} rules")
        else:
            self._mark_skipped()
        return df

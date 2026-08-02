"""Stable SHA-256 business hash columns."""

from __future__ import annotations

from datacoolie.core.constants import TRAILING_COLUMNS
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models import DataFlow
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.transformers.base import BaseTransformer


class HashColumnAdder(BaseTransformer[DF]):
    """Add explicitly configured hash targets after schema conversion."""

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine

    @property
    def order(self) -> int:
        return 18

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        definitions = dataflow.transform.hash_columns
        if not definitions:
            self._mark_skipped()
            return df

        applied = 0
        reserved = {column.lower() for column in TRAILING_COLUMNS}
        for definition in definitions:
            current = self._engine.get_columns(df)
            current_lower = {column.lower() for column in current}
            missing = [
                column
                for column in definition.columns
                if column.lower() not in current_lower
            ]
            if missing:
                if dataflow.transform.missing_column_policy == "ignore":
                    continue
                raise ConfigurationError(
                    "hash_columns source column not found",
                    details={"columns": missing, "target": definition.target_column},
                )
            target = definition.target_column.lower()
            if target in current_lower:
                raise ConfigurationError(
                    "hash_columns cannot overwrite an existing column",
                    details={"target": definition.target_column},
                )
            if target in reserved:
                raise ConfigurationError(
                    "hash_columns cannot create a framework-reserved column",
                    details={"target": definition.target_column},
                )
            df = self._engine.add_hash_column(df, definition)
            applied += 1

        if applied:
            self._mark_applied(f"{applied} columns")
        else:
            self._mark_skipped()
        return df

"""Transformer components.

Provides :class:`BaseTransformer`, :class:`TransformerPipeline`, and
concrete transformers: :class:`SchemaConverter`, :class:`Deduplicator`,
:class:`SCD2ColumnAdder`, :class:`ColumnAdder`, :class:`PartitionHandler`,
:class:`ColumnNameSanitizer`, :class:`SystemColumnAdder`.
"""

from datacoolie.transformers.base import (
    BaseTransformer,
    TransformerPipeline,
)
from datacoolie.transformers.column_adder import ColumnAdder, SCD2ColumnAdder, SystemColumnAdder
from datacoolie.transformers.column_name_sanitizer import ColumnNameSanitizer
from datacoolie.transformers.deduplicator import Deduplicator
from datacoolie.transformers.partition_handler import PartitionHandler
from datacoolie.transformers.schema_converter import SchemaConverter

__all__ = [
    "BaseTransformer",
    "ColumnAdder",
    "ColumnNameSanitizer",
    "Deduplicator",
    "PartitionHandler",
    "SCD2ColumnAdder",
    "SchemaConverter",
    "SystemColumnAdder",
    "TransformerPipeline",
]

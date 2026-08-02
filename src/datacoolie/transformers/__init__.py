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
from datacoolie.transformers.column_projector import ColumnProjector
from datacoolie.transformers.column_value_transformer import ColumnValueTransformer
from datacoolie.transformers.column_name_sanitizer import ColumnNameSanitizer
from datacoolie.transformers.data_masker import DataMasker
from datacoolie.transformers.deduplicator import Deduplicator
from datacoolie.transformers.hash_column_adder import HashColumnAdder
from datacoolie.transformers.partition_handler import PartitionHandler
from datacoolie.transformers.schema_converter import SchemaConverter
from datacoolie.transformers.row_filter import RowFilter

__all__ = [
    "BaseTransformer",
    "ColumnAdder",
    "ColumnProjector",
    "ColumnValueTransformer",
    "ColumnNameSanitizer",
    "Deduplicator",
    "DataMasker",
    "HashColumnAdder",
    "PartitionHandler",
    "SCD2ColumnAdder",
    "SchemaConverter",
    "RowFilter",
    "SystemColumnAdder",
    "TransformerPipeline",
]

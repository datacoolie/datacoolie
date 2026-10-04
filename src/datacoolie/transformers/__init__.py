"""Transformer components.

Provides :class:`BaseTransformer`, :class:`TransformerPipeline`, and
concrete transformers: :class:`SchemaConverter`, :class:`Deduplicator`,
:class:`SCD2ColumnAdder`, :class:`ColumnAdder`, :class:`PartitionHandler`,
:class:`ColumnNameSanitizer`, :class:`SystemColumnAdder`.
"""

from importlib import import_module


def __getattr__(name):
    modules = {
        "BaseTransformer": "base", "ColumnMapping": "base", "TransformerPipeline": "base",
        "ColumnAdder": "column_adder", "SCD2ColumnAdder": "column_adder", "SystemColumnAdder": "column_adder",
        "ColumnProjector": "column_projector", "ColumnValueTransformer": "column_value_transformer",
        "ColumnNameSanitizer": "column_name_sanitizer", "DataMasker": "data_masker",
        "Deduplicator": "deduplicator", "HashColumnAdder": "hash_column_adder",
        "PartitionHandler": "partition_handler", "SchemaConverter": "schema_converter", "RowFilter": "row_filter",
    }
    if name not in modules:
        raise AttributeError(name)
    return getattr(import_module(f"{__name__}.{modules[name]}"), name)

__all__ = [
    "BaseTransformer",
    "ColumnMapping",
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

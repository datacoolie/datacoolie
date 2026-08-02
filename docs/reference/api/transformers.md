---
title: Transformers — Python API Reference | DataCoolie
description: Python API reference for DataCoolie transformers covering pipeline orchestration, schema conversion, deduplication, row filtering, partition handling, and column adders.
---

# Transformers

::: datacoolie.transformers.base
    options:
      members:
        - BaseTransformer
        - TransformerPipeline

::: datacoolie.transformers.schema_converter
::: datacoolie.transformers.column_value_transformer
::: datacoolie.transformers.hash_column_adder
::: datacoolie.transformers.deduplicator
::: datacoolie.transformers.column_adder
    options:
      members:
        - ColumnAdder
        - SystemColumnAdder
        - SCD2ColumnAdder
::: datacoolie.transformers.row_filter
::: datacoolie.transformers.partition_handler
::: datacoolie.transformers.data_masker
::: datacoolie.transformers.column_projector
::: datacoolie.transformers.column_name_sanitizer

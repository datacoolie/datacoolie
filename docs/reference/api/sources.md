---
title: Sources — Python API Reference | DataCoolie
description: Python API reference for DataCoolie sources covering file, database, Delta, Iceberg, API, and Python function readers.
---

# Sources

::: datacoolie.sources.base

::: datacoolie.sources.file_reader
::: datacoolie.sources.delta_reader
::: datacoolie.sources.iceberg_reader
::: datacoolie.sources.database_reader
::: datacoolie.sources.api_reader
::: datacoolie.sources.python_function_reader

## Source reader extension hooks

Custom source readers implement the selected protected hooks below. The base reader owns range
validation, watermark filtering, timing, and error wrapping; subclasses provide the source read
operation and may opt into exact range support or typed watermark ordering.

::: datacoolie.sources.base.BaseSourceReader._supports_read_range
::: datacoolie.sources.base.BaseSourceReader._watermark_ordering_kinds
::: datacoolie.sources.base.BaseSourceReader._read_internal
::: datacoolie.sources.base.BaseSourceReader._read_data

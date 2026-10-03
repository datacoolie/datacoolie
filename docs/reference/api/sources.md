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

The ETL orchestration boundary requires a complete reader contract: callable
`read`, `get_runtime_info`, `get_new_watermark`,
`_validate_watermark_comparison`, and `merge_watermark` methods. Registered
readers inherit these from `BaseSourceReader`; structurally injected readers
may implement the same contract without inheriting the ABC. A missing hook is
rejected before checkpoint reads and source I/O. Runtime calls must return
`SourceRuntimeInfo`; a missing or malformed runtime object is a contract error,
not an empty-runtime fallback.

::: datacoolie.sources.base.BaseSourceReader._supports_read_range
::: datacoolie.sources.base.BaseSourceReader._watermark_ordering_kinds
::: datacoolie.sources.base.BaseSourceReader._read_internal
::: datacoolie.sources.base.BaseSourceReader._read_data

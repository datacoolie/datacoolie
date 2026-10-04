---
title: Sources — Python API Reference | DataCoolie
description: Python API reference for DataCoolie sources covering file, database, Delta, Iceberg, API, and Python function readers.
---

# Sources

::: datacoolie.sources.base

::: datacoolie.sources.contracts
    options:
      members:
        - SourceReadMode
        - SourceReadUpperOrigin
        - SourceReadRange
        - SourceReadRequest
        - SourceReadSelection
        - SourceReadResult

::: datacoolie.sources.runtime
    options:
      members:
        - SourceRuntimeInfo

::: datacoolie.sources.file_reader
::: datacoolie.sources.delta_reader
::: datacoolie.sources.iceberg_reader
::: datacoolie.sources.database_reader
::: datacoolie.sources.api_reader
::: datacoolie.sources.python_function_reader

## Source reader extension hooks

Custom source readers implement the selected protected hooks below. The base reader owns request
validation, range and watermark filtering, timing, result construction, and error wrapping;
subclasses provide the source read operation and may opt into exact range support or typed
watermark ordering.

The sole public read entry is
`read(source, *, request: SourceReadRequest) -> SourceReadResult[DF]`. The
request carries the caller's original bounds, optional `SourceReadRange`, and
required `reference_time`. The result carries `.frame`, `.rows_read`, the
resolved `.selection`, an optional typed `.candidate`, and an optional detached
`.runtime` projection. Orchestration uses the result for selection, replacement,
and the source-empty gate; runtime observations never become control input.

The ETL orchestration boundary requires callable `read`, `validate_watermark`,
and `merge_watermark` methods. Registered readers inherit these from
`BaseSourceReader`; structurally injected readers may implement the same
contract without inheriting the ABC. Capability and request validation fail
before source I/O. Source runtime evidence is returned with the read result or
attached to a source error; there is no separate source runtime accessor.

Keep `_read_internal(source, request)` as the normal extension seam. The base
template calls it once and constructs the typed result. If a custom reader
replaces the public `read()` template, it must preserve the same request/result
contract and return a `SourceReadResult`.

::: datacoolie.sources.base.BaseSourceReader._supports_read_range
::: datacoolie.sources.base.BaseSourceReader._watermark_ordering_kinds
::: datacoolie.sources.base.BaseSourceReader._read_internal
::: datacoolie.sources.base.BaseSourceReader._read_data

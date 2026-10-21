---
title: Destinations — Python API Reference | DataCoolie
description: Python API reference for DataCoolie destinations covering base writers and format-specific destination implementations.
---

# Destinations

::: datacoolie.destinations.base

::: datacoolie.destinations.runtime
    options:
      members:
        - DestinationRuntimeInfo

## Destination target contract

Destination writers and maintenance use the same pure target projection. A
`resolve_destination_target(destination, engine=engine)` reports the effective
table or path handle, format, and addressing mode. Its `storage_path` retains an
auxiliary path for catalog registration. It performs no catalog or storage I/O.
An unqualified named backend has `identity=None`; use `require_identity=True`
to reject it before maintenance dispatch. Engine-free calls retain the legacy
declared projection for inspection and do not establish physical equality.

::: datacoolie.destinations.resolution.target
    options:
      members:
        - AddressingMode
        - ResolvedDestination
        - resolve_destination_target

## Writer extension hooks

Implementations subclass `BaseDestinationWriter` and provide the two protected
backend hooks below. The public `write(...)` and `run_maintenance(...)` methods
own timing, error wrapping, runtime metrics, and `WindowSpec` validation.

::: datacoolie.destinations.base.BaseDestinationWriter._write_internal

::: datacoolie.destinations.base.BaseDestinationWriter._maintain_internal

::: datacoolie.destinations.file_writer
::: datacoolie.destinations.delta_writer
::: datacoolie.destinations.iceberg_writer
::: datacoolie.destinations.strategies.load

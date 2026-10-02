---
title: Destinations — Python API Reference | DataCoolie
description: Python API reference for DataCoolie destinations covering base writers and format-specific destination implementations.
---

# Destinations

::: datacoolie.destinations.base

## Destination target contract

Destination writers and maintenance use the same pure target projection. A
`ResolvedDestination` reports the effective table or path handle, its format,
addressing mode, and deterministic identity; `resolve_destination_target(...)`
performs no catalog or storage I/O.

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

---
title: Logging — Python API Reference | DataCoolie
description: Python API reference for the DataCoolie logging package — ETLLogger, LogPurpose, and the create_etl_logger factory.
---

# Logging

DataCoolie uses `datacoolie` as its root Python logging namespace. Logger names
returned by `get_logger(__name__)` retain the full module path, and
`datacoolie.*` child loggers are captured by the configured `SystemLogger`.
Forced reconfiguration keeps an enabled `CaptureHandler` attached and migrates
memory/file buffering in place, preventing a detach window for concurrent
records.

::: datacoolie.logging.base

::: datacoolie.logging.etl_logger
    options:
      members:
        - ETLLogger
        - create_etl_logger

::: datacoolie.logging.system_logger
    options:
      members:
        - SystemLogger
        - create_system_logger

::: datacoolie.logging.context

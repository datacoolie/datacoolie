---
title: Orchestration — Python API Reference | DataCoolie
description: Python API reference for the DataCoolie orchestration package — DataCoolieDriver, JobDistributor, and ParallelExecutor.
---

# Orchestration

`RetryHandler` is shared infrastructure in `datacoolie.utils.retry`; it is
documented below but is no longer exported from `datacoolie.orchestration`.

::: datacoolie.orchestration.driver
    options:
      members:
        - DataCoolieDriver

::: datacoolie.orchestration.factory
    options:
      members:
        - create_driver

## Driver transformer hook

Driver subclasses that need to alter the registered transformer pipeline can override the
following narrow hook. Preserve both arguments when delegating to the base implementation so
the run-specific identifier and configured column-name mode reach the pipeline.

::: datacoolie.orchestration.driver.DataCoolieDriver._create_transformer_pipeline

::: datacoolie.orchestration.scheduling.job_distributor
::: datacoolie.orchestration.scheduling.parallel_executor
::: datacoolie.utils.retry
::: datacoolie.orchestration.maintenance

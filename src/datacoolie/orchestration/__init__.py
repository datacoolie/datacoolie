"""Orchestration package — driver, distributor, executor, retry.

Provides the top-level :class:`DataCoolieDriver` and its supporting components:

* :class:`JobDistributor` — assigns dataflows to jobs.
* :class:`ParallelExecutor` / :class:`ExecutionResult` — thread-pool execution.
* :class:`RetryHandler` — exponential-backoff retry logic.
* :func:`create_driver` — convenience factory.
"""

from datacoolie.orchestration.driver import DataCoolieDriver, create_driver
from datacoolie.orchestration.job_distributor import JobDistributor
from datacoolie.orchestration.parallel_executor import ExecutionResult, ParallelExecutor
from datacoolie.orchestration.retry_handler import RetryHandler

__all__ = [
    "DataCoolieDriver",
    "ExecutionResult",
    "JobDistributor",
    "ParallelExecutor",
    "RetryHandler",
    "create_driver",
]

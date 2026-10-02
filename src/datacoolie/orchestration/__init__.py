"""Orchestration package — driver, distributor, and executor.

Provides the top-level :class:`DataCoolieDriver` and its supporting components:

* :class:`JobDistributor` — assigns dataflows to jobs.
* :class:`ParallelExecutor` / :class:`ExecutionResult` — thread-pool execution.
* :func:`create_driver` — convenience factory.
* :func:`generate_chunk_boundaries` — replay chunk boundary generation (from :mod:`datacoolie.utils.chunking`).
"""

from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.orchestration.factory import create_driver
from datacoolie.orchestration.scheduling.job_distributor import JobDistributor
from datacoolie.orchestration.scheduling.parallel_executor import ExecutionResult, ParallelExecutor

__all__ = [
    "DataCoolieDriver",
    "ExecutionResult",
    "JobDistributor",
    "ParallelExecutor",
    "create_driver",
]

"""Orchestration package — driver, distributor, and executor.

Provides the top-level :class:`DataCoolieDriver` and its supporting components:

* :class:`JobDistributor` — assigns dataflows to jobs.
* :class:`ParallelExecutor` / :class:`ExecutionResult` — thread-pool execution.
* :func:`create_driver` — convenience factory.
* :func:`generate_chunk_boundaries` — replay chunk boundary generation (from :mod:`datacoolie.utils.chunking`).
"""

from importlib import import_module


def __getattr__(name):
    modules = {
        "DataCoolieDriver": "driver", "create_driver": "factory",
        "JobDistributor": "scheduling.job_distributor",
        "ExecutionResult": "scheduling.parallel_executor",
        "ParallelExecutor": "scheduling.parallel_executor",
    }
    if name not in modules:
        raise AttributeError(name)
    return getattr(import_module(f"{__name__}.{modules[name]}"), name)

__all__ = [
    "DataCoolieDriver",
    "ExecutionResult",
    "JobDistributor",
    "ParallelExecutor",
    "create_driver",
]

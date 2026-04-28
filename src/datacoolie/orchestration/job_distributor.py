"""Job distribution logic for parallel execution of dataflows.

``JobDistributor`` assigns dataflows to jobs using either hash-based or
group-based distribution, and groups them for sequential execution within
a group.

Distribution rules:
    * **group_number is not None** → ``group_number % job_num == job_index``
    * **group_number is None** → ``hash(dataflow_id) % job_num == job_index``

Within a group, dataflows are sorted by ``execution_order``.
"""

from __future__ import annotations

import hashlib
from typing import Dict, List, Optional

from datacoolie.core.models import DataFlow


class JobDistributor:
    """Distribute dataflows across parallel jobs.

    Example::

        distributor = JobDistributor(job_num=4, job_index=0)
        my_dataflows = distributor.filter_dataflows(all_dataflows)
        groups = distributor.group_dataflows(my_dataflows)
    """

    def __init__(self, job_num: int = 1, job_index: int = 0) -> None:
        if job_num < 1:
            raise ValueError("job_num must be at least 1")
        if job_index < 0 or job_index >= job_num:
            raise ValueError(f"job_index must be between 0 and {job_num - 1}")

        self._job_num = job_num
        self._job_index = job_index

    @property
    def job_num(self) -> int:
        return self._job_num

    @property
    def job_index(self) -> int:
        return self._job_index

    # ------------------------------------------------------------------
    # Assignment
    # ------------------------------------------------------------------

    def should_process(self, dataflow: DataFlow) -> bool:
        """Check whether this job should process the given dataflow.

        Uses ``group_number`` when set, otherwise hash-based distribution.
        """
        if self._job_num == 1:
            return True

        group_number = dataflow.group_number
        if group_number is not None:
            return (group_number % self._job_num) == self._job_index

        return self._hash_distribution(dataflow.dataflow_id)

    def _hash_distribution(self, dataflow_id: str) -> bool:
        """Consistent hash-based assignment using MD5."""
        hash_value = int(
            hashlib.md5(str(dataflow_id).encode("utf-8"), usedforsecurity=False).hexdigest(), 16
        )
        return (hash_value % self._job_num) == self._job_index

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def filter_dataflows(
        self,
        dataflows: List[DataFlow],
        active_only: bool = True,
    ) -> List[DataFlow]:
        """Return the subset of *dataflows* assigned to this job.

        Args:
            dataflows: All available dataflows.
            active_only: Skip inactive dataflows.

        Returns:
            Filtered list of dataflows for this job.
        """
        result: List[DataFlow] = []
        for df in dataflows:
            if active_only and not df.is_active:
                continue
            if self.should_process(df):
                result.append(df)
        return result

    # ------------------------------------------------------------------
    # Grouping
    # ------------------------------------------------------------------

    def group_dataflows(
        self,
        dataflows: List[DataFlow],
    ) -> Dict[Optional[int], List[DataFlow]]:
        """Group dataflows by ``group_number``, sorted by ``execution_order``.

        Dataflows with ``group_number = None`` are placed under the
        ``None`` key and can run independently in parallel.

        Returns:
            Mapping of group_number → sorted list of dataflows.
        """
        groups: Dict[Optional[int], List[DataFlow]] = {}

        for df in dataflows:
            key = df.group_number
            groups.setdefault(key, []).append(df)

        for key in groups:
            groups[key].sort(key=lambda x: x.execution_order or 0)

        return groups

    def get_independent_dataflows(
        self,
        dataflows: List[DataFlow],
    ) -> List[DataFlow]:
        """Return dataflows with no group (``group_number is None``)."""
        return [df for df in dataflows if df.group_number is None]

    def get_grouped_dataflows(
        self,
        dataflows: List[DataFlow],
    ) -> Dict[int, List[DataFlow]]:
        """Return only grouped dataflows (``group_number is not None``).

        Returns:
            Mapping of group_number → sorted list of dataflows.
        """
        groups = self.group_dataflows(dataflows)
        return {k: v for k, v in groups.items() if k is not None}

    def __repr__(self) -> str:
        return f"JobDistributor(job_num={self._job_num}, job_index={self._job_index})"

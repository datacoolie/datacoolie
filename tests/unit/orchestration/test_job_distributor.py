"""Tests for datacoolie.orchestration.job_distributor."""

from __future__ import annotations

import pytest

from datacoolie.core.models import Connection, DataFlow, Destination, Source
from datacoolie.orchestration.job_distributor import JobDistributor


# ============================================================================
# Helpers
# ============================================================================


def _make_dataflow(
    dataflow_id: str = "df-1",
    group_number: int | None = None,
    execution_order: int | None = None,
    is_active: bool = True,
) -> DataFlow:
    conn = Connection(name="test_conn", format="delta", configure={"base_path": "/data"})
    return DataFlow(
        dataflow_id=dataflow_id,
        group_number=group_number,
        execution_order=execution_order,
        is_active=is_active,
        source=Source(connection=conn, table="src"),
        destination=Destination(connection=conn, table="dst"),
    )


# ============================================================================
# Init / validation
# ============================================================================


class TestJobDistributorInit:
    def test_defaults(self):
        d = JobDistributor()
        assert d.job_num == 1
        assert d.job_index == 0

    def test_custom(self):
        d = JobDistributor(job_num=4, job_index=2)
        assert d.job_num == 4
        assert d.job_index == 2

    def test_invalid_job_num(self):
        with pytest.raises(ValueError, match="job_num"):
            JobDistributor(job_num=0)

    def test_invalid_job_index_negative(self):
        with pytest.raises(ValueError, match="job_index"):
            JobDistributor(job_num=2, job_index=-1)

    def test_invalid_job_index_too_large(self):
        with pytest.raises(ValueError, match="job_index"):
            JobDistributor(job_num=2, job_index=2)

    def test_repr(self):
        d = JobDistributor(job_num=3, job_index=1)
        assert "JobDistributor" in repr(d)
        assert "job_num=3" in repr(d)


# ============================================================================
# should_process
# ============================================================================


class TestShouldProcess:
    def test_single_job_always_processes(self):
        d = JobDistributor(job_num=1, job_index=0)
        df = _make_dataflow("anything")
        assert d.should_process(df) is True

    def test_group_based_distribution(self):
        d0 = JobDistributor(job_num=2, job_index=0)
        d1 = JobDistributor(job_num=2, job_index=1)

        df_even = _make_dataflow(group_number=10)
        df_odd = _make_dataflow(group_number=11)

        # 10 % 2 == 0 → job_index 0; 11 % 2 == 1 → job_index 1
        assert d0.should_process(df_even) is True
        assert d0.should_process(df_odd) is False
        assert d1.should_process(df_even) is False
        assert d1.should_process(df_odd) is True

    def test_hash_based_distribution_is_deterministic(self):
        d = JobDistributor(job_num=4, job_index=0)
        df = _make_dataflow(dataflow_id="stable-id")
        result1 = d.should_process(df)
        result2 = d.should_process(df)
        assert result1 == result2

    def test_hash_distribution_covers_all_jobs(self):
        """All dataflows are assigned to some job."""
        dfs = [_make_dataflow(dataflow_id=f"df-{i}") for i in range(20)]
        assigned: set[int] = set()
        for idx in range(4):
            d = JobDistributor(job_num=4, job_index=idx)
            for df in dfs:
                if d.should_process(df):
                    assigned.add(idx)
        # At least some jobs got work
        assert len(assigned) > 0


# ============================================================================
# filter_dataflows
# ============================================================================


class TestFilterDataflows:
    def test_active_only_filter(self):
        d = JobDistributor()
        active = _make_dataflow(is_active=True)
        inactive = _make_dataflow(is_active=False)
        result = d.filter_dataflows([active, inactive], active_only=True)
        assert result == [active]

    def test_include_inactive(self):
        d = JobDistributor()
        active = _make_dataflow(is_active=True)
        inactive = _make_dataflow(is_active=False)
        result = d.filter_dataflows([active, inactive], active_only=False)
        assert len(result) == 2

    def test_empty_list(self):
        d = JobDistributor()
        assert d.filter_dataflows([]) == []


# ============================================================================
# group_dataflows
# ============================================================================


class TestGroupDataflows:
    def test_none_group(self):
        d = JobDistributor()
        df1 = _make_dataflow(dataflow_id="a")
        df2 = _make_dataflow(dataflow_id="b")
        groups = d.group_dataflows([df1, df2])
        assert None in groups
        assert len(groups[None]) == 2

    def test_numbered_groups(self):
        d = JobDistributor()
        df1 = _make_dataflow(dataflow_id="a", group_number=1, execution_order=2)
        df2 = _make_dataflow(dataflow_id="b", group_number=1, execution_order=1)
        df3 = _make_dataflow(dataflow_id="c", group_number=2, execution_order=0)
        groups = d.group_dataflows([df1, df2, df3])

        assert 1 in groups
        assert 2 in groups
        # Sorted by execution_order within group 1
        assert groups[1][0].dataflow_id == "b"  # order=1
        assert groups[1][1].dataflow_id == "a"  # order=2

    def test_mixed_groups(self):
        d = JobDistributor()
        independent = _make_dataflow(dataflow_id="ind")
        grouped = _make_dataflow(dataflow_id="grp", group_number=5)
        groups = d.group_dataflows([independent, grouped])
        assert None in groups
        assert 5 in groups


# ============================================================================
# get_independent / get_grouped
# ============================================================================


class TestGetHelpers:
    def test_get_independent(self):
        d = JobDistributor()
        ind = _make_dataflow(dataflow_id="ind")
        grp = _make_dataflow(dataflow_id="grp", group_number=1)
        result = d.get_independent_dataflows([ind, grp])
        assert len(result) == 1
        assert result[0].dataflow_id == "ind"

    def test_get_grouped(self):
        d = JobDistributor()
        ind = _make_dataflow(dataflow_id="ind")
        g1 = _make_dataflow(dataflow_id="g1", group_number=1, execution_order=1)
        g2 = _make_dataflow(dataflow_id="g2", group_number=1, execution_order=2)
        result = d.get_grouped_dataflows([ind, g1, g2])
        assert 1 in result
        assert None not in result
        assert len(result[1]) == 2


# ============================================================================
# Additional edge cases (merged from edge-case module)
# ============================================================================


class TestDistributorEdgeCases:
    def test_get_grouped_dataflows_when_no_none_group(self):
        d = JobDistributor()
        grouped = d.get_grouped_dataflows([
            _make_dataflow("a", group_number=1),
            _make_dataflow("b", group_number=2),
        ])
        assert set(grouped.keys()) == {1, 2}

    def test_get_grouped_dataflows_when_only_none_group(self):
        d = JobDistributor()
        grouped = d.get_grouped_dataflows([_make_dataflow("a", group_number=None)])
        assert grouped == {}

    def test_filter_dataflows_excludes_unassigned_for_job(self):
        d = JobDistributor(job_num=2, job_index=0)
        result = d.filter_dataflows([_make_dataflow("a", group_number=1)], active_only=True)
        assert result == []

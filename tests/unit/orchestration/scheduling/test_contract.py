"""Observable scheduling boundaries; timeouts guard deadlocks, not performance."""

from collections import Counter
from threading import Barrier, Event, Lock
import time

import pytest

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.runtime import DataFlowRuntimeInfo
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.source import Source
from datacoolie.orchestration.scheduling.job_distributor import JobDistributor
from datacoolie.orchestration.scheduling.parallel_executor import ParallelExecutor


def _flow(name, group=None, order=None):
    connection = Connection(name="test", format="delta", configure={"base_path": "/data"})
    return DataFlow(
        dataflow_id=name, group_number=group, execution_order=order,
        source=Source(connection=connection, table="source"),
        destination=Destination(connection=connection, table=name),
    )


def _success(flow):
    return DataFlowRuntimeInfo(dataflow_id=flow.dataflow_id, status="succeeded")


def _run(flows, process, *, workers=2, stop=False, callback=None):
    # All inputs belong to this one job; groups are scheduling units inside it.
    distributor = JobDistributor()
    selected = distributor.filter_dataflows(flows)
    assert selected == flows
    return ParallelExecutor(max_workers=workers, stop_on_error=stop).execute_with_groups(
        distributor.group_dataflows(selected), process, callback=callback,
    )


@pytest.mark.parametrize("groups", [(None, None), (1, 8)], ids=["ungrouped", "distinct-groups"])
def test_different_orders_overlap_in_one_job_unless_they_share_a_group(groups):
    rendezvous = Barrier(2, timeout=5)

    def process(flow):
        rendezvous.wait()  # A serial scheduler cannot complete either callback.
        return _success(flow)

    result = _run([_flow("early", groups[0], 0), _flow("late", groups[1], 99)], process)
    assert (result.succeeded, result.failed, result.pending) == (2, 0, 0), result.errors


@pytest.mark.parametrize("group", [0, 7])
def test_tied_orders_overlap_and_later_order_waits_for_every_completion(group):
    tied = Barrier(2, timeout=5)
    completed = {name: Event() for name in ("null", "zero")}
    later_started = Event()

    def process(flow):
        if flow.dataflow_id in completed:
            assert not later_started.is_set()
            tied.wait()  # Null order and explicit zero must share one bucket.
        else:
            later_started.set()
            assert all(event.is_set() for event in completed.values())
        return _success(flow)

    def callback(result):
        if result.dataflow_id in completed:
            completed[result.dataflow_id].set()

    # Deliberately reverse the dependency order in metadata.
    result = _run(
        [_flow("later", group, 1), _flow("null", group), _flow("zero", group, 0)],
        process, callback=callback,
    )
    assert later_started.is_set()
    assert (result.succeeded, result.failed, result.pending) == (3, 0, 0), result.errors


def test_global_group_scheduler_respects_max_workers():
    state = {"active": 0, "maximum": 0}
    lock = Lock()

    def process(flow):
        with lock:
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
        time.sleep(0.02)
        with lock:
            state["active"] -= 1
        return _success(flow)

    flows = [_flow(f"{group}-{item}", group, 0) for group in (1, 2) for item in (1, 2)]
    result = _run(flows, process, workers=2)
    assert (result.succeeded, result.failed) == (4, 0), result.errors
    assert state["maximum"] <= 2


@pytest.mark.parametrize("stop", [False, True])
def test_numbered_group_failure_only_stops_its_later_orders(stop):
    failure_observed = Event()
    visited = []

    def process(flow):
        visited.append(flow.dataflow_id)
        if flow.dataflow_id == "failure":
            raise RuntimeError("deliberate failure")
        assert failure_observed.wait(5)
        return _success(flow)

    def callback(result):
        if result.dataflow_id == "failure":
            failure_observed.set()

    result = _run(
        [_flow("failure", 0, 0), _flow("dependent", 0, 1),
         _flow("other-group", 2, 0), _flow("independent")],
        process, stop=stop, workers=3, callback=callback,
    )
    assert set(visited) == {"failure", "other-group", "independent"} | (
        set() if stop else {"dependent"}
    )
    assert (result.failed, result.pending, result.succeeded) == (1, int(stop), 3 - int(stop))
    assert result.errors == {"failure": "deliberate failure"}


def test_stop_on_error_withholds_unadmitted_independent_and_group_work():
    failure_observed = Event()
    visited = []

    def process(flow):
        visited.append(flow.dataflow_id)
        if flow.dataflow_id == "failure":
            raise RuntimeError("independent failure")
        assert failure_observed.wait(5)
        return _success(flow)

    result = _run(
        [_flow("failure", order=0), _flow("queued", order=1),
         _flow("group-first", 0, 0), _flow("group-next", 0, 1)],
        process, workers=1, stop=True,
    )
    assert visited == ["failure"]
    assert (result.succeeded, result.failed, result.pending) == (0, 1, 3)
    assert result.errors == {"failure": "independent failure"}


@pytest.mark.parametrize("jobs,hash_owners", [(1, [0] * 6), (3, [1, 1, 1, 0, 1, 2]),
                                             (4, [1, 3, 3, 1, 2, 3])])
def test_partitions_are_stable_exhaustive_disjoint_and_keep_explicit_groups(jobs, hash_owners):
    # Golden MD5 assignments make stability stronger than merely calling twice.
    flows = [_flow(name) for name in "abcdef"]
    flows += [_flow(f"group-{group}-{order}", group, order)
              for group in (0, 1, 4, 8) for order in (0, 1)]
    expected = dict(zip("abcdef", hash_owners))
    expected.update({f"group-{group}-{order}": group % jobs
                     for group in (0, 1, 4, 8) for order in (0, 1)})
    partitions = [
        {flow.dataflow_id for flow in JobDistributor(jobs, index).filter_dataflows(flows)}
        for index in range(jobs)
    ]
    assert Counter(name for partition in partitions for name in partition) == Counter(expected.keys())
    for index, partition in enumerate(partitions):
        assert partition == {name for name, owner in expected.items() if owner == index}
        reconstructed = [_flow(f.dataflow_id, f.group_number, f.execution_order)
                         for f in reversed(flows)]
        assert {f.dataflow_id for f in JobDistributor(jobs, index).filter_dataflows(reconstructed)} == partition

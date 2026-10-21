import pytest

from datacoolie.core.models.metrics import METRIC_FIELDS, ExecutionMetrics, MetricAccumulator


def _compact_metrics() -> ExecutionMetrics:
    return ExecutionMetrics(
        files_added=3,
        files_removed=2,
        bytes_added=300,
        bytes_removed=200,
        bytes_saved=100,
    )


def _cleanup_metrics() -> ExecutionMetrics:
    return ExecutionMetrics(files_removed=1, bytes_removed=100)


def test_metric_inventory_is_fixed_and_nullable_by_default() -> None:
    metrics = ExecutionMetrics()
    assert tuple(metrics.__dataclass_fields__) == METRIC_FIELDS
    assert all(getattr(metrics, name) is None for name in METRIC_FIELDS)


@pytest.mark.parametrize("values", [
    (_compact_metrics(), _cleanup_metrics()),
    (_cleanup_metrics(), _compact_metrics()),
])
def test_observed_values_sum_independently_in_either_order(values) -> None:
    accumulator = MetricAccumulator()
    for metrics in values:
        accumulator.add(metrics)

    assert accumulator.metrics.files_added == 3
    assert accumulator.metrics.files_removed == 3
    assert accumulator.metrics.bytes_added == 300
    assert accumulator.metrics.bytes_removed == 300
    assert accumulator.metrics.bytes_saved == 100


def test_observed_zero_is_preserved_and_missing_is_ignored() -> None:
    measured_zero = MetricAccumulator().add(ExecutionMetrics(files_added=0))
    measured_zero.add(ExecutionMetrics())
    assert measured_zero.metrics.files_added == 0
    assert measured_zero.metrics.files_removed is None

    observed = MetricAccumulator().add(ExecutionMetrics(files_added=4))
    observed.add(ExecutionMetrics())
    assert observed.metrics.files_added == 4


def test_empty_and_all_null_inputs_remain_unknown() -> None:
    assert MetricAccumulator().metrics.files_added is None
    accumulator = MetricAccumulator().add(ExecutionMetrics())
    assert accumulator.metrics.files_added is None


@pytest.mark.parametrize("value", [True, False, -1, 1.5, "1"])
def test_invalid_metric_values_are_not_observations(value) -> None:
    accumulator = MetricAccumulator().add({"files_added": value})
    assert accumulator.metrics.files_added is None


def test_reset_clears_contributions_and_returns_unknown_defaults() -> None:
    accumulator = MetricAccumulator().add(ExecutionMetrics(files_added=1))
    accumulator.reset()

    assert all(getattr(accumulator.metrics, name) is None for name in METRIC_FIELDS)


def test_bytes_saved_sums_independent_per_contribution_observations() -> None:
    accumulator = MetricAccumulator()
    accumulator.add(
        ExecutionMetrics(bytes_added=100, bytes_removed=200, bytes_saved=100),
    )
    accumulator.add(
        ExecutionMetrics(bytes_added=200, bytes_removed=100, bytes_saved=0),
    )

    assert accumulator.metrics.bytes_added == 300
    assert accumulator.metrics.bytes_removed == 300
    assert accumulator.metrics.bytes_saved == 100


def test_execution_metrics_sum_uses_the_observed_reducer() -> None:
    assert ExecutionMetrics.sum(
        [ExecutionMetrics(files_added=100), ExecutionMetrics()]
    ).files_added == 100
    assert ExecutionMetrics.sum(
        [ExecutionMetrics(files_added=0), ExecutionMetrics()]
    ).files_added == 0
    assert ExecutionMetrics.sum([]).files_added is None

"""Runtime reference rendering and documented boundary examples."""

from dataclasses import dataclass, field, fields
import inspect
import re

import pytest

from datacoolie.core.exceptions import ConfigurationError, SourceError
from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.sources.python_function_reader import PythonFunctionReader
from docs.scripts._runtime_reference import render_field_table, render_runtime_reference


def test_every_runtime_field_has_a_visible_table_row() -> None:
    content = render_runtime_reference()
    for model in (DataCoolieRunConfig, ReplayConfig, LogConfig):
        for item in fields(model):
            assert f"| `{item.name}` | `{item.type}` |" in content
    assert "generated per instance (`generate_unique_id`)" in content
    assert "new `[]` per instance" in content
    assert '| `output_path` | `Optional[str]` | `None` |' in content


def test_renderer_does_not_evaluate_default_factories_and_detects_drift() -> None:
    def forbidden():
        raise AssertionError("Rendering must not allocate runtime identity or state")

    @dataclass
    class Example:
        identity: str = field(default_factory=forbidden)

    assert "generated per instance (`forbidden`)" in render_field_table(Example, {"identity": "Identity"})
    with pytest.raises(ValueError, match="field notes disagree"):
        render_field_table(Example, {})


def test_exact_runtime_example_executes_and_detaches_correlation_context() -> None:
    content = render_runtime_reference()
    script = re.search(r"```python\n(.*?)\n```", content, re.DOTALL).group(1)
    namespace = {}
    exec(compile(script, "runtime-reference-example", "exec"), namespace)
    assert namespace["config"].max_workers == 2
    attributes = {"nested": [1]}
    config = DataCoolieRunConfig(run_attributes=attributes)
    attributes["nested"].append(2)
    assert config.run_attributes == {"nested": [1]}


def test_documented_driver_config_keyword_matches_actual_constructor() -> None:
    from datacoolie.orchestration.driver import DataCoolieDriver

    content = render_runtime_reference()
    keyword = re.search(r"Supply this object as `(\w+)=`", content).group(1)
    signature = inspect.signature(DataCoolieDriver)
    signature.bind_partial(**{keyword: DataCoolieRunConfig()})
    assert keyword == "config"
    assert "accepts run fields" in content


@pytest.mark.parametrize("kwargs", [
    {"job_num": 0}, {"job_num": 2, "job_index": 2}, {"max_workers": 0},
    {"retry_count": -1}, {"retention_hours": -1}, {"run_attributes": "{}"},
    {"run_attributes": {"bad": float("inf")}},
])
def test_documented_run_constraints_use_real_model_validation(kwargs) -> None:
    with pytest.raises(ConfigurationError):
        DataCoolieRunConfig(**kwargs)


@pytest.mark.parametrize("kwargs", [
    {"spool_max_bytes": 1, "buffer_memory_bytes": 2},
    {"flush_interval_seconds": float("inf")}, {"flush_batch_bytes": True},
    {"close_timeout_seconds": 0}, {"partition_pattern": "{day}"},
])
def test_documented_logging_constraints_use_real_model_validation(kwargs) -> None:
    with pytest.raises(ValueError):
        LogConfig(**kwargs)


def test_zero_flush_interval_is_valid_and_replay_deep_validation_is_deferred() -> None:
    assert LogConfig(flush_interval_seconds=0).flush_interval_seconds == 0
    # Construction accepts the shape; execution owns ordering and chunk units.
    assert ReplayConfig(start=2, end=1, chunk_interval={"unknown": 1}).end == 1


def test_function_prefix_defaults_are_unrestricted_and_nonempty_values_restrict() -> None:
    import json

    config = DataCoolieRunConfig()
    assert config.allowed_function_prefixes == []
    assert PythonFunctionReader._resolve_function("json.loads", config.allowed_function_prefixes) is json.loads
    assert PythonFunctionReader._resolve_function("json.loads", ["json."]) is json.loads
    with pytest.raises(SourceError, match="is not allowed"):
        PythonFunctionReader._resolve_function("json.loads", ["trusted_project.sources."])
    assert "An empty list applies no prefix restriction" in render_runtime_reference()

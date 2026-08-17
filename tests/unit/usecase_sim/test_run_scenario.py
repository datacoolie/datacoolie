"""Tests for declarative validation in the usecase-sim scenario runner."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
RUNNER_PATH = REPO_ROOT / "usecase-sim" / "runner" / "run_scenario.py"
TRANSFORMER_METADATA_PATH = (
    REPO_ROOT / "usecase-sim" / "metadata" / "file" / "transformer_features.json"
)
SCENARIOS_PATH = REPO_ROOT / "usecase-sim" / "scenarios" / "scenarios.json"
TRANSFORM_FEATURE_KEYS = {
    "value_rules",
    "schema_hints",
    "hash_columns",
    "masking_rules",
    "select_columns",
    "drop_columns",
    "rename_columns",
    "deduplicate_columns",
}
PRIMARY_FEATURE_BY_NAME_PREFIX = {
    "transform_value__": "value_rules",
    "transform_schema__": "schema_hints",
    "transform_hash__": "hash_columns",
    "transform_mask__": "masking_rules",
    "transform_projection__select": "select_columns",
    "transform_projection__drop": "drop_columns",
    "transform_projection__batch_rename": "rename_columns",
    "transform_missing_policy__value_rule": "value_rules",
    "transform_missing_policy__hash": "hash_columns",
    "transform_missing_policy__masking": "masking_rules",
    "transform_missing_policy__projection": "select_columns",
    "transform_dedup__": "deduplicate_columns",
    "transform_sanitizer__": "column_name_sanitizer",
}
SPEC = importlib.util.spec_from_file_location("usecase_sim_run_scenario", RUNNER_PATH)
assert SPEC and SPEC.loader
run_scenario = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(run_scenario)

UTILS_PATH = REPO_ROOT / "usecase-sim" / "runner" / "_runner_utils.py"
UTILS_SPEC = importlib.util.spec_from_file_location(
    "usecase_sim_runner_utils", UTILS_PATH
)
assert UTILS_SPEC and UTILS_SPEC.loader
runner_utils = importlib.util.module_from_spec(UTILS_SPEC)
UTILS_SPEC.loader.exec_module(runner_utils)


def test_transformer_fixture_declares_one_primary_feature_per_dataflow() -> None:
    metadata = json.loads(TRANSFORMER_METADATA_PATH.read_text(encoding="utf-8"))
    output_tables: list[str] = []

    for dataflow in metadata["dataflows"]:
        transform = dataflow["transform"]
        matching_primary_features = {
            feature
            for prefix, feature in PRIMARY_FEATURE_BY_NAME_PREFIX.items()
            if dataflow["name"].startswith(prefix)
        }
        assert len(matching_primary_features) == 1, (
            f"{dataflow['name']} must identify exactly one primary feature by name; "
            f"found {sorted(matching_primary_features)}"
        )
        primary_feature = matching_primary_features.pop()
        if primary_feature == "column_name_sanitizer":
            assert transform == {}
            assert dataflow["source"]["table"] == "collision"
        else:
            assert primary_feature in TRANSFORM_FEATURE_KEYS
            assert transform.get(primary_feature), (
                f"{dataflow['name']} does not configure its primary feature "
                f"{primary_feature!r}"
            )
        output_tables.append(dataflow["destination"]["table"])

    assert len(output_tables) == len(set(output_tables))


def test_transformer_failure_stages_are_isolated() -> None:
    metadata = json.loads(TRANSFORMER_METADATA_PATH.read_text(encoding="utf-8"))
    failure_stages = {
        "transform_features_invalid_fill",
        "transform_features_invalid_redact",
        "transform_features_sanitizer_collision",
        "transform_features_dedup_strict",
    }
    counts = {
        stage: sum(dataflow["stage"] == stage for dataflow in metadata["dataflows"])
        for stage in failure_stages
    }

    assert counts == {stage: 1 for stage in failure_stages}


def test_format_specific_file_scenarios_use_their_real_metadata_files() -> None:
    scenarios = json.loads(SCENARIOS_PATH.read_text(encoding="utf-8"))

    yaml_scenario = scenarios["local_polars_file_yaml"]
    excel_scenario = scenarios["local_polars_file_excel"]

    assert Path(yaml_scenario["metadata_path"]).suffix == ".yaml"
    assert not yaml_scenario.get("skip_api_sources", False)
    assert Path(excel_scenario["metadata_path"]).suffix == ".xlsx"
    assert excel_scenario["skip_api_sources"] is True


def test_broad_spark_scenarios_isolate_growing_delta_schema_history() -> None:
    scenarios = json.loads(SCENARIOS_PATH.read_text(encoding="utf-8"))
    expected_paths = {
        "usecase-sim/data/output/delta/orders_full_load",
        "usecase-sim/data/output/delta/orders_overwritten",
        "usecase-sim/data/output/delta/orders_merged_nwm",
        "usecase-sim/data/output/delta/orders_schema_evolve",
        "usecase-sim/data/output/delta/orders_read_delta_schema_evolve",
        "usecase-sim/data/output/delta/orders_read_delta_schema_evolve_fewer",
    }

    for scenario_name in (
        "local_spark_file",
        "local_spark_database",
        "local_spark_api",
    ):
        assert set(scenarios[scenario_name]["pre_clean_paths"]) == expected_paths


def test_pre_clean_paths_rejects_targets_outside_output_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(run_scenario, "DATACOOLIE_ROOT", tmp_path)

    with pytest.raises(ValueError, match="must resolve below"):
        run_scenario._pre_clean_paths(
            {"pre_clean_paths": ["usecase-sim/data/output/../../../outside"]}
        )


def test_expected_failure_passes_when_console_contains_required_text(
    tmp_path: Path,
) -> None:
    console_log = tmp_path / "scenario.log"
    console_log.write_text("Column 'missing_order' not found", encoding="utf-8")
    scenario = {
        "validation": {
            "expected_exit_code": 2,
            "required_console_text": ["missing_order"],
        }
    }

    result = run_scenario._validate_scenario_result(scenario, 2, console_log)

    assert result == (0, "PASS (expected exit 2)")


def test_missing_required_console_text_fails(tmp_path: Path) -> None:
    console_log = tmp_path / "scenario.log"
    console_log.write_text("different error", encoding="utf-8")
    scenario = {
        "validation": {
            "expected_exit_code": 2,
            "required_console_text": ["missing_order"],
        }
    }

    result_code, status = run_scenario._validate_scenario_result(
        scenario, 2, console_log
    )

    assert result_code == 1
    assert "console missing expected text" in status


@pytest.mark.parametrize(
    "script_body, expected_code",
    [("print('validated')", 0), ("raise SystemExit(3)", 3)],
)
def test_repository_local_validation_script_controls_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    script_body: str,
    expected_code: int,
) -> None:
    console_log = tmp_path / "scenario.log"
    console_log.write_text("pipeline complete", encoding="utf-8")
    validator = tmp_path / "validator.py"
    validator.write_text(script_body, encoding="utf-8")
    monkeypatch.setattr(run_scenario, "DATACOOLIE_ROOT", tmp_path)
    scenario = {"validation": {"script": "validator.py"}}

    result_code, _ = run_scenario._validate_scenario_result(
        scenario, 0, console_log
    )

    assert result_code == expected_code


def test_non_iceberg_spark_session_omits_iceberg_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}

    def _fake_session(*, app_name: str, config: dict):
        captured.update(config)
        return object()

    monkeypatch.setattr(runner_utils, "get_or_create_spark_session", _fake_session)
    monkeypatch.setattr(runner_utils, "_resolve_packages", lambda **_: None)

    runner_utils.build_spark_session(needs_iceberg=False)

    assert captured["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    assert "spark.sql.iceberg.merge-schema" not in captured
    assert not any(key.startswith("spark.sql.catalog.local_catalog") for key in captured)


def test_local_spark_session_can_disable_local_checksum_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = MagicMock()
    local_fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.getLocal.return_value
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration.return_value

    monkeypatch.setattr(
        runner_utils,
        "get_or_create_spark_session",
        lambda **_: spark,
    )
    monkeypatch.setattr(runner_utils, "_resolve_packages", lambda **_: None)

    result = runner_utils.build_spark_session(
        needs_iceberg=False,
        verify_local_file_checksums=False,
    )

    assert result is spark
    spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.getLocal.assert_called_once_with(
        hadoop_conf
    )
    local_fs.setVerifyChecksum.assert_called_once_with(False)

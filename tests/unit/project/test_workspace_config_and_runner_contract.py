from __future__ import annotations

from pathlib import Path

import pytest

from datacoolie.project.config import project_config_from_mapping
from datacoolie.project.errors import ProjectConfigError


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
LOCAL_RUNNER_PATH = PRODUCT_ROOT / "docs" / "examples" / "files" / "runners" / "local" / "run.py"


def test_project_contract_accepts_multiple_sql_and_function_roots() -> None:
    config = project_config_from_mapping({
        "schema_version": 1,
        "project": {"name": "example"},
        "components": {
            "metadata": {"path": "metadata"},
            "sql": [{"path": "sql/orders"}, {"path": "sql/reporting"}],
            "functions": [{"path": "functions/loaders", "packaging": "auto"}],
        },
        "environments": {"dev": {"platform": "local"}},
    })
    assert config.component_paths["sql"] == ("sql/orders", "sql/reporting")
    assert config.component_paths["functions"] == ("functions/loaders",)


def test_project_contract_rejects_component_overlap_and_unknown_fields() -> None:
    with pytest.raises(ProjectConfigError, match="overlap"):
        project_config_from_mapping({
            "project": {"name": "example"},
            "components": {"metadata": {"path": "metadata"}, "sql": {"path": "metadata/sql"}},
            "environments": {"dev": {"platform": "local"}},
        })
    with pytest.raises(ProjectConfigError, match="Unsupported"):
        project_config_from_mapping({
            "project": {"name": "example"},
            "components": {"metadata": {"path": "metadata", "paths": ["metadata"]}},
            "environments": {"dev": {"platform": "local"}},
        })


def test_runner_exposes_artifact_sql_state_log_and_run_attributes() -> None:
    content = LOCAL_RUNNER_PATH.read_text(encoding="utf-8")
    for token in (
        "--artifact-base-path",
        "--metadata-base-path",
        "--sql-base-path",
        "--state-base-path",
        "--watermark-base-path",
        "--log-base-path",
        "--run-attributes-json",
        "run_attributes=args.run_attributes_json",
        "log_base_path=args.log_base_path",
    ):
        assert token in content
    assert "base_log_path" not in content


def test_runner_keeps_stage_as_one_framework_argument() -> None:
    content = LOCAL_RUNNER_PATH.read_text(encoding="utf-8")
    assert "result = driver.run(stage=args.stage)" in content
    assert content.count("driver.run(") == 1



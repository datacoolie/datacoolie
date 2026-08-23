"""Unit tests for real-cloud integration configuration and safety gates."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.integration.cloud_config import (
    MissingCloudVariables,
    pytest_configure_cloud,
    pytest_gate_cloud_items,
    resolve_aws_config,
    resolve_databricks_config,
    resolve_fabric_config,
)


class StubPytestConfig:
    def __init__(self, *, enabled: bool, env_file: str | None = None) -> None:
        self.options = {
            "--cloud-integration": enabled,
            "--integration-env-file": env_file,
        }

    def getoption(self, name: str):
        return self.options[name]


class StubGateConfig:
    def __init__(
        self,
        *,
        cloud_integration: bool,
        cloud_platform: list[str] | None = None,
        run_benchmarks: bool = False,
    ) -> None:
        self.options = {
            "--cloud-integration": cloud_integration,
            "--cloud-platform": cloud_platform,
            "--run-benchmarks": run_benchmarks,
        }

    def getoption(self, name: str):
        return self.options[name]


class StubMarker:
    def __init__(self, *args: object) -> None:
        self.args = args


class StubCloudItem:
    def __init__(self, *markers: str) -> None:
        self.markers = set(markers)
        self.added_markers: list[pytest.MarkDecorator] = []

    def get_closest_marker(self, name: str):
        if name == "cloud_platform" and name in self.markers:
            return StubMarker("aws")
        return object() if name in self.markers else None

    def add_marker(self, marker: pytest.MarkDecorator) -> None:
        self.added_markers.append(marker)


def test_fabric_config_derives_onelake_uri_from_resource_ids() -> None:
    config = resolve_fabric_config(
        {
            "DATACOOLIE_FABRIC_TENANT_ID": "tenant-id",
            "DATACOOLIE_FABRIC_WORKSPACE_ID": "workspace-id",
            "DATACOOLIE_FABRIC_LAKEHOUSE_ID": "lakehouse-id",
        }
    )

    assert config.onelake_root == (
        "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/lakehouse-id/Files"
    )
    assert config.benchmark_root == "Files"


def test_explicit_onelake_uri_does_not_require_separate_ids() -> None:
    config = resolve_fabric_config(
        {"DATACOOLIE_FABRIC_ONELAKE_TEST_URI": "abfss://workspace@host/item/Files/"}
    )

    assert config.workspace_id == ""
    assert config.lakehouse_id == ""
    assert config.onelake_root == "abfss://workspace@host/item/Files"


def test_fabric_config_reports_only_missing_resource_coordinates() -> None:
    with pytest.raises(MissingCloudVariables) as exc_info:
        resolve_fabric_config({"DATACOOLIE_FABRIC_WORKSPACE_ID": "workspace-id"})

    assert exc_info.value.platform == "fabric"
    assert exc_info.value.names == ("DATACOOLIE_FABRIC_LAKEHOUSE_ID",)


def test_aws_config_uses_stable_default_prefix() -> None:
    config = resolve_aws_config(
        {
            "DATACOOLIE_AWS_REGION": "ap-southeast-1",
            "DATACOOLIE_AWS_BUCKET": "integration-bucket",
        }
    )

    assert config.prefix == "datacoolie-integration"
    assert config.benchmark_root == "datacoolie-integration"
    assert config.endpoint_url is None


def test_aws_config_resolves_relative_benchmark_root() -> None:
    config = resolve_aws_config(
        {
            "DATACOOLIE_AWS_REGION": "ap-southeast-1",
            "DATACOOLIE_AWS_BUCKET": "integration-bucket",
            "DATACOOLIE_AWS_TEST_PREFIX": "test-prefix",
            "DATACOOLIE_AWS_LIST_BENCHMARK_ROOT": "metadata",
        }
    )

    assert config.benchmark_root == "test-prefix/metadata"


def test_aws_config_accepts_optional_s3_compatible_endpoint() -> None:
    config = resolve_aws_config(
        {
            "DATACOOLIE_AWS_REGION": "us-east-1",
            "DATACOOLIE_AWS_BUCKET": "minio-test",
            "DATACOOLIE_AWS_ENDPOINT_URL": "http://localhost:9000",
        }
    )

    assert config.endpoint_url == "http://localhost:9000"


def test_databricks_config_requires_all_volume_coordinates() -> None:
    with pytest.raises(MissingCloudVariables) as exc_info:
        resolve_databricks_config({"DATACOOLIE_DATABRICKS_HOST": "https://example"})

    assert exc_info.value.names == (
        "DATACOOLIE_DATABRICKS_CATALOG",
        "DATACOOLIE_DATABRICKS_SCHEMA",
        "DATACOOLIE_DATABRICKS_VOLUME",
    )


def test_databricks_config_builds_portable_volume_and_optional_coordinates() -> None:
    config = resolve_databricks_config(
        {
            "DATACOOLIE_DATABRICKS_HOST": "https://workspace.example",
            "DATACOOLIE_DATABRICKS_CATALOG": "main",
            "DATACOOLIE_DATABRICKS_SCHEMA": "default",
            "DATACOOLIE_DATABRICKS_VOLUME": "logs",
            "DATACOOLIE_DATABRICKS_SECRET_SCOPE": "test-scope",
            "DATACOOLIE_DATABRICKS_SECRET_KEY": "test-key",
            "DATACOOLIE_DATABRICKS_CLUSTER_ID": "cluster-id",
        }
    )

    assert config.volume_root == "/Volumes/main/default/logs"
    assert config.benchmark_root == config.volume_root
    assert config.secret_scope == "test-scope"
    assert config.secret_key == "test-key"
    assert config.cluster_id == "cluster-id"


def test_databricks_config_resolves_relative_benchmark_root() -> None:
    config = resolve_databricks_config(
        {
            "DATACOOLIE_DATABRICKS_HOST": "https://workspace.example",
            "DATACOOLIE_DATABRICKS_CATALOG": "main",
            "DATACOOLIE_DATABRICKS_SCHEMA": "default",
            "DATACOOLIE_DATABRICKS_VOLUME": "logs",
            "DATACOOLIE_DATABRICKS_LIST_BENCHMARK_ROOT": "metadata",
        }
    )

    assert config.benchmark_root == "/Volumes/main/default/logs/metadata"


def test_databricks_config_preserves_qualified_benchmark_root() -> None:
    config = resolve_databricks_config(
        {
            "DATACOOLIE_DATABRICKS_HOST": "https://workspace.example",
            "DATACOOLIE_DATABRICKS_CATALOG": "main",
            "DATACOOLIE_DATABRICKS_SCHEMA": "default",
            "DATACOOLIE_DATABRICKS_VOLUME": "logs",
            "DATACOOLIE_DATABRICKS_LIST_BENCHMARK_ROOT": (
                "/Volumes/main/default/logs/metadata"
            ),
        }
    )

    assert config.benchmark_root == "/Volumes/main/default/logs/metadata"


def test_env_file_never_overrides_process_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "integration.env"
    env_file.write_text(
        "DATACOOLIE_AWS_BUCKET=from-file\nDATACOOLIE_AWS_REGION=from-file\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DATACOOLIE_AWS_BUCKET", "from-process")
    monkeypatch.delenv("DATACOOLIE_AWS_REGION", raising=False)

    pytest_configure_cloud(
        StubPytestConfig(enabled=True, env_file=str(env_file))  # type: ignore[arg-type]
    )

    config = resolve_aws_config()
    assert config.bucket == "from-process"
    assert config.region == "from-file"


def test_env_file_supports_export_and_quoted_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "integration.env"
    env_file.write_text(
        "# coordinates\nexport DATACOOLIE_AWS_BUCKET='test bucket'\n"
        'DATACOOLIE_AWS_REGION="ap-southeast-1"\n',
        encoding="utf-8",
    )
    monkeypatch.delenv("DATACOOLIE_AWS_BUCKET", raising=False)
    monkeypatch.delenv("DATACOOLIE_AWS_REGION", raising=False)

    pytest_configure_cloud(
        StubPytestConfig(enabled=True, env_file=str(env_file))  # type: ignore[arg-type]
    )

    config = resolve_aws_config()
    assert config.bucket == "test bucket"
    assert config.region == "ap-southeast-1"


def test_disabled_cloud_tests_do_not_load_env_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "integration.env"
    env_file.write_text("DATACOOLIE_AWS_BUCKET=from-file\n", encoding="utf-8")
    monkeypatch.delenv("DATACOOLIE_AWS_BUCKET", raising=False)

    pytest_configure_cloud(
        StubPytestConfig(enabled=False, env_file=str(env_file))  # type: ignore[arg-type]
    )

    assert "DATACOOLIE_AWS_BUCKET" not in os.environ


def test_explicit_missing_env_file_is_a_usage_error(tmp_path: Path) -> None:
    missing = tmp_path / "missing.env"

    with pytest.raises(pytest.UsageError, match="Integration env file does not exist"):
        pytest_configure_cloud(
            StubPytestConfig(enabled=True, env_file=str(missing))  # type: ignore[arg-type]
        )


def test_benchmark_items_require_explicit_run_flag() -> None:
    benchmark = StubCloudItem("cloud_integration", "cloud_platform", "benchmark")
    contract = StubCloudItem("cloud_integration", "cloud_platform")

    pytest_gate_cloud_items(
        StubGateConfig(cloud_integration=True, cloud_platform=["aws"]),
        [benchmark, contract],
    )

    assert len(benchmark.added_markers) == 1
    assert benchmark.added_markers[0].mark.kwargs == {
        "reason": "benchmark requires --run-benchmarks",
    }
    assert contract.added_markers == []


def test_benchmark_items_are_allowed_when_explicitly_enabled() -> None:
    benchmark = StubCloudItem("cloud_integration", "cloud_platform", "benchmark")

    pytest_gate_cloud_items(
        StubGateConfig(
            cloud_integration=True,
            cloud_platform=["aws"],
            run_benchmarks=True,
        ),
        [benchmark],
    )

    assert benchmark.added_markers == []


def test_benchmark_items_still_obey_platform_filter() -> None:
    benchmark = StubCloudItem("cloud_integration", "cloud_platform", "benchmark")

    pytest_gate_cloud_items(
        StubGateConfig(
            cloud_integration=True,
            cloud_platform=["fabric"],
            run_benchmarks=True,
        ),
        [benchmark],
    )

    assert len(benchmark.added_markers) == 1
    assert benchmark.added_markers[0].mark.kwargs == {
        "reason": "cloud platform 'aws' was not selected",
    }


def test_benchmark_items_still_require_cloud_opt_in() -> None:
    benchmark = StubCloudItem("cloud_integration", "cloud_platform", "benchmark")

    pytest_gate_cloud_items(
        StubGateConfig(
            cloud_integration=False,
            cloud_platform=["aws"],
            run_benchmarks=True,
        ),
        [benchmark],
    )

    assert len(benchmark.added_markers) == 1
    assert benchmark.added_markers[0].mark.kwargs == {
        "reason": "cloud integration requires --cloud-integration",
    }


def test_local_benchmark_items_require_explicit_run_flag() -> None:
    benchmark = StubCloudItem("benchmark")

    pytest_gate_cloud_items(
        StubGateConfig(cloud_integration=False, run_benchmarks=False),
        [benchmark],
    )

    assert len(benchmark.added_markers) == 1
    assert benchmark.added_markers[0].mark.kwargs == {
        "reason": "benchmark requires --run-benchmarks",
    }


def test_local_benchmark_items_are_allowed_when_explicitly_enabled() -> None:
    benchmark = StubCloudItem("benchmark")

    pytest_gate_cloud_items(
        StubGateConfig(cloud_integration=False, run_benchmarks=True),
        [benchmark],
    )

    assert benchmark.added_markers == []

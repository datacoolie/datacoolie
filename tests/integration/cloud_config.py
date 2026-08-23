"""Configuration and safety gates for real-cloud integration tests."""

from __future__ import annotations

import os
from ast import literal_eval
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILE = REPOSITORY_ROOT / ".env.integration.local"
SUPPORTED_PLATFORMS = ("aws", "databricks", "fabric")


class MissingCloudVariables(ValueError):
    """Raised when a selected platform lacks required resource coordinates."""

    def __init__(self, platform: str, names: Sequence[str]) -> None:
        self.platform = platform
        self.names = tuple(names)
        super().__init__(
            f"{platform} cloud integration requires: {', '.join(self.names)}"
        )


@dataclass(frozen=True, slots=True)
class FabricIntegrationConfig:
    tenant_id: str | None
    workspace_id: str
    lakehouse_id: str
    onelake_root: str
    benchmark_root: str
    key_vault_url: str | None
    key_vault_secret: str | None


@dataclass(frozen=True, slots=True)
class AwsIntegrationConfig:
    region: str
    bucket: str
    prefix: str
    benchmark_root: str
    endpoint_url: str | None


@dataclass(frozen=True, slots=True)
class DatabricksIntegrationConfig:
    host: str
    catalog: str
    schema: str
    volume: str
    prefix: str
    benchmark_root: str
    secret_scope: str | None
    secret_key: str | None
    cluster_id: str | None

    @property
    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"


def _value(environment: Mapping[str, str], name: str) -> str | None:
    value = environment.get(name, "").strip()
    return value or None


def _load_env_file(path: Path) -> None:
    """Load simple dotenv coordinates without replacing process values."""
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8-sig").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").lstrip()
        if "=" not in line:
            raise pytest.UsageError(f"Invalid env entry at {path}:{line_number}")
        name, raw_value = line.split("=", 1)
        name = name.strip()
        if not name or not name.replace("_", "a").isalnum() or name[0].isdigit():
            raise pytest.UsageError(f"Invalid env name at {path}:{line_number}")
        value = raw_value.strip()
        if value.startswith(("'", '"')):
            try:
                parsed = literal_eval(value)
            except (SyntaxError, ValueError) as exc:
                raise pytest.UsageError(
                    f"Invalid quoted env value at {path}:{line_number}"
                ) from exc
            if not isinstance(parsed, str):
                raise pytest.UsageError(
                    f"Env value must be text at {path}:{line_number}"
                )
            value = parsed
        os.environ.setdefault(name, value)


def _resolve_databricks_benchmark_root(
    volume_root: str,
    configured_root: str | None,
) -> str:
    """Resolve a benchmark root relative to the configured Volume.

    A qualified Volume path remains accepted for compatibility with existing
    local env files, while the documented form is a Volume-relative root such
    as ``metadata``.
    """
    if configured_root is None:
        return volume_root

    value = configured_root.rstrip("/")
    if value == volume_root or value.startswith(f"{volume_root}/"):
        return value
    if value.startswith("dbfs:/Volumes/"):
        return value
    if value.startswith("/"):
        raise ValueError(
            "DATACOOLIE_DATABRICKS_LIST_BENCHMARK_ROOT must be relative to "
            "the configured Volume or a /Volumes/... path."
        )
    return f"{volume_root}/{value.lstrip('/')}"


def _required(
    environment: Mapping[str, str],
    platform: str,
    names: Sequence[str],
) -> list[str]:
    missing = [name for name in names if _value(environment, name) is None]
    if missing:
        raise MissingCloudVariables(platform, missing)
    return [str(_value(environment, name)) for name in names]


def resolve_fabric_config(
    environment: Mapping[str, str] = os.environ,
) -> FabricIntegrationConfig:
    explicit_root = _value(environment, "DATACOOLIE_FABRIC_ONELAKE_TEST_URI")
    workspace_id = _value(environment, "DATACOOLIE_FABRIC_WORKSPACE_ID")
    lakehouse_id = _value(environment, "DATACOOLIE_FABRIC_LAKEHOUSE_ID")
    missing = [
        name
        for name, value in (
            ("DATACOOLIE_FABRIC_WORKSPACE_ID", workspace_id),
            ("DATACOOLIE_FABRIC_LAKEHOUSE_ID", lakehouse_id),
        )
        if value is None
    ]
    if missing and explicit_root is None:
        raise MissingCloudVariables("fabric", missing)
    workspace_id = workspace_id or ""
    lakehouse_id = lakehouse_id or ""
    onelake_root = explicit_root or (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files"
    )
    return FabricIntegrationConfig(
        tenant_id=_value(environment, "DATACOOLIE_FABRIC_TENANT_ID"),
        workspace_id=workspace_id,
        lakehouse_id=lakehouse_id,
        onelake_root=onelake_root.rstrip("/"),
        benchmark_root=_value(
            environment,
            "DATACOOLIE_FABRIC_LIST_BENCHMARK_ROOT",
        )
        or "Files",
        key_vault_url=_value(environment, "DATACOOLIE_FABRIC_KEYVAULT_TEST_URL"),
        key_vault_secret=_value(
            environment,
            "DATACOOLIE_FABRIC_KEYVAULT_TEST_SECRET_NAME",
        ),
    )


def resolve_aws_config(
    environment: Mapping[str, str] = os.environ,
) -> AwsIntegrationConfig:
    region, bucket = _required(
        environment,
        "aws",
        ("DATACOOLIE_AWS_REGION", "DATACOOLIE_AWS_BUCKET"),
    )
    prefix = (
        _value(environment, "DATACOOLIE_AWS_TEST_PREFIX") or "datacoolie-integration"
    )
    configured_root = _value(environment, "DATACOOLIE_AWS_LIST_BENCHMARK_ROOT")
    benchmark_root = (
        f"{prefix}/{configured_root.lstrip('/')}" if configured_root else prefix
    )
    return AwsIntegrationConfig(
        region=region,
        bucket=bucket,
        prefix=prefix,
        benchmark_root=benchmark_root,
        endpoint_url=_value(environment, "DATACOOLIE_AWS_ENDPOINT_URL"),
    )


def resolve_databricks_config(
    environment: Mapping[str, str] = os.environ,
) -> DatabricksIntegrationConfig:
    host, catalog, schema, volume = _required(
        environment,
        "databricks",
        (
            "DATACOOLIE_DATABRICKS_HOST",
            "DATACOOLIE_DATABRICKS_CATALOG",
            "DATACOOLIE_DATABRICKS_SCHEMA",
            "DATACOOLIE_DATABRICKS_VOLUME",
        ),
    )
    volume_root = f"/Volumes/{catalog}/{schema}/{volume}"
    return DatabricksIntegrationConfig(
        host=host,
        catalog=catalog,
        schema=schema,
        volume=volume,
        prefix=_value(environment, "DATACOOLIE_DATABRICKS_TEST_PREFIX")
        or "datacoolie-integration",
        benchmark_root=_resolve_databricks_benchmark_root(
            volume_root,
            _value(environment, "DATACOOLIE_DATABRICKS_LIST_BENCHMARK_ROOT"),
        ),
        secret_scope=_value(environment, "DATACOOLIE_DATABRICKS_SECRET_SCOPE"),
        secret_key=_value(environment, "DATACOOLIE_DATABRICKS_SECRET_KEY"),
        cluster_id=_value(environment, "DATACOOLIE_DATABRICKS_CLUSTER_ID"),
    )


def pytest_add_cloud_options(parser: pytest.Parser) -> None:
    group = parser.getgroup("cloud integration")
    group.addoption(
        "--cloud-integration",
        action="store_true",
        help="Enable tests that access and mutate explicitly configured cloud resources.",
    )
    group.addoption(
        "--cloud-platform",
        action="append",
        choices=SUPPORTED_PLATFORMS,
        help="Run only the selected cloud platform; may be supplied more than once.",
    )
    group.addoption(
        "--run-benchmarks",
        action="store_true",
        help=(
            "Enable benchmark-marked tests. Cloud benchmarks also require "
            "--cloud-integration."
        ),
    )
    group.addoption(
        "--integration-env-file",
        help="Path to a local integration env file, relative to the repository root.",
    )


def pytest_configure_cloud(config: pytest.Config) -> None:
    if not config.getoption("--cloud-integration"):
        return
    configured_path = config.getoption("--integration-env-file")
    env_path = Path(configured_path) if configured_path else DEFAULT_ENV_FILE
    if not env_path.is_absolute():
        env_path = REPOSITORY_ROOT / env_path
    if configured_path and not env_path.is_file():
        raise pytest.UsageError(f"Integration env file does not exist: {env_path}")
    if env_path.is_file():
        _load_env_file(env_path)


def pytest_gate_cloud_items(config: pytest.Config, items: list[pytest.Item]) -> None:
    enabled = config.getoption("--cloud-integration")
    run_benchmarks = config.getoption("--run-benchmarks")
    selected = set(config.getoption("--cloud-platform") or SUPPORTED_PLATFORMS)
    disabled = pytest.mark.skip(reason="cloud integration requires --cloud-integration")
    for item in items:
        if item.get_closest_marker("benchmark") is not None and not run_benchmarks:
            item.add_marker(
                pytest.mark.skip(reason="benchmark requires --run-benchmarks")
            )
            continue
        if item.get_closest_marker("cloud_integration") is None:
            continue
        if not enabled:
            item.add_marker(disabled)
            continue
        platform_marker = item.get_closest_marker("cloud_platform")
        platform = str(platform_marker.args[0]) if platform_marker else None
        if platform not in selected:
            item.add_marker(
                pytest.mark.skip(reason=f"cloud platform '{platform}' was not selected")
            )
            continue

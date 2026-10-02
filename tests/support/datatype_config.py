"""Opt-in gates and local prerequisite probes for datatype qualification.

The default pytest process must remain import-only for this module.  Docker,
database drivers and Spark are discovered only when a qualification run is
explicitly enabled and a test asks for a preflight.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from urllib.error import URLError
from urllib.request import urlopen
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[2]
USECASE_SIM_ROOT = PRODUCT_ROOT / "usecase-sim"
COMPOSE_FILE = USECASE_SIM_ROOT / "docker" / "docker-compose.yml"

# Database containers expose an application health check in the simulator.
# For those services, a running container is not sufficient: the source driver
# may still race database startup. Other services (notably Spark) do not expose
# a compose health check and retain the weaker running-state check.
_DATABASE_SERVICES = frozenset({"postgres", "mysql", "mssql", "oracle"})
_HTTP_PROBES = {
    "minio": "http://localhost:9000/minio/health/live",
    "iceberg-rest": "http://localhost:8181/v1/config",
}


@dataclass(frozen=True, slots=True)
class DockerServiceStatus:
    """Observable state returned by a Docker service probe."""

    service: str
    container: str
    running: bool
    health: str | None


def pytest_add_datatype_options(parser: pytest.Parser) -> None:
    group = parser.getgroup("datatype qualification")
    group.addoption(
        "--datatype-qualification",
        action="store_true",
        help=(
            "Enable opt-in datatype extraction, Spark and persisted-format "
            "qualification tests."
        ),
    )
    group.addoption(
        "--spark4-qualification",
        action="store_true",
        help=(
            "Enable the opt-in Spark 4.x container qualification cell in "
            "addition to --datatype-qualification."
        ),
    )


def pytest_configure_datatype(config: pytest.Config) -> None:
    # The collection hook is the single owner of this gate.  Do not probe
    # Docker or construct a qualification context during configuration.
    _ = config


def pytest_gate_datatype_items(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if config.getoption("--datatype-qualification"):
        datatype_skip = None
    else:
        datatype_skip = pytest.mark.skip(
            reason="datatype qualification requires --datatype-qualification"
        )
    for item in items:
        if (
            datatype_skip is not None
            and item.get_closest_marker("datatype_qualification") is not None
        ):
            item.add_marker(datatype_skip)
        if item.get_closest_marker("spark4_qualification") is not None:
            if not config.getoption("--spark4-qualification"):
                item.add_marker(
                    pytest.mark.skip(
                        reason=(
                            "Spark 4.x qualification requires "
                            "--spark4-qualification"
                        )
                    )
                )


def _docker_command(*args: str) -> list[str]:
    docker = shutil.which("docker")
    if docker is None:
        raise pytest.UsageError(
            "Datatype qualification requires the Docker CLI on PATH"
        )
    return [docker, *args]


def docker_service_status(service: str) -> DockerServiceStatus:
    """Inspect one compose container without starting or mutating it."""

    container = f"datacoolie-{service}"
    command = _docker_command(
        "inspect",
        "--format",
        "{{json .State}}",
        container,
    )
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if completed.returncode != 0:
        return DockerServiceStatus(service, container, False, None)
    try:
        state = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise pytest.UsageError(
            f"Docker returned invalid state for {container}: {completed.stdout!r}"
        ) from exc
    return DockerServiceStatus(
        service=service,
        container=container,
        running=state.get("Running") is True,
        health=(state.get("Health") or {}).get("Status"),
    )


def _http_probe_error(service: str) -> str | None:
    """Return a readiness error for a service with no Compose healthcheck."""

    url = _HTTP_PROBES.get(service)
    if url is None:
        return None
    try:
        with urlopen(url, timeout=5) as response:  # noqa: S310 - fixed local probe URL
            if response.status < 200 or response.status >= 300:
                return f"HTTP {response.status}"
    except (OSError, URLError) as exc:
        return f"{type(exc).__name__}: {exc}"
    return None


def require_docker_services(services: Iterable[str]) -> tuple[DockerServiceStatus, ...]:
    """Require already-started, healthy selected services.

    Starting containers belongs to the explicit usecase-sim preparation command;
    pytest only checks the state and fails clearly when the opt-in prerequisites
    were not prepared.
    """

    if not COMPOSE_FILE.is_file():
        raise pytest.UsageError(f"usecase-sim Compose file not found: {COMPOSE_FILE}")
    statuses = tuple(docker_service_status(service) for service in services)
    unhealthy = []
    probe_errors: dict[str, str] = {}
    for status in statuses:
        if not status.running:
            unhealthy.append(status)
            continue
        if status.service in _DATABASE_SERVICES and status.health != "healthy":
            unhealthy.append(status)
            continue
        if status.health not in {None, "healthy"}:
            unhealthy.append(status)
            continue
        probe_error = _http_probe_error(status.service)
        if probe_error is not None:
            unhealthy.append(status)
            probe_errors[status.service] = probe_error
    if unhealthy:
        details = ", ".join(
            f"{status.service} running={status.running} "
            f"health={status.health or 'none'}"
            + (
                f" probe={probe_errors[status.service]}"
                if status.service in probe_errors
                else ""
            )
            for status in unhealthy
        )
        raise pytest.UsageError(
            "Datatype qualification services are not ready: "
            f"{details}. Run usecase-sim/scripts/setup_platform.py first."
        )
    return statuses

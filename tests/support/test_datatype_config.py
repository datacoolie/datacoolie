"""Tests for the explicit datatype qualification gate and Docker probe."""

from __future__ import annotations

import json

import pytest

from tests.support.datatype_config import (
    DockerServiceStatus,
    docker_service_status,
    pytest_gate_datatype_items,
    require_docker_services,
)


class StubConfig:
    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def getoption(self, name: str) -> bool:
        assert name == "--datatype-qualification"
        return self.enabled


class StubMarker:
    def __init__(self, *args: object) -> None:
        self.args = args


class StubItem:
    def __init__(self, marked: bool) -> None:
        self.marked = marked
        self.added_markers: list[pytest.MarkDecorator] = []

    def get_closest_marker(self, name: str):
        return StubMarker() if name == "datatype_qualification" and self.marked else None

    def add_marker(self, marker: pytest.MarkDecorator) -> None:
        self.added_markers.append(marker)


def test_qualification_items_are_skipped_without_explicit_flag() -> None:
    item = StubItem(marked=True)
    pytest_gate_datatype_items(StubConfig(False), [item])

    assert len(item.added_markers) == 1
    assert item.added_markers[0].mark.kwargs == {
        "reason": "datatype qualification requires --datatype-qualification"
    }


def test_qualification_items_are_enabled_only_with_explicit_flag() -> None:
    item = StubItem(marked=True)
    pytest_gate_datatype_items(StubConfig(True), [item])

    assert item.added_markers == []


def test_unmarked_items_are_never_gated() -> None:
    item = StubItem(marked=False)
    pytest_gate_datatype_items(StubConfig(False), [item])

    assert item.added_markers == []


def test_docker_service_status_reports_container_state(monkeypatch: pytest.MonkeyPatch) -> None:
    class Completed:
        returncode = 0
        stdout = json.dumps({"Running": True, "Health": {"Status": "healthy"}})

    monkeypatch.setattr("tests.support.datatype_config.shutil.which", lambda _: "docker")
    monkeypatch.setattr(
        "tests.support.datatype_config.subprocess.run",
        lambda *args, **kwargs: Completed(),
    )

    assert docker_service_status("postgres") == DockerServiceStatus(
        "postgres", "datacoolie-postgres", True, "healthy"
    )


def test_database_preflight_requires_healthy_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "tests.support.datatype_config.docker_service_status",
        lambda service: DockerServiceStatus(
            service, f"datacoolie-{service}", True, None
        ),
    )

    with pytest.raises(pytest.UsageError, match="not ready"):
        require_docker_services(("postgres",))

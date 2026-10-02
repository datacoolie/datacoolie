"""Opt-in gate for native runtime qualification tests.

Collection remains side-effect free.  Runtime resources are owned by the
qualified tests rather than initialized during pytest configuration.
"""

from __future__ import annotations

import os

import pytest


def pytest_add_runtime_options(parser: pytest.Parser) -> None:
    group = parser.getgroup("runtime qualification")
    group.addoption(
        "--runtime-qualification",
        action="store_true",
        help=(
            "Enable opt-in runtime replacement and persisted-format qualification "
            "tests."
        ),
    )


def pytest_configure_runtime(config: pytest.Config) -> None:
    # Keep test collection and configuration side-effect free.
    _ = config


def pytest_gate_runtime_items(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if config.getoption("--runtime-qualification"):
        return
    skip = pytest.mark.skip(
        reason="runtime qualification requires --runtime-qualification"
    )
    for item in items:
        if item.get_closest_marker("runtime_qualification") is not None:
            item.add_marker(skip)


def runtime_qualification_enabled(config: pytest.Config) -> bool:
    """Return whether the explicit runtime gate was enabled."""
    return bool(config.getoption("--runtime-qualification"))


def native_runtime_requested() -> bool:
    """Whether a caller explicitly requested a native local runtime check."""
    return os.getenv("DATACOOLIE_RUNTIME_NATIVE", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


__all__ = [
    "native_runtime_requested",
    "pytest_add_runtime_options",
    "pytest_configure_runtime",
    "pytest_gate_runtime_items",
    "runtime_qualification_enabled",
]

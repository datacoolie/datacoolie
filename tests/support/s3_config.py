"""Test-facing adapter for the simulator's canonical S3 settings."""

from __future__ import annotations

import sys
from pathlib import Path


SIM_ROOT = Path(__file__).resolve().parents[2] / "usecase-sim"
if str(SIM_ROOT) not in sys.path:
    sys.path.insert(0, str(SIM_ROOT))

from runner.s3_config import S3Config, get_s3_config  # noqa: E402


def host_s3_config() -> S3Config:
    """Resolve the host-side qualification endpoint for the current process."""

    return get_s3_config()


def host_s3_properties() -> dict[str, str]:
    """Return PyIceberg properties for host-side integration tests."""

    return host_s3_config().iceberg_properties()


def host_storage_options() -> dict[str, str]:
    """Return Polars object-store options for host-side integration tests."""

    return host_s3_config().storage_options()


__all__ = ["S3Config", "get_s3_config", "host_s3_config", "host_s3_properties", "host_storage_options"]

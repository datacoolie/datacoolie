"""Canonical, import-light paths for the usecase simulator runtime.

This module deliberately imports only the standard-library path helpers.  It
is safe to import from argument parsing, fixture preparation, or tests before
the DataCoolie package (and its optional engine dependencies) is loaded.
"""

from __future__ import annotations

import os
from pathlib import Path


USECASE_SIM_DIR = Path(__file__).resolve().parent
DATACOOLIE_ROOT = USECASE_SIM_DIR.parent
RUNTIME_DIR = USECASE_SIM_DIR / ".runtime"
DATA_DIR = RUNTIME_DIR / "data"
LOG_DIR = RUNTIME_DIR / "logs"
WATERMARKS_DIR = RUNTIME_DIR / "watermarks"
DEPENDENCIES_DIR = RUNTIME_DIR / "dependencies"

AWS_LOG_PATH = "s3://datacoolie-test/logs"
AWS_WATERMARK_PATH = "s3://datacoolie-test/watermarks"
RUNTIME_URI_PREFIXES = ("s3://", "s3a://", "abfs://", "dbfs:/", "file://")


def runtime_child(root: object, child: str) -> str:
    """Join a local path or storage URI without changing its scheme."""
    value = str(root)
    if value.startswith(RUNTIME_URI_PREFIXES):
        return value.rstrip("/") + "/" + child
    return str(Path(value) / child)


def apply_runtime_defaults(args):
    """Resolve simulator log and watermark roots once for each CLI run."""
    is_aws = args.platform == "aws"
    if args.log_path is None:
        args.log_path = AWS_LOG_PATH if is_aws else str(LOG_DIR)
    if args.watermark_path is None:
        state = getattr(args, "state_base_path", None)
        if state:
            args.watermark_path = runtime_child(state, "watermarks")
        else:
            args.watermark_path = AWS_WATERMARK_PATH if is_aws else str(WATERMARKS_DIR)
    return args


def spark_profile(value: str | None = None) -> str:
    """Return the canonical Spark runtime profile name.

    The simulator supports the pinned Spark 3.5 line and an isolated Spark
    4.x qualification line.  Any unset or unknown value uses the default
    Spark 3.5 profile so a typo cannot accidentally share the Spark 4 state.
    """

    requested = value if value is not None else os.environ.get(
        "DATACOOLIE_SPARK_PROFILE", "spark35"
    )
    return "spark4" if requested == "spark4" else "spark35"


def spark_data_dir(profile: str | None = None) -> Path:
    """Return the profile-isolated Spark data root."""

    return DATA_DIR / "spark" / spark_profile(profile)


def spark_dependencies_dir(profile: str | None = None) -> Path:
    """Return the profile-isolated Spark dependency/Ivy root."""

    return DEPENDENCIES_DIR / "spark" / spark_profile(profile)


def spark_log_dir(profile: str | None = None) -> Path:
    """Return the profile-isolated Derby/Spark diagnostic log root."""

    return LOG_DIR / "spark" / spark_profile(profile)


__all__ = [
    "DATA_DIR",
    "DATACOOLIE_ROOT",
    "DEPENDENCIES_DIR",
    "LOG_DIR",
    "RUNTIME_DIR",
    "USECASE_SIM_DIR",
    "WATERMARKS_DIR",
    "AWS_LOG_PATH",
    "AWS_WATERMARK_PATH",
    "RUNTIME_URI_PREFIXES",
    "runtime_child",
    "apply_runtime_defaults",
    "spark_data_dir",
    "spark_dependencies_dir",
    "spark_log_dir",
    "spark_profile",
]

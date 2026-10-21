"""Select Compose files for simulator service startup in one place."""

from __future__ import annotations

import os
from pathlib import Path
import re
from collections.abc import Mapping


OVERRIDE_ENV = "DATACOOLIE_COMPOSE_OVERRIDE"


def compose_file_arguments(
    base: str | Path, env: Mapping[str, str] | None = None,
) -> list[str]:
    """Keep ordinary startup unchanged; validate an explicit isolated override."""
    environment = os.environ if env is None else env
    arguments = ["-f", str(base)]
    if OVERRIDE_ENV not in environment:
        return arguments
    value = environment[OVERRIDE_ENV]
    if not value or not value.strip():
        raise ValueError(f"{OVERRIDE_ENV} must name an absolute Compose file")
    supplied = Path(value)
    if not supplied.is_absolute() or supplied.is_symlink() or not supplied.is_file():
        raise ValueError(f"{OVERRIDE_ENV} must name an absolute regular Compose file")
    project = environment.get("COMPOSE_PROJECT_NAME", "")
    if re.fullmatch(r"[a-z0-9][a-z0-9_-]*", project) is None:
        raise ValueError("An isolated Compose override requires a valid COMPOSE_PROJECT_NAME")
    return [*arguments, "-f", str(supplied.resolve())]


def compose_startup_arguments(env: Mapping[str, str] | None = None) -> list[str]:
    """Use the captured local image pins when an isolated override is selected."""
    environment = os.environ if env is None else env
    return ["--no-build", "--pull", "never"] if OVERRIDE_ENV in environment else []

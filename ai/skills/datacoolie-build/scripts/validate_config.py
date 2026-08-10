#!/usr/bin/env python3
"""Validate a DataCoolie workspace config and its environment/platform binding."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _load_yaml(path: Path) -> Any:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - dependency failure path
        raise RuntimeError("PyYAML is required: pip install pyyaml") from exc

    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _load_schema() -> dict[str, Any]:
    schema_path = Path(__file__).resolve().parent.parent / "schemas" / "workspace-config.schema.json"
    with schema_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def validate_config(
    config_path: Path,
    *,
    selected_environments: list[str] | None = None,
    expected_environment: str | None = None,
    expected_platform: str | None = None,
) -> dict[str, Any]:
    try:
        from jsonschema import Draft202012Validator
    except ImportError as exc:  # pragma: no cover - dependency failure path
        raise RuntimeError("jsonschema is required: pip install jsonschema") from exc

    config = _load_yaml(config_path)
    validator = Draft202012Validator(_load_schema())
    errors = sorted(
        validator.iter_errors(config),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        details = []
        for error in errors:
            location = ".".join(str(part) for part in error.absolute_path) or "<root>"
            details.append(f"{location}: {error.message}")
        raise ValueError("Invalid workspace config:\n- " + "\n- ".join(details))

    try:
        from datacoolie import platform_registry
    except ImportError as exc:  # pragma: no cover - dependency failure path
        raise RuntimeError("datacoolie must be installed before config validation") from exc

    environments = config["environments"]
    if expected_environment is not None and selected_environments is not None:
        raise ValueError("expected_environment and selected_environments are mutually exclusive")
    selected = list(
        dict.fromkeys(
            [expected_environment]
            if expected_environment is not None
            else selected_environments or environments.keys()
        )
    )
    unknown = sorted(set(selected) - set(environments))
    if unknown:
        raise ValueError(f"Unknown environment(s): {', '.join(unknown)}")

    supported_platforms = set(platform_registry.list_plugins())
    unsupported = sorted(
        (env, values["platform"])
        for env, values in environments.items()
        if env in selected and values["platform"] not in supported_platforms
    )
    if unsupported:
        pairs = ", ".join(f"{env}={platform}" for env, platform in unsupported)
        available = ", ".join(sorted(supported_platforms)) or "<none>"
        raise ValueError(f"Unsupported environment platform(s): {pairs}. Available: {available}")

    if expected_environment is not None:
        actual_platform = environments[expected_environment]["platform"]
        if expected_platform is not None and actual_platform != expected_platform:
            raise ValueError(
                f"Environment {expected_environment!r} maps to platform {actual_platform!r}, "
                f"not {expected_platform!r}"
            )
    elif expected_platform is not None:
        raise ValueError("--expected-platform requires --environment")

    return config


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path, help="Path to workspace config.yaml")
    parser.add_argument(
        "--environment",
        action="append",
        dest="environments",
        help="Environment to validate against installed platforms; repeat or omit for all",
    )
    parser.add_argument("--expected-platform", help="Platform encoded by the selected runner")
    args = parser.parse_args()

    try:
        if args.expected_platform and (not args.environments or len(args.environments) != 1):
            raise ValueError("--expected-platform requires exactly one --environment")
        config = validate_config(
            args.config,
            selected_environments=None if args.expected_platform else args.environments,
            expected_platform=args.expected_platform,
            expected_environment=args.environments[0] if args.expected_platform else None,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.environments:
        pairs = ", ".join(
            f"{environment} -> {config['environments'][environment]['platform']}"
            for environment in args.environments
        )
        print(f"OK: {pairs}")
    else:
        print(f"OK: {len(config['environments'])} environment(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Receipt identity and current-worktree helpers for the simulator catalog."""

from __future__ import annotations

import hashlib
import platform
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence


def worktree_fingerprint(root: str | Path | None = None) -> str:
    """Fingerprint relevant source and catalog inputs in the current worktree."""

    # Imports are intentionally lazy: cases.catalog imports this helper after
    # defining its path utilities, while direct callers can still import this
    # module without loading the scenario runner.
    from cases.catalog import _product_root, _slash, simulator_root

    sim_root = simulator_root(root)
    product_root = _product_root(sim_root)
    roots = [
        product_root / "src",
        product_root / "tests",
        sim_root / "cases",
        sim_root / "scenarios",
        sim_root / "runner",
        sim_root / "scripts",
        sim_root / "metadata",
        sim_root / "platforms",
        sim_root / "artifacts",
        sim_root / "docker",
        product_root / "pyproject.toml",
        product_root / "poetry.lock",
        product_root / "docker",
    ]
    files: set[Path] = set()
    extensions = {".py", ".json", ".ipynb", ".sql", ".yaml", ".yml"}
    for item in roots:
        if item.is_file():
            files.add(item)
        elif item.is_dir():
            files.update(
                path
                for path in item.rglob("*")
                if path.is_file()
                and (
                    path.suffix.lower() in extensions
                    or path.name.lower().startswith("dockerfile")
                )
                and ".runtime" not in path.parts
                and "__pycache__" not in path.parts
            )
    digest = hashlib.sha256()
    # Path ordering folds case on Windows but not Linux. Fingerprint the same
    # mounted bytes in one canonical order on both sides of Docker execution.
    for path in sorted(files, key=lambda item: _slash(item.relative_to(product_root))):
        try:
            relative = _slash(path.relative_to(product_root))
            payload = path.read_bytes()
        except (OSError, ValueError):
            continue
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(payload)
        digest.update(b"\0")
    return digest.hexdigest()


def runtime_identity() -> dict[str, Any]:
    """Return portable runtime identity fields for a qualification receipt."""

    try:
        from importlib.metadata import version
    except Exception:  # pragma: no cover - importlib is part of Python
        version = None  # type: ignore[assignment]

    packages: dict[str, str] = {}
    for package in ("datacoolie", "polars", "deltalake", "pyspark", "pyiceberg"):
        try:
            packages[package] = version(package) if version is not None else "unknown"
        except Exception:
            packages[package] = "uninstalled"
    return {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": sys.platform,
        "datacoolie": packages["datacoolie"],
        "packages": packages,
    }


def _receipt_identity(receipt: Mapping[str, Any]) -> tuple[Any, Any, Any]:
    revision = receipt.get("revision", receipt.get("catalog_revision"))
    profile = receipt.get("profile")
    command = receipt.get("command")
    return revision, profile, command


def classify_receipt(
    receipt: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    revision: str | None = None,
    profile: Mapping[str, Any] | None = None,
    command: object | None = None,
    runtime: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Classify an explicit receipt without trusting legacy receipt files."""

    from cases.catalog import _profile

    records: list[Mapping[str, Any]]
    if isinstance(receipt, Mapping):
        records = [receipt]
    elif isinstance(receipt, Sequence) and not isinstance(receipt, (str, bytes)):
        records = [item for item in receipt if isinstance(item, Mapping)]
    else:
        records = []
    if not records:
        return {"status": "not_run", "execution_status": "not_run", "classification": "none"}

    expected_profile = _profile(profile) if profile is not None else None
    identity_complete = True
    current = True
    execution_statuses: list[str] = []
    for record in records:
        rec_revision, rec_profile, rec_command = _receipt_identity(record)
        raw_status = record.get("status")
        receipt_runtime = record.get("runtime")
        runtime_verified = record.get("runtime_verified", True)
        if (
            rec_revision in (None, "")
            or _profile(rec_profile) is None
            or rec_command in (None, "")
            or not isinstance(receipt_runtime, Mapping)
            or not receipt_runtime
            or raw_status not in {"passed", "failed", "blocked"}
        ):
            identity_complete = False
        if revision is not None and rec_revision != revision:
            current = False
        if expected_profile is not None and _profile(rec_profile) != expected_profile:
            current = False
        if profile is not None and isinstance(rec_profile, Mapping):
            for key, expected in profile.items():
                if rec_profile.get(key) != expected:
                    current = False
        if command is not None and rec_command != command:
            current = False
        if runtime is not None:
            for key, expected in runtime.items():
                actual_runtime = receipt_runtime
                if not isinstance(actual_runtime, Mapping):
                    current = False
                    break
                if isinstance(expected, Mapping):
                    actual_nested = actual_runtime.get(key)
                    if not isinstance(actual_nested, Mapping) or any(
                        actual_nested.get(nested_key) != nested_value
                        for nested_key, nested_value in expected.items()
                    ):
                        current = False
                        break
                elif actual_runtime.get(key) != expected:
                    current = False
                    break
        actual = record.get("actual_exit_code", record.get("exit_code"))
        if runtime_verified is False:
            execution_statuses.append("blocked")
        elif raw_status == "failed" or (raw_status == "passed" and actual is not None and actual != 0):
            execution_statuses.append("failed")
        elif raw_status == "blocked":
            execution_statuses.append("blocked")
        elif raw_status == "passed":
            execution_statuses.append("passed")
        else:
            execution_statuses.append("blocked")
    if "failed" in execution_statuses:
        execution_status = "failed"
    elif "blocked" in execution_statuses:
        execution_status = "blocked"
    elif execution_statuses and all(value == "passed" for value in execution_statuses):
        execution_status = "passed"
    else:
        execution_status = "blocked"
    if not identity_complete or not current:
        return {"status": "historical", "execution_status": execution_status, "classification": "historical"}
    return {"status": execution_status, "execution_status": execution_status, "classification": "current"}


def execution_statuses(
    root: str | Path | None = None,
    scenarios: Mapping[str, Mapping[str, Any]] | None = None,
    *,
    revision: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Read scenario receipts, defaulting absent or legacy runs safely."""

    from cases.catalog import (
        CatalogError,
        _read_json,
        _relative,
        is_scenario_bundle,
        load_scenario_registry,
        scenario_leaf_views,
        simulator_root,
    )

    sim_root = simulator_root(root)
    registry = scenarios if scenarios is not None else load_scenario_registry(sim_root)
    leaves = scenario_leaf_views(registry)
    current_revision = revision if revision is not None else worktree_fingerprint(sim_root)
    directory = sim_root / ".runtime" / "logs" / "scenarios"
    result: dict[str, dict[str, Any]] = {}
    attempts: dict[str, Any] = {}
    for name, scenario in leaves.items():
        receipt_path = None
        if directory.is_dir():
            for suffix in (".receipt.json", ".invocations.json", ".json"):
                candidate = directory / f"{name}{suffix}"
                if candidate.is_file():
                    receipt_path = candidate
                    break
        if receipt_path is None:
            result[name] = {"status": "not_run", "execution_status": "not_run", "classification": "none"}
            continue
        try:
            receipt = _read_json(receipt_path)
        except CatalogError:
            result[name] = {"status": "historical", "execution_status": "blocked", "classification": "historical", "receipt": _relative(sim_root, receipt_path)}
            continue
        profile = {"engine": scenario.get("engine"), "platform": scenario.get("platform", "local")}
        classified = classify_receipt(
            receipt,
            revision=current_revision,
            profile=profile,
            runtime=runtime_identity(),
        )
        classified["receipt"] = _relative(sim_root, receipt_path)
        result[name] = classified
        if isinstance(receipt, Mapping):
            attempts[name] = receipt.get("attempt_id")
    # A bundle receipt is an aggregate of its qualified leaf receipts.  It is
    # deliberately classified without an engine/platform profile because one
    # bundle may contain Polars, Spark, local, and cloud steps together.
    for bundle_name, bundle in registry.items():
        if not is_scenario_bundle(bundle):
            continue
        receipt_path = directory / f"{bundle_name}.receipt.json"
        if not receipt_path.is_file():
            result[bundle_name] = {
                "status": "not_run",
                "execution_status": "not_run",
                "classification": "none",
            }
            continue
        try:
            payload = _read_json(receipt_path)
        except CatalogError:
            result[bundle_name] = {
                "status": "historical",
                "execution_status": "blocked",
                "classification": "historical",
                "receipt": _relative(sim_root, receipt_path),
            }
            continue
        steps = payload.get("steps", []) if isinstance(payload, Mapping) else []
        expected = [name for name in leaves if name.startswith(bundle_name + ".")]
        refs = [step.get("ref") for step in steps if isinstance(step, Mapping)]
        complete = len(refs) == len(steps) and refs == expected
        current = (
            isinstance(payload, Mapping)
            and payload.get("revision") == current_revision
            and bool(steps)
            and all(
                isinstance(step, Mapping)
                and step.get("ref") in expected
                and step.get("attempt_id")
                and step.get("attempt_id") == attempts.get(step["ref"])
                and result[step["ref"]]["classification"] == "current"
                for step in steps
            )
        )
        statuses = [result[ref]["execution_status"] for ref in refs if ref in result]
        status = (
            "failed" if "failed" in statuses else
            "blocked" if not complete or "blocked" in statuses or "not_run" in statuses else
            "passed" if statuses and all(value == "passed" for value in statuses) else "blocked"
        )
        result[bundle_name] = {
            "status": status if current else "historical",
            "execution_status": status,
            "classification": "current" if current else "historical",
            "receipt": _relative(sim_root, receipt_path),
        }
    return result


__all__ = ["classify_receipt", "execution_statuses", "runtime_identity", "worktree_fingerprint"]

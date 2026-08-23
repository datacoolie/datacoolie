"""Packaging contract tests for DataCoolie's composable extras."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any


EXPECTED_EXTRAS = {
    "spark",
    "polars",
    "polars-sql",
    "polars-hash",
    "spark-delta",
    "polars-delta",
    "polars-iceberg",
    "aws",
    "fabric-external",
    "databricks-external",
    "source-api",
    "source-excel-polars",
    "source-db-polars",
    "source-db-oracle-polars",
    "source-db-mssql-odbc-polars",
    "metadata-yaml",
    "metadata-excel",
    "metadata-db",
    "all",
}


def _requirement_name(requirement: str) -> str:
    """Return a normalized distribution name from a PEP 508 requirement."""
    match = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", requirement)
    assert match, f"Invalid requirement string: {requirement!r}"
    return re.sub(r"[-_.]+", "-", match.group(1)).lower()


def _load_pyproject() -> dict[str, Any]:
    root = Path(__file__).resolve().parents[2]
    with (root / "pyproject.toml").open("rb") as stream:
        return tomllib.load(stream)


def _load_lock() -> dict[str, Any]:
    root = Path(__file__).resolve().parents[2]
    with (root / "poetry.lock").open("rb") as stream:
        return tomllib.load(stream)


def _poetry_optional_names(pyproject: dict[str, Any]) -> set[str]:
    dependencies = pyproject["tool"]["poetry"]["dependencies"]
    return {
        _requirement_name(name)
        for name, value in dependencies.items()
        if isinstance(value, dict) and value.get("optional") is True
    }


def test_extra_names_are_the_use_case_contract() -> None:
    """The public extra names stay explicit and do not regress to a matrix."""
    pyproject = _load_pyproject()
    extras = set(pyproject["project"]["optional-dependencies"])

    assert extras == EXPECTED_EXTRAS


def test_pep621_and_poetry_surfaces_declare_the_same_optional_distributions() -> None:
    """Every PEP 621 dependency has one matching optional Poetry declaration."""
    pyproject = _load_pyproject()
    extras = pyproject["project"]["optional-dependencies"]
    pep621_names = {
        _requirement_name(requirement)
        for requirements in extras.values()
        for requirement in requirements
    }

    assert pep621_names == _poetry_optional_names(pyproject)


def test_all_contains_the_union_of_every_profile() -> None:
    """The kitchen-sink profile cannot silently omit a capability dependency."""
    pyproject = _load_pyproject()
    extras = pyproject["project"]["optional-dependencies"]
    all_names = [_requirement_name(requirement) for requirement in extras["all"]]
    profile_names = {
        _requirement_name(requirement)
        for extra, requirements in extras.items()
        if extra != "all"
        for requirement in requirements
    }

    assert len(all_names) == len(set(all_names))
    assert set(all_names) == profile_names


def test_lock_extras_match_the_published_profiles() -> None:
    """The committed Poetry lock exposes the same extras as the wheel."""
    pyproject = _load_pyproject()
    lock = _load_lock()
    extras = pyproject["project"]["optional-dependencies"]
    profile_names = {
        _requirement_name(requirement)
        for extra, requirements in extras.items()
        if extra != "all"
        for requirement in requirements
    }

    assert set(lock["extras"]) == EXPECTED_EXTRAS
    assert set(lock["extras"]["all"]) == profile_names
    assert {package["name"] for package in lock["package"]} >= {
        "pyodbc",
        "pyyaml",
    }


def test_every_profile_dependency_is_declared_optional() -> None:
    """A profile must never reference a package absent from Poetry metadata."""
    pyproject = _load_pyproject()
    extras = pyproject["project"]["optional-dependencies"]
    poetry_names = _poetry_optional_names(pyproject)

    for extra, requirements in extras.items():
        missing = {
            _requirement_name(requirement)
            for requirement in requirements
            if _requirement_name(requirement) not in poetry_names
        }
        assert not missing, f"{extra!r} has undeclared dependencies: {missing}"

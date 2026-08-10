"""Contracts for bundled build dependencies, schemas, and capability evidence."""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

import _schema_resolver as schema_resolver
import inspect_capabilities as capability_tool


BUILD_SKILL = Path(__file__).resolve().parents[2] / "datacoolie-build"


class _Registry:
    def __init__(self, *names: str) -> None:
        self._names = list(names)

    def list_plugins(self) -> list[str]:
        return list(reversed(self._names))


def test_build_dependency_manifests_cover_required_and_optional_imports() -> None:
    base = (BUILD_SKILL / "scripts/requirements.txt").read_text(encoding="utf-8").lower()
    excel = (BUILD_SKILL / "scripts/requirements-excel.txt").read_text(encoding="utf-8").lower()

    assert "pyyaml" in base
    assert "jsonschema" in base
    assert "openpyxl" not in base
    assert "-r requirements.txt" in excel
    assert "openpyxl" in excel


def test_schema_resolution_is_bundled_only_and_understands_published_reference() -> None:
    schemas_dir = schema_resolver.find_schemas_dir()
    metadata = {
        "$schema": "https://datacoolie.github.io/datacoolie/schema/0.1.0/metadata.schema.json"
    }
    assert schema_resolver.resolve_schema_version(metadata, schemas_dir) == "0.1.0"
    assert schema_resolver.load_schema("0.1.0", schemas_dir)["$schema"].endswith("2020-12/schema")
    assert schema_resolver.resolve_schema_version({}, schemas_dir) == "0.1.0"

    source = (BUILD_SKILL / "scripts/_schema_resolver.py").read_text(encoding="utf-8")
    for prohibited in (
        "urllib",
        "Path.home",
        "DATACOOLIE_SCHEMAS_DIR",
        "fetch_latest",
        "github.com",
    ):
        assert prohibited not in source


def test_schema_resolution_rejects_unbundled_or_malformed_version() -> None:
    schemas_dir = schema_resolver.find_schemas_dir()
    with pytest.raises(ValueError, match="supported version"):
        schema_resolver.resolve_schema_version({"$schema": "https://example/schema.json"}, schemas_dir)
    with pytest.raises(FileNotFoundError, match="not found"):
        schema_resolver.load_schema("9.9.9", schemas_dir)


def test_capability_inventory_is_sorted_complete_and_secret_free(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = types.ModuleType("datacoolie")
    fake.__file__ = str(tmp_path / "datacoolie/__init__.py")
    fake.__version__ = "1.2.3"
    fake.engine_registry = _Registry("spark", "polars")  # type: ignore[attr-defined]
    fake.platform_registry = _Registry("local", "cloud")  # type: ignore[attr-defined]
    fake.source_registry = _Registry("sql", "csv")  # type: ignore[attr-defined]
    fake.destination_registry = _Registry("delta", "parquet")  # type: ignore[attr-defined]
    fake.transformer_registry = _Registry("row_filter", "schema_converter")  # type: ignore[attr-defined]
    fake.resolver_registry = _Registry("vault", "env")  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "datacoolie", fake)
    def fake_version(name: str) -> str:
        if name == "datacoolie":
            return "1.2.3"
        if name == "base-package":
            return "2.4.0"
        raise capability_tool.PackageNotFoundError(name)

    monkeypatch.setattr(capability_tool, "version", fake_version)
    monkeypatch.setattr(
        capability_tool,
        "requires",
        lambda name: ["optional-package>=1", "base-package>=2"],
    )
    monkeypatch.setattr(capability_tool, "entry_points", lambda group: [])

    first = capability_tool.collect_capabilities()
    second = capability_tool.collect_capabilities()

    assert first == second
    assert list(first["registries"]) == [
        "engines",
        "platforms",
        "sources",
        "destinations",
        "transformers",
        "resolvers",
    ]
    assert first["registries"]["engines"] == ["polars", "spark"]
    assert first["requirements"] == ["base-package>=2", "optional-package>=1"]
    assert first["distribution"]["version_match"] is True
    assert first["dependency_status"] == {
        "base-package": {
            "declarations": ["base-package>=2"],
            "installed_version": "2.4.0",
        },
        "optional-package": {
            "declarations": ["optional-package>=1"],
            "installed_version": None,
        },
    }
    serialized = json.dumps(first).lower()
    for prohibited in ("password", "secret", "token", "connection_config"):
        assert prohibited not in serialized

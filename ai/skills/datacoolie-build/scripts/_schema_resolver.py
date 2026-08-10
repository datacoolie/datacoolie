"""Resolve versioned metadata schemas bundled beside build tooling."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


SKILL_SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"
_SCHEMA_REF_PATTERN = re.compile(
    r"/(?:schema|schemas)/(?P<version>[^/]+)/metadata\.schema\.json(?:[#?].*)?$"
)
_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def find_schemas_dir() -> Path:
    """Return the adjacent bundled schema directory."""
    if not SKILL_SCHEMAS_DIR.is_dir():
        raise FileNotFoundError(
            f"Bundled schemas directory not found: {SKILL_SCHEMAS_DIR}. "
            "Reinstall the skill or refresh project-owned automation."
        )
    return SKILL_SCHEMAS_DIR


def load_compatibility(schemas_dir: Path) -> dict[str, Any]:
    """Load and validate the bundled schema compatibility selector."""
    compatibility_path = schemas_dir / "compatibility.json"
    try:
        payload = json.loads(compatibility_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {compatibility_path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Schema compatibility file must contain an object: {compatibility_path}")
    latest = payload.get("latest_schema")
    if not isinstance(latest, str) or not _VERSION_PATTERN.fullmatch(latest):
        raise ValueError(f"Schema compatibility file has invalid latest_schema: {compatibility_path}")
    return payload


def resolve_schema_version(metadata: dict[str, Any], schemas_dir: Path) -> str:
    """Resolve the metadata-declared version or bundled default version."""
    schema_ref = metadata.get("$schema")
    if schema_ref is not None:
        if not isinstance(schema_ref, str):
            raise ValueError("Metadata $schema must be a string")
        match = _SCHEMA_REF_PATTERN.search(schema_ref)
        if match is None:
            raise ValueError(f"Metadata $schema does not contain a supported version: {schema_ref}")
        version = match.group("version")
        if not _VERSION_PATTERN.fullmatch(version):
            raise ValueError(f"Metadata $schema contains an invalid version: {schema_ref}")
        return version
    return str(load_compatibility(schemas_dir)["latest_schema"])


def load_schema(version: str, schemas_dir: Path) -> dict[str, Any]:
    """Load one bundled JSON Schema version."""
    if not _VERSION_PATTERN.fullmatch(version):
        raise ValueError(f"Invalid schema version: {version!r}")
    schema_path = schemas_dir / version / "metadata.schema.json"
    if not schema_path.is_file():
        available = sorted(path.name for path in schemas_dir.iterdir() if path.is_dir())
        raise FileNotFoundError(
            f"Bundled schema version {version!r} not found at {schema_path}. "
            f"Available versions: {available}"
        )
    try:
        payload = json.loads(schema_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {schema_path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Metadata schema must contain an object: {schema_path}")
    return payload

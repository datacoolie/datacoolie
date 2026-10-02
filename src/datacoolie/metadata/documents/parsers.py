"""Format-neutral metadata document decoding and validation."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, Set

from datacoolie.core.exceptions import MetadataError


METADATA_SECTION_KEYS: frozenset[str] = frozenset(
    {"connections", "dataflows", "schema_hints"}
)
SUPPORTED_METADATA_SUFFIXES: tuple[str, ...] = (".json", ".yaml", ".yml", ".xlsx")


def parse_json_document(raw: str, path: str) -> Any:
    """Decode JSON text and attach the source path to parse failures."""

    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise MetadataError(f"Cannot parse metadata config: {path}") from exc


def parse_yaml_document(raw: str, path: str) -> Any:
    """Decode YAML text using the optional PyYAML dependency."""

    try:
        import yaml  # noqa: WPS433 — optional dependency
    except ImportError as exc:
        raise MetadataError(
            "PyYAML is required for YAML metadata files.  "
            "Install it with:  pip install pyyaml"
        ) from exc
    try:
        return yaml.safe_load(raw)
    except Exception as exc:
        raise MetadataError(f"Cannot parse metadata config: {path}") from exc


def validate_document(
    document: Any,
    path: str,
    *,
    section_keys: Optional[Set[str]] = None,
    required_section: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate one metadata document before it is merged.

    Every document must identify at least one known section.  This prevents an
    empty or arbitrary mapping from being silently accepted as a shard whose
    contents are never loaded.  A top-level ``metadata`` wrapper is accepted
    for project manifests, but the wrapper itself must contain the sections.
    """

    section_keys = section_keys or set(METADATA_SECTION_KEYS)
    if not isinstance(document, dict):
        raise MetadataError(
            f"Metadata document must contain a mapping at root level: {path}"
        )
    if "metadata" in document:
        wrapper = document["metadata"]
        if not isinstance(wrapper, dict):
            raise MetadataError(f"Metadata wrapper must contain a mapping: {path}")
        document = wrapper

    present = section_keys.intersection(document)
    if required_section is not None and required_section not in present:
        raise MetadataError(
            f"Explicit {required_section}_path must contain a '{required_section}' "
            f"list: {path}"
        )
    if not present:
        raise MetadataError(
            "Metadata document must contain a connections, dataflows, or "
            f"schema_hints section: {path}"
        )
    for section in present:
        if not isinstance(document[section], list):
            raise MetadataError(f"Metadata section '{section}' must be a list: {path}")
    return document


__all__ = [
    "METADATA_SECTION_KEYS",
    "SUPPORTED_METADATA_SUFFIXES",
    "parse_json_document",
    "parse_yaml_document",
    "validate_document",
]

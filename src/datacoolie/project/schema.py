"""Project-owned metadata schema resolution and validation.

The versioned JSON Schemas in :mod:`datacoolie.project.schemas` define the
authored metadata contract used by preparation and authoring tooling. Runtime
metadata providers load typed ``core.models`` directly and do not import this
service. The CLI invokes this service but does not own its policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import hashlib
import json
from importlib import resources
from pathlib import PurePosixPath
from typing import Any, Iterable

from jsonschema import Draft202012Validator
from packaging.version import InvalidVersion, Version

from datacoolie import __version__
from datacoolie.core.exceptions import MetadataError


SCHEMA_INDEX_FILENAME = "index.json"
PUBLIC_SCHEMA_BASE_URL = "https://datacoolie.github.io/datacoolie/schema"
LATEST_SCHEMA_URL = f"{PUBLIC_SCHEMA_BASE_URL}/latest/metadata.schema.json"


class MetadataSchemaError(MetadataError):
    """Raised when a bundled metadata schema cannot be resolved or validated."""


@dataclass(frozen=True)
class SchemaDescriptor:
    """Validated metadata schema entry from the bundled index."""

    version: str
    resource_path: str
    public_url: str
    sha256: str

    @property
    def parsed_version(self) -> Version:
        return Version(self.version)


@dataclass(frozen=True)
class ResolvedMetadataSchema:
    """Schema document and identity selected for one framework version."""

    descriptor: SchemaDescriptor
    document: dict[str, Any]
    framework_version: str


@dataclass(frozen=True)
class SchemaDiagnostic:
    """One JSON Schema validation failure with a metadata JSON pointer."""

    message: str
    path: str
    schema_path: str
    validator: str


def _schema_package_root():
    try:
        return resources.files("datacoolie.project.schemas")
    except (ModuleNotFoundError, FileNotFoundError) as exc:
        raise MetadataSchemaError("Bundled metadata schema resources are unavailable") from exc


def _read_resource(relative_path: str) -> bytes:
    candidate = PurePosixPath(relative_path)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise MetadataSchemaError(f"Unsafe metadata schema resource path: {relative_path!r}")
    try:
        resource = _schema_package_root()
        for part in candidate.parts:
            resource = resource.joinpath(part)
        if not resource.is_file():
            raise MetadataSchemaError(f"Metadata schema resource not found: {relative_path}")
        return resource.read_bytes()
    except MetadataSchemaError:
        raise
    except OSError as exc:
        raise MetadataSchemaError(f"Cannot read metadata schema resource: {relative_path}") from exc


def _load_index_payload() -> dict[str, Any]:
    try:
        value = json.loads(_read_resource(SCHEMA_INDEX_FILENAME).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MetadataSchemaError("Bundled metadata schema index is not valid JSON") from exc
    if not isinstance(value, dict) or value.get("index_version") != 1:
        raise MetadataSchemaError("Unsupported bundled metadata schema index")
    if value.get("schema_kind") != "metadata" or not isinstance(value.get("schemas"), list):
        raise MetadataSchemaError("Bundled metadata schema index has an invalid shape")
    return value


def _descriptor(value: Any) -> SchemaDescriptor:
    if not isinstance(value, dict):
        raise MetadataSchemaError("Metadata schema index entries must be objects")
    required = ("version", "path", "url", "sha256")
    if any(not isinstance(value.get(key), str) or not value[key].strip() for key in required):
        raise MetadataSchemaError("Metadata schema index entry is missing required strings")
    try:
        parsed = Version(value["version"])
    except InvalidVersion as exc:
        raise MetadataSchemaError(f"Invalid metadata schema version: {value['version']!r}") from exc
    path = PurePosixPath(value["path"])
    if path.is_absolute() or ".." in path.parts:
        raise MetadataSchemaError(f"Unsafe metadata schema path: {value['path']!r}")
    checksum = value["sha256"].lower()
    if len(checksum) != 64 or any(character not in "0123456789abcdef" for character in checksum):
        raise MetadataSchemaError(f"Invalid metadata schema SHA-256: {value['sha256']!r}")
    if value["url"] != f"{PUBLIC_SCHEMA_BASE_URL}/{parsed}/{path.name}":
        raise MetadataSchemaError(
            f"Metadata schema URL does not match version/path: {value['url']!r}"
        )
    return SchemaDescriptor(
        version=str(parsed),
        resource_path=path.as_posix(),
        public_url=value["url"],
        sha256=checksum,
    )


def available_schemas() -> tuple[SchemaDescriptor, ...]:
    """Return all bundled metadata schemas in parsed-version order."""

    descriptors = [_descriptor(item) for item in _load_index_payload()["schemas"]]
    by_version: dict[Version, SchemaDescriptor] = {}
    for item in descriptors:
        parsed = item.parsed_version
        if parsed in by_version:
            raise MetadataSchemaError(f"Duplicate metadata schema version: {item.version}")
        by_version[parsed] = item
    return tuple(by_version[key] for key in sorted(by_version))


def _framework_version(value: str | None) -> Version:
    if value is not None:
        try:
            return Version(value)
        except InvalidVersion as exc:
            raise MetadataSchemaError(f"Invalid framework version: {value!r}") from exc
    return Version(__version__)


def resolve_metadata_schema(
    framework_version: str | None = None,
    *,
    marker: str | None = None,
) -> ResolvedMetadataSchema:
    """Select the greatest compatible schema version and load its document.

    A stable framework release only considers stable schema entries.  A
    prerelease can select an explicitly matching prerelease contract but never
    a future final release. No network access or public ``latest`` lookup
    occurs; a ``latest`` marker is only an alias for this local selection.
    """

    target = _framework_version(framework_version)
    candidates = [
        item
        for item in available_schemas()
        if item.parsed_version <= target
        and (target.is_prerelease or not item.parsed_version.is_prerelease)
    ]
    if not candidates:
        raise MetadataSchemaError(
            f"No metadata schema is compatible with DataCoolie {target}"
        )
    descriptor = max(candidates, key=lambda item: item.parsed_version)
    raw = _read_resource(descriptor.resource_path)
    actual_hash = hashlib.sha256(raw).hexdigest()
    if actual_hash != descriptor.sha256:
        raise MetadataSchemaError(
            f"Metadata schema checksum mismatch for {descriptor.version}: "
            f"expected {descriptor.sha256}, got {actual_hash}"
        )
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MetadataSchemaError(
            f"Metadata schema {descriptor.version} is not valid JSON"
        ) from exc
    if not isinstance(document, dict):
        raise MetadataSchemaError(f"Metadata schema {descriptor.version} must be an object")
    try:
        Draft202012Validator.check_schema(document)
    except Exception as exc:
        raise MetadataSchemaError(
            f"Metadata schema {descriptor.version} is not a valid Draft 2020-12 schema"
        ) from exc
    if document.get("$id") != descriptor.public_url:
        raise MetadataSchemaError(
            f"Metadata schema {descriptor.version} has an inconsistent $id"
        )
    if marker is not None and marker not in {descriptor.public_url, LATEST_SCHEMA_URL}:
        raise MetadataSchemaError(
            f"Metadata declares $schema {marker!r}, but DataCoolie {target} resolves "
            f"schema {descriptor.public_url!r}"
        )
    return ResolvedMetadataSchema(
        descriptor=descriptor,
        document=document,
        framework_version=str(target),
    )


def _pointer(parts: Iterable[Any]) -> str:
    result = ""
    for part in parts:
        value = str(part).replace("~", "~0").replace("/", "~1")
        result += f"/{value}"
    return result or "/"


@lru_cache(maxsize=8)
def _compiled_validator(schema_version: str) -> Draft202012Validator:
    """Compile one immutable bundled schema for repeated provider rows."""

    resolved = resolve_metadata_schema(schema_version)
    return Draft202012Validator(resolved.document)


def validate_metadata_schema(
    metadata: Any,
    *,
    framework_version: str | None = None,
    source: str | None = None,
) -> tuple[ResolvedMetadataSchema, tuple[SchemaDiagnostic, ...]]:
    """Validate one normalized authored metadata document against its schema."""

    if not isinstance(metadata, dict):
        raise MetadataSchemaError("Metadata document must be an object")
    marker = metadata.get("$schema")
    if marker is not None and not isinstance(marker, str):
        raise MetadataSchemaError("Metadata $schema must be a string when provided")
    resolved = resolve_metadata_schema(framework_version, marker=marker)
    validator = _compiled_validator(resolved.descriptor.version)
    diagnostics = tuple(
        SchemaDiagnostic(
            message=(f"{source}: {error.message}" if source else error.message),
            path=_pointer(error.absolute_path),
            schema_path=_pointer(error.absolute_schema_path),
            validator=error.validator,
        )
        for error in sorted(
            validator.iter_errors(metadata),
            key=lambda item: tuple(str(part) for part in item.absolute_path),
        )
    )
    return resolved, diagnostics


__all__ = [
    "MetadataSchemaError",
    "LATEST_SCHEMA_URL",
    "PUBLIC_SCHEMA_BASE_URL",
    "ResolvedMetadataSchema",
    "SchemaDescriptor",
    "SchemaDiagnostic",
    "available_schemas",
    "resolve_metadata_schema",
    "validate_metadata_schema",
]

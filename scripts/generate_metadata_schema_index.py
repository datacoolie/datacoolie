"""Generate or verify the bundled framework metadata-schema index.

The versioned schema files are the source of truth. ``index.json`` is a
checked inventory used by the package resolver; the docs publication hook
augments its public copy with a generated ``latest`` alias descriptor. This
script is intentionally repository-local so release/docs jobs can fail before
publishing a stale or inconsistent index.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from packaging.version import InvalidVersion, Version


PUBLIC_SCHEMA_BASE_URL = "https://datacoolie.github.io/datacoolie/schema"
INDEX_FILENAME = "index.json"
LATEST_SCHEMA_PATH = "latest/metadata.schema.json"
LATEST_SCHEMA_URL = f"{PUBLIC_SCHEMA_BASE_URL}/{LATEST_SCHEMA_PATH}"


def _schema_root(root: Path | str | None = None) -> Path:
    if root is not None:
        return Path(root).expanduser().resolve()
    return Path(__file__).resolve().parents[1] / "src" / "datacoolie" / "project" / "schemas"


def _json_pointer(document: Any, pointer: str) -> Any:
    value = document
    for part in pointer.lstrip("/").split("/"):
        if not part:
            continue
        part = part.replace("~1", "/").replace("~0", "~")
        if isinstance(value, list):
            value = value[int(part)]
        elif isinstance(value, dict):
            value = value[part]
        else:
            raise KeyError(pointer)
    return value


def _validate_local_refs(document: dict[str, Any], *, path: Path) -> None:
    def walk(value: Any) -> None:
        if isinstance(value, dict):
            reference = value.get("$ref")
            if isinstance(reference, str) and reference.startswith("#"):
                try:
                    _json_pointer(document, reference[1:])
                except (KeyError, IndexError, ValueError) as exc:
                    raise ValueError(f"{path}: unresolved local $ref {reference!r}") from exc
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(document)


def _schema_entry(path: Path, *, root: Path) -> dict[str, str]:
    relative = path.relative_to(root).as_posix()
    parts = Path(relative).parts
    if len(parts) != 2 or parts[1] != "metadata.schema.json":
        raise ValueError(
            f"Schema must be located at <version>/metadata.schema.json: {relative}"
        )
    version_text = parts[0]
    try:
        version = Version(version_text)
    except InvalidVersion as exc:
        raise ValueError(f"Invalid schema directory version: {version_text!r}") from exc
    if str(version) != version_text:
        raise ValueError(f"Schema directory must use normalized version: {relative}")
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read schema JSON: {path}") from exc
    if not isinstance(document, dict):
        raise ValueError(f"Schema must be a JSON object: {path}")
    Draft202012Validator.check_schema(document)
    _validate_local_refs(document, path=path)
    public_url = f"{PUBLIC_SCHEMA_BASE_URL}/{version_text}/metadata.schema.json"
    if document.get("$id") != public_url:
        raise ValueError(f"Schema $id must be {public_url!r}: {path}")
    raw = path.read_bytes()
    return {
        "version": version_text,
        "path": relative,
        "url": public_url,
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


def build_index(root: Path | str | None = None) -> dict[str, Any]:
    schema_root = _schema_root(root)
    if not schema_root.is_dir():
        raise ValueError(f"Metadata schema root does not exist: {schema_root}")
    paths = sorted(
        (path for path in schema_root.glob("*/metadata.schema.json") if path.is_file()),
        key=lambda path: Version(path.parent.name),
        reverse=True,
    )
    if not paths:
        raise ValueError(f"No versioned metadata schemas found under {schema_root}")
    entries = [_schema_entry(path, root=schema_root) for path in paths]
    versions = [entry["version"] for entry in entries]
    if len(set(versions)) != len(versions):
        raise ValueError("Duplicate metadata schema versions")
    return {
        "index_version": 1,
        "schema_kind": "metadata",
        "schemas": entries,
    }


def build_public_index(root: Path | str | None = None) -> dict[str, Any]:
    """Build the public index with a generated stable ``latest`` alias."""

    index = build_index(root)
    stable_entries = [
        entry
        for entry in index["schemas"]
        if not Version(entry["version"]).is_prerelease
    ]
    if not stable_entries:
        raise ValueError("No stable metadata schema is available for the latest alias")
    source = stable_entries[0]
    return {
        "index_version": index["index_version"],
        "schema_kind": index["schema_kind"],
        "latest": {
            "version": source["version"],
            "path": LATEST_SCHEMA_PATH,
            "url": LATEST_SCHEMA_URL,
            "source_path": source["path"],
            "source_url": source["url"],
            "sha256": source["sha256"],
        },
        "schemas": index["schemas"],
    }


def index_bytes(root: Path | str | None = None) -> bytes:
    return (json.dumps(build_index(root), indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def public_index_bytes(root: Path | str | None = None) -> bytes:
    return (
        json.dumps(build_public_index(root), indent=2, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def check_index(root: Path | str | None = None) -> None:
    schema_root = _schema_root(root)
    index_path = schema_root / INDEX_FILENAME
    try:
        actual = index_path.read_bytes()
    except OSError as exc:
        raise ValueError(f"Metadata schema index is missing: {index_path}") from exc
    expected = index_bytes(schema_root)
    if actual != expected:
        raise ValueError(
            f"Metadata schema index is stale: {index_path}. "
            "Run generate_metadata_schema_index.py to regenerate it."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, help="Versioned schema root")
    parser.add_argument("--check", action="store_true", help="Fail when index.json is stale")
    args = parser.parse_args(argv)
    try:
        if args.check:
            check_index(args.root)
        else:
            root = _schema_root(args.root)
            (root / INDEX_FILENAME).write_bytes(index_bytes(root))
    except ValueError as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

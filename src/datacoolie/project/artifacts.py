"""Shared file inventory helpers for immutable project artifacts.

The project builder and validators must hash the same relative file set.  This
module keeps that small, deterministic rule in one place; it does not interpret
the manifest or access a remote platform.
"""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Mapping

from .documents import canonical_json
from .manifest import MANIFEST_FILENAME
from .errors import ProjectError
from datacoolie.utils.path_utils import ensure_relative_path


_IGNORED_FILENAMES = frozenset({".gitkeep", ".DS_Store", "Thumbs.db"})


def file_hashes(root: Path | str) -> dict[str, str]:
    """Return deterministic SHA-256 hashes for files below *root*.

    The root ``manifest.json`` is metadata about the inventory and is therefore
    excluded.  Empty-directory markers and common platform noise are ignored.
    Symlinks are rejected so an inventory cannot change meaning outside the
    artifact root.
    """

    candidate = Path(root).expanduser()
    if candidate.is_symlink():
        raise ProjectError(f"Artifact root must not be a symlink: {candidate}")
    base = candidate.resolve()
    if not base.is_dir():
        raise ProjectError(f"Artifact directory not found: {base}")
    result: dict[str, str] = {}
    for path in sorted(base.rglob("*"), key=lambda item: (item.as_posix().casefold(), item.as_posix())):
        if path.is_symlink():
            raise ProjectError(f"Artifact must not contain symlinks: {path}")
        if not path.is_file() or path.name in _IGNORED_FILENAMES:
            continue
        if path == base / MANIFEST_FILENAME:
            continue
        digest = sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        result[path.relative_to(base).as_posix()] = digest.hexdigest()
    return result


def inventory_entries(root: Path | str) -> list[dict[str, str]]:
    """Return the canonical manifest inventory for *root*."""

    return [
        {"path": path, "sha256": digest}
        for path, digest in file_hashes(root).items()
    ]


def inventory_digest(inventory: Mapping[str, str]) -> str:
    """Hash a canonical relative-path/hash inventory."""

    payload = [
        {"path": path, "sha256": digest}
        for path, digest in sorted(
            inventory.items(),
            key=lambda item: (item[0].casefold(), item[0]),
        )
    ]
    return sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def declared_inventory(manifest: Mapping[str, object]) -> dict[str, str]:
    """Parse the root manifest's file inventory with strict path/hash checks."""

    entries = manifest.get("artifacts")
    if not isinstance(entries, list):
        raise ProjectError("Build manifest artifact inventory must be an array")
    result: dict[str, str] = {}
    for index, entry in enumerate(entries):
        if not isinstance(entry, Mapping):
            raise ProjectError(f"Invalid artifact inventory entry at index {index}")
        path = entry.get("path")
        digest = entry.get("sha256")
        if not isinstance(path, str) or not path.strip():
            raise ProjectError(f"Artifact inventory path is invalid at index {index}")
        if not isinstance(digest, str) or len(digest) != 64 or any(
            character not in "0123456789abcdefABCDEF" for character in digest
        ):
            raise ProjectError(f"Artifact inventory hash is invalid at index {index}")
        try:
            normalized = ensure_relative_path(path)
        except ValueError as exc:
            raise ProjectError(f"Artifact inventory path is invalid at index {index}: {path!r}") from exc
        if normalized == MANIFEST_FILENAME:
            raise ProjectError("Artifact inventory must not include the root manifest")
        if normalized in result:
            raise ProjectError(f"Duplicate artifact inventory path: {normalized}")
        result[normalized] = digest.lower()
    return result


def inventory_difference(
    expected: Mapping[str, str],
    actual: Mapping[str, str],
) -> tuple[list[str], list[str], list[str]]:
    """Return missing, extra and changed relative paths in stable order."""

    missing = sorted(set(expected) - set(actual), key=lambda value: (value.casefold(), value))
    extra = sorted(set(actual) - set(expected), key=lambda value: (value.casefold(), value))
    changed = sorted(
        [
            path
            for path in set(expected) & set(actual)
            if expected[path].lower() != actual[path].lower()
        ],
        key=lambda value: (value.casefold(), value),
    )
    return missing, extra, changed


__all__ = [
    "declared_inventory",
    "file_hashes",
    "inventory_difference",
    "inventory_digest",
    "inventory_entries",
]

"""Compile simulator metadata references into provider-ready runtime files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path

# Support direct CLI execution as well as imports through runner.
if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from runner.metadata_document_rules import (  # noqa: E402
    COMPILED_METADATA_DIRNAME, SIMULATOR_ROOT, MetadataCompositionError,
    _is_uri, _read_json, _root_path, _source_candidate,
)
from runner.metadata_documents import load_metadata  # noqa: E402


def _is_composed_json(path: Path) -> bool:
    try:
        raw = _read_json(path)
    except (OSError, MetadataCompositionError):
        # Materialization is a path adapter.  Leave malformed/plain files for
        # the provider so startup diagnostics retain their normal ownership.
        return False
    return isinstance(raw, dict) and (
        "metadata_documents" in raw or "shared_metadata" in raw
    )


def _atomic_write(path: Path, payload: bytes, *, immutable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if immutable and path.exists():
        existing = path.read_bytes()
        if existing != payload:
            raise MetadataCompositionError(f"Immutable metadata artifact changed: {path}")
        return
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _stable_output_path(source: str | os.PathLike[str], root: Path) -> Path:
    return root / COMPILED_METADATA_DIRNAME / Path(source).name


def write_composed_metadata(
    source: str | os.PathLike[str],
    output: str | os.PathLike[str] | None = None,
    *,
    root: str | os.PathLike[str] | None = None,
) -> Path:
    """Compose *source* and atomically write a stable JSON artifact.

    This helper is used by bootstrap/export paths that need a predictable
    basename.  It accepts plain JSON for compatibility, while
    :func:`materialize_metadata_path` intentionally leaves plain JSON at its
    authored path.
    """

    root_path = _root_path(root)
    source_input = Path(source).expanduser()
    if not source_input.is_absolute() and source_input.is_file():
        source_path = source_input.resolve()
    else:
        try:
            source_path = _source_candidate(source, root_path)
        except MetadataCompositionError:
            # Bootstrap callers may pass an existing plain JSON artifact from
            # a deployed/project path outside the simulator tree.  Keep that
            # compatibility path; composed sources still have to live below
            # the simulator root so their references cannot escape it.
            candidate = Path(source).expanduser().resolve()
            if not candidate.is_file():
                raise
            raw = _read_json(candidate)
            if isinstance(raw, dict) and ("metadata_documents" in raw or "shared_metadata" in raw):
                raise
            source_path = candidate
    payload = load_metadata(source_path, root=root_path)
    if output is None:
        target = _stable_output_path(source_path, root_path)
    elif Path(output).is_absolute():
        target = Path(output).expanduser().resolve()
    else:
        target = _source_candidate(output, root_path)
    try:
        target.relative_to(root_path)
    except ValueError as exc:
        raise MetadataCompositionError(
            f"Compiled metadata output escapes simulator root {root_path}: {target}"
        ) from exc
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    _atomic_write(target, text.encode("utf-8"))
    return target


def materialize_metadata_path(
    path: str | os.PathLike[str],
    root: str | os.PathLike[str] | None = None,
) -> str:
    """Return a provider-ready path, compiling only composed local JSON.

    Storage URIs, non-JSON metadata and plain JSON are returned unchanged.
    Composed JSON is written as an immutable SHA-256-named artifact below the
    simulator runtime metadata directory; changing the source creates a new
    artifact and never mutates the old one.
    """

    original = str(path)
    if _is_uri(original):
        return original
    if Path(original).suffix.lower() != ".json":
        return original
    root_path = _root_path(root)
    # Prefer an existing caller-relative path.  Direct runner invocations can
    # point at a plain JSON artifact outside ``usecase-sim``; it must pass
    # through instead of being reinterpreted as ``<sim-root>/...``.
    caller_path = Path(original).expanduser()
    if not caller_path.is_absolute() and caller_path.is_file():
        source_path = caller_path.resolve()
    else:
        source_path = None
    try:
        if source_path is None:
            source_path = _source_candidate(original, root_path)
    except MetadataCompositionError:
        candidate = Path(original).expanduser().resolve()
        if not candidate.is_file():
            return original
        if _is_composed_json(candidate):
            raise
        return original
    if not _is_composed_json(source_path):
        return original
    payload = load_metadata(source_path, root=root_path)
    pretty = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    digest = hashlib.sha256(pretty).hexdigest()
    target = root_path / COMPILED_METADATA_DIRNAME / f"{source_path.stem}.{digest}.json"
    _atomic_write(target, pretty, immutable=True)
    return str(target)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compile a simulator metadata workflow")
    parser.add_argument("--source", required=True, help="Product- or simulator-relative JSON path")
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output file; defaults to .runtime/data/metadata/<source basename>",
    )
    args = parser.parse_args()
    root = SIMULATOR_ROOT
    source = _source_candidate(args.source, root)
    target = write_composed_metadata(source, args.output, root=root)
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "COMPILED_METADATA_DIRNAME",
    "MetadataCompositionError",
    "SIMULATOR_ROOT",
    "load_metadata",
    "materialize_metadata_path",
    "write_composed_metadata",
]

"""Copy one verified environment artifact to a local deployment directory.

This is a deterministic fake/local target adapter for tests and local release
workflows. Cloud uploads should use the target platform's official command,
while preserving the same artifact-first/current-second contract.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any, Callable
import uuid


_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_MANIFEST = "manifest.json"
_IGNORED = {".gitkeep", ".DS_Store", "Thumbs.db"}


class UploadError(ValueError):
    """Raised when a local upload cannot satisfy the transfer contract."""


def _validate_id(value: str, label: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise UploadError(f"Invalid {label}")
    return value


def _local_target(value: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise UploadError("deployment_path must be a non-empty local path")
    # This adapter deliberately does not pretend to support cloud URI writes.
    if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", value.strip()):
        raise UploadError("upload_local.py accepts a local deployment_path, not a URI")
    raw_target = Path(value).expanduser()
    # Check the spelling supplied by the caller before resolving it; otherwise
    # a symlinked parent could disappear from the resolved path and become an
    # implicit write outside the requested deployment directory.
    component = raw_target
    while True:
        if component.is_symlink():
            raise UploadError(f"deployment_path must not contain symlinks: {component}")
        parent = component.parent
        if parent == component:
            break
        component = parent
    target = raw_target.resolve()
    return target


def _assert_no_overlap(source: Path, target: Path) -> None:
    source_resolved = source.resolve()
    target_resolved = target.resolve()
    if (
        source_resolved == target_resolved
        or source_resolved in target_resolved.parents
        or target_resolved in source_resolved.parents
    ):
        raise UploadError("source and deployment_path must not overlap")


def _source_files(source: Path, build_id: str, environment: str) -> list[Path]:
    if not source.is_dir() or source.is_symlink():
        raise UploadError(f"Upload source must be a real directory: {source}")
    manifest = source / _MANIFEST
    if not manifest.is_file() or manifest.is_symlink():
        raise UploadError(f"Upload source is missing a regular {_MANIFEST}: {source}")
    try:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UploadError(f"Cannot read source manifest: {manifest}") from exc
    if not isinstance(payload, dict) or payload.get("build_id") != build_id:
        raise UploadError("Source manifest build_id does not match the selected build")
    manifest_environment = payload.get("environment")
    if manifest_environment != environment:
        raise UploadError("Source manifest environment does not match the selected environment")
    files: list[Path] = []
    for path in sorted(source.rglob("*"), key=lambda item: (item.as_posix().casefold(), item.as_posix())):
        if path.name in _IGNORED:
            continue
        if path.is_symlink():
            raise UploadError(f"Upload source must not contain symlinks: {path}")
        if path.is_file():
            files.append(path)
    if not files:
        raise UploadError(f"Upload source contains no files: {source}")
    return files


def _assert_target_parent(path: Path, deployment: Path) -> None:
    """Reject existing symlink components before a file is replaced."""

    current = path
    components: list[Path] = []
    while True:
        components.append(current)
        if current == deployment:
            break
        parent = current.parent
        if parent == current:
            raise UploadError(f"Target path escapes deployment_path: {path}")
        current = parent
    for component in reversed(components):
        if component.is_symlink():
            raise UploadError(f"Upload target must not contain symlinks: {component}")


def _copy_file(source: Path, target: Path, deployment: Path) -> None:
    _assert_target_parent(target.parent, deployment)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_symlink():
        raise UploadError(f"Upload target must not be a symlink: {target}")
    # Replace each object atomically while retaining unrelated objects in the
    # target.  A failed copy cannot leave a truncated existing object.
    temporary_name: str | None = None
    try:
        fd, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
        os.close(fd)
        shutil.copy2(source, temporary_name)
        os.replace(temporary_name, target)
        temporary_name = None
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass


def _copy_phase(
    source: Path,
    destination: Path,
    deployment: Path,
    files: list[Path],
    copy: Callable[[Path, Path, Path], None] = _copy_file,
) -> dict[str, Any]:
    attempted = 0
    try:
        for path in files:
            attempted += 1
            copy(path, destination / path.relative_to(source), deployment)
    except (OSError, UploadError, ValueError) as exc:
        return {"status": "failed", "files": attempted, "error": str(exc)}
    return {"status": "success", "files": attempted}


def _write_record(path: Path, value: dict[str, Any]) -> None:
    path = path.expanduser().resolve()
    if path.is_symlink():
        raise UploadError(f"Upload record must not be a symlink: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def upload_environment(
    source: Path | str,
    deployment_path: Path | str,
    *,
    build_id: str,
    environment: str,
    release_id: str,
    copy: Callable[[Path, Path, Path], None] = _copy_file,
) -> dict[str, Any]:
    """Upload *source* to artifact history, then to current, in that order."""

    source_candidate = Path(source).expanduser()
    if source_candidate.is_symlink():
        raise UploadError(f"Upload source must not be a symlink: {source_candidate}")
    source_path = source_candidate.resolve()
    target = _local_target(str(deployment_path))
    build_id = _validate_id(build_id, "build_id")
    environment = _validate_id(environment, "environment")
    release_id = _validate_id(release_id, "release_id")
    _assert_no_overlap(source_path, target)
    files = _source_files(source_path, build_id, environment)
    artifact_target = target / "artifacts" / build_id
    current_target = target / "current"
    artifact = _copy_phase(source_path, artifact_target, target, files, copy)
    current = (
        _copy_phase(source_path, current_target, target, files, copy)
        if artifact["status"] == "success"
        else {"status": "skipped", "files": 0}
    )
    if artifact["status"] == "failed":
        status = "failed"
    elif current["status"] == "failed":
        status = "partial_failure"
    else:
        status = "success"
    return {
        "schema_version": 1,
        "release_id": release_id,
        "build_id": build_id,
        "environment": environment,
        "deployment_path": str(target),
        "source": str(source_path),
        "status": status,
        "uploads": {"artifact": artifact, "current": current},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--deployment-path", required=True)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--record", type=Path)
    args = parser.parse_args(argv)
    try:
        result = upload_environment(
            args.source,
            args.deployment_path,
            build_id=args.build_id,
            environment=args.environment,
            release_id=args.release_id,
        )
        if args.record:
            _write_record(args.record, result)
    except (OSError, UploadError, ValueError) as exc:
        result = {
            "schema_version": 1,
            "release_id": args.release_id,
            "build_id": args.build_id,
            "environment": args.environment,
            "status": "failed",
            "error": str(exc),
        }
        print(json.dumps(result, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] == "success" else 1


__all__ = ["UploadError", "upload_environment"]


if __name__ == "__main__":
    raise SystemExit(main())

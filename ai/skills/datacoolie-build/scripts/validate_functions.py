#!/usr/bin/env python3
"""Inspect and import one immutable project Python-functions artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import zipfile
from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from typing import Any


COMPILED_SUFFIXES = {".dll", ".dylib", ".pyd", ".so"}
FUNCTION_PARAMETERS = {"engine", "source", "watermark_start", "watermark_end"}
IMPORT_PREFIX_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_members(archive: zipfile.ZipFile) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for item in archive.infolist():
        raw = item.filename.replace("\\", "/")
        path = PurePosixPath(raw)
        if not raw or path.is_absolute() or ".." in path.parts or ":" in path.parts[0]:
            raise ValueError(f"Functions artifact contains an unsafe member: {item.filename!r}")
        normalized = path.as_posix().rstrip("/")
        if not normalized:
            continue
        if normalized in seen:
            raise ValueError(f"Functions artifact contains a duplicate member: {normalized}")
        seen.add(normalized)
        names.append(normalized)
        unix_mode = (item.external_attr >> 16) & 0o170000
        if unix_mode == 0o120000:
            raise ValueError(f"Functions artifact contains a symlink: {normalized}")
        if PurePosixPath(normalized).suffix.lower() in COMPILED_SUFFIXES:
            raise ValueError("Project functions must be a pure-Python artifact")
    return names


def _import_roots(names: list[str], *, excluded_suffix: str | None = None) -> list[str]:
    roots = {
        PurePosixPath(name).parts[0]
        for name in names
        if len(PurePosixPath(name).parts) > 1
        and name.endswith("/__init__.py")
        and (excluded_suffix is None or not PurePosixPath(name).parts[0].endswith(excluded_suffix))
    }
    top_level = {
        root
        for root in roots
        if f"{root}/__init__.py" in names and IMPORT_PREFIX_PATTERN.fullmatch(root)
    }
    if len(top_level) != 1:
        raise ValueError("Functions artifact must contain exactly one top-level import package")
    return sorted(top_level)


def inspect_artifact(path: Path) -> dict[str, str | None]:
    """Return the typed package identity for one validated WHL or ZIP."""
    path = path.resolve()
    if not path.is_file() or path.suffix.lower() not in {".whl", ".zip"}:
        raise ValueError("Functions artifact must be an existing .whl or .zip")
    with zipfile.ZipFile(path) as archive:
        names = _safe_members(archive)
        if path.suffix.lower() == ".zip":
            prefix = _import_roots(names)[0]
            unexpected = [name for name in names if PurePosixPath(name).parts[0] != prefix]
            if unexpected:
                raise ValueError(
                    "Functions ZIP may contain only its declared top-level import package: "
                    f"{unexpected[0]}"
                )
            return {
                "format": "zip",
                "import_prefix": prefix,
                "distribution": None,
                "version": None,
            }

        wheel_files = [name for name in names if name.endswith(".dist-info/WHEEL")]
        metadata_files = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(wheel_files) != 1 or len(metadata_files) != 1:
            raise ValueError("Wheel must contain one dist-info WHEEL and METADATA pair")
        if PurePosixPath(wheel_files[0]).parent != PurePosixPath(metadata_files[0]).parent:
            raise ValueError("Wheel WHEEL and METADATA must use the same dist-info directory")
        wheel_text = archive.read(wheel_files[0]).decode("utf-8", errors="strict")
        tags = [line.partition(":")[2].strip() for line in wheel_text.splitlines() if line.startswith("Tag:")]
        if "Root-Is-Purelib: true" not in wheel_text or not tags or any(
            not tag.endswith("-none-any") for tag in tags
        ):
            raise ValueError("Project functions wheel must be pure Python and platform independent")
        metadata = BytesParser().parsebytes(archive.read(metadata_files[0]))
        distribution = metadata.get("Name")
        package_version = metadata.get("Version")
        if not distribution or not package_version:
            raise ValueError("Wheel METADATA must declare Name and Version")
        prefix = _import_roots(names, excluded_suffix=".dist-info")[0]
        return {
            "format": "wheel",
            "import_prefix": prefix,
            "distribution": distribution,
            "version": package_version,
        }


def referenced_functions(metadata: Any) -> list[str]:
    """Collect configured dotted Python-function paths from resolved metadata."""
    found: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            function = value.get("python_function")
            if isinstance(function, str) and function.strip():
                found.add(function.strip())
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(metadata)
    return sorted(found)


def _validate_prefix(functions: list[str], prefix: str) -> None:
    invalid = [path for path in functions if not path.startswith(f"{prefix}.")]
    if invalid:
        raise ValueError(
            f"Python function path is outside the artifact import prefix {prefix!r}: {invalid[0]}"
        )


def validate_imports(
    artifact_path: Path,
    identity: dict[str, str | None],
    functions: list[str],
) -> None:
    """Import configured callables from only the generated artifact in an isolated process."""
    prefix = identity["import_prefix"]
    if not isinstance(prefix, str):
        raise ValueError("Functions artifact import prefix is invalid")
    if not functions:
        raise ValueError("Functions artifact exists but resolved metadata references no Python function")
    _validate_prefix(functions, prefix)

    probe = """
import importlib, inspect, json, sys
root, raw = sys.argv[1], sys.argv[2]
sys.path.insert(0, root)
required = {"engine", "source", "watermark_start", "watermark_end"}
for dotted in json.loads(raw):
    module_name, attribute = dotted.rsplit(".", 1)
    value = getattr(importlib.import_module(module_name), attribute)
    if not callable(value):
        raise TypeError(f"{dotted} is not callable")
    signature = inspect.signature(value)
    parameters = signature.parameters
    accepts_kwargs = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values())
    keyword_parameters = {
        name
        for name, parameter in parameters.items()
        if parameter.kind is not inspect.Parameter.POSITIONAL_ONLY
    }
    missing = sorted(required - keyword_parameters)
    if missing and not accepts_kwargs:
        raise TypeError(f"{dotted} does not accept framework keywords: {', '.join(missing)}")
"""
    artifact_path = artifact_path.resolve()
    with tempfile.TemporaryDirectory(prefix="datacoolie-functions-import-") as temporary:
        temporary_path = Path(temporary)
        if identity["format"] == "wheel":
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    "--no-deps",
                    "--no-compile",
                    "--target",
                    str(temporary_path / "site"),
                    str(artifact_path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            import_root = temporary_path / "site"
        else:
            import_root = artifact_path
        environment = os.environ.copy()
        environment.pop("PYTHONPATH", None)
        environment.pop("PYTHONHOME", None)
        subprocess.run(
            [sys.executable, "-I", "-c", probe, str(import_root), json.dumps(functions)],
            cwd=temporary_path,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )


def validate_metadata_files(artifact_path: Path, metadata_paths: list[Path]) -> dict[str, Any]:
    identity = inspect_artifact(artifact_path)
    functions: set[str] = set()
    for path in metadata_paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        functions.update(referenced_functions(payload))
    selected = sorted(functions)
    validate_imports(artifact_path, identity, selected)
    return {**identity, "functions": selected, "sha256": sha256_file(artifact_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, action="append", required=True)
    args = parser.parse_args()
    try:
        result = validate_metadata_files(args.artifact, args.metadata)
    except (OSError, ValueError, subprocess.CalledProcessError, zipfile.BadZipFile) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

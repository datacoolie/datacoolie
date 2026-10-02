"""Bounded function-root packaging used by the project build."""

from __future__ import annotations

from hashlib import sha256
from dataclasses import dataclass
import importlib.util
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import zipfile
from typing import Any

from ..errors import ProjectDependencyError, ProjectError


_IGNORED_NAMES = frozenset({".gitkeep", "__pycache__"})
_IGNORED_SUFFIXES = frozenset({".pyc", ".pyo"})


@dataclass(frozen=True)
class FunctionPackagingPlan:
    """The packaging decision made for one configured functions root."""

    root: Path
    requested_packaging: str
    packaging: str
    backend: str | None
    root_name: str | None
    wrapped: bool
    source_file_count: int
    output_name: str | None

    def to_dict(self) -> dict[str, object]:
        """Return a serializable preview of the decision."""

        return {
            "requested_packaging": self.requested_packaging,
            "packaging": self.packaging,
            "backend": self.backend,
            "root_name": self.root_name,
            "wrapped": self.wrapped,
            "source_file_count": self.source_file_count,
            "output_name": self.output_name,
        }


def _iter_files(
    root: Path,
    *,
    missing_message: str,
    symlink_root_message: str,
    symlink_file_message: str,
) -> list[Path]:
    """List build inputs using the same noise and symlink policy everywhere."""

    if root.is_symlink():
        raise ProjectError(f"{symlink_root_message}: {root}")
    if not root.is_dir():
        raise ProjectError(f"{missing_message}: {root}")
    result: list[Path] = []
    for path in sorted(
        root.rglob("*"),
        key=lambda item: (
            item.relative_to(root).as_posix().casefold(),
            item.relative_to(root).as_posix(),
        ),
    ):
        # Check containment before applying ignored-name rules.  A symlink
        # named ``__pycache__`` must not become an escape hatch.
        if path.is_symlink():
            raise ProjectError(f"{symlink_file_message}: {path}")
        relative_parts = path.relative_to(root).parts
        if (
            path.name in _IGNORED_NAMES
            or any(part.casefold() == "__pycache__" for part in relative_parts)
            or path.suffix.casefold() in _IGNORED_SUFFIXES
        ):
            continue
        if path.is_file():
            result.append(path)
    return result


def _files(root: Path) -> list[Path]:
    return _iter_files(
        root,
        missing_message="Configured functions directory not found",
        symlink_root_message="Function build does not allow symlink roots",
        symlink_file_message="Function build does not allow symlinks",
    )


def _resource_files(root: Path) -> list[Path]:
    """Return files copied into SQL/function artifact projections."""

    return _iter_files(
        root,
        missing_message="Resource directory not found",
        symlink_root_message="Build resources must not use symlink roots",
        symlink_file_message="Build resources must not contain symlinks",
    )


def _component_files(root: Path) -> list[Path]:
    """Return authored component inputs for the build fingerprint."""

    return _iter_files(
        root,
        missing_message="Configured component directory not found",
        symlink_root_message="Build inputs must not use symlink roots",
        symlink_file_message="Build inputs must not contain symlinks",
    )


def _declared_backend(root: Path) -> tuple[bool, str | None]:
    path = root / "pyproject.toml"
    if not path.is_file():
        return False, None
    try:
        with path.open("rb") as handle:
            document = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ProjectError(f"Cannot parse functions pyproject.toml: {path}") from exc
    build_system = document.get("build-system")
    if build_system is None:
        raise ProjectError(
            f"Functions pyproject.toml must declare [build-system] for wheel packaging: {path}"
        )
    if (
        not isinstance(build_system, dict)
        or not isinstance(build_system.get("build-backend"), str)
        or not build_system["build-backend"].strip()
        or not isinstance(build_system.get("requires"), list)
        or any(not isinstance(item, str) or not item.strip() for item in build_system["requires"])
    ):
        raise ProjectError(
            f"Functions pyproject.toml has an invalid [build-system].build-backend: {path}"
        )
    return True, build_system["build-backend"].strip()


def _sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _deterministic_zip(root: Path, destination: Path, *, wrapper: str | None = None) -> None:
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        members: list[tuple[str, Path | None]] = []
        directories: set[str] = set()
        for source in _files(root):
            relative = source.relative_to(root).as_posix()
            if wrapper:
                relative = f"{wrapper}/{relative}"
            parts = relative.split("/")
            directories.update("/".join(parts[:index]) + "/" for index in range(1, len(parts)))
            members.append((relative, source))
        if wrapper:
            directories.add(f"{wrapper}/")
        for directory in sorted(directories):
            info = zipfile.ZipInfo(directory, date_time=(1980, 1, 1, 0, 0, 0))
            info.external_attr = (0o755 << 16) | 0x10
            archive.writestr(info, b"")
        for relative, source in sorted(members, key=lambda item: item[0]):
            assert source is not None
            info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, source.read_bytes())


def _copy_tree(root: Path, destination: Path) -> list[str]:
    destination.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []
    for source in _files(root):
        relative = source.relative_to(root)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        paths.append(relative.as_posix())
    return paths


def plan_function_packaging(
    root: Path,
    mode: str,
    *,
    check_dependencies: bool = True,
) -> FunctionPackagingPlan:
    """Validate and decide how one functions root will be packaged.

    The planner performs no writes, subprocess calls, backend imports or
    network access.  It deliberately checks only the local build frontend;
    backend requirements are resolved by the actual build command.
    """

    if mode not in {"auto", "copy", "wheel", "zip"}:
        raise ProjectError(
            "Function packaging mode must be one of auto, copy, wheel, or zip"
        )
    if root.is_symlink():
        raise ProjectError(f"Function build does not allow symlink roots: {root}")
    if not root.is_dir():
        raise ProjectError(f"Configured functions directory not found: {root}")
    files = _files(root)
    has_payload = bool(files)
    if mode in {"auto", "wheel"}:
        declared, backend = _declared_backend(root)
    else:
        declared, backend = False, None
    if mode == "auto":
        if declared:
            effective = "wheel"
        elif (root / "__init__.py").is_file():
            effective = "zip"
        else:
            effective = "copy"
    else:
        effective = mode
    if not has_payload and mode in {"wheel", "zip"}:
        raise ProjectError(
            f"Function packaging '{mode}' cannot package an empty functions directory"
        )
    if effective == "wheel" and not declared:
        raise ProjectError(
            "Function packaging 'wheel' requires a valid [build-system].build-backend"
        )
    if effective == "zip" and not root.name.isidentifier():
        raise ProjectError(
            f"ZIP functions root name must be a valid Python identifier: {root.name!r}"
        )
    if effective == "wheel" and check_dependencies and importlib.util.find_spec("build") is None:
        raise ProjectDependencyError(
            "Wheel packaging requires the 'build' package; install datacoolie[cli]",
            dependency="build",
        )
    return FunctionPackagingPlan(
        root=root,
        requested_packaging=mode,
        packaging=effective,
        backend=backend,
        root_name=root.name if effective == "zip" else None,
        wrapped=effective == "zip" and (root / "__init__.py").is_file(),
        source_file_count=len(files),
        output_name=f"{root.name}.zip" if effective == "zip" else None,
    )


def package_functions(
    root: Path,
    mode: str,
    destination: Path,
    *,
    plan: FunctionPackagingPlan | None = None,
) -> dict[str, Any] | None:
    """Package one functions root according to ``auto|copy|wheel|zip``.

    ``auto`` chooses a wheel for a declared build backend, a wrapped ZIP for
    a package root with ``__init__.py``, and a source-tree copy otherwise.
    The decision is made only at the configured root; nested packages do not
    cause a parent container to be split implicitly.
    """

    selected_plan = plan or plan_function_packaging(root, mode)
    if selected_plan.root.resolve() != root.resolve():
        raise ProjectError("Function packaging plan does not match its source root")
    if selected_plan.requested_packaging != mode:
        raise ProjectError("Function packaging plan does not match its requested mode")
    effective = selected_plan.packaging
    destination.mkdir(parents=True, exist_ok=True)
    if effective == "copy":
        _copy_tree(root, destination)
        return {
            "requested_packaging": mode,
            "packaging": "copy",
            "path": destination.as_posix(),
            "files": selected_plan.source_file_count,
        }
    if effective == "zip":
        target = destination / f"{root.name}.zip"
        wrapper = root.name if selected_plan.wrapped else None
        _deterministic_zip(root, target, wrapper=wrapper)
        return {
            "requested_packaging": mode,
            "packaging": "zip",
            "path": target.as_posix(),
            "sha256": _sha256(target),
            "root_name": selected_plan.root_name,
            "wrapped": selected_plan.wrapped,
        }
    if effective != "wheel":
        raise ProjectError(f"Unsupported functions packaging mode: {mode}")
    backend = selected_plan.backend
    with tempfile.TemporaryDirectory(prefix="datacoolie-functions-") as temporary:
        source_copy = Path(temporary) / "source"
        wheel_output = Path(temporary) / "wheel"
        # Keep backend metadata/configuration (pyproject.toml, setup.cfg,
        # MANIFEST.in, and similar files) while excluding interpreter cache
        # noise.  The copied source tree is also rechecked for symlinks.
        _copy_tree(root, source_copy)
        wheel_output.mkdir()
        try:
            subprocess.run(
                [sys.executable, "-m", "build", "--wheel", "--outdir", str(wheel_output)],
                cwd=source_copy,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except FileNotFoundError as exc:
            raise ProjectDependencyError(
                "Wheel packaging requires the 'build' package; install datacoolie[cli]",
                dependency="build",
            ) from exc
        except subprocess.CalledProcessError as exc:
            combined = f"{exc.stderr or ''}\n{exc.stdout or ''}"
            if "No module named build" in combined:
                raise ProjectDependencyError(
                    "Wheel packaging requires the 'build' package; install datacoolie[cli]",
                    dependency="build",
                ) from exc
            detail = (exc.stderr or exc.stdout or "").strip().splitlines()[-1:]
            raise ProjectError(
                "Function wheel build failed" + (f": {detail[0]}" if detail else "")
            ) from exc
        wheels = sorted(wheel_output.glob("*.whl"))
        if len(wheels) != 1:
            raise ProjectError("Function wheel build must produce exactly one wheel")
        target = destination / wheels[0].name
        shutil.copy2(wheels[0], target)
    return {
        "requested_packaging": mode,
        "packaging": "wheel",
        "backend": backend,
        "path": target.as_posix(),
        "sha256": _sha256(target),
    }


__all__ = ["FunctionPackagingPlan", "package_functions", "plan_function_packaging"]

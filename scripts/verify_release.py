"""Run the local quality gate required before a DataCoolie release.

The command intentionally does not inspect or require a clean Git worktree:
maintainers run it while changes are still uncommitted. It does require the
Poetry environment to contain the project's development and documentation
dependencies (including Twine).
"""

from __future__ import annotations

import argparse
import importlib.util
import re
import subprocess
import sys
import tempfile
import tomllib
import venv
from pathlib import Path
from typing import Sequence


VERSION_PATTERN = re.compile(r"^__version__\s*=\s*[\"']([^\"']+)[\"']", re.MULTILINE)
DIST_SUFFIXES = {".whl", ".tar.gz", ".zip"}


def read_package_version(repo_root: Path) -> str:
    """Read the package version declared in ``pyproject.toml``."""

    with (repo_root / "pyproject.toml").open("rb") as stream:
        return str(tomllib.load(stream)["project"]["version"])


def read_runtime_version(repo_root: Path) -> str:
    """Read ``__version__`` from the source package without importing it."""

    init_path = repo_root / "src" / "datacoolie" / "__init__.py"
    match = VERSION_PATTERN.search(init_path.read_text(encoding="utf-8"))
    if match is None:
        raise ValueError(f"Could not find __version__ in {init_path}")
    return match.group(1)


def validate_versions(repo_root: Path, expected_tag: str | None = None) -> str:
    """Return the package version, failing on source, metadata, or tag drift."""

    metadata_version = read_package_version(repo_root)
    runtime_version = read_runtime_version(repo_root)
    if metadata_version != runtime_version:
        raise ValueError(
            "Version mismatch: "
            f"pyproject.toml={metadata_version}, __version__={runtime_version}"
        )

    if expected_tag is not None:
        normalized_tag = expected_tag.removeprefix("v")
        if normalized_tag != metadata_version:
            raise ValueError(
                f"Tag/version mismatch: tag={expected_tag}, package={metadata_version}"
            )
    return metadata_version


def distribution_files(repo_root: Path, version: str | None = None) -> list[Path]:
    """Return current-version build artifacts that Twine must validate."""

    dist_dir = repo_root / "dist"
    artifacts = sorted(
        path
        for path in dist_dir.iterdir()
        if path.is_file() and any(path.name.endswith(suffix) for suffix in DIST_SUFFIXES)
    ) if dist_dir.exists() else []
    if version is None:
        return artifacts
    version_prefix = f"datacoolie-{version}"
    return [
        path
        for path in artifacts
        if path.name.startswith(version_prefix)
        and path.name[len(version_prefix) : len(version_prefix) + 1] in {".", "-"}
    ]


def run_stage(repo_root: Path, name: str, command: Sequence[str]) -> None:
    """Run one release stage and stop immediately when it fails."""

    print(f"\n==> {name}")
    print("    " + " ".join(command))
    completed = subprocess.run(command, cwd=repo_root, check=False)
    if completed.returncode != 0:
        raise SystemExit(f"Release verification failed at: {name}")


def require_twine() -> None:
    """Fail early with setup guidance when the release checker is absent."""

    if importlib.util.find_spec("twine") is None:
        raise SystemExit(
            "Release verification requires Twine in the active Python environment. "
            "Install it with: python -m pip install --upgrade twine"
        )


def wheel_install_smoke(repo_root: Path, version: str, wheel: Path) -> None:
    """Install the wheel into a clean temporary venv and import the package."""

    print("\n==> Wheel install smoke test")
    with tempfile.TemporaryDirectory(prefix="datacoolie-release-") as temp_dir:
        environment = Path(temp_dir) / "venv"
        venv.EnvBuilder(with_pip=True, clear=True).create(environment)
        python_name = "python.exe" if sys.platform == "win32" else "python"
        isolated_python = environment / ("Scripts" if sys.platform == "win32" else "bin") / python_name
        install_command = [
            str(isolated_python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            str(wheel),
        ]
        completed = subprocess.run(install_command, cwd=repo_root, check=False)
        if completed.returncode != 0:
            raise SystemExit("Release verification failed at: wheel install smoke test")
        import_command = [
            str(isolated_python),
            "-c",
            "import datacoolie; assert datacoolie.__version__ == "
            f"{version!r}",
        ]
        completed = subprocess.run(import_command, cwd=repo_root, check=False)
        if completed.returncode != 0:
            raise SystemExit("Release verification failed at: wheel import smoke test")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tag",
        help="Release tag to validate, for example v0.1.3 (used by CI tag jobs).",
    )
    parser.add_argument(
        "--with-spark",
        action="store_true",
        help="Also run the local Spark test module; Spark is excluded by default.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    try:
        version = validate_versions(repo_root, args.tag)
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise SystemExit(f"Release verification failed at: version parity ({exc})") from exc

    print(f"DataCoolie {version}: local release verification")
    require_twine()

    run_stage(repo_root, "Poetry metadata and lock", ["poetry", "check", "--lock", "--strict"])
    run_stage(repo_root, "Build distributions", ["poetry", "build"])

    all_artifacts = distribution_files(repo_root)
    artifacts = distribution_files(repo_root, version)
    if not artifacts:
        raise SystemExit("Release verification failed at: artifact discovery (dist is empty)")
    stale_artifacts = sorted(set(all_artifacts) - set(artifacts))
    if stale_artifacts:
        print("Ignoring historical dist artifacts:")
        for artifact in stale_artifacts:
            print(f"    {artifact.name}")
    if len(artifacts) != 2:
        raise SystemExit(
            "Release verification failed at: artifact discovery "
            f"(expected one wheel and one sdist for {version}, found {len(artifacts)})"
        )
    sdists = [path for path in artifacts if path.name.endswith(".tar.gz")]
    if len(sdists) != 1:
        raise SystemExit(
            "Release verification failed at: artifact discovery "
            f"(expected one sdist, found {len(sdists)})"
        )
    run_stage(
        repo_root,
        "Validate distributions",
        [sys.executable, "-m", "twine", "check", *(str(path) for path in artifacts)],
    )
    wheels = [path for path in artifacts if path.suffix == ".whl"]
    if len(wheels) != 1:
        raise SystemExit(
            "Release verification failed at: artifact discovery "
            f"(expected one wheel, found {len(wheels)})"
        )
    wheel_install_smoke(repo_root, version, wheels[0])
    run_stage(repo_root, "Strict documentation build", ["poetry", "run", "properdocs", "build", "--strict"])
    run_stage(
        repo_root,
        "Non-Spark test suite",
        ["poetry", "run", "pytest", "tests/", "-m", "not spark", "-n", "0", "--tb=short"],
    )

    if args.with_spark:
        run_stage(
            repo_root,
            "Local Spark test module",
            [
                "poetry",
                "run",
                "pytest",
                "tests/unit/engines/test_spark_engine.py",
                "-m",
                "spark",
                "-n",
                "auto",
                "--dist",
                "loadgroup",
            ],
        )

    print(f"\nRelease verification passed for DataCoolie {version}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

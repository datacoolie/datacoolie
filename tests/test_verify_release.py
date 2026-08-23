from pathlib import Path
import sys
import tomllib

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.verify_release import distribution_files, validate_versions  # noqa: E402


def project_version() -> str:
    with (REPO_ROOT / "pyproject.toml").open("rb") as stream:
        return str(tomllib.load(stream)["project"]["version"])


def test_repository_versions_match() -> None:
    assert validate_versions(REPO_ROOT) == project_version()


def test_tag_must_match_repository_version() -> None:
    with pytest.raises(ValueError, match="Tag/version mismatch"):
        validate_versions(REPO_ROOT, f"v{project_version()}-invalid")


def test_distribution_files_ignores_unrelated_files(tmp_path: Path) -> None:
    version = project_version()
    (tmp_path / "dist").mkdir()
    (tmp_path / "dist" / f"datacoolie-{version}-py3-none-any.whl").touch()
    (tmp_path / "dist" / f"datacoolie-{version}.tar.gz").touch()
    (tmp_path / "dist" / "README.txt").touch()

    assert [path.name for path in distribution_files(tmp_path)] == [
        f"datacoolie-{version}-py3-none-any.whl",
        f"datacoolie-{version}.tar.gz",
    ]
    assert [path.name for path in distribution_files(tmp_path, version)] == [
        f"datacoolie-{version}-py3-none-any.whl",
        f"datacoolie-{version}.tar.gz",
    ]

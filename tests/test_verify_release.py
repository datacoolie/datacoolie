from pathlib import Path
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.verify_release import distribution_files, validate_versions  # noqa: E402


def test_repository_versions_match() -> None:
    assert validate_versions(REPO_ROOT) == "0.1.7"


def test_tag_must_match_repository_version() -> None:
    with pytest.raises(ValueError, match="Tag/version mismatch"):
        validate_versions(REPO_ROOT, "v9.9.9")


def test_distribution_files_ignores_unrelated_files(tmp_path: Path) -> None:
    (tmp_path / "dist").mkdir()
    (tmp_path / "dist" / "datacoolie-0.1.3-py3-none-any.whl").touch()
    (tmp_path / "dist" / "datacoolie-0.1.3.tar.gz").touch()
    (tmp_path / "dist" / "README.txt").touch()

    assert [path.name for path in distribution_files(tmp_path)] == [
        "datacoolie-0.1.3-py3-none-any.whl",
        "datacoolie-0.1.3.tar.gz",
    ]
    assert [path.name for path in distribution_files(tmp_path, "0.1.3")] == [
        "datacoolie-0.1.3-py3-none-any.whl",
        "datacoolie-0.1.3.tar.gz",
    ]

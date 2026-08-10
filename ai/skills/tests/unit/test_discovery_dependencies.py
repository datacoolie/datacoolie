"""Dependency routing contract for discovery probes."""
from __future__ import annotations

from pathlib import Path

import introspect_files


SKILL = Path(__file__).parent.parent.parent / "datacoolie-discover"
SCRIPTS = SKILL / "scripts"


def _entries(name: str) -> set[str]:
    return {
        line.strip()
        for line in (SCRIPTS / name).read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


def test_capability_requirements_do_not_install_unrelated_groups():
    assert not (SCRIPTS / "requirements.txt").exists()
    assert _entries("requirements-api.txt") == {"requests>=2.28,<3", "pyyaml>=6,<7"}
    assert _entries("requirements-databases.txt") == {"sqlalchemy>=2,<3"}
    assert _entries("requirements-files.txt") == {"fsspec>=2024.1", "pyarrow>=14"}
    assert _entries("requirements-lakehouse.txt") == {"requests>=2.28,<3"}
    assert _entries("requirements-hive.txt") == {"pyhive[hive]"}


def test_dependency_reference_routes_optional_packages_and_external_clis():
    content = (SKILL / "references/dependency-routing.md").read_text(encoding="utf-8")
    normalized = " ".join(content.split())
    for value in (
        "Exactly one SQLAlchemy driver",
        "`deltalake`, `fastavro`, or `openpyxl`",
        "`s3fs`, `adlfs`, or `gcsfs`",
        "current `databricks` CLI",
        "AWS CLI v2",
    ):
        assert value in normalized
    assert "install every database driver" in normalized


def test_legacy_xls_is_not_advertised_as_openpyxl_compatible():
    assert ".xlsx" in introspect_files.KNOWN_EXTENSIONS
    assert ".xls" not in introspect_files.KNOWN_EXTENSIONS
    assert introspect_files._detect_format_from_path("book.xls") is None

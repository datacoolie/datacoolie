"""Manifest-derived dependency documentation cannot silently omit profiles."""

from copy import deepcopy
from pathlib import Path
import tomllib

import pytest

from docs.scripts._dependency_reference import render_dependency_reference


def _manifest():
    path = Path(__file__).resolve().parents[3] / "pyproject.toml"
    return tomllib.loads(path.read_text(encoding="utf-8"))


def test_reference_includes_every_profile_and_exact_requirement():
    manifest = _manifest()
    content = render_dependency_reference(manifest)
    for name, requirements in manifest["project"]["optional-dependencies"].items():
        row = next(line for line in content.splitlines() if line.startswith(f"| `{name}` |"))
        for requirement in requirements:
            assert f"`{requirement}`" in row
    for requirement in manifest["project"]["dependencies"]:
        assert f"- `{requirement}`" in content
    assert "system ODBC Driver 18" in content
    assert "metadata-provider dependencies" in content


def test_reference_uses_manifest_versions_and_rejects_profile_drift():
    manifest = deepcopy(_manifest())
    manifest["project"]["optional-dependencies"]["polars"] = ["polars>=999"]
    assert "`polars>=999`" in render_dependency_reference(manifest)
    manifest["project"]["optional-dependencies"]["new-profile"] = ["new-package"]
    with pytest.raises(ValueError, match="profile notes disagree"):
        render_dependency_reference(manifest)

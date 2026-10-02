from __future__ import annotations

from pathlib import Path

import pytest

from datacoolie.project.build.functions import plan_function_packaging
from datacoolie.project.errors import ProjectError


def test_auto_packaging_uses_root_init_not_nested_init(tmp_path: Path) -> None:
    root = tmp_path / "functions"
    root.mkdir()
    (root / "loaders").mkdir()
    (root / "loaders" / "__init__.py").write_text("", encoding="utf-8")
    plan = plan_function_packaging(root, "auto")
    assert plan.packaging == "copy"

    (root / "__init__.py").write_text("", encoding="utf-8")
    assert plan_function_packaging(root, "auto").packaging == "zip"


def test_explicit_packaging_is_validated_without_a_skill_validator(tmp_path: Path) -> None:
    root = tmp_path / "functions"
    root.mkdir()
    with pytest.raises(ProjectError, match="empty functions directory"):
        plan_function_packaging(root, "wheel")

from __future__ import annotations

from pathlib import Path

import pytest

from datacoolie.project.artifacts import (
    declared_inventory,
    file_hashes,
    inventory_entries,
    inventory_difference,
    inventory_digest,
)
from datacoolie.project.errors import ProjectError


def test_file_inventory_excludes_root_manifest_and_markers(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text("{}", encoding="utf-8")
    (tmp_path / ".gitkeep").write_text("", encoding="utf-8")
    (tmp_path / "payload.txt").write_text("payload", encoding="utf-8")
    assert set(file_hashes(tmp_path)) == {"payload.txt"}


def test_inventory_entries_keep_file_hash_order(tmp_path: Path) -> None:
    (tmp_path / "z.txt").write_text("z", encoding="utf-8")
    (tmp_path / "a.txt").write_text("a", encoding="utf-8")
    (tmp_path / "m.txt").write_text("m", encoding="utf-8")

    entries = inventory_entries(tmp_path)

    assert [entry["path"] for entry in entries] == ["a.txt", "m.txt", "z.txt"]


def test_declared_inventory_rejects_manifest_and_unsafe_paths() -> None:
    with pytest.raises(ProjectError, match="must not include"):
        declared_inventory({"artifacts": [{"path": "manifest.json", "sha256": "a" * 64}]})
    with pytest.raises(ProjectError, match="invalid"):
        declared_inventory({"artifacts": [{"path": "../payload.txt", "sha256": "a" * 64}]})


def test_inventory_difference_and_digest_are_deterministic() -> None:
    missing, extra, changed = inventory_difference(
        {"a.txt": "a", "b.txt": "b"},
        {"b.txt": "changed", "c.txt": "c"},
    )
    assert missing == ["a.txt"]
    assert extra == ["c.txt"]
    assert changed == ["b.txt"]
    assert inventory_digest({"b.txt": "b", "a.txt": "a"}) == inventory_digest(
        {"a.txt": "a", "b.txt": "b"}
    )

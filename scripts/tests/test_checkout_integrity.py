"""Byte-checked resources must survive Windows Git checkout conversion."""

import hashlib
import json
from pathlib import Path
import shutil
import subprocess

from scripts.generate_metadata_schema_index import check_index


ROOT = Path(__file__).resolve().parents[2]


def test_schema_and_log_hashes_survive_autocrlf_checkout(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    shutil.copyfile(ROOT / ".gitattributes", repo / ".gitattributes")
    for relative in ("src/datacoolie/project/schemas", "tests/fixtures/logging"):
        shutil.copytree(ROOT / relative, repo / relative, ignore=shutil.ignore_patterns("__pycache__"))
    for args in (
        ["init", "--initial-branch", "main"],
        ["config", "user.name", "checkout-test"],
        ["config", "user.email", "checkout@example.invalid"],
        ["config", "core.autocrlf", "true"],
        ["add", "."],
        ["commit", "-m", "byte-checked resources"],
    ):
        subprocess.run(["git", *args], cwd=repo, capture_output=True, check=True)
    # These are synthetic copies in pytest's temporary directory, not user files.
    resources = [
        path
        for relative in ("src/datacoolie/project/schemas", "tests/fixtures/logging")
        for path in (repo / relative).rglob("*")
        if path.is_file()
    ]
    for path in resources:
        path.unlink()
    subprocess.run(["git", "checkout", "HEAD", "--", "."], cwd=repo, capture_output=True, check=True)
    for path in resources:
        assert b"\r\n" not in path.read_bytes(), path
        committed = subprocess.check_output(
            ["git", "show", f"HEAD:{path.relative_to(repo).as_posix()}"], cwd=repo
        )
        assert path.read_bytes() == committed, path
    check_index(repo / "src/datacoolie/project/schemas")
    for version in ("v5", "v6", "v7"):
        manifest = repo / "tests/fixtures/logging" / version / "manifest.json"
        document = json.loads(manifest.read_text(encoding="utf-8"))
        for filename, stream in document["streams"].items():
            payload = (manifest.parent / filename).read_bytes()
            assert hashlib.sha256(payload).hexdigest() == stream["sha256"], filename

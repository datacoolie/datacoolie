from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

from datacoolie.project.build.publication import (
    ProjectPublicationLock,
    PublicationBusyError,
    publish_current,
)
from datacoolie.project.errors import ProjectError


def _build(root: Path, content: str = "candidate") -> Path:
    source = root / "build"
    source.mkdir()
    (source / "manifest.json").write_text("{}", encoding="utf-8")
    (source / "payload.txt").write_text(content, encoding="utf-8")
    return source


def test_publication_keeps_current_when_old_rename_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    current = project / ".builds" / "current"
    current.mkdir(parents=True)
    (current / "payload.txt").write_text("previous", encoding="utf-8")
    source = _build(tmp_path)
    original_rename = Path.rename

    def fail_old_current(self: Path, target: Path) -> Path:
        if self == current:
            raise OSError("simulated current rename failure")
        return original_rename(self, target)

    monkeypatch.setattr(Path, "rename", fail_old_current)
    with ProjectPublicationLock(project):
        with pytest.raises(OSError, match="simulated"):
            publish_current(project, source)
    assert (current / "payload.txt").read_text(encoding="utf-8") == "previous"
    assert not list((project / ".builds").glob(".current-*"))


def test_publication_restores_current_when_candidate_rename_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    current = project / ".builds" / "current"
    current.mkdir(parents=True)
    (current / "payload.txt").write_text("previous", encoding="utf-8")
    source = _build(tmp_path)
    original_rename = Path.rename

    def fail_candidate(self: Path, target: Path) -> Path:
        if self.parent.name == ".builds" and self.name.startswith(".current-") and not self.name.startswith(".current-backup-") and target == current:
            raise OSError("simulated candidate rename failure")
        return original_rename(self, target)

    monkeypatch.setattr(Path, "rename", fail_candidate)
    with ProjectPublicationLock(project):
        with pytest.raises(OSError, match="simulated"):
            publish_current(project, source)
    assert (current / "payload.txt").read_text(encoding="utf-8") == "previous"
    assert not list((project / ".builds").glob(".current-*"))


def test_publication_retains_backup_when_restore_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    current = project / ".builds" / "current"
    current.mkdir(parents=True)
    (current / "payload.txt").write_text("previous", encoding="utf-8")
    source = _build(tmp_path)
    original_rename = Path.rename

    def fail_replacement_and_restore(self: Path, target: Path) -> Path:
        if target == current and (
            self.name.startswith(".current-")
            or self.name.startswith(".current-backup-")
        ):
            raise OSError("simulated replacement/restore failure")
        return original_rename(self, target)

    monkeypatch.setattr(Path, "rename", fail_replacement_and_restore)
    with ProjectPublicationLock(project):
        with pytest.raises(ProjectError, match="recoverable at"):
            publish_current(project, source)
    backups = list((project / ".builds").glob(".current-backup-*"))
    assert len(backups) == 1
    assert (backups[0] / "payload.txt").read_text(encoding="utf-8") == "previous"


def test_publication_lock_rejects_same_process_contender(tmp_path: Path) -> None:
    project = tmp_path / "project"
    with ProjectPublicationLock(project):
        with pytest.raises(PublicationBusyError):
            with ProjectPublicationLock(project):
                pass


def test_publication_lock_serializes_cli_processes(tmp_path: Path) -> None:
    project = tmp_path / "project"
    code = (
        "from pathlib import Path\n"
        "import sys\n"
        "from datacoolie.project.build.publication import ProjectPublicationLock\n"
        "with ProjectPublicationLock(Path(sys.argv[1])):\n"
        "    print('locked', flush=True)\n"
        "    sys.stdin.read()\n"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", code, str(project)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "locked"
        with pytest.raises(PublicationBusyError):
            with ProjectPublicationLock(project):
                pass
    finally:
        assert child.stdin is not None
        child.stdin.close()
        child.wait(timeout=10)
    assert child.returncode == 0, child.stderr.read() if child.stderr is not None else ""


def test_publication_locks_are_project_scoped(tmp_path: Path) -> None:
    with ProjectPublicationLock(tmp_path / "one"):
        with ProjectPublicationLock(tmp_path / "two"):
            pass

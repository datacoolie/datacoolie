"""In-memory Databricks SDK fakes at the public client boundary."""

from __future__ import annotations

import io
from pathlib import PurePosixPath
from types import SimpleNamespace
from typing import BinaryIO


class AlreadyExists(Exception):
    """SDK-shaped conflict for optional-dependency-free unit tests."""


class BadRequest(Exception):
    """SDK-shaped invalid operation for optional-dependency-free unit tests."""


class NotFound(Exception):
    """SDK-shaped exact-path miss for optional-dependency-free unit tests."""


class PermissionDenied(Exception):
    """SDK-shaped authorization failure for optional-dependency-free tests."""


class FakeFiles:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.directories = {
            "/Volumes/main/default/logs",
        }
        self.calls: list[tuple[str, str]] = []

    @staticmethod
    def _normalized(path: str) -> str:
        return path.rstrip("/")

    def _parents(self, path: str) -> list[str]:
        current = PurePosixPath(path).parent
        parents: list[str] = []
        while len(current.parts) > 4:
            parents.append(str(current))
            current = current.parent
        return parents

    def create_directory(self, directory_path: str) -> None:
        path = self._normalized(directory_path)
        self.calls.append(("create_directory", path))
        self.directories.add(path)
        self.directories.update(self._parents(f"{path}/child"))

    def upload(
        self,
        file_path: str,
        contents: BinaryIO,
        *,
        overwrite: bool | None = None,
    ) -> None:
        path = self._normalized(file_path)
        self.calls.append(("upload", path))
        if path in self.files and overwrite is False:
            raise AlreadyExists("exists")
        self.directories.update(self._parents(path))
        self.files[path] = contents.read()

    def download(self, file_path: str) -> SimpleNamespace:
        path = self._normalized(file_path)
        self.calls.append(("download", path))
        if path not in self.files:
            raise NotFound("missing")
        return SimpleNamespace(contents=io.BytesIO(self.files[path]))

    def get_metadata(self, file_path: str) -> SimpleNamespace:
        path = self._normalized(file_path)
        self.calls.append(("get_metadata", path))
        if path not in self.files:
            raise NotFound("missing")
        return SimpleNamespace(
            content_length=len(self.files[path]),
            last_modified="Wed, 01 Jan 2025 00:00:00 GMT",
        )

    def get_directory_metadata(self, directory_path: str) -> None:
        path = self._normalized(directory_path)
        self.calls.append(("get_directory_metadata", path))
        if path not in self.directories:
            raise NotFound("missing")

    def list_directory_contents(self, directory_path: str):
        path = self._normalized(directory_path)
        self.calls.append(("list_directory_contents", path))
        if path not in self.directories:
            raise NotFound("missing")
        prefix = f"{path}/"
        children: dict[str, SimpleNamespace] = {}
        for directory in self.directories:
            if not directory.startswith(prefix):
                continue
            relative = directory.removeprefix(prefix)
            if not relative or "/" in relative:
                continue
            children[directory] = SimpleNamespace(
                name=relative,
                path=directory,
                is_directory=True,
                file_size=0,
                last_modified=None,
            )
        for file_path, data in self.files.items():
            if not file_path.startswith(prefix):
                continue
            relative = file_path.removeprefix(prefix)
            if "/" in relative:
                continue
            children[file_path] = SimpleNamespace(
                name=relative,
                path=file_path,
                is_directory=False,
                file_size=len(data),
                last_modified=1735689600000,
            )
        return iter(children.values())

    def delete(self, file_path: str) -> None:
        path = self._normalized(file_path)
        self.calls.append(("delete", path))
        if path not in self.files:
            raise NotFound("missing")
        del self.files[path]

    def delete_directory(self, directory_path: str) -> None:
        path = self._normalized(directory_path)
        self.calls.append(("delete_directory", path))
        if path not in self.directories:
            raise NotFound("missing")
        prefix = f"{path}/"
        if any(value.startswith(prefix) for value in self.files) or any(
            value.startswith(prefix) for value in self.directories if value != path
        ):
            raise BadRequest("directory is not empty")
        self.directories.remove(path)


class FakeWorkspaceClient:
    def __init__(self) -> None:
        self.files = FakeFiles()
        self.dbutils = SimpleNamespace(
            secrets=SimpleNamespace(
                get=lambda scope, key: f"{scope}:{key}",
                getBytes=lambda scope, key: f"{scope}:{key}".encode(),
            )
        )

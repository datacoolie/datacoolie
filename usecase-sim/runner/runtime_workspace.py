"""Owned Linux runtime coordinates and immutable, host-visible evidence export.

Only fresh standalone validators use this boundary. It never relocates shared
workflow outputs, rewrites native receipts, or changes process-owner registration.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import runpy
import shutil
import stat
import sys
import time
import zipfile
import zlib
from pathlib import Path

ROOT_ENV = "DATACOOLIE_SIM_RUNTIME_ROOT"
WORKSPACES = Path("/tmp/datacoolie-usecase-sim/workspaces")
PRODUCT = Path(__file__).resolve().parents[2]
EXPORTS = PRODUCT / "usecase-sim/.runtime/logs/scenarios/native"
SCRIPTS = frozenset({
    "usecase-sim/scripts/features/incremental/validate_api_date.py",
    "usecase-sim/scripts/features/replay/validate_replay_window_safety.py",
})
MANIFEST_SCHEMA = 2
MEMBER_PREFIX = "runtime/"
ARCHIVE_CHUNK_SIZE = 1024 * 1024
NATIVE_PRODUCT_ROOT = Path("/datacoolie")
MANIFEST_KEYS = frozenset({
    "schema_version",
    "attempt_id",
    "complete",
    "runtime_root",
    "archive_path",
    "member_prefix",
    "revision",
    "runtime",
    "validator_exit_code",
    "validator_wall_s",
    "directories",
    "files",
})


def in_container():
    return Path("/.dockerenv").is_file()


def identity(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise ValueError("workspace identity must be a UUID hex token")
    return value


def runtime_root(default):
    """Resolve the private child coordinate before any runtime aliases import."""
    requested = os.environ.get(ROOT_ENV)
    if requested is None:
        return default
    root = Path(requested)
    if (not in_container()
            or os.environ.get("DATACOOLIE_SIM_EXECUTION_MODE") != "optimized"
            or not root.is_absolute() or root.resolve() != root
            or root.parent.parent != WORKSPACES or root.name != "runtime"):
        raise ValueError("runtime override requires an owned optimized Linux workspace")
    token = identity(root.parent.name)
    owner = json.loads((root.parent / "owner.json").read_text(encoding="utf-8"))
    if owner != {"schema_version": 1, "attempt_id": token, "runtime_root": str(root)}:
        raise ValueError("runtime workspace ownership mismatch")
    if not root.is_dir():
        raise ValueError("runtime workspace is missing")
    return root


def enabled(scenario, *, use_docker):
    value = scenario.get("runtime_workspace")
    if value is None:
        return False
    validation = scenario.get("validation") or {}
    if (value != "linux_fresh" or scenario.get("engine") != "spark"
            or scenario.get("platform", "local") != "local"
            or not scenario.get("skip_pipeline") or not validation.get("in_container")
            or validation.get("script") not in SCRIPTS or scenario.get("setup")
            or scenario.get("invocations")):
        raise ValueError("Linux workspace requires a supported fresh standalone Spark validator")
    return use_docker and os.environ.get("DATACOOLIE_SIM_EXECUTION_MODE") == "optimized"


def command_arguments(token, script, arguments):
    """Arguments after python3; the dispatcher owns Docker/process supervision."""
    identity(token)
    if script not in SCRIPTS:
        raise ValueError("unsupported workspace validator")
    return [str(PRODUCT / "usecase-sim/runner/runtime_workspace.py"),
            "--workspace-id", token, "--script", script, "--", *arguments]


def mapped_arguments(arguments, root):
    args = list(arguments)
    if "--root" in args:
        index = args.index("--root") + 1
        if index >= len(args) or args.count("--root") != 1:
            raise ValueError("workspace validator requires one root argument value")
        requested = Path(args[index])
        requested = (PRODUCT / requested).resolve()
        base = (PRODUCT / "usecase-sim/.runtime/data").resolve()
        if requested == base or not requested.is_relative_to(base):
            raise ValueError("validator root must stay below the canonical data root")
        args[index] = str(root / "data" / requested.relative_to(base))
    return args


def _native_posix(path):
    """Return the path as seen by the Linux Spark container."""
    path = Path(path)
    try:
        relative = path.resolve().relative_to(PRODUCT.resolve())
    except ValueError:
        return path.as_posix()
    return (NATIVE_PRODUCT_ROOT / relative).as_posix()


def _relative_name(root, path):
    return path.relative_to(root).as_posix()


def _validate_relative_name(name, *, kind):
    if (not isinstance(name, str) or not name or name.startswith("/")
            or "\\" in name or "\x00" in name or re.match(r"^[A-Za-z]:/", name)):
        raise ValueError(f"workspace export has unsafe {kind} name: {name!r}")
    parts = name.split("/")
    if (any(part in {"", ".", ".."} for part in parts)
            or any(re.match(r"^[A-Za-z]:", part) for part in parts)):
        raise ValueError(f"workspace export has unsafe {kind} name: {name!r}")
    return name


def _file_record(path, relative):
    try:
        before = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("workspace export requires regular files/directories")
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as source:
            while chunk := source.read(ARCHIVE_CHUNK_SIZE):
                digest.update(chunk)
                size += len(chunk)
        after = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise ValueError(f"workspace export cannot read file: {path}") from exc
    if (before.st_size != size or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns):
        raise ValueError(f"workspace export file changed while reading: {path}")
    return {
        "path": relative,
        "size": size,
        "sha256": digest.hexdigest(),
        "mtime_ns": before.st_mtime_ns,
    }


def _workspace_records(root):
    root = Path(root)
    try:
        root_stat = root.stat(follow_symlinks=False)
    except OSError as exc:
        raise ValueError(f"workspace export root is unavailable: {root}") from exc
    if root.is_symlink() or not stat.S_ISDIR(root_stat.st_mode):
        raise ValueError("workspace export requires a regular directory root")

    directories = []
    files = []
    paths = sorted(
        root.rglob("*"),
        key=lambda path: _relative_name(root, path),
    )
    for path in paths:
        relative = _relative_name(root, path)
        if path.is_symlink():
            raise ValueError("workspace export cannot follow symlinks")
        _validate_relative_name(relative, kind="path")
        try:
            item = path.stat(follow_symlinks=False)
        except OSError as exc:
            raise ValueError(f"workspace export cannot inspect path: {path}") from exc
        if stat.S_ISDIR(item.st_mode):
            directories.append(relative)
        elif stat.S_ISREG(item.st_mode):
            files.append(_file_record(path, relative))
        else:
            raise ValueError("workspace export requires regular files/directories")
    return {"directories": directories, "files": files}


def file_records(root):
    """Return the exact file records for a runtime tree."""
    return _workspace_records(root)["files"]


def _expected_members(directories, files, prefix=MEMBER_PREFIX):
    return {
        prefix,
        *(prefix + directory + "/" for directory in directories),
        *(prefix + record["path"] for record in files),
    }


def _zip_info(name, *, directory):
    info = zipfile.ZipInfo(name)
    info.create_system = 3
    info.compress_type = zipfile.ZIP_DEFLATED
    if directory:
        info.external_attr = (stat.S_IFDIR | 0o755) << 16 | 0x10
    else:
        info.external_attr = (stat.S_IFREG | 0o644) << 16
    return info


def _write_archive(root, archive_path, records):
    root = Path(root)
    archive_path = Path(archive_path)
    if archive_path.resolve().is_relative_to(root.resolve()):
        raise ValueError("workspace archive must be built outside the runtime root")
    with zipfile.ZipFile(
        archive_path,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        allowZip64=True,
    ) as archive:
        archive.writestr(_zip_info(MEMBER_PREFIX, directory=True), b"")
        for directory in records["directories"]:
            archive.writestr(
                _zip_info(MEMBER_PREFIX + directory + "/", directory=True),
                b"",
            )
        for record in records["files"]:
            info = _zip_info(MEMBER_PREFIX + record["path"], directory=False)
            with archive.open(info, mode="w") as target:
                with (root / Path(record["path"])).open("rb") as source:
                    shutil.copyfileobj(source, target, length=ARCHIVE_CHUNK_SIZE)


def _member_kind(name, prefix):
    if (not isinstance(name, str) or not name or "\x00" in name
            or name.startswith("/") or "\\" in name
            or re.match(r"^[A-Za-z]:/", name) or not name.startswith(prefix)):
        raise ValueError(f"workspace archive has unsafe member: {name!r}")
    if name == prefix:
        return "directory"
    relative = name[len(prefix):]
    directory = relative.endswith("/")
    relative = relative[:-1] if directory else relative
    _validate_relative_name(relative, kind="member")
    return "directory" if directory else "file"


def _zip_mode(info):
    mode = (info.external_attr >> 16) & 0o170000
    if mode not in {stat.S_IFREG, stat.S_IFDIR}:
        raise ValueError(f"workspace archive has non-regular member: {info.filename!r}")
    return mode


def _verify_archive(archive_path, directories, files, *, prefix=MEMBER_PREFIX):
    expected = _expected_members(directories, files, prefix)
    file_by_member = {
        prefix + record["path"]: record
        for record in files
    }
    try:
        with zipfile.ZipFile(archive_path, mode="r") as archive:
            infos = {}
            for info in archive.infolist():
                if info.filename in infos:
                    raise ValueError(f"workspace archive has duplicate member: {info.filename!r}")
                kind = _member_kind(info.filename, prefix)
                _zip_mode(info)
                if kind == "directory" and not info.filename.endswith("/"):
                    raise ValueError(f"workspace archive has invalid directory member: {info.filename!r}")
                infos[info.filename] = info
            actual = set(infos)
            if actual != expected:
                missing = sorted(expected - actual)
                extra = sorted(actual - expected)
                raise ValueError(
                    f"workspace archive member set mismatch; missing={missing!r} extra={extra!r}"
                )
            for name in expected:
                info = infos[name]
                if name.endswith("/"):
                    mode = _zip_mode(info)
                    if mode != stat.S_IFDIR or not info.is_dir() or info.file_size != 0:
                        raise ValueError(f"workspace archive has invalid directory member: {name!r}")
                    continue
                mode = _zip_mode(info)
                if mode != stat.S_IFREG or info.is_dir():
                    raise ValueError(f"workspace archive has invalid file member: {name!r}")
                record = file_by_member[name]
                if info.file_size != record["size"]:
                    raise ValueError(f"workspace archive size mismatch: {name!r}")
                digest = hashlib.sha256()
                size = 0
                with archive.open(info, mode="r") as source:
                    while chunk := source.read(ARCHIVE_CHUNK_SIZE):
                        digest.update(chunk)
                        size += len(chunk)
                if size != record["size"] or digest.hexdigest() != record["sha256"]:
                    raise ValueError(f"workspace archive checksum mismatch: {name!r}")
    except (OSError, EOFError, RuntimeError, zlib.error,
            zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
        raise ValueError("workspace archive is unreadable or corrupt") from exc


def export_runtime(root, destination, *, token, exit_code, revision, runtime, elapsed):
    """Publish completion last; incomplete exports have no passing manifest."""
    root = Path(root)
    destination = Path(destination)
    if exit_code == 0 and not all(
        (root / name).is_dir() for name in ("data", "logs", "watermarks")
    ):
        raise ValueError("successful validator must retain data, logs and watermarks")
    records = _workspace_records(root)
    archive_path = destination / "runtime.zip"
    local_archive_path = root.parent / "runtime.zip"
    _write_archive(root, local_archive_path, records)
    _verify_archive(
        local_archive_path,
        records["directories"],
        records["files"],
    )
    shutil.copyfile(local_archive_path, archive_path)
    _verify_archive(
        archive_path,
        records["directories"],
        records["files"],
    )
    manifest = {
        "schema_version": MANIFEST_SCHEMA,
        "attempt_id": token,
        "complete": True,
        "runtime_root": _native_posix(root),
        "archive_path": _native_posix(EXPORTS / token / "runtime.zip"),
        "member_prefix": MEMBER_PREFIX,
        "revision": revision,
        "runtime": runtime,
        "validator_exit_code": exit_code,
        "validator_wall_s": elapsed,
        "directories": records["directories"],
        "files": records["files"],
    }
    with (destination / "manifest.json").open("x", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
    print(
        "SIM_RUNTIME_WORKSPACE="
        + json.dumps(manifest | {"files": len(records["files"])}),
        flush=True,
    )
    return manifest


def _validate_manifest(manifest, destination, token, *, revision):
    if not isinstance(manifest, dict) or set(manifest) != MANIFEST_KEYS:
        raise ValueError("missing, failed or unreconciled current workspace export")
    expected_token = identity(token)
    expected_root = _native_posix(WORKSPACES / expected_token / "runtime")
    expected_archive = _native_posix(EXPORTS / expected_token / "runtime.zip")
    if (isinstance(manifest["schema_version"], bool)
            or not isinstance(manifest["schema_version"], int)
            or manifest["schema_version"] != MANIFEST_SCHEMA
            or manifest["attempt_id"] != expected_token
            or manifest["complete"] is not True
            or (isinstance(manifest["validator_exit_code"], bool)
                or not isinstance(manifest["validator_exit_code"], int)
                or manifest["validator_exit_code"] != 0)
            or manifest["runtime_root"] != expected_root
            or manifest["archive_path"] != expected_archive
            or manifest["member_prefix"] != MEMBER_PREFIX
            or revision is not None and manifest["revision"] != revision
            or not isinstance(manifest["runtime"], dict)
            or isinstance(manifest["validator_wall_s"], bool)
            or not isinstance(manifest["validator_wall_s"], (int, float))
            or not math.isfinite(manifest["validator_wall_s"])
            or manifest["validator_wall_s"] < 0):
        raise ValueError("missing, failed or unreconciled current workspace export")
    directories = manifest["directories"]
    files = manifest["files"]
    if (not isinstance(directories, list)
            or any(not isinstance(directory, str) for directory in directories)
            or directories != sorted(set(directories))):
        raise ValueError("missing, failed or unreconciled current workspace export")
    if not isinstance(files, list):
        raise ValueError("missing, failed or unreconciled current workspace export")
    directory_set = set()
    for directory in directories:
        _validate_relative_name(directory, kind="directory")
        if directory in directory_set:
            raise ValueError("missing, failed or unreconciled current workspace export")
        directory_set.add(directory)
    file_names = []
    file_set = set()
    for record in files:
        if not isinstance(record, dict) or set(record) != {"path", "size", "sha256", "mtime_ns"}:
            raise ValueError("missing, failed or unreconciled current workspace export")
        relative = _validate_relative_name(record["path"], kind="file")
        if relative in file_set:
            raise ValueError("missing, failed or unreconciled current workspace export")
        if (isinstance(record["size"], bool) or not isinstance(record["size"], int)
                or record["size"] < 0 or isinstance(record["mtime_ns"], bool)
                or not isinstance(record["mtime_ns"], int)
                or record["mtime_ns"] < 0
                or not isinstance(record["sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", record["sha256"])):
            raise ValueError("missing, failed or unreconciled current workspace export")
        file_names.append(relative)
        file_set.add(relative)
    if file_names != sorted(file_names) or directory_set & file_set:
        raise ValueError("missing, failed or unreconciled current workspace export")
    if not {"data", "logs", "watermarks"}.issubset(directory_set):
        raise ValueError("missing, failed or unreconciled current workspace export")
    for relative in (*directories, *file_names):
        parts = relative.split("/")
        if any("/".join(parts[:index]) not in directory_set for index in range(1, len(parts))):
            raise ValueError("missing, failed or unreconciled current workspace export")
    return directories, files


def verify_export(destination, token, *, revision=None):
    destination = Path(destination)
    try:
        manifest = json.loads(
            (destination / "manifest.json").read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("missing, failed or unreconciled current workspace export") from exc
    return verify_manifest_archive(manifest, destination / "runtime.zip", token, revision=revision)


def verify_manifest_archive(manifest, archive_path, token, *, revision=None):
    """Verify captured manifest/ZIP evidence through the native export contract."""
    directories, files = _validate_manifest(
        manifest,
        Path(archive_path).parent,
        token,
        revision=revision,
    )
    _verify_archive(
        archive_path,
        directories,
        files,
        prefix=manifest["member_prefix"],
    )
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--script", choices=sorted(SCRIPTS), required=True)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    token = identity(args.workspace_id)
    if not in_container() or os.environ.get("DATACOOLIE_SIM_EXECUTION_MODE") != "optimized":
        raise ValueError("workspace wrapper requires optimized Docker execution")
    if ROOT_ENV in os.environ:
        raise ValueError("workspace wrapper cannot nest or inherit a runtime override")
    root = WORKSPACES / token / "runtime"
    WORKSPACES.mkdir(parents=True, exist_ok=True)
    root.parent.mkdir(exist_ok=False)
    root.mkdir()
    owner = {"schema_version": 1, "attempt_id": token, "runtime_root": str(root)}
    (root.parent / "owner.json").write_text(json.dumps(owner), encoding="utf-8")
    destination = EXPORTS / token
    destination.mkdir(parents=True, exist_ok=False)
    arguments = args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments
    arguments = mapped_arguments(arguments, root)
    os.environ[ROOT_ENV] = str(root)
    os.chdir(PRODUCT)
    sys.path.insert(0, str(PRODUCT / "usecase-sim"))
    from cases.receipts import worktree_fingerprint, runtime_identity
    revision, runtime = worktree_fingerprint(), runtime_identity()
    started = time.perf_counter()
    exit_code = 1
    try:
        sys.argv = [str(PRODUCT / args.script), *arguments]
        try:
            runpy.run_path(sys.argv[0], run_name="__main__")
        except SystemExit as exc:
            exit_code = exc.code if isinstance(exc.code, int) else 0 if exc.code is None else 1
        else:
            exit_code = 0
    finally:
        export_runtime(root, destination, token=token, exit_code=exit_code,
                       revision=revision, runtime=runtime, elapsed=time.perf_counter() - started)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

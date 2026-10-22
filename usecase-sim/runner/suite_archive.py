"""Durable, immutable evidence archives for scenario suites.

The dispatcher keeps its receipts in a rolling directory because that is
convenient for day-to-day use.  A completed dispatch therefore has to retain
the bytes it relied on before a later dispatch can overwrite those receipts.
This module owns that boundary.  It deliberately does not interpret a
scenario result as acceptance: an execution may be ``passed`` while evidence
retention is rejected.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence


class SuiteArchiveError(RuntimeError):
    """Raised when a suite cannot be retained as a complete evidence bundle."""


@dataclass(frozen=True)
class SuiteArchiveResult:
    """The published archive and its independent evidence-acceptance status."""

    suite_id: str
    archive_dir: Path
    manifest_path: Path
    latest_pointer: Path
    execution_status: str
    evidence_status: str = "accepted"


_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
_JSON_SUFFIXES = {".json", ".jsonl"}


class _ExclusiveLock:
    """Small cross-process lock based on an exclusively-created lock file."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._owned = False

    def acquire(self) -> "_ExclusiveLock":
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError as exc:
            raise SuiteArchiveError(
                f"conflicting suite writer/archive lock: {self.path}"
            ) from exc
        except OSError as exc:
            raise SuiteArchiveError(f"cannot acquire suite lock: {self.path}") from exc
        self._owned = True
        try:
            payload = f"pid={os.getpid()}\n"
            os.write(fd, payload.encode("ascii", "replace"))
        except OSError as exc:
            os.close(fd)
            self.release()
            raise SuiteArchiveError(f"cannot write suite lock: {self.path}") from exc
        else:
            os.close(fd)
        return self

    def release(self) -> None:
        if not self._owned:
            return
        self._owned = False
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise SuiteArchiveError(f"cannot release suite lock: {self.path}") from exc

    def __enter__(self) -> "_ExclusiveLock":
        return self.acquire()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


@contextmanager
def suite_writer_lock(suites_root: Path) -> Iterator[None]:
    """Serialize writers that use one rolling scenario-log directory.

    The dispatcher should hold this lock from the first pending receipt write
    until suite finalization.  Archive publication has a separate lock so a
    caller can retain an already-produced suite directly without taking the
    dispatcher lock.
    """

    with _ExclusiveLock(Path(suites_root) / ".suite-writer.lock"):
        yield


def acquire_suite_writer_lock(suites_root: Path) -> _ExclusiveLock:
    """Acquire the rolling-log writer lock for a long-lived dispatch."""

    return _ExclusiveLock(Path(suites_root) / ".suite-writer.lock").acquire()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _utc_now(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).isoformat()


def _safe_suite_id(value: str | None) -> str:
    candidate = value or os.urandom(16).hex()
    if not _SAFE_ID.fullmatch(candidate):
        raise SuiteArchiveError(f"invalid suite id: {candidate!r}")
    return candidate


def _parse_json(raw: bytes, path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SuiteArchiveError(f"corrupt {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise SuiteArchiveError(f"{label} must be a JSON object: {path}")
    return payload


def _project_roots(scenario_log_dir: Path) -> list[Path]:
    """Return roots useful for host and container paths in receipts."""

    roots: list[Path] = []
    resolved = scenario_log_dir.resolve()
    roots.extend([scenario_log_dir, resolved])
    parts = list(resolved.parts)
    if "usecase-sim" in parts:
        index = len(parts) - 1 - parts[::-1].index("usecase-sim")
        roots.append(Path(*parts[:index]))
        roots.append(Path(*parts[: index + 1]))
    for parent in resolved.parents:
        if parent not in roots:
            roots.append(parent)
    return roots


def _resolve_reference(
    raw: str,
    *,
    scenario_log_dir: Path,
    base_dir: Path | None = None,
    sibling_name: str | None = None,
) -> Path:
    """Resolve a receipt/native path without accepting a missing reference."""

    if not isinstance(raw, str) or not raw.strip():
        raise SuiteArchiveError(f"empty evidence reference: {raw!r}")
    normalised = raw.replace("\\", "/")
    candidate_values: list[Path] = []
    raw_path = Path(normalised)
    if raw_path.is_absolute():
        candidate_values.append(raw_path)
    else:
        if base_dir is not None:
            candidate_values.append(base_dir / raw_path)
        candidate_values.append(scenario_log_dir / raw_path)
        # Bundle receipts use repository-relative ``.runtime/...`` paths.
        for root in _project_roots(scenario_log_dir):
            candidate_values.append(root / raw_path)

    # Native exports may preserve the Linux container prefix in their JSON
    # even though the copied host archive is beside the manifest.
    if normalised.startswith("/datacoolie/"):
        suffix = Path(normalised.removeprefix("/datacoolie/"))
        for root in _project_roots(scenario_log_dir):
            candidate_values.append(root / suffix)

    if sibling_name and base_dir is not None:
        candidate_values.append(base_dir / sibling_name)

    seen: set[Path] = set()
    for candidate in candidate_values:
        try:
            resolved = candidate.resolve()
        except OSError:
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.is_file():
            return resolved
    raise SuiteArchiveError(
        f"missing evidence reference {raw!r} from {scenario_log_dir}"
    )


def _safe_relative(path: Path) -> str:
    """Turn an arbitrary path into a stable, traversal-free archive name."""

    value = path.as_posix().replace(":", "")
    value = re.sub(r"[^A-Za-z0-9._/-]+", "_", value)
    value = value.lstrip("/")
    return value or "evidence.bin"


def _validate_native_archive(
    archive_path: Path,
    manifest: Mapping[str, Any] | None = None,
) -> None:
    from runner.runtime_workspace import verify_manifest_archive

    try:
        if manifest is None:
            raise ValueError("native archive requires a manifest")
        verify_manifest_archive(dict(manifest), archive_path, manifest.get("attempt_id"))
    except (OSError, ValueError, TypeError) as exc:
        raise SuiteArchiveError(f"invalid native manifest/archive: {archive_path}: {exc}") from exc


def _execution_status(
    results: Mapping[str, int],
    *,
    aborted: bool,
) -> str:
    if aborted:
        return "aborted"
    return "passed" if all(int(value) == 0 for value in results.values()) else "failed"


class _Collector:
    """Collect and retain a recursive receipt/native evidence closure."""

    def __init__(self, scenario_log_dir: Path, temp_dir: Path) -> None:
        self.scenario_log_dir = scenario_log_dir
        self.temp_dir = temp_dir
        self.project_roots = _project_roots(scenario_log_dir)
        self.entries: dict[Path, tuple[str, str, bytes, str]] = {}
        self.mappings: list[dict[str, Any]] = []
        self._mapping_keys: set[tuple[str, str]] = set()
        self._queued: set[tuple[Path, str]] = set()

    def _retained_name(self, source: Path, kind: str) -> str:
        if kind == "suite":
            return "evidence/suite.timing.json"
        if kind == "bundle":
            return f"evidence/receipts/bundles/{source.name}"
        if kind == "leaf":
            return f"evidence/receipts/leaves/{source.name}"
        if kind == "native-manifest":
            return f"evidence/native/{source.parent.name}/manifest.json"
        if kind == "native-archive":
            return f"evidence/native/{source.parent.name}/runtime.zip"
        return f"evidence/files/{_safe_relative(source)}"

    def retain(
        self,
        source: Path,
        *,
        original_path: str,
        kind: str,
    ) -> tuple[Path, bytes]:
        try:
            resolved = source.resolve(strict=True)
            if not resolved.is_file():
                raise FileNotFoundError(resolved)
            data = resolved.read_bytes()
        except OSError as exc:
            raise SuiteArchiveError(f"unreadable evidence file: {source}") from exc

        digest = _sha256(data)
        retained_name = self._retained_name(resolved, kind)
        existing = self.entries.get(resolved)
        if existing is not None:
            old_name, old_digest, _, old_kind = existing
            if old_digest != digest:
                raise SuiteArchiveError(f"evidence changed during capture: {resolved}")
            retained_name = old_name
            kind = old_kind
        else:
            target = self.temp_dir / retained_name
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                raise SuiteArchiveError(f"retained evidence path collision: {target}")
            target.write_bytes(data)
            self.entries[resolved] = (retained_name, digest, data, kind)

        mapping_key = (original_path, retained_name)
        if mapping_key not in self._mapping_keys:
            self._mapping_keys.add(mapping_key)
            self.mappings.append(
                {
                    "original_path": original_path,
                    "resolved_path": resolved.as_posix(),
                    "retained_path": retained_name,
                    "kind": kind,
                    "size": len(data),
                    "sha256": digest,
                }
            )
        return resolved, data

    def enqueue(
        self,
        source: Path,
        *,
        original_path: str,
        kind: str,
    ) -> dict[str, Any] | None:
        key = (source.resolve(), kind)
        resolved, raw = self.retain(source, original_path=original_path, kind=kind)
        payload = _parse_json(raw, resolved, label=kind) if source.suffix.lower() in _JSON_SUFFIXES else None
        if key in self._queued:
            return payload
        self._queued.add(key)
        if payload is not None:
            self._follow_json(source, payload, kind=kind)
        return payload

    def _follow_json(
        self,
        source: Path,
        payload: Mapping[str, Any],
        *,
        kind: str,
    ) -> None:
        if kind in {"bundle", "suite"}:
            if kind == "bundle":
                steps = payload.get("steps")
                if not isinstance(steps, list):
                    raise SuiteArchiveError(f"bundle receipt has no steps: {source}")
                for step in steps:
                    if not isinstance(step, dict) or not isinstance(step.get("receipt"), str):
                        raise SuiteArchiveError(f"invalid bundle receipt step: {source}")
                    raw = step["receipt"]
                    child = _resolve_reference(
                        raw,
                        scenario_log_dir=self.scenario_log_dir,
                        base_dir=source.parent,
                    )
                    leaf = self.enqueue(child, original_path=raw, kind="leaf") or {}
                    if step.get("attempt_id") != leaf.get("attempt_id"):
                        raise SuiteArchiveError(f"bundle/leaf attempt mismatch: {source}")
                    expected_name = step.get("ref") or step.get("scenario")
                    if leaf.get("scenario") is not None and expected_name is not None and leaf["scenario"] != expected_name:
                        raise SuiteArchiveError(f"bundle/leaf scenario mismatch: {source}")
            return

        if kind != "leaf":
            return

        workspace = payload.get("runtime_workspace")
        if workspace is None:
            return
        if not isinstance(workspace, dict):
            raise SuiteArchiveError(f"invalid runtime_workspace in receipt: {source}")
        if workspace.get("complete") is not True:
            raise SuiteArchiveError(f"incomplete native workspace export: {source}")

        manifest_raw = workspace.get("manifest_path")
        archive_raw = workspace.get("archive_path")
        if manifest_raw is not None and not isinstance(manifest_raw, str):
            raise SuiteArchiveError(f"invalid native manifest reference: {source}")
        if archive_raw is not None and not isinstance(archive_raw, str):
            raise SuiteArchiveError(f"invalid native archive reference: {source}")
        if not manifest_raw:
            raise SuiteArchiveError(f"native workspace missing manifest reference: {source}")
        if workspace.get("attempt_id") != payload.get("attempt_id"):
            raise SuiteArchiveError(f"leaf/native workspace attempt mismatch: {source}")

        manifest_source: Path | None = None
        manifest_payload: dict[str, Any] | None = None
        if manifest_raw:
            manifest_source = _resolve_reference(
                manifest_raw,
                scenario_log_dir=self.scenario_log_dir,
                base_dir=source.parent,
            )
            manifest_payload = self.enqueue(
                manifest_source,
                original_path=manifest_raw,
                kind="native-manifest",
            )
            if manifest_payload is None or manifest_payload.get("attempt_id") != workspace.get("attempt_id"):
                raise SuiteArchiveError(f"native manifest attempt mismatch: {manifest_source}")
            if manifest_payload.get("complete") is False:
                raise SuiteArchiveError(f"incomplete native manifest: {manifest_source}")
            if not archive_raw:
                archive_raw = manifest_payload.get("archive_path")

        archive_references: list[str] = []
        if archive_raw:
            archive_references.append(archive_raw)
        if manifest_payload and isinstance(manifest_payload.get("archive_path"), str):
            manifest_archive = manifest_payload["archive_path"]
            if manifest_archive not in archive_references:
                archive_references.append(manifest_archive)
        if not archive_references:
            raise SuiteArchiveError(f"native workspace missing archive reference: {source}")
        for archive_reference in archive_references:
            fallback_name = Path(archive_reference.replace("\\", "/")).name
            archive_source = _resolve_reference(
                archive_reference,
                scenario_log_dir=self.scenario_log_dir,
                base_dir=manifest_source.parent if manifest_source else source.parent,
                sibling_name=fallback_name,
            )
            self.retain(
                archive_source,
                original_path=archive_reference,
                kind="native-archive",
            )
            retained_name = self.entries[archive_source.resolve()][0]
            _validate_native_archive(self.temp_dir / retained_name, manifest_payload)


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.urandom(8).hex()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    except OSError as exc:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise SuiteArchiveError(f"atomic publication failed: {path}") from exc


def _verify_retained_files(
    archive_dir: Path,
    mappings: Sequence[Mapping[str, Any]],
) -> None:
    for item in mappings:
        retained = archive_dir / str(item["retained_path"])
        try:
            data = retained.read_bytes()
        except OSError as exc:
            raise SuiteArchiveError(f"retained evidence missing: {retained}") from exc
        if _sha256(data) != item["sha256"]:
            raise SuiteArchiveError(f"retained evidence hash mismatch: {retained}")


def finalize_suite_archive(
    *,
    scenario_log_dir: Path,
    selected_names: Sequence[str],
    bundle_names: Sequence[str] = (),
    requested_names: Sequence[str] = (),
    results: Mapping[str, int] | None = None,
    revision_before: str | None = None,
    revision_after: str | None = None,
    total_seconds: float | None = None,
    aborted: bool = False,
    error: str | None = None,
    suite_id: str | None = None,
    archive_root: Path | None = None,
    recorded_at: datetime | None = None,
) -> SuiteArchiveResult:
    """Retain one rolling suite and atomically publish its latest pointer.

    The function never changes any rolling evidence.  It only copies bytes to
    a new suite directory.  Any missing/corrupt reference or publication
    failure raises :class:`SuiteArchiveError` before ``latest-retained.json``
    is changed.
    """

    log_dir = Path(scenario_log_dir).resolve()
    root = Path(archive_root or (log_dir / "suites")).resolve()
    try:
        root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise SuiteArchiveError(f"cannot create suite archive root: {root}") from exc
    chosen_id = _safe_suite_id(suite_id)
    target = root / chosen_id
    pointer = root / "latest-retained.json"
    timing_path = log_dir / "suite.timing.json"
    execution_results = dict(results or {})

    with _ExclusiveLock(root / ".archive.lock"):
        if target.exists():
            raise SuiteArchiveError(f"suite archive collision: {target}")
        try:
            temporary_dir = Path(tempfile.mkdtemp(prefix=f".{chosen_id}.", dir=str(root)))
        except OSError as exc:
            raise SuiteArchiveError(f"cannot stage suite archive: {root}") from exc
        published = False
        try:
            collector = _Collector(log_dir, temporary_dir)
            timing_payload = collector.enqueue(
                timing_path,
                original_path=str(timing_path),
                kind="suite",
            ) or {}
            if timing_payload.get("suite_id") not in {None, chosen_id}:
                raise SuiteArchiveError(f"suite timing belongs to another suite: {timing_path}")
            for name in bundle_names:
                receipt = log_dir / f"{name}.receipt.json"
                collector.enqueue(receipt, original_path=str(receipt), kind="bundle")
            attempts: list[dict[str, Any]] = []
            for name in selected_names:
                receipt = log_dir / f"{name}.receipt.json"
                receipt_payload = collector.enqueue(receipt, original_path=str(receipt), kind="leaf") or {}
                attempt_id = receipt_payload.get("attempt_id")
                if not isinstance(attempt_id, str) or not attempt_id:
                    raise SuiteArchiveError(f"leaf receipt has no attempt_id: {receipt}")
                if receipt_payload.get("scenario") not in {None, name}:
                    raise SuiteArchiveError(f"leaf scenario mismatch: {receipt}")
                attempts.append(
                    {
                        "scenario": name,
                        "attempt_id": attempt_id,
                        "status": receipt_payload.get("status"),
                        "result": receipt_payload.get("result"),
                        "receipt": str(receipt),
                    }
                )

            execution_status = _execution_status(
                execution_results,
                aborted=aborted,
            )
            manifest: dict[str, Any] = {
                "schema_version": 1,
                "suite_id": chosen_id,
                "retained_status": "accepted",
                "execution_status": execution_status,
                "retained_at": _utc_now(recorded_at),
                "source": {
                    "scenario_log_dir": log_dir.as_posix(),
                    "suite_timing": str(timing_path),
                    "requested": list(requested_names),
                    "selected": list(selected_names),
                    "bundles": list(bundle_names),
                    "results": execution_results,
                    "revision_before": revision_before,
                    "revision_after": revision_after,
                    "total_s": total_seconds,
                    "aborted": bool(aborted),
                    "error": error,
                },
                "attempts": attempts,
                "path_mappings": collector.mappings,
                "files": [
                    {
                        "retained_path": item["retained_path"],
                        "sha256": item["sha256"],
                        "size": item["size"],
                        "kind": item["kind"],
                    }
                    for item in collector.mappings
                ],
            }
            manifest_path = temporary_dir / "manifest.json"
            _write_json_atomic(manifest_path, manifest)
            manifest_bytes = manifest_path.read_bytes()
            manifest_sha = _sha256(manifest_bytes)
            manifest["manifest_sha256"] = manifest_sha
            # The digest describes the manifest before its self-referential
            # field; this keeps the pointer independently checkable without a
            # recursive hash value.
            _write_json_atomic(manifest_path, manifest)
            manifest_sha = _sha256(manifest_path.read_bytes())

            # A directory rename publishes the evidence tree as one unit.
            # ``target`` was checked above while the archive lock was held.
            try:
                os.replace(temporary_dir, target)
            except OSError as exc:
                raise SuiteArchiveError(
                    f"atomic suite archive publication failed: {target}"
                ) from exc
            published = True
            _verify_retained_files(target, collector.mappings)
            pointer_payload = {
                "schema_version": 1,
                "suite_id": chosen_id,
                "manifest_path": (target / "manifest.json").as_posix(),
                "manifest_sha256": manifest_sha,
                "updated_at": _utc_now(recorded_at),
            }
            _write_json_atomic(pointer, pointer_payload)
            return SuiteArchiveResult(
                suite_id=chosen_id,
                archive_dir=target,
                manifest_path=target / "manifest.json",
                latest_pointer=pointer,
                execution_status=execution_status,
            )
        except SuiteArchiveError:
            if published:
                shutil.rmtree(target, ignore_errors=True)
            else:
                shutil.rmtree(temporary_dir, ignore_errors=True)
            raise
        except (OSError, ValueError, TypeError) as exc:
            if published:
                shutil.rmtree(target, ignore_errors=True)
            else:
                shutil.rmtree(temporary_dir, ignore_errors=True)
            raise SuiteArchiveError(f"suite archive finalization failed: {exc}") from exc

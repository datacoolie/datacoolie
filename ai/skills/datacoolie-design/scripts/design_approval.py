"""Hash, record, and verify approval for the canonical architecture artifact."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

CANONICAL_ARCHITECTURE = Path("architecture/current.md")
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "design-approval.schema.json"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def approval_receipt_name(architecture_sha256: str) -> str:
    """Return the readable filename while the payload retains the full digest binding."""
    if len(architecture_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in architecture_sha256
    ):
        raise ValueError("Architecture SHA-256 must be 64 lowercase hexadecimal characters")
    return f"architecture-{architecture_sha256[:12]}.approved.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_architecture(workspace: Path, architecture: Path) -> tuple[Path, Path]:
    workspace = workspace.resolve()
    architecture = architecture.resolve()
    try:
        relative = architecture.relative_to(workspace)
    except ValueError as exc:
        raise ValueError("Architecture must be inside the workspace") from exc
    if relative != CANONICAL_ARCHITECTURE:
        raise ValueError("Approval is valid only for architecture/current.md")
    if not architecture.is_file():
        raise ValueError("Architecture file does not exist")
    return workspace, architecture


@lru_cache(maxsize=1)
def _receipt_validator() -> Draft202012Validator:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def validate_receipt(payload: dict[str, Any], architecture: Path) -> str:
    try:
        _receipt_validator().validate(payload)
    except ValidationError as exc:
        location = ".".join(str(part) for part in exc.absolute_path) or "receipt"
        raise ValueError(f"Approval receipt violates schema at {location}: {exc.message}") from exc
    for field in ("approved_by", "approval_reference", "approved_scope"):
        if not payload[field].strip():
            raise ValueError(f"Approval receipt requires non-empty {field}")
    digest = payload["architecture_sha256"]
    actual = sha256_file(architecture)
    if digest != actual:
        raise ValueError("Approval receipt is stale for the current architecture")
    return actual


def record_approval(
    *,
    workspace: Path,
    architecture: Path,
    approved_by: str,
    approval_reference: str,
    approved_scope: str,
    approved_at: str | None = None,
) -> Path:
    workspace, architecture = _canonical_architecture(workspace, architecture)
    timestamp = approved_at or _utc_now()
    payload = {
        "schema_version": 1,
        "artifact_type": "design_approval",
        "decision": "approved",
        "architecture_path": CANONICAL_ARCHITECTURE.as_posix(),
        "architecture_sha256": sha256_file(architecture),
        "approved_at": timestamp,
        "approved_by": approved_by.strip(),
        "approval_reference": approval_reference.strip(),
        "approved_scope": approved_scope.strip(),
    }
    validate_receipt(payload, architecture)
    receipt = (
        workspace
        / ".approvals"
        / "design"
        / approval_receipt_name(payload["architecture_sha256"])
    )
    receipt.parent.mkdir(parents=True, exist_ok=True)
    if receipt.exists():
        existing = json.loads(receipt.read_text(encoding="utf-8"))
        validate_receipt(existing, architecture)
        return receipt
    fd, temp_name = tempfile.mkstemp(prefix=f".{receipt.name}.", dir=receipt.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
        os.replace(temp_name, receipt)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise
    return receipt


def verify_approval(*, workspace: Path, architecture: Path, receipt: Path | None = None) -> str:
    workspace, architecture = _canonical_architecture(workspace, architecture)
    digest = sha256_file(architecture)
    expected_name = approval_receipt_name(digest)
    target = (
        receipt.resolve()
        if receipt
        else workspace / ".approvals" / "design" / expected_name
    )
    expected_parent = (workspace / ".approvals" / "design").resolve()
    if target.parent != expected_parent or target.name != expected_name:
        raise ValueError("Approval receipt path must match the current architecture hash")
    if not target.is_file():
        raise ValueError("Matching design approval receipt does not exist")
    payload = json.loads(target.read_text(encoding="utf-8"))
    return validate_receipt(payload, architecture)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("hash", "verify"):
        sub = subparsers.add_parser(command, allow_abbrev=False)
        sub.add_argument("--workspace", required=True, type=Path)
        sub.add_argument("--architecture", required=True, type=Path)
        if command == "verify":
            sub.add_argument("--receipt", type=Path)
    record = subparsers.add_parser("record", allow_abbrev=False)
    record.add_argument("--workspace", required=True, type=Path)
    record.add_argument("--architecture", required=True, type=Path)
    record.add_argument("--approved-by", required=True)
    record.add_argument("--approval-reference", required=True)
    record.add_argument("--approved-scope", required=True)
    record.add_argument(
        "--confirmed",
        action="store_true",
        help="Assert that explicit approval was received in the current session",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        if args.command == "hash":
            _, architecture = _canonical_architecture(args.workspace, args.architecture)
            print(sha256_file(architecture))
        elif args.command == "verify":
            print(verify_approval(
                workspace=args.workspace,
                architecture=args.architecture,
                receipt=args.receipt,
            ))
        else:
            if not args.confirmed:
                raise ValueError("record requires --confirmed after explicit current-session approval")
            print(record_approval(
                workspace=args.workspace,
                architecture=args.architecture,
                approved_by=args.approved_by,
                approval_reference=args.approval_reference,
                approved_scope=args.approved_scope,
            ))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"ERROR: {exc}") from exc


if __name__ == "__main__":
    main()

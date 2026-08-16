"""Pure identifier and structural signals for watermark shortlisting."""
from __future__ import annotations

import re
from typing import Any, Mapping

from _observation_contract import canonicalize_watermark_candidate

_TEMPORAL_TYPES = {"date", "datetime", "datetime2", "time", "timestamp", "timestamptz"}
_SEQUENCE_TYPES = {
    "bigint", "binary", "byte", "bytes", "decimal", "integer", "long", "number",
    "numeric", "smallint", "tinyint", "uint",
}


def tokenize_identifier(value: Any) -> tuple[str, ...]:
    """Split common source identifier styles without changing the observed name."""
    text = str(value or "").strip()
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    return tuple(part.lower() for part in re.split(r"[^A-Za-z0-9]+", text) if part)


def _is_temporal(data_type: str) -> bool:
    return any(
        token in _TEMPORAL_TYPES or token.startswith("timestamp")
        for token in tokenize_identifier(data_type)
    )


def _is_sequence(data_type: str) -> bool:
    return any(
        token in _SEQUENCE_TYPES or re.fullmatch(r"u?int\d*", token) is not None
        for token in tokenize_identifier(data_type)
    )


def _has_any(tokens: tuple[str, ...], values: set[str]) -> bool:
    return bool(set(tokens) & values)


def _has_phrase(tokens: tuple[str, ...], phrases: set[tuple[str, ...]]) -> bool:
    return tokens in phrases


def suggest_roles(row: Mapping[str, Any]) -> tuple[str, str]:
    """Return explainable shortlist roles; never a confirmed watermark decision."""
    tokens = tokenize_identifier(row.get("column", ""))
    data_type = str(row.get("data_type", ""))
    native_type = str(row.get("native_type", ""))
    native_tokens = tokenize_identifier(native_type)
    sequence_type = _is_sequence(data_type) or _is_sequence(native_type)
    temporal_type = _is_temporal(data_type) or _is_temporal(native_type)
    roles: list[str] = []
    reasons: list[str] = []

    change_phrase = _has_phrase(tokens, {
        ("change", "sequence"), ("change", "version"), ("row", "version"),
        ("rowversion",), ("system", "version"),
    })
    native_change_type = _has_phrase(native_tokens, {
        ("row", "version"), ("rowversion",), ("system", "version"),
    })
    if (change_phrase or native_change_type) and (temporal_type or sequence_type):
        roles.append("change")
        reasons.append("change-version identifier/native type with ordered-compatible type")

    if temporal_type and _has_any(tokens, {"created", "inserted"}):
        roles.append("insert")
        reasons.append("creation identifier with temporal type")

    if temporal_type and _has_any(tokens, {"updated", "update", "modified", "modify"}):
        roles.append("update")
        reasons.append("update identifier with temporal type")

    if (temporal_type or sequence_type) and _has_any(
        tokens, {"deleted", "delete", "removed", "remove", "tombstone"},
    ):
        roles.append("delete")
        reasons.append("persistent-delete identifier with ordered-compatible type")

    append_phrase = _has_phrase(tokens, {
        ("identity",), ("sequence",), ("sequence", "id"),
        ("sequence", "number"), ("surrogate", "id"),
    })
    declared_identity = (
        bool(tokens)
        and tokens[-1] == "id"
        and str(row.get("key", "")) == "primary"
    )
    if sequence_type and (append_phrase or declared_identity):
        roles.extend(("append", "auxiliary"))
        reasons.append("declared numeric identity/sequence; mutability still requires confirmation")

    backward_phrase = _has_phrase(tokens, {
        ("business", "date"), ("event", "date"), ("posted", "date"),
        ("posting", "date"), ("transaction", "date"),
    })
    if temporal_type and backward_phrase:
        roles.append("backward")
        reasons.append("business/transaction date may bound a correction lookback")

    roles = list(dict.fromkeys(roles))
    return canonicalize_watermark_candidate("|".join(roles)), "; ".join(reasons)

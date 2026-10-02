"""Execution eligibility for an authored dataflow snapshot."""

from __future__ import annotations

from datacoolie.core.models.dataflow import DataFlow


def inactive_reason(dataflow: DataFlow) -> str | None:
    """Explain which authored activation flags prevent this run."""
    reasons = []
    if not dataflow.is_active:
        reasons.append("dataflow is inactive")
    if not dataflow.source.connection.is_active:
        reasons.append(f"source connection '{dataflow.source.connection.name}' is inactive")
    if not dataflow.destination.connection.is_active:
        reasons.append(
            f"destination connection '{dataflow.destination.connection.name}' is inactive"
        )
    return "; ".join(reasons) if reasons else None

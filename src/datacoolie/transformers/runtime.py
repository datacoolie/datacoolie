"""Transformer observation contract."""

from dataclasses import dataclass, field

from datacoolie.core.models.runtime import RuntimeInfo


@dataclass
class TransformRuntimeInfo(RuntimeInfo):
    transformers_applied: list[str] = field(default_factory=list)

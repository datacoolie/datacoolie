"""Typed watermark value and status contracts."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping


class WatermarkCandidateKind(str, Enum):
    """Source-owned meaning of a watermark candidate."""

    OBSERVED_MAX = "observed_max"
    REQUEST_END = "request_end"
    OPAQUE = "opaque"
    METADATA = "metadata"


class WatermarkLoadStatus(str, Enum):
    """Provider-load outcome for one attempt."""

    NOT_REQUESTED = "not_requested"
    LOADED = "loaded"
    REUSED = "reused"
    LOAD_ERROR = "load_error"


class WatermarkSaveStatus(str, Enum):
    """Provider-save outcome for one attempt."""

    NOT_REQUESTED = "not_requested"
    NOT_ATTEMPTED = "not_attempted"
    NO_CANDIDATE = "no_candidate"
    SAVED = "saved"
    SAVE_ERROR = "save_error"


@dataclass(frozen=True, slots=True)
class WatermarkCandidate:
    """Pure source observation returned with a successful read."""

    values: dict[str, Any]
    kind: WatermarkCandidateKind

    def __post_init__(self) -> None:
        if not isinstance(self.values, Mapping):
            raise TypeError("WatermarkCandidate.values must be a mapping")
        if any(not isinstance(key, str) for key in self.values):
            raise TypeError("WatermarkCandidate.values keys must be strings")
        object.__setattr__(self, "values", copy.deepcopy(dict(self.values)))
        object.__setattr__(self, "kind", WatermarkCandidateKind(self.kind))

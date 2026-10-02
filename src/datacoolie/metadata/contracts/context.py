"""Typed startup context shared with metadata providers.

The context carries defaults supplied by the runtime owner.  Providers decide
which values they need and retain their effective configuration themselves.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from datacoolie.platforms.base import BasePlatform


@dataclass(frozen=True, slots=True)
class MetadataProviderStartupContext:
    """Optional runtime defaults offered during provider startup."""

    platform: Optional["BasePlatform"] = None
    metadata_base_path: Optional[str] = None
    artifact_base_path: Optional[str] = None
    state_base_path: Optional[str] = None
    log_base_path: Optional[str] = None
    sql_base_path: str | Sequence[str] | None = None

    def __post_init__(self) -> None:
        """Detach caller-owned SQL root sequences from the frozen context."""
        if self.sql_base_path is None or isinstance(self.sql_base_path, str):
            return
        if not isinstance(self.sql_base_path, Sequence):
            raise TypeError(
                "sql_base_path must be a path string or a sequence of path strings"
            )
        object.__setattr__(self, "sql_base_path", tuple(self.sql_base_path))


__all__ = ["MetadataProviderStartupContext"]

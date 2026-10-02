"""Platform abstraction — file and directory operations across storage systems.

Concrete platform adapters are optional dependencies.  Keep this package
initializer lightweight so importing a platform-neutral helper (for example
the pure datatype resolver) does not import cloud SDKs or notebook clients.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datacoolie.platforms.aws_platform import AWSPlatform
    from datacoolie.platforms.base import BasePlatform
    from datacoolie.platforms.databricks_platform import DatabricksPlatform
    from datacoolie.platforms.fabric_platform import FabricPlatform
    from datacoolie.platforms.local_platform import LocalPlatform

__all__ = [
    "BasePlatform",
    "LocalPlatform",
    "AWSPlatform",
    "DatabricksPlatform",
    "FabricPlatform",
]


def __getattr__(name: str):
    """Load a platform adapter only when the caller explicitly requests it."""
    if name == "BasePlatform":
        from datacoolie.platforms.base import BasePlatform

        return BasePlatform
    if name == "LocalPlatform":
        from datacoolie.platforms.local_platform import LocalPlatform

        return LocalPlatform
    if name == "AWSPlatform":
        from datacoolie.platforms.aws_platform import AWSPlatform

        return AWSPlatform
    if name == "DatabricksPlatform":
        from datacoolie.platforms.databricks_platform import DatabricksPlatform

        return DatabricksPlatform
    if name == "FabricPlatform":
        from datacoolie.platforms.fabric_platform import FabricPlatform

        return FabricPlatform
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

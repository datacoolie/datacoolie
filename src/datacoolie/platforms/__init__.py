"""Platform abstraction — file and directory operations across storage systems."""

from datacoolie.platforms.base import BasePlatform
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.platforms.aws_platform import AWSPlatform
from datacoolie.platforms.databricks_platform import DatabricksPlatform
from datacoolie.platforms.fabric_platform import FabricPlatform

__all__ = [
    "BasePlatform",
    "LocalPlatform",
    "AWSPlatform",
    "DatabricksPlatform",
    "FabricPlatform",
]

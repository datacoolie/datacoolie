"""DataCoolie — Metadata-driven, engine-unified, cloud-agnostic ETL framework.

The package root is intentionally lazy.  Importing a pure framework utility
must not import optional engine/platform SDKs; registries and built-in plugins
are initialized the first time a runtime factory or registry is requested.
"""

from importlib.metadata import PackageNotFoundError, version as distribution_version
from typing import TYPE_CHECKING, Any

__all__ = [
    # Base classes
    "PluginRegistry",
    "BaseSecretResolver",
    "BaseEngine",
    "BasePlatform",
    "BaseSourceReader",
    "BaseDestinationWriter",
    "BaseTransformer",
    # Registries
    "engine_registry",
    "platform_registry",
    "source_registry",
    "destination_registry",
    "transformer_registry",
    "resolver_registry",
    # Factory functions
    "create_engine",
    "create_platform",
    "create_source",
    "create_destination",
    "create_transformer",
    "create_resolver",
]

if TYPE_CHECKING:
    from datacoolie.core.registry import PluginRegistry
    from datacoolie.core.secrets.resolver import BaseSecretResolver
    from datacoolie.destinations.base import BaseDestinationWriter
    from datacoolie.engines.base import BaseEngine
    from datacoolie.platforms.base import BasePlatform
    from datacoolie.sources.base import BaseSourceReader
    from datacoolie.transformers.base import BaseTransformer

try:
    __version__ = distribution_version("datacoolie")
except PackageNotFoundError as error:
    raise RuntimeError("DataCoolie distribution metadata is unavailable") from error

def __getattr__(name: str) -> Any:
    """Resolve public classes and registries lazily."""
    registry_names = {
        "engine_registry",
        "platform_registry",
        "source_registry",
        "destination_registry",
        "transformer_registry",
        "resolver_registry",
    }
    if name in registry_names:
        from . import _bootstrap
        return _bootstrap._runtime_state()[name]

    class_imports = {
        "PluginRegistry": ("datacoolie.core.registry", "PluginRegistry"),
        "BaseSecretResolver": ("datacoolie.core.secrets.resolver", "BaseSecretResolver"),
        "BaseEngine": ("datacoolie.engines.base", "BaseEngine"),
        "BasePlatform": ("datacoolie.platforms.base", "BasePlatform"),
        "BaseSourceReader": ("datacoolie.sources.base", "BaseSourceReader"),
        "BaseDestinationWriter": ("datacoolie.destinations.base", "BaseDestinationWriter"),
        "BaseTransformer": ("datacoolie.transformers.base", "BaseTransformer"),
    }
    if name in class_imports:
        module_name, attribute_name = class_imports[name]
        module = __import__(module_name, fromlist=[attribute_name])
        return getattr(module, attribute_name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------

def create_engine(name: str, **kwargs: object) -> Any:
    """Create an engine instance by name (e.g. ``"spark"``, ``"polars"``)."""
    from . import _bootstrap
    return _bootstrap._runtime_state()["engine_registry"].get(name, **kwargs)


def create_platform(name: str, **kwargs: object) -> Any:
    """Create a platform instance by name (e.g. ``"local"``, ``"fabric"``)."""
    from . import _bootstrap
    return _bootstrap._runtime_state()["platform_registry"].get(name, **kwargs)


def create_source(name: str, **kwargs: object) -> Any:
    """Create a source reader by name (e.g. ``"delta"``, ``"csv"``)."""
    from . import _bootstrap
    return _bootstrap._runtime_state()["source_registry"].get(name, **kwargs)


def create_destination(name: str, **kwargs: object) -> Any:
    """Create a destination writer by name (e.g. ``"delta"``, ``"parquet"``)."""
    from . import _bootstrap
    return _bootstrap._runtime_state()["destination_registry"].get(name, **kwargs)


def create_transformer(name: str, **kwargs: object) -> Any:
    """Create a transformer by name (e.g. ``"schema_converter"``)."""
    from . import _bootstrap
    return _bootstrap._runtime_state()["transformer_registry"].get(name, **kwargs)


def create_resolver(name: str, **kwargs: object) -> Any:
    """Create a secret resolver by name (e.g. ``"env"``)."""
    from . import _bootstrap
    return _bootstrap._runtime_state()["resolver_registry"].get(name, **kwargs)

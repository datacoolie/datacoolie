"""Generic plugin registry with lazy discovery via entry points.

:class:`PluginRegistry` provides a thread-safe registry for classes
(engines, platforms, readers, writers, transformers) that supports:

* Manual registration via :meth:`register`
* Lazy auto-discovery via ``importlib.metadata`` entry points
* Instantiation via :meth:`get`
"""

from __future__ import annotations

import logging
import threading
from importlib.metadata import entry_points
from typing import Generic, TypeVar

from datacoolie.core.exceptions import DataCoolieError

logger = logging.getLogger(__name__)

T = TypeVar("T")


class PluginRegistry(Generic[T]):
    """Generic plugin registry with lazy discovery via entry points.

    Type parameter *T* is the base class that all registered plugins
    must be a subclass of (e.g. ``BaseEngine``, ``BasePlatform``).

    Args:
        entry_point_group: The entry-point group name used for
            auto-discovery (e.g. ``"datacoolie.engines"``).
        base_class: The base class that registered plugins must subclass.
    """

    def __init__(self, entry_point_group: str, base_class: type[T]) -> None:
        self._entry_point_group = entry_point_group
        self._base_class = base_class
        self._plugins: dict[str, type[T]] = {}
        self._singletons: dict[str, T] = {}
        self._discovered = False
        self._lock = threading.Lock()

    def register(self, name: str, cls: type[T]) -> None:
        """Manually register a plugin class.

        Args:
            name: Unique plugin name (e.g. ``"spark"``, ``"local"``).
            cls: The plugin class (must be a subclass of *base_class*).

        Raises:
            DataCoolieError: If *cls* is not a subclass of *base_class*.
        """
        if not (isinstance(cls, type) and issubclass(cls, self._base_class)):
            raise DataCoolieError(
                f"Plugin '{name}' must be a subclass of "
                f"{self._base_class.__name__}, got {cls!r}"
            )
        with self._lock:
            existing = self._plugins.get(name)
            if existing is not None and existing is not cls:
                logger.warning(
                    "Plugin '%s' overridden in group '%s': %r → %r",
                    name,
                    self._entry_point_group,
                    existing,
                    cls,
                )
            self._plugins[name] = cls

    def unregister(self, name: str) -> None:
        """Remove a previously registered plugin.

        Useful for testing and dynamic plugin management.

        Args:
            name: Plugin name to remove.

        Raises:
            DataCoolieError: If *name* is not registered.
        """
        with self._lock:
            if name not in self._plugins:
                available = ", ".join(sorted(self._plugins)) or "(none)"
                raise DataCoolieError(
                    f"Cannot unregister '{name}': not registered. "
                    f"Available: {available}"
                )
            del self._plugins[name]

    def get(self, name: str, **kwargs: object) -> T:
        """Get a plugin instance by name.

        On the first call, triggers entry-point discovery so that
        externally installed plugins are found automatically.

        Args:
            name: Plugin name.
            **kwargs: Constructor arguments forwarded to the plugin class.

        Returns:
            An instance of the requested plugin.

        Raises:
            DataCoolieError: If no plugin is registered under *name*.
        """
        with self._lock:
            if not self._discovered:
                self._discover_plugins()

            cls = self._plugins.get(name)
            if cls is None:
                available = ", ".join(sorted(self._plugins)) or "(none)"
                raise DataCoolieError(
                    f"No plugin registered for '{name}'. "
                    f"Available: {available}"
                )
        return cls(**kwargs)

    def get_or_create(self, name: str, **kwargs: object) -> T:
        """Get a cached singleton instance, creating it on first call.

        Unlike :meth:`get`, subsequent calls with the same *name* return
        the **same** instance.  Useful for stateless plugins (e.g.
        secret resolvers) where per-call instantiation is wasteful.

        Args:
            name: Plugin name.
            **kwargs: Constructor arguments forwarded to the plugin class
                on first creation only.

        Returns:
            A (possibly cached) instance of the requested plugin.

        Raises:
            DataCoolieError: If no plugin is registered under *name*.
        """
        with self._lock:
            cached = self._singletons.get(name)
            if cached is not None:
                return cached

            if not self._discovered:
                self._discover_plugins()

            cls = self._plugins.get(name)
            if cls is None:
                available = ", ".join(sorted(self._plugins)) or "(none)"
                raise DataCoolieError(
                    f"No plugin registered for '{name}'. "
                    f"Available: {available}"
                )
            instance = cls(**kwargs)
            self._singletons[name] = instance
        return instance

    def clear_singletons(self) -> None:
        """Evict all cached singleton instances.

        Useful in tests to reset state between runs.
        """
        with self._lock:
            self._singletons.clear()

    def list_plugins(self) -> list[str]:
        """List all registered plugin names (triggers discovery)."""
        with self._lock:
            if not self._discovered:
                self._discover_plugins()
            return sorted(self._plugins)

    def is_available(self, name: str) -> bool:
        """Check if a plugin is registered (triggers discovery)."""
        with self._lock:
            if not self._discovered:
                self._discover_plugins()
            return name in self._plugins

    def _discover_plugins(self) -> None:
        """Scan installed packages for entry-point plugins.

        Called lazily on the first :meth:`get`, :meth:`list_plugins`, or
        :meth:`is_available` invocation.  Must be called while holding
        ``self._lock``.
        """
        try:
            eps = entry_points(group=self._entry_point_group)
            for ep in eps:
                if ep.name not in self._plugins:
                    try:
                        cls = ep.load()
                        if isinstance(cls, type) and issubclass(cls, self._base_class):
                            self._plugins[ep.name] = cls
                    except Exception as exc:
                        logger.debug(
                            "Skipping entry-point plugin '%s' in group '%s': %s",
                            ep.name,
                            self._entry_point_group,
                            exc,
                        )
        except Exception as exc:
            logger.debug(
                "Entry-point discovery failed for group '%s': %s",
                self._entry_point_group,
                exc,
            )
        self._discovered = True

"""Thread-safe plugin registry with lazy entry-point discovery.

Registry state transitions are synchronized, while entry-point loading and
user constructors run outside the state lock. This keeps plugin imports from
deadlocking the registry and makes failed construction visible to callers.
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
    """Registry for manually registered and entry-point plugins."""

    def __init__(self, entry_point_group: str, base_class: type[T]) -> None:
        self._entry_point_group = entry_point_group
        self._base_class = base_class
        self._plugins: dict[str, type[T]] = {}
        self._singletons: dict[str, T] = {}
        self._singleton_failures: dict[str, BaseException] = {}
        self._singleton_inflight: dict[str, tuple[int, int, type[T]]] = {}
        self._generations: dict[str, int] = {}
        self._discovered = False
        self._discovery_in_progress = False
        self._discovery_owner: int | None = None
        self._discovery_error: BaseException | None = None
        self._discovery_plugin_errors: dict[str, BaseException] = {}
        self._condition = threading.Condition(threading.Lock())

    def register(self, name: str, cls: type[T]) -> None:
        """Register or replace a plugin class and invalidate its instance."""

        if not (isinstance(cls, type) and issubclass(cls, self._base_class)):
            raise DataCoolieError(
                f"Plugin '{name}' must be a subclass of "
                f"{self._base_class.__name__}, got {cls!r}"
            )
        with self._condition:
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
            self._generations[name] = self._generations.get(name, 0) + 1
            self._singletons.pop(name, None)
            self._singleton_failures.pop(name, None)
            self._condition.notify_all()

    def unregister(self, name: str) -> None:
        """Remove a registered plugin and invalidate its cached state."""

        with self._condition:
            if name not in self._plugins:
                available = ", ".join(sorted(self._plugins)) or "(none)"
                raise DataCoolieError(
                    f"Cannot unregister '{name}': not registered. "
                    f"Available: {available}"
                )
            del self._plugins[name]
            self._generations[name] = self._generations.get(name, 0) + 1
            self._singletons.pop(name, None)
            self._singleton_failures.pop(name, None)
            self._condition.notify_all()

    def get(self, name: str, **kwargs: object) -> T:
        """Construct a fresh plugin instance outside the registry lock."""

        self._ensure_discovered()
        with self._condition:
            cls = self._plugins.get(name)
            if cls is None:
                self._raise_missing(name)
            assert cls is not None
        try:
            return cls(**kwargs)
        except DataCoolieError:
            raise
        except Exception as exc:
            raise DataCoolieError(
                f"Failed to construct plugin '{name}' in group "
                f"'{self._entry_point_group}': {exc}"
            ) from exc

    def get_or_create(self, name: str, **kwargs: object) -> T:
        """Return one singleton, with deterministic concurrent construction."""

        self._ensure_discovered()
        owner = threading.get_ident()
        with self._condition:
            while True:
                cached = self._singletons.get(name)
                if cached is not None:
                    return cached
                failure = self._singleton_failures.get(name)
                if failure is not None:
                    raise DataCoolieError(
                        f"Plugin '{name}' in group '{self._entry_point_group}' "
                        "failed during singleton construction"
                    ) from failure
                inflight = self._singleton_inflight.get(name)
                if inflight is not None:
                    inflight_owner, _generation, _cls = inflight
                    if inflight_owner == owner:
                        raise DataCoolieError(
                            f"Recursive singleton construction detected for plugin "
                            f"'{name}' in group '{self._entry_point_group}'"
                        )
                    self._condition.wait()
                    continue
                cls = self._plugins.get(name)
                if cls is None:
                    self._raise_missing(name)
                generation = self._generations.get(name, 0)
                self._singleton_inflight[name] = (owner, generation, cls)
                break

        try:
            instance = cls(**kwargs)
        except BaseException as exc:
            with self._condition:
                inflight = self._singleton_inflight.get(name)
                if inflight == (owner, generation, cls):
                    self._singleton_inflight.pop(name, None)
                    current_cls = self._plugins.get(name)
                    current_generation = self._generations.get(name, 0)
                    if current_cls is cls and current_generation == generation:
                        self._singleton_failures[name] = exc
                self._condition.notify_all()
            if isinstance(exc, DataCoolieError):
                raise
            raise DataCoolieError(
                f"Failed to construct singleton plugin '{name}' in group "
                f"'{self._entry_point_group}': {exc}"
            ) from exc

        with self._condition:
            inflight = self._singleton_inflight.get(name)
            if inflight != (owner, generation, cls):
                self._condition.notify_all()
                raise DataCoolieError(
                    f"Plugin '{name}' singleton construction was invalidated; "
                    "retry with the current registration"
                )
            self._singleton_inflight.pop(name, None)
            current_cls = self._plugins.get(name)
            current_generation = self._generations.get(name, 0)
            if current_cls is not cls or current_generation != generation:
                self._condition.notify_all()
                raise DataCoolieError(
                    f"Plugin '{name}' changed while its singleton was being "
                    "constructed; retry with the current registration"
                )
            self._singletons[name] = instance
            self._condition.notify_all()
            return instance

    def clear_singletons(self) -> None:
        """Evict cached instances and remembered construction failures."""

        with self._condition:
            self._singletons.clear()
            self._singleton_failures.clear()
            # Bump every active generation so constructors already running
            # cannot publish into the cleared singleton cache.  Keep their
            # in-flight records until they finish so waiters remain bounded
            # and the terminal path can notify them.
            names = set(self._plugins) | set(self._singleton_inflight)
            for name in names:
                self._generations[name] = self._generations.get(name, 0) + 1
            self._condition.notify_all()

    def list_plugins(self) -> list[str]:
        """List registered plugins, triggering one discovery pass."""

        self._ensure_discovered()
        with self._condition:
            return sorted(self._plugins)

    def is_available(self, name: str) -> bool:
        """Return whether a plugin loaded successfully."""

        self._ensure_discovered()
        with self._condition:
            return name in self._plugins

    def _raise_missing(self, name: str) -> None:
        plugin_error = self._discovery_plugin_errors.get(name)
        if plugin_error is not None:
            raise DataCoolieError(
                f"Plugin '{name}' failed to load from entry points in group "
                f"'{self._entry_point_group}'"
            ) from plugin_error
        if self._discovery_error is not None:
            raise DataCoolieError(
                f"Plugin discovery failed for group '{self._entry_point_group}'"
            ) from self._discovery_error
        available = ", ".join(sorted(self._plugins)) or "(none)"
        raise DataCoolieError(
            f"No plugin registered for '{name}'. Available: {available}"
        )

    def _ensure_discovered(self) -> None:
        """Run discovery once outside the state lock, detecting recursion."""

        owner = threading.get_ident()
        with self._condition:
            while not self._discovered:
                if not self._discovery_in_progress:
                    self._discovery_in_progress = True
                    self._discovery_owner = owner
                    break
                if self._discovery_owner == owner:
                    raise DataCoolieError(
                        f"Recursive plugin discovery detected for group "
                        f"'{self._entry_point_group}'"
                    )
                self._condition.wait()
            else:
                return
        try:
            self._discover_plugins()
        finally:
            with self._condition:
                self._discovered = True
                self._discovery_in_progress = False
                self._discovery_owner = None
                self._condition.notify_all()

    def _discover_plugins(self) -> None:
        """Load entry points without holding the registry state lock."""

        try:
            eps = entry_points(group=self._entry_point_group)
            for ep in eps:
                with self._condition:
                    if ep.name in self._plugins:
                        continue
                try:
                    cls = ep.load()
                    if not (isinstance(cls, type) and issubclass(cls, self._base_class)):
                        continue
                    with self._condition:
                        if ep.name not in self._plugins:
                            self._plugins[ep.name] = cls
                            self._generations.setdefault(ep.name, 0)
                except Exception as exc:
                    self._discovery_plugin_errors[ep.name] = exc
                    logger.debug(
                        "Entry-point plugin '%s' in group '%s' failed to load",
                        ep.name,
                        self._entry_point_group,
                        exc_info=True,
                    )
        except Exception as exc:
            self._discovery_error = exc
            logger.debug(
                "Entry-point discovery failed for group '%s'",
                self._entry_point_group,
                exc_info=True,
            )

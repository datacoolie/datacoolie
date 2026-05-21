"""Secret provider abstraction and resolution utilities.

:class:`BaseSecretProvider` defines the contract for fetching secrets
from platform-specific vaults.  :func:`resolve_secrets` wires a provider
(or any :class:`~datacoolie.core.secret_resolver.BaseSecretResolver`) into
a :class:`Connection` by replacing vault key references with real values.

:class:`SecretStr` is an opaque wrapper that prevents accidental exposure
of secret values through ``str()``, ``repr()``, ``print()``, f-strings,
and tracebacks.  The raw value is **never** exposed via a public method.
Framework internals use :func:`unwrap_secret` / :func:`unwrap_configure`
to obtain plain strings at I/O boundaries (HTTP auth, JDBC, etc.).
"""

from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from datacoolie.core.models import Connection
from datacoolie.core.exceptions import DataCoolieError
from datacoolie.core.secret_resolver import (
    NativeProviderResolver,
    ResolverLookup,
    parse_source,
)

logger = logging.getLogger(__name__)


# ============================================================================
# SecretStr — opaque secret wrapper
# ============================================================================


class SecretStr:
    """Opaque wrapper that hides secret values from all public access.

    ``str()``, ``repr()``, ``print()``, ``format()``, and ``logging``
    all render ``***`` instead of the real value.

    There is **no** public method to extract the underlying string.
    Framework internals access the value via :func:`unwrap_secret` or
    :func:`unwrap_configure`.

    The wrapper is immutable, unhashable-by-value, and unpicklable to
    prevent accidental persistence or cache-key leaks.
    """

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        object.__setattr__(self, "_value", value)

    # -- Block casual exposure -------------------------------------------

    def __str__(self) -> str:
        return "***"

    def __repr__(self) -> str:
        return "SecretStr('***')"

    def __format__(self, format_spec: str) -> str:  # noqa: A003
        return "***"

    # -- Immutability ----------------------------------------------------

    def __setattr__(self, _name: str, _value: Any) -> None:
        raise AttributeError("SecretStr is immutable")

    def __delattr__(self, _name: str) -> None:
        raise AttributeError("SecretStr is immutable")

    # -- Equality (identity-only, not value-based) -----------------------

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SecretStr):
            return NotImplemented
        return object.__getattribute__(self, "_value") == object.__getattribute__(other, "_value")

    def __hash__(self) -> int:
        return id(self)

    # -- Prevent serialisation -------------------------------------------

    def __reduce__(self) -> None:  # type: ignore[override]
        raise TypeError("Cannot pickle SecretStr")

    # -- Length (needed by truthiness checks) ----------------------------

    def __len__(self) -> int:
        return len(object.__getattribute__(self, "_value"))

    def __bool__(self) -> bool:
        return bool(object.__getattribute__(self, "_value"))


def unwrap_secret(value: Any) -> Any:
    """Return the raw string if *value* is a :class:`SecretStr`, else pass through."""
    if isinstance(value, SecretStr):
        return object.__getattribute__(value, "_value")
    return value


def unwrap_configure(configure: Dict[str, Any]) -> Dict[str, Any]:
    """Return a shallow copy with all :class:`SecretStr` values unwrapped.

    Call this **once** at the I/O boundary (HTTP auth, JDBC connect, etc.)
    to obtain a plain-string dict for external libraries.  The original
    *configure* dict retains its ``SecretStr`` wrappers.
    """
    return {
        k: object.__getattribute__(v, "_value") if isinstance(v, SecretStr) else v
        for k, v in configure.items()
    }

class BaseSecretProvider(ABC):
    """Abstract base class for secret providers.

    Implementations fetch secrets from a platform-specific vault
    (Azure Key Vault, AWS Secrets Manager, Databricks Secrets, env vars).

    Supports optional in-memory caching with a configurable TTL.

    Args:
        cache_ttl: Time-to-live for cached secrets in seconds.
            ``0`` disables caching.  Default is 300 (5 minutes).
    """

    def __init__(self, cache_ttl: int = 300, **kwargs: Any) -> None:
        self._cache_ttl = cache_ttl
        self._cache: Dict[tuple[str, str], tuple[str, float]] = {}
        self._cache_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_secret(self, key: str, source: str = "") -> str:
        """Fetch a single secret by *key* from the given *source*.

        Uses the cache when available and not expired, otherwise
        delegates to :meth:`_fetch_secret`.

        Args:
            key: Vault key / secret name.  For AWS this is the JSON field
                name within the secret; for other platforms it is the
                secret key within the scope / vault.
            source: Source identifier whose meaning depends on the platform:
                scope for Databricks, vault URL for Fabric, env prefix for
                Local, and secret name/ARN (``SecretId``) for AWS.
                Defaults to ``""``.

        Raises:
            DataCoolieError: If the secret cannot be retrieved.
        """
        cache_key = (source, key)
        if self._cache_ttl > 0:
            with self._cache_lock:
                entry = self._cache.get(cache_key)
                if entry is not None:
                    value, ts = entry
                    if time.monotonic() - ts < self._cache_ttl:
                        return value

        value = self._fetch_secret(key, source)

        if self._cache_ttl > 0:
            with self._cache_lock:
                self._cache[cache_key] = (value, time.monotonic())

        return value

    def get_secrets(self, keys: List[str], source: str = "") -> Dict[str, str]:
        """Fetch multiple secrets by *keys* from the given *source*.

        Args:
            keys: Secret key names (JSON field names for AWS).
            source: Source identifier (scope, vault URL, env prefix, or
                secret name/ARN for AWS).  Defaults to ``""``.

        Returns:
            Mapping of key → secret value.
        """
        return {k: self.get_secret(k, source) for k in keys}

    def clear_cache(self) -> None:
        """Evict all cached entries."""
        with self._cache_lock:
            self._cache.clear()

    # ------------------------------------------------------------------
    # Abstract — subclasses must override
    # ------------------------------------------------------------------

    @abstractmethod
    def _fetch_secret(self, key: str, source: str) -> str:
        """Retrieve a secret from the underlying vault.

        Args:
            key: Vault key / secret name.  Its meaning is platform-specific:
                secret key name for Databricks/Fabric/Local, or JSON field
                name within the secret for AWS.
            source: Source identifier whose meaning is platform-specific:
                scope for Databricks, vault URL for Fabric, env prefix for
                Local, and secret name/ARN (``SecretId``) for AWS.

        Returns:
            The secret value as a string.

        Raises:
            DataCoolieError: On failure.
        """


# ============================================================================
# Connection secret resolution
# ============================================================================


def resolve_secrets(
    connection: Connection,
    provider: BaseSecretProvider,
    *,
    resolver_lookup: ResolverLookup | None = None,
) -> None:
    """Resolve secret references in *connection*.

    ``connection.secrets_ref`` maps each source (scope / vault URL / prefix /
    SecretId) to a **list of** ``configure`` field names.  Each listed field
    must already be present in ``configure`` with the vault key name as its
    value; ``resolve_secrets`` replaces that value with the real secret.

    Source keys may contain a ``<resolver>:<arg>`` prefix (e.g.
    ``"env:APP_"``).  When *resolver_lookup* is provided and returns a
    resolver for that prefix, the plugin resolver is used.  Otherwise the
    platform-native *provider* handles the request via
    :class:`~datacoolie.core.secret_resolver.NativeProviderResolver`.

    Args:
        connection: A :class:`~datacoolie.core.models.Connection` instance.
        provider: The platform-native secret provider.
        resolver_lookup: Optional callable that maps a prefix string to
            a :class:`~datacoolie.core.secret_resolver.BaseSecretResolver`,
            or returns ``None`` if the prefix is unrecognized.

    Raises:
        DataCoolieError: If ``secrets_ref`` is invalid, a listed field is
            absent from ``configure``, or a secret cannot be retrieved.
    """
    raw = connection.secrets_ref
    if not raw:
        return

    if not isinstance(raw, dict):
        raise DataCoolieError(
            f"secrets_ref must be a dict, got {type(raw).__name__}"
        )

    total = sum(len(v) for v in raw.values() if isinstance(v, list))
    logger.info(
        "Resolving %d secret(s) for connection '%s'",
        total,
        connection.name,
    )

    native = NativeProviderResolver(provider)

    for source, fields in raw.items():
        if not isinstance(fields, list):
            raise DataCoolieError(
                f"secrets_ref values must be lists of field names, "
                f"got {type(fields).__name__} for source '{source}'"
            )

        resolver, source_arg = parse_source(source, native, resolver_lookup)

        for config_field in fields:
            vault_key = connection.configure.get(config_field)
            if vault_key is None:
                raise DataCoolieError(
                    f"Field '{config_field}' listed in secrets_ref is missing from "
                    f"configure on connection '{connection.name}'. "
                    f"Set configure[\"{config_field}\"] to the vault key name."
                )
            try:
                value = resolver.resolve(str(vault_key), source_arg)
            except Exception as exc:
                raise DataCoolieError(
                    f"Failed to resolve secret '{vault_key}' for field "
                    f"'{config_field}' on connection '{connection.name}': {exc}"
                ) from exc
            connection.configure[config_field] = SecretStr(value)

    # Re-populate model attributes that were lifted from configure at
    # construction time — they still hold the original vault key names.
    connection.refresh_from_configure()

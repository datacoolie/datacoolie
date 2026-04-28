"""Pluggable secret resolver abstraction.

:class:`BaseSecretResolver` defines the single plugin contract for fetching
secrets — whether from environment variables, a cloud vault, HashiCorp Vault,
1Password, or any other backend.

Resolvers are registered via :data:`datacoolie.resolver_registry` and
dispatched by prefix in ``secrets_ref`` source keys (``"env:APP_"``).

Sources **without** a prefix use the platform-native provider via
:class:`NativeProviderResolver`.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Optional

from datacoolie.core.exceptions import DataCoolieError

if TYPE_CHECKING:
    from datacoolie.core.secret_provider import BaseSecretProvider


class BaseSecretResolver(ABC):
    """Abstract base class for pluggable secret resolvers.

    This is the **single interface** that every secret backend implements.
    Subclasses must implement :meth:`resolve` which fetches a single
    secret value given a *key* and a *source* argument.
    """

    @abstractmethod
    def resolve(self, key: str, source: str) -> str:
        """Fetch a single secret.

        Args:
            key: The secret / field name (value from ``configure``).
            source: Backend-specific source identifier.  For prefixed
                sources (``"env:APP_"``), this is the portion after
                the ``:``.  For native providers, this is the full
                source key from ``secrets_ref``.

        Returns:
            The secret value as a string.

        Raises:
            DataCoolieError: If the secret cannot be retrieved.
        """


# ---------------------------------------------------------------------------
# Built-in resolvers
# ---------------------------------------------------------------------------


class EnvResolver(BaseSecretResolver):
    """Resolve secrets from environment variables.

    The environment variable name is ``{source}{key}``.  For example
    with source ``"APP_"`` and key ``"DB_PASSWORD"`` this looks up
    ``os.environ["APP_DB_PASSWORD"]``.
    """

    def resolve(self, key: str, source: str) -> str:
        full_key = f"{source}{key}" if source else key
        value = os.environ.get(full_key)
        if value is None:
            raise DataCoolieError(
                f"Secret not found: environment variable '{full_key}' is not set."
            )
        return value


class NativeProviderResolver(BaseSecretResolver):
    """Adapter that wraps a :class:`BaseSecretProvider` as a resolver.

    This bridges the existing platform secret providers (Local, Fabric,
    Databricks, AWS) into the unified :class:`BaseSecretResolver`
    interface so that ``resolve_secrets`` needs only one code path.
    """

    def __init__(self, provider: BaseSecretProvider, **_: object) -> None:
        self._provider = provider

    def resolve(self, key: str, source: str) -> str:
        return self._provider.get_secret(key, source)


# ---------------------------------------------------------------------------
# Source key parsing
# ---------------------------------------------------------------------------


def parse_source(
    source: str,
    fallback: BaseSecretResolver,
    resolver_lookup: ResolverLookup | None = None,
) -> tuple[BaseSecretResolver, str]:
    """Parse an optional ``<prefix>:<arg>`` source key.

    Args:
        source: The raw source key from ``secrets_ref``
            (e.g. ``"env:APP_"`` or ``"my-vault-scope"``).
        fallback: The resolver to use when *source* has no recognized
            prefix (the :class:`NativeProviderResolver` in practice).
        resolver_lookup: Optional callable that maps a prefix string to
            a :class:`BaseSecretResolver` instance, or ``None`` if
            unrecognized.

    Returns:
        A ``(resolver, source_arg)`` tuple ready for
        ``resolver.resolve(key, source_arg)``.
    """
    if ":" in source and resolver_lookup is not None:
        prefix, arg = source.split(":", 1)
        resolver = resolver_lookup(prefix)
        if resolver is not None:
            return resolver, arg
    return fallback, source


# Callable that maps a resolver prefix to a resolver instance, or None.
ResolverLookup = Callable[[str], Optional[BaseSecretResolver]]

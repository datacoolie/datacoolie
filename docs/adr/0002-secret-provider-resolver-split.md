---
title: ADR-0002 — Secret Provider and Resolver Split | DataCoolie
description: Why DataCoolie separates secret storage access from secret value resolution so platforms and credentials stay decoupled.
---

# ADR-0002 — Split secret **provider** from secret **resolver**

**Status** · Accepted

## Context

Secret lookup needs a platform-native fallback and an explicit way to select
other backends from metadata without hard-coding them into the driver.

## Decision

Two responsibilities, two abstractions:

- **`BaseSecretProvider`** — *fetches* a secret from a backend
  (`get_secret(key, source) -> str`, backed by `_fetch_secret`). Implemented by each platform
  (Fabric, Databricks, AWS, Local).
- **`BaseSecretResolver`** — resolves a key for a source argument
  (`resolve(key, source) -> str`).

`secrets_ref` sources of the form `<prefix>:<argument>` select a registered
resolver by prefix. Unrecognised or unprefixed sources use
`NativeProviderResolver`, which adapts the active platform provider. Resolvers
are discovered via `datacoolie.resolvers`; providers are constructor-selected
through the active platform.

## Consequences

- Adding a new resolver prefix/backend integration = one class.
- Adding a new backend = one class.
- `EnvResolver` resolves a configure key through `os.environ` using the
  source argument as a prefix.
- Resolver instances are cached as singletons by the driver registry lookup
  and are constructed without arguments.

## Related

- [Extending · Write a secret resolver](../extending/writing-a-secret-resolver.md)
